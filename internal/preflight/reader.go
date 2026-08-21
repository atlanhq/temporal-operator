// Licensed to Alexandre VILAIN under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Alexandre VILAIN licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package preflight

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	// defaultMetricsPort is the port a CNPG instance serves its exporter on,
	// including any custom monitoring queries.
	defaultMetricsPort = 9187

	// defaultMetricFamily is what CNPG names the custom query series: the
	// exporter joins the query name and the column name. A query named
	// temporal_visibility_table returning a `bytes` column therefore surfaces as
	// cnpg_temporal_visibility_table_bytes.
	defaultMetricFamily = "cnpg_temporal_visibility_table_bytes"

	// freeBytesFamily is emitted by the kubelet for every mounted volume. This
	// is the same series pvc-autoresizer reads, so the two components agree on
	// what "free" means.
	freeBytesFamily = "kubelet_volume_stats_available_bytes"

	// primaryPodSelector picks the writable instance. Replicas report the same
	// table size, but reading the primary avoids a stale measurement on a lagging
	// replica.
	primaryPodSelector = "cnpg.io/cluster=%s,cnpg.io/instanceRole=primary"

	defaultTimeout = 10 * time.Second
)

// Target describes the Postgres instance to measure.
type Target struct {
	// Namespace and ClusterName locate the CNPG cluster backing the datastore.
	Namespace   string
	ClusterName string
	// MetricsPort and MetricFamily override the CNPG defaults when needed.
	MetricsPort  int
	MetricFamily string
}

// Checker reads the two measurements the gate compares.
//
// Both reads are deliberately uncached and on demand. The manager's cache is
// scoped to the namespaces it reconciles, and the Postgres cluster lives outside
// them, so a cached read would mean watching pods in another namespace on every
// tenant. One request per reconcile costs less than a fleet-wide watch.
type Checker struct {
	clientset kubernetes.Interface
	// fetch retrieves a metrics endpoint body, and fetchNode retrieves a node's
	// kubelet metrics through the API server proxy. Both are fields so tests can
	// drive the full check, including each refusal path, without a cluster.
	fetch     func(ctx context.Context, url string) (string, error)
	fetchNode func(ctx context.Context, node string) (string, error)
	timeout   time.Duration
}

// NewChecker builds a Checker from the manager's REST configuration.
func NewChecker(cfg *rest.Config) (*Checker, error) {
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("can't build clientset for the headroom check: %w", err)
	}

	client := &http.Client{Timeout: defaultTimeout}

	return &Checker{
		clientset: clientset,
		timeout:   defaultTimeout,
		fetchNode: func(ctx context.Context, node string) (string, error) {
			raw, err := clientset.CoreV1().RESTClient().
				Get().
				AbsPath("/api/v1/nodes/" + node + "/proxy/metrics").
				DoRaw(ctx)
			if err != nil {
				return "", err
			}

			return string(raw), nil
		},
		fetch: func(ctx context.Context, url string) (string, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				return "", err
			}

			resp, err := client.Do(req)
			if err != nil {
				return "", err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return "", fmt.Errorf("metrics endpoint returned %s", resp.Status)
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return "", err
			}

			return string(body), nil
		},
	}, nil
}

// Check measures the target and applies the headroom test.
//
// It never returns an error for a measurement it could not take: an
// unmeasurable target is a refusal carrying a Cause, so a transient read
// failure cannot be mistaken by a caller for permission to proceed.
func (c *Checker) Check(ctx context.Context, cfg Config, target Target) Result {
	if !cfg.Enabled {
		return Result{Skipped: true}
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	pod, err := c.primaryPod(ctx, target)
	if err != nil {
		return Result{
			Cause:   CauseTargetNotFound,
			Message: fmt.Sprintf("can't locate the primary Postgres instance for %s/%s: %v", target.Namespace, target.ClusterName, err),
		}
	}

	claim := dataVolumeClaim(pod)
	if claim == "" {
		return Result{
			Cause:   CauseTargetNotFound,
			Message: fmt.Sprintf("pod %s/%s has no persistent volume claim to measure", pod.Namespace, pod.Name),
		}
	}
	if pod.Spec.NodeName == "" || pod.Status.PodIP == "" {
		return Result{
			Cause:   CauseTargetNotFound,
			Message: fmt.Sprintf("pod %s/%s is not yet scheduled and running, so it cannot be measured", pod.Namespace, pod.Name),
		}
	}

	freeBytes, result := c.freeBytes(ctx, pod.Spec.NodeName, pod.Namespace, claim)
	if result.Cause != CauseNone {
		return result
	}

	tableBytes, relation, result := c.tableBytes(ctx, cfg, target, pod.Status.PodIP)
	if result.Cause != CauseNone {
		return result
	}

	return Decide(cfg, relation, tableBytes, freeBytes)
}

// primaryPod finds the writable instance of the CNPG cluster.
func (c *Checker) primaryPod(ctx context.Context, target Target) (*corev1.Pod, error) {
	pods, err := c.clientset.CoreV1().Pods(target.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf(primaryPodSelector, target.ClusterName),
	})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no pod matches %s", fmt.Sprintf(primaryPodSelector, target.ClusterName))
	}

	return &pods.Items[0], nil
}

// freeBytes reads the kubelet's view of the volume through the API server's node
// proxy, the same path pvc-autoresizer uses.
func (c *Checker) freeBytes(ctx context.Context, node, namespace, claim string) (int64, Result) {
	raw, err := c.fetchNode(ctx, node)
	if err != nil {
		return 0, Result{
			Cause:   CauseUnavailable,
			Message: fmt.Sprintf("can't read kubelet metrics from node %s: %v", node, err),
		}
	}

	value, found := findSample(raw, freeBytesFamily, map[string]string{
		"namespace":             namespace,
		"persistentvolumeclaim": claim,
	})
	if !found {
		return 0, Result{
			Cause: CauseAbsent,
			Message: fmt.Sprintf(
				"node %s reports no %s for claim %s/%s; refusing rather than assuming the volume is empty",
				node, freeBytesFamily, namespace, claim),
		}
	}

	return int64(value), Result{}
}

// tableBytes reads the size of the largest configured relation from the CNPG
// instance's exporter. The largest is used because the migration must fit the
// worst case among the tables it may rewrite.
func (c *Checker) tableBytes(ctx context.Context, cfg Config, target Target, podIP string) (int64, string, Result) {
	family := target.MetricFamily
	if family == "" {
		family = defaultMetricFamily
	}

	port := target.MetricsPort
	if port == 0 {
		port = defaultMetricsPort
	}

	url := "http://" + net.JoinHostPort(podIP, strconv.Itoa(port)) + "/metrics"

	body, err := c.fetch(ctx, url)
	if err != nil {
		return 0, "", Result{
			Cause:   CauseUnavailable,
			Message: fmt.Sprintf("can't read Postgres metrics at %s: %v", url, err),
		}
	}

	var (
		largest         int64
		largestRelation string
		missing         []string
	)

	for _, relation := range cfg.Relations {
		value, found := findSample(body, family, map[string]string{"relation": relation})
		if !found {
			missing = append(missing, relation)
			continue
		}

		if int64(value) >= largest {
			largest = int64(value)
			largestRelation = relation
		}
	}

	// Every configured relation must be measurable. Accepting a partial answer
	// would compare against the largest of the tables we happen to see, which can
	// be a fraction of the real worst case: a relation added to the configuration
	// but never added to the monitoring query would silently drop out of the
	// comparison it was added to enforce.
	if len(missing) > 0 {
		return 0, "", Result{
			Cause: CauseAbsent,
			Message: fmt.Sprintf(
				"%s exposes no %s for %v; the custom monitoring query does not cover those relations, or is missing entirely",
				url, family, missing),
		}
	}

	return largest, largestRelation, Result{}
}

// dataVolumeClaim returns the claim backing the instance's data directory.
func dataVolumeClaim(pod *corev1.Pod) string {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		// CNPG names the data volume after the instance, and mounts WAL as
		// <instance>-wal when separate WAL storage is configured. The data volume
		// is the one whose claim matches the pod name.
		if volume.PersistentVolumeClaim.ClaimName == pod.Name {
			return volume.PersistentVolumeClaim.ClaimName
		}
	}

	return ""
}
