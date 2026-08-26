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
	// metricsPort is where a CNPG instance serves its exporter, custom monitoring
	// queries included.
	metricsPort = 9187

	// tableBytesFamily is how CNPG names the custom query series: the exporter
	// joins the query name and the column name.
	tableBytesFamily = "cnpg_temporal_visibility_table_bytes"

	// freeBytesFamily is emitted by the kubelet for every mounted volume. This
	// is the same series pvc-autoresizer reads, so the two components agree on
	// what "free" means.
	freeBytesFamily = "kubelet_volume_stats_available_bytes"

	// capacityBytesFamily sits on the same kubelet page as the free-space series,
	// so reading it costs nothing extra and lets the check tell a volume that is
	// short from a floor that the volume can never satisfy.
	capacityBytesFamily = "kubelet_volume_stats_capacity_bytes"

	// primaryPodSelector picks the writable instance. Replicas report the same
	// table size, but reading the primary avoids a stale measurement on a lagging
	// replica.
	primaryPodSelector = "cnpg.io/cluster=%s,cnpg.io/instanceRole=primary"

	defaultTimeout = 10 * time.Second
)

// Target locates the CNPG cluster backing the datastore.
type Target struct {
	Namespace   string
	ClusterName string
}

// Checker reads the two measurements the gate compares.
//
// Both reads are uncached and on demand: the manager's cache is scoped to the
// namespaces it reconciles and Postgres lives outside them, so caching would
// mean watching pods in another namespace on every tenant.
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

// Check measures the target and applies the headroom test. It never returns an
// error: an unmeasurable target is a refusal carrying a Cause, so a read failure
// cannot be mistaken for permission to proceed.
func (c *Checker) Check(ctx context.Context, cfg Config, target Target) Result {
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

	freeBytes, result := c.freeBytes(ctx, cfg, pod.Spec.NodeName, pod.Namespace, claim)
	if result.Cause != CauseNone {
		return result
	}

	tableBytes, relation, result := c.tableBytes(ctx, cfg, pod.Status.PodIP)
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
func (c *Checker) freeBytes(ctx context.Context, cfg Config, node, namespace, claim string) (int64, Result) {
	raw, err := c.fetchNode(ctx, node)
	if err != nil {
		return 0, Result{
			Cause:   CauseUnavailable,
			Message: fmt.Sprintf("can't read kubelet metrics from node %s: %v", node, err),
		}
	}

	labels := map[string]string{
		"namespace":             namespace,
		"persistentvolumeclaim": claim,
	}

	value, found := findSample(raw, freeBytesFamily, labels)
	if !found {
		return 0, Result{
			Cause: CauseAbsent,
			Message: fmt.Sprintf(
				"node %s reports no %s for claim %s/%s; refusing rather than assuming the volume is empty",
				node, freeBytesFamily, namespace, claim),
		}
	}

	// A floor larger than the volume can never be met, so the check would hold the
	// migration forever while reporting a shortfall that no resize can close.
	// Caught here, where the volume's real size is already in hand.
	if capacity, ok := findSample(raw, capacityBytesFamily, labels); ok && cfg.MinFreeBytes > int64(capacity) {
		return 0, Result{
			Cause: CauseFloorUnsatisfiable,
			Message: fmt.Sprintf(
				"the configured minimum of %d free bytes exceeds the %d byte capacity of %s/%s, so it can never be satisfied",
				cfg.MinFreeBytes, int64(capacity), namespace, claim),
		}
	}

	return int64(value), Result{}
}

// tableBytes reads the size of the largest configured relation from the CNPG
// instance's exporter. The largest is used because the migration must fit the
// worst case among the tables it may rewrite.
func (c *Checker) tableBytes(ctx context.Context, cfg Config, podIP string) (int64, string, Result) {
	url := "http://" + net.JoinHostPort(podIP, strconv.Itoa(metricsPort)) + "/metrics"

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
		value, found := findSample(body, tableBytesFamily, map[string]string{"relation": relation})
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
				url, tableBytesFamily, missing),
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
