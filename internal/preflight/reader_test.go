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
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	testNamespace = "postgres-system"
	testCluster   = "postgres-cluster"
	testPod       = "postgres-cluster-3"
	testNode      = "node-1.example.internal"
	testPodIP     = "192.0.2.10"
)

// primaryPodFixture mirrors a real CNPG primary: the data volume's claim is named
// after the instance, and the pod carries the cluster and role labels.
func primaryPodFixture() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testPod,
			Namespace: testNamespace,
			Labels: map[string]string{
				"cnpg.io/cluster":      testCluster,
				"cnpg.io/instanceRole": "primary",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: testNode,
			Volumes: []corev1.Volume{
				{
					Name: "pgdata",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: testPod,
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{PodIP: testPodIP},
	}
}

func nodeBodyWithFree(bytes string) string {
	return `kubelet_volume_stats_available_bytes{namespace="` + testNamespace +
		`",persistentvolumeclaim="` + testPod + `"} ` + bytes + "\n"
}

func nodeBodyWithCapacity(bytes string) string {
	return `kubelet_volume_stats_capacity_bytes{namespace="` + testNamespace +
		`",persistentvolumeclaim="` + testPod + `"} ` + bytes + "\n"
}

func podBodyWithTable(bytes string) string {
	return `cnpg_temporal_visibility_table_bytes{relation="executions_visibility"} ` + bytes + "\n"
}

// newTestChecker wires a Checker against a fake API server and canned metrics
// bodies, so the whole Check path runs, not just the decision at the end.
func newTestChecker(objects []runtime.Object, nodeBody, podBody string, nodeErr, podErr error) *Checker {
	clientset := fake.NewSimpleClientset(objects...)

	return &Checker{
		clientset: clientset,

		fetchNode: func(context.Context, string) (string, error) {
			if nodeErr != nil {
				return "", nodeErr
			}
			return nodeBody, nil
		},
		fetch: func(context.Context, string) (string, error) {
			if podErr != nil {
				return "", podErr
			}
			return podBody, nil
		},
	}
}

func testTarget() Target {
	return Target{Namespace: testNamespace, ClusterName: testCluster}
}

// The absolute floor is disabled throughout the reader tests. They exercise what
// gets measured and how a read failure is reported, using fixture sizes far below
// the floor; leaving it on would make every case block on the floor alone and
// prove nothing about the reading.
func enabledConfig(t *testing.T) Config {
	t.Helper()
	cfg, err := ResolveConfig("3", nil, 0, noFloor())
	require.NoError(t, err)
	return cfg
}

func TestCheckPassesWithHeadroom(t *testing.T) {
	checker := newTestChecker(
		[]runtime.Object{primaryPodFixture()},
		nodeBodyWithFree("1.1798802432e+10"),
		podBodyWithTable("1.29859584e+09"),
		nil, nil,
	)

	result := checker.Check(context.Background(), enabledConfig(t), testTarget())

	assert.True(t, result.OK)
	assert.False(t, result.Blocked())
	assert.Equal(t, int64(11798802432), result.FreeBytes)
	assert.Equal(t, int64(1298595840), result.TableBytes)
	assert.Equal(t, DefaultRelation, result.Relation)
}

func TestCheckBlocksOnShortfall(t *testing.T) {
	checker := newTestChecker(
		[]runtime.Object{primaryPodFixture()},
		nodeBodyWithFree("1.5e+09"),
		podBodyWithTable("1.29859584e+09"),
		nil, nil,
	)

	result := checker.Check(context.Background(), enabledConfig(t), testTarget())

	assert.False(t, result.OK)
	assert.True(t, result.Blocked())
	assert.False(t, result.InputInvalid(), "a genuine shortfall is not an input failure")
	assert.Equal(t, int64(2395787520), result.ShortfallBytes)
}

// Each of these is a way the check can go blind. Every one must refuse, because
// each would otherwise present as "there is room".
func TestCheckFailsClosed(t *testing.T) {
	tests := []struct {
		name      string
		objects   []runtime.Object
		nodeBody  string
		podBody   string
		nodeErr   error
		podErr    error
		wantCause Cause
	}{
		{
			name:      "no primary pod",
			objects:   nil,
			wantCause: CauseTargetNotFound,
		},
		{
			name:      "kubelet unreachable",
			objects:   []runtime.Object{primaryPodFixture()},
			nodeErr:   errors.New("connection refused"),
			podBody:   podBodyWithTable("1.29859584e+09"),
			wantCause: CauseUnavailable,
		},
		{
			name:      "kubelet reports nothing for this claim",
			objects:   []runtime.Object{primaryPodFixture()},
			nodeBody:  `kubelet_volume_stats_available_bytes{namespace="other",persistentvolumeclaim="other"} 1` + "\n",
			podBody:   podBodyWithTable("1.29859584e+09"),
			wantCause: CauseAbsent,
		},
		{
			name:      "postgres metrics unreachable",
			objects:   []runtime.Object{primaryPodFixture()},
			nodeBody:  nodeBodyWithFree("1.1798802432e+10"),
			podErr:    errors.New("connection refused"),
			wantCause: CauseUnavailable,
		},
		{
			name:      "custom query series absent, as when the GRANT is missing",
			objects:   []runtime.Object{primaryPodFixture()},
			nodeBody:  nodeBodyWithFree("1.1798802432e+10"),
			podBody:   `cnpg_backends_total{datname="temporal"} 3` + "\n",
			wantCause: CauseAbsent,
		},
		{
			name:      "table reported as zero",
			objects:   []runtime.Object{primaryPodFixture()},
			nodeBody:  nodeBodyWithFree("1.1798802432e+10"),
			podBody:   podBodyWithTable("0"),
			wantCause: CauseZero,
		},
		{
			name:      "table implausibly small",
			objects:   []runtime.Object{primaryPodFixture()},
			nodeBody:  nodeBodyWithFree("1.1798802432e+10"),
			podBody:   podBodyWithTable("4096"),
			wantCause: CauseBelowFloor,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker := newTestChecker(tt.objects, tt.nodeBody, tt.podBody, tt.nodeErr, tt.podErr)

			result := checker.Check(context.Background(), enabledConfig(t), testTarget())

			assert.False(t, result.OK, "must not approve")
			assert.True(t, result.Blocked(), "must block")
			assert.True(t, result.InputInvalid(), "must be reported as an input failure")
			assert.Equal(t, tt.wantCause, result.Cause)
			assert.NotEmpty(t, result.Message)
		})
	}
}

// An unscheduled pod has no node and no IP, so it cannot be measured. Reading
// through it would produce a zero rather than a refusal.
func TestCheckRefusesUnscheduledPod(t *testing.T) {
	pod := primaryPodFixture()
	pod.Spec.NodeName = ""
	pod.Status.PodIP = ""

	checker := newTestChecker([]runtime.Object{pod}, "", "", nil, nil)

	result := checker.Check(context.Background(), enabledConfig(t), testTarget())

	assert.True(t, result.Blocked())
	assert.Equal(t, CauseTargetNotFound, result.Cause)
}

// With several relations configured, the largest governs: the migration has to
// fit the worst case among the tables it might rewrite.
func TestCheckUsesLargestConfiguredRelation(t *testing.T) {
	cfg, err := ResolveConfig("2", []string{"executions_visibility", "executions"}, 0, noFloor())
	require.NoError(t, err)

	body := `cnpg_temporal_visibility_table_bytes{relation="executions_visibility"} 1.0e+09` + "\n" +
		`cnpg_temporal_visibility_table_bytes{relation="executions"} 4.0e+09` + "\n"

	checker := newTestChecker([]runtime.Object{primaryPodFixture()}, nodeBodyWithFree("1.0e+10"), body, nil, nil)

	result := checker.Check(context.Background(), cfg, testTarget())

	assert.Equal(t, "executions", result.Relation)
	assert.Equal(t, int64(4000000000), result.TableBytes)
	assert.Equal(t, int64(8000000000), result.RequiredBytes)
	assert.True(t, result.OK)
}

// A floor larger than the volume can never be met. Left as a shortfall it would
// hold the migration forever while pointing at a resize that cannot help, and it
// would route to the self-resolving alert rather than the one that says the gate
// cannot be trusted.
func TestCheckRefusesAFloorLargerThanTheVolume(t *testing.T) {
	const capacity int64 = 20 << 30

	cfg, err := ResolveConfig("3", nil, 0, floorOf(capacity+1))
	require.NoError(t, err)

	body := nodeBodyWithFree("1.5e+10") + nodeBodyWithCapacity("2.147483648e+10")
	checker := newTestChecker([]runtime.Object{primaryPodFixture()}, body, podBodyWithTable("1.0e+09"), nil, nil)

	result := checker.Check(context.Background(), cfg, testTarget())

	assert.True(t, result.Blocked())
	assert.True(t, result.InputInvalid(), "an unsatisfiable floor is a configuration failure, not a shortfall")
	assert.Equal(t, CauseFloorUnsatisfiable, result.Cause)
}

// The same floor at or below capacity is a legitimate setting and must still be
// applied as an ordinary headroom requirement.
func TestCheckAcceptsAFloorWithinTheVolume(t *testing.T) {
	cfg, err := ResolveConfig("3", nil, 0, floorOf(18<<30))
	require.NoError(t, err)

	body := nodeBodyWithFree("1.5e+10") + nodeBodyWithCapacity("2.147483648e+10")
	checker := newTestChecker([]runtime.Object{primaryPodFixture()}, body, podBodyWithTable("1.0e+09"), nil, nil)

	result := checker.Check(context.Background(), cfg, testTarget())

	assert.True(t, result.Blocked(), "15GiB free does not cover an 18GiB floor")
	assert.False(t, result.InputInvalid(), "a satisfiable floor that is not met is a shortfall")
	assert.Equal(t, CauseNone, result.Cause)
}

// endlessReader never reaches EOF, standing in for a metrics endpoint that
// streams without bound.
type endlessReader struct{}

func (endlessReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}

	return len(p), nil
}

// Both metrics bodies are read into memory, and the operator reconciles
// everything else under the same memory limit, so an unbounded response would be
// an operator-wide outage rather than one failed check.
func TestReadBoundedRefusesAnOversizedBody(t *testing.T) {
	_, err := readBounded(endlessReader{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds")
}

// The refusal must not come at the cost of ordinary bodies, and it must return
// them whole: a silently truncated exposition parses as a missing series, which
// the gate would report as "no measurement" rather than "response too large".
func TestReadBoundedReturnsANormalBodyWhole(t *testing.T) {
	body := "cnpg_temporal_visibility_table_bytes{relation=\"executions_visibility\"} 1.0e+09\n"

	got, err := readBounded(strings.NewReader(body))

	require.NoError(t, err)
	assert.Equal(t, body, got)
}

// kubeletBody mirrors the shape of a kubelet /metrics response: many volumes on
// the node, only one of which belongs to the cluster under test.
const kubeletBody = `
# HELP kubelet_volume_stats_available_bytes Number of available bytes in the volume
# TYPE kubelet_volume_stats_available_bytes gauge
kubelet_volume_stats_available_bytes{namespace="kafka",persistentvolumeclaim="kafka-0"} 5.36870912e+09
kubelet_volume_stats_available_bytes{namespace="cloudnative-postgres",persistentvolumeclaim="postgres-cluster-3"} 1.1798802432e+10
kubelet_volume_stats_capacity_bytes{namespace="cloudnative-postgres",persistentvolumeclaim="postgres-cluster-3"} 2.0957446144e+10
`

// cnpgBody mirrors a CNPG instance /metrics response carrying a custom query
// series alongside the built-in ones.
const cnpgBody = `
# TYPE cnpg_backends_total gauge
cnpg_backends_total{application_name="",datname="argo",state="idle",usename="argo"} 6
cnpg_backends_total{application_name="",datname="temporal_visibility",state="idle",usename="temporal"} 15
# TYPE cnpg_temporal_visibility_table_bytes gauge
cnpg_temporal_visibility_table_bytes{relation="executions_visibility"} 1.29859584e+09
`

func TestFindSample(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		family string
		match  map[string]string
		want   float64
		wantOK bool
	}{
		{
			name:   "kubelet available bytes for the right pvc",
			body:   kubeletBody,
			family: "kubelet_volume_stats_available_bytes",
			match:  map[string]string{"namespace": "cloudnative-postgres", "persistentvolumeclaim": "postgres-cluster-3"},
			want:   11798802432,
			wantOK: true,
		},
		{
			name:   "does not bleed across volumes on the same node",
			body:   kubeletBody,
			family: "kubelet_volume_stats_available_bytes",
			match:  map[string]string{"namespace": "kafka", "persistentvolumeclaim": "kafka-0"},
			want:   5368709120,
			wantOK: true,
		},
		{
			name:   "unknown pvc is not found rather than defaulted",
			body:   kubeletBody,
			family: "kubelet_volume_stats_available_bytes",
			match:  map[string]string{"namespace": "cloudnative-postgres", "persistentvolumeclaim": "postgres-cluster-9"},
			wantOK: false,
		},
		{
			name:   "custom query table bytes",
			body:   cnpgBody,
			family: "cnpg_temporal_visibility_table_bytes",
			match:  map[string]string{"relation": "executions_visibility"},
			want:   1298595840,
			wantOK: true,
		},
		{
			name:   "absent family is not found",
			body:   cnpgBody,
			family: "cnpg_temporal_visibility_table_bytes_missing",
			wantOK: false,
		},
		{
			name:   "family present but relation label differs",
			body:   cnpgBody,
			family: "cnpg_temporal_visibility_table_bytes",
			match:  map[string]string{"relation": "executions"},
			wantOK: false,
		},
		{
			name:   "unlabelled sample",
			body:   "some_metric_bytes 4096\n",
			family: "some_metric_bytes",
			want:   4096,
			wantOK: true,
		},
		{
			name:   "trailing timestamp is discarded",
			body:   `some_metric_bytes{a="b"} 4096 1700000000000` + "\n",
			family: "some_metric_bytes",
			match:  map[string]string{"a": "b"},
			want:   4096,
			wantOK: true,
		},
		{
			name:   "comment lines are skipped even when they mention the family",
			body:   "# HELP some_metric_bytes 999999\nsome_metric_bytes 7\n",
			family: "some_metric_bytes",
			want:   7,
			wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := findSample(tt.body, tt.family, tt.match)
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

// A value that cannot represent a byte count must be rejected, never returned.
// Returning zero here would make the requirement zero, which makes
// "free > required" trivially true and silently disables the gate.
func TestFindSampleRejectsNonByteValues(t *testing.T) {
	for _, value := range []string{"NaN", "+Inf", "-Inf", "-1", "not-a-number"} {
		t.Run(value, func(t *testing.T) {
			body := `table_bytes{relation="executions_visibility"} ` + value + "\n"
			_, ok := findSample(body, "table_bytes", map[string]string{"relation": "executions_visibility"})
			assert.False(t, ok, "value %q must not be accepted as a byte count", value)
		})
	}
}

// A single very long line must not truncate the scan and hide later samples.
func TestFindSampleHandlesLongLines(t *testing.T) {
	padding := strings.Repeat("x", 200*1024)
	body := `noise{pad="` + padding + `"} 1` + "\n" +
		`table_bytes{relation="executions_visibility"} 1298595840` + "\n"

	got, ok := findSample(body, "table_bytes", map[string]string{"relation": "executions_visibility"})
	assert.True(t, ok)
	assert.Equal(t, float64(1298595840), got)
}
