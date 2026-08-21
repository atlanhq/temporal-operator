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
	"testing"
	"time"

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

func podBodyWithTable(bytes string) string {
	return `cnpg_temporal_visibility_table_bytes{relation="executions_visibility"} ` + bytes + "\n"
}

// newTestChecker wires a Checker against a fake API server and canned metrics
// bodies, so the whole Check path runs, not just the decision at the end.
func newTestChecker(objects []runtime.Object, nodeBody, podBody string, nodeErr, podErr error) *Checker {
	clientset := fake.NewSimpleClientset(objects...)

	return &Checker{
		clientset: clientset,
		timeout:   5 * time.Second,
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

func enabledConfig(t *testing.T) Config {
	t.Helper()
	cfg, err := ResolveConfig("3", nil, 0)
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
	cfg, err := ResolveConfig("2", []string{"executions_visibility", "executions"}, 0)
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
