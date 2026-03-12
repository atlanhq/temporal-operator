// Licensed to Alexandre VILAIN under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Alexandre VILAIN licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package controllers

import (
	"testing"
	"time"

	"github.com/alexandrevilain/temporal-operator/api/v1beta1"
	"github.com/alexandrevilain/temporal-operator/pkg/version"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func newReconciler() *TemporalClusterReconciler {
	return &TemporalClusterReconciler{}
}

func clusterWithStatus(specVersion, statusVersion string, schemaVersion string) *v1beta1.TemporalCluster {
	c := &v1beta1.TemporalCluster{
		Spec: v1beta1.TemporalClusterSpec{
			Version: version.MustNewVersionFromString(specVersion),
		},
	}
	if statusVersion != "" {
		c.Status.Version = statusVersion
	}
	if schemaVersion != "" {
		c.Status.Persistence = &v1beta1.TemporalPersistenceStatus{
			DefaultStore: &v1beta1.DatastoreStatus{
				SchemaVersion: version.MustNewVersionFromString(schemaVersion),
			},
		}
	}
	return c
}

// --- getCurrentVersion tests ---

func TestGetCurrentVersion(t *testing.T) {
	r := newReconciler()

	tests := map[string]struct {
		cluster  *v1beta1.TemporalCluster
		expected string
	}{
		"prefers status.version over schemaVersion": {
			cluster:  clusterWithStatus("1.28.2", "1.25.2", "1.26.3"),
			expected: "1.25.2",
		},
		"falls back to schemaVersion when status.version is empty": {
			cluster:  clusterWithStatus("1.28.2", "", "1.26.3"),
			expected: "1.26.3",
		},
		"falls back to spec.version when both are empty": {
			cluster:  clusterWithStatus("1.28.2", "", ""),
			expected: "1.28.2",
		},
		"status.version takes priority even when behind schemaVersion": {
			cluster:  clusterWithStatus("1.28.2", "1.24.2", "1.27.4"),
			expected: "1.24.2",
		},
		"status.version at target version": {
			cluster:  clusterWithStatus("1.28.2", "1.28.2", "1.28.2"),
			expected: "1.28.2",
		},
	}

	for name, test := range tests {
		t.Run(name, func(tt *testing.T) {
			result := r.getCurrentVersion(test.cluster)
			assert.Equal(tt, test.expected, result.String())
		})
	}
}

// --- computeEffectiveVersion tests ---

func TestComputeEffectiveVersion(t *testing.T) {
	r := newReconciler()

	tests := map[string]struct {
		specVersion   string
		statusVersion string
		schemaVersion string
		intermediates []string
		expected      string
		expectError   bool
	}{
		"single hop returns target": {
			specVersion:   "1.26.3",
			statusVersion: "1.25.2",
			expected:      "1.26.3",
		},
		"multi-hop returns first intermediate from registry": {
			specVersion:   "1.28.2",
			statusVersion: "1.25.2",
			expected:      "1.26.3",
		},
		"multi-hop with user-specified intermediates": {
			specVersion:   "1.28.2",
			statusVersion: "1.25.2",
			intermediates: []string{"1.26.3", "1.27.4"},
			expected:      "1.26.3",
		},
		"same version returns target": {
			specVersion:   "1.25.2",
			statusVersion: "1.25.2",
			expected:      "1.25.2",
		},
		"uses status.version not schemaVersion for hop computation": {
			specVersion:   "1.28.2",
			statusVersion: "1.25.2",
			schemaVersion: "1.27.4", // schema is ahead, but status.version matters
			expected:      "1.26.3",
		},
	}

	for name, test := range tests {
		t.Run(name, func(tt *testing.T) {
			cluster := clusterWithStatus(test.specVersion, test.statusVersion, test.schemaVersion)
			if len(test.intermediates) > 0 {
				cluster.Spec.VersionUpgrade = &v1beta1.VersionUpgradeSpec{
					IntermediateVersions: test.intermediates,
				}
			}

			result, err := r.computeEffectiveVersion(cluster)
			if test.expectError {
				assert.Error(tt, err)
				return
			}
			require.NoError(tt, err)
			assert.Equal(tt, test.expected, result.String())
		})
	}
}

// --- getStabilityDuration tests ---

func TestGetStabilityDuration(t *testing.T) {
	r := newReconciler()

	tests := map[string]struct {
		cluster  *v1beta1.TemporalCluster
		expected time.Duration
	}{
		"no version upgrade config defaults to 5m": {
			cluster:  &v1beta1.TemporalCluster{},
			expected: 5 * time.Minute,
		},
		"version upgrade without stability duration defaults to 5m": {
			cluster: &v1beta1.TemporalCluster{
				Spec: v1beta1.TemporalClusterSpec{
					VersionUpgrade: &v1beta1.VersionUpgradeSpec{},
				},
			},
			expected: 5 * time.Minute,
		},
		"version upgrade with stability duration": {
			cluster: &v1beta1.TemporalCluster{
				Spec: v1beta1.TemporalClusterSpec{
					VersionUpgrade: &v1beta1.VersionUpgradeSpec{
						StabilityDuration: &metav1.Duration{Duration: 5 * time.Minute},
					},
				},
			},
			expected: 5 * time.Minute,
		},
	}

	for name, test := range tests {
		t.Run(name, func(tt *testing.T) {
			assert.Equal(tt, test.expected, r.getStabilityDuration(test.cluster))
		})
	}
}

// --- Hop annotation tests ---

func TestSetHopCompletionAnnotation(t *testing.T) {
	r := newReconciler()
	cluster := &v1beta1.TemporalCluster{}

	v := version.MustNewVersionFromString("1.26.3")
	r.setHopCompletionAnnotation(cluster, v)

	annotations := cluster.GetAnnotations()
	assert.Equal(t, "1.26.3", annotations[annotationLastHopVersion])
	assert.NotEmpty(t, annotations[annotationLastHopTime])

	// Verify the time is parseable and recent
	hopTime, err := time.Parse(time.RFC3339, annotations[annotationLastHopTime])
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now(), hopTime, 5*time.Second)
}

func TestSetHopCompletionAnnotationPreservesExisting(t *testing.T) {
	r := newReconciler()
	cluster := &v1beta1.TemporalCluster{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"existing-annotation": "value",
			},
		},
	}

	v := version.MustNewVersionFromString("1.27.4")
	r.setHopCompletionAnnotation(cluster, v)

	annotations := cluster.GetAnnotations()
	assert.Equal(t, "value", annotations["existing-annotation"])
	assert.Equal(t, "1.27.4", annotations[annotationLastHopVersion])
}

func TestClearHopAnnotations(t *testing.T) {
	r := newReconciler()

	t.Run("removes hop annotations", func(tt *testing.T) {
		cluster := &v1beta1.TemporalCluster{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					annotationLastHopTime:    time.Now().UTC().Format(time.RFC3339),
					annotationLastHopVersion: "1.26.3",
					"other-annotation":       "keep",
				},
			},
		}

		r.clearHopAnnotations(cluster)

		annotations := cluster.GetAnnotations()
		assert.Equal(tt, "keep", annotations["other-annotation"])
		_, hasTime := annotations[annotationLastHopTime]
		_, hasVersion := annotations[annotationLastHopVersion]
		assert.False(tt, hasTime)
		assert.False(tt, hasVersion)
	})

	t.Run("no-op when no annotations", func(_ *testing.T) {
		cluster := &v1beta1.TemporalCluster{}
		r.clearHopAnnotations(cluster) // should not panic
	})

	t.Run("no-op when no hop annotations", func(tt *testing.T) {
		cluster := &v1beta1.TemporalCluster{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{"other": "value"},
			},
		}
		r.clearHopAnnotations(cluster)
		assert.Equal(tt, "value", cluster.GetAnnotations()["other"])
	})
}

// --- remainingStabilityWait tests ---

func TestRemainingStabilityWait(t *testing.T) {
	r := newReconciler()

	tests := map[string]struct {
		annotations       map[string]string
		stabilityDuration *metav1.Duration
		expectWaiting     bool
		expectRemaining   time.Duration // approximate
	}{
		"no annotations - not waiting": {
			annotations:   nil,
			expectWaiting: false,
		},
		"no hop time annotation - not waiting": {
			annotations:   map[string]string{"other": "value"},
			expectWaiting: false,
		},
		"no stability duration configured - uses 5m default, still waiting": {
			annotations: map[string]string{
				annotationLastHopTime:    time.Now().UTC().Format(time.RFC3339),
				annotationLastHopVersion: "1.26.3",
			},
			stabilityDuration: nil,
			expectWaiting:     true,
			expectRemaining:   5 * time.Minute,
		},
		"zero stability duration - not waiting": {
			annotations: map[string]string{
				annotationLastHopTime:    time.Now().UTC().Format(time.RFC3339),
				annotationLastHopVersion: "1.26.3",
			},
			stabilityDuration: &metav1.Duration{Duration: 0},
			expectWaiting:     false,
		},
		"recent hop with 5m stability - still waiting": {
			annotations: map[string]string{
				annotationLastHopTime:    time.Now().UTC().Format(time.RFC3339),
				annotationLastHopVersion: "1.26.3",
			},
			stabilityDuration: &metav1.Duration{Duration: 5 * time.Minute},
			expectWaiting:     true,
			expectRemaining:   5 * time.Minute,
		},
		"old hop with 5m stability - not waiting": {
			annotations: map[string]string{
				annotationLastHopTime:    time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339),
				annotationLastHopVersion: "1.26.3",
			},
			stabilityDuration: &metav1.Duration{Duration: 5 * time.Minute},
			expectWaiting:     false,
		},
		"hop 3 minutes ago with 5m stability - waiting ~2m": {
			annotations: map[string]string{
				annotationLastHopTime:    time.Now().Add(-3 * time.Minute).UTC().Format(time.RFC3339),
				annotationLastHopVersion: "1.26.3",
			},
			stabilityDuration: &metav1.Duration{Duration: 5 * time.Minute},
			expectWaiting:     true,
			expectRemaining:   2 * time.Minute,
		},
		"invalid time format - not waiting": {
			annotations: map[string]string{
				annotationLastHopTime:    "not-a-time",
				annotationLastHopVersion: "1.26.3",
			},
			stabilityDuration: &metav1.Duration{Duration: 5 * time.Minute},
			expectWaiting:     false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(tt *testing.T) {
			cluster := &v1beta1.TemporalCluster{
				Spec: v1beta1.TemporalClusterSpec{
					Version: version.MustNewVersionFromString("1.28.2"),
				},
			}
			if test.annotations != nil {
				cluster.Annotations = test.annotations
			}
			if test.stabilityDuration != nil {
				cluster.Spec.VersionUpgrade = &v1beta1.VersionUpgradeSpec{
					StabilityDuration: test.stabilityDuration,
				}
			}

			remaining, waiting := r.remainingStabilityWait(cluster)
			assert.Equal(tt, test.expectWaiting, waiting)

			if test.expectWaiting {
				// Allow 5 seconds tolerance for time-based checks
				assert.InDelta(tt, test.expectRemaining.Seconds(), remaining.Seconds(), 5.0)
			}
		})
	}
}

// --- Multi-hop flow integration tests ---
// These test the interaction between getCurrentVersion, computeEffectiveVersion,
// and the stability annotations to verify the full hop sequencing logic.

func TestMultiHopFlowSequencing(t *testing.T) {
	r := newReconciler()

	// Simulate the full multi-hop flow: 1.25.2 -> 1.28.2 with intermediates 1.26.3, 1.27.4

	// Phase 1: Initial state. status.version=1.25.2, target=1.28.2
	cluster := &v1beta1.TemporalCluster{
		Spec: v1beta1.TemporalClusterSpec{
			Version: version.MustNewVersionFromString("1.28.2"),
			VersionUpgrade: &v1beta1.VersionUpgradeSpec{
				StabilityDuration:    &metav1.Duration{Duration: 5 * time.Minute},
				IntermediateVersions: []string{"1.26.3", "1.27.4"},
			},
		},
		Status: v1beta1.TemporalClusterStatus{
			Version: "1.25.2",
		},
	}

	// Should compute first hop as 1.26.3
	effective, err := r.computeEffectiveVersion(cluster)
	require.NoError(t, err)
	assert.Equal(t, "1.26.3", effective.String())

	// Verify multi-hop is in progress
	assert.NotEqual(t, cluster.Spec.Version.String(), effective.String())

	// Not in stability wait (no annotations)
	_, waiting := r.remainingStabilityWait(cluster)
	assert.False(t, waiting)

	// Phase 2: Simulate hop 1 completion. Server deployed and ready at 1.26.3.
	// Schema also at 1.26.3 but that should NOT affect hop computation.
	cluster.Status.Version = "1.26.3"
	cluster.Status.Persistence = &v1beta1.TemporalPersistenceStatus{
		DefaultStore: &v1beta1.DatastoreStatus{
			SchemaVersion: version.MustNewVersionFromString("1.26.3"),
		},
	}

	// Set hop completion annotation (simulates what Reconcile does)
	r.setHopCompletionAnnotation(cluster, version.MustNewVersionFromString("1.26.3"))

	// Should be in stability wait now
	remaining, waiting := r.remainingStabilityWait(cluster)
	assert.True(t, waiting)
	assert.InDelta(t, 300.0, remaining.Seconds(), 5.0) // ~5 minutes

	// Even though status.version is 1.26.3, while in stability wait
	// the reconciler should NOT advance. The stability check at the top
	// of Reconcile returns early before computing effectiveVersion.

	// Phase 3: Simulate stability wait expired. Clear annotations.
	r.clearHopAnnotations(cluster)

	_, waiting = r.remainingStabilityWait(cluster)
	assert.False(t, waiting)

	// Now computeEffectiveVersion should return 1.27.4 (next hop)
	effective, err = r.computeEffectiveVersion(cluster)
	require.NoError(t, err)
	assert.Equal(t, "1.27.4", effective.String())

	// Phase 4: Simulate hop 2 completion. Server at 1.27.4.
	cluster.Status.Version = "1.27.4"
	cluster.Status.Persistence.DefaultStore.SchemaVersion = version.MustNewVersionFromString("1.27.4")

	r.setHopCompletionAnnotation(cluster, version.MustNewVersionFromString("1.27.4"))

	_, waiting = r.remainingStabilityWait(cluster)
	assert.True(t, waiting)

	// Phase 5: Stability expired, final hop.
	r.clearHopAnnotations(cluster)

	effective, err = r.computeEffectiveVersion(cluster)
	require.NoError(t, err)
	assert.Equal(t, "1.28.2", effective.String())

	// Final hop: effective == target, multiHopInProgress should be false
	assert.Equal(t, cluster.Spec.Version.String(), effective.String())
}

func TestMultiHopSchemaAheadDoesNotAdvanceHop(t *testing.T) {
	r := newReconciler()

	// This is the key bug scenario: schema migrations complete (schemaVersion
	// advances to 1.26.3) but the server hasn't been deployed yet
	// (status.version is still 1.25.2). The hop should NOT advance.
	cluster := &v1beta1.TemporalCluster{
		Spec: v1beta1.TemporalClusterSpec{
			Version: version.MustNewVersionFromString("1.28.2"),
		},
		Status: v1beta1.TemporalClusterStatus{
			Version: "1.25.2",
			Persistence: &v1beta1.TemporalPersistenceStatus{
				DefaultStore: &v1beta1.DatastoreStatus{
					SchemaVersion: version.MustNewVersionFromString("1.26.3"),
				},
			},
		},
	}

	effective, err := r.computeEffectiveVersion(cluster)
	require.NoError(t, err)
	// Should still be 1.26.3 (same hop), NOT 1.27.4
	assert.Equal(t, "1.26.3", effective.String())
}

func TestMultiHopWatchEventDuringStabilityWait(t *testing.T) {
	r := newReconciler()

	// Simulate the scenario where a deployment watch event triggers a reconcile
	// during the stability wait period. The reconciler should return early.
	cluster := &v1beta1.TemporalCluster{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				annotationLastHopTime:    time.Now().UTC().Format(time.RFC3339),
				annotationLastHopVersion: "1.26.3",
			},
		},
		Spec: v1beta1.TemporalClusterSpec{
			Version: version.MustNewVersionFromString("1.28.2"),
			VersionUpgrade: &v1beta1.VersionUpgradeSpec{
				StabilityDuration: &metav1.Duration{Duration: 5 * time.Minute},
			},
		},
		Status: v1beta1.TemporalClusterStatus{
			Version: "1.26.3",
			Persistence: &v1beta1.TemporalPersistenceStatus{
				DefaultStore: &v1beta1.DatastoreStatus{
					SchemaVersion: version.MustNewVersionFromString("1.26.3"),
				},
			},
		},
	}

	// The stability check should catch this and indicate waiting
	remaining, waiting := r.remainingStabilityWait(cluster)
	assert.True(t, waiting)
	assert.InDelta(t, 300.0, remaining.Seconds(), 5.0)

	// Even though getCurrentVersion would return 1.26.3 and
	// computeEffectiveVersion would return 1.27.4, the stability
	// gate at the top of Reconcile prevents any of that from running.
	// Let's verify what would happen if the gate wasn't there:
	effective, err := r.computeEffectiveVersion(cluster)
	require.NoError(t, err)
	assert.Equal(t, "1.27.4", effective.String())
	// ^ This proves the stability annotation is ESSENTIAL.
	// Without it, the watch event would advance the hop immediately.
}

func TestNoMultiHopForSingleMinorUpgrade(t *testing.T) {
	r := newReconciler()

	cluster := clusterWithStatus("1.26.3", "1.25.2", "1.25.2")

	effective, err := r.computeEffectiveVersion(cluster)
	require.NoError(t, err)
	assert.Equal(t, "1.26.3", effective.String())

	// effective == target, so multiHopInProgress should be false
	assert.Equal(t, cluster.Spec.Version.String(), effective.String())
}

func TestNoMultiHopWhenAlreadyAtTarget(t *testing.T) {
	r := newReconciler()

	cluster := clusterWithStatus("1.28.2", "1.28.2", "1.28.2")

	effective, err := r.computeEffectiveVersion(cluster)
	require.NoError(t, err)
	assert.Equal(t, "1.28.2", effective.String())
	assert.Equal(t, cluster.Spec.Version.String(), effective.String())
}

// --- getCurrentVersion safety tests ---

func TestGetCurrentVersionStatusSetSchemaAhead(t *testing.T) {
	r := newReconciler()

	// Critical safety test: when status.version IS set but schema is ahead
	// (e.g., schema migration completed but pods haven't rolled yet),
	// getCurrentVersion must return status.version, NOT schema version.
	// Otherwise computeEffectiveVersion would skip a hop.
	cluster := &v1beta1.TemporalCluster{
		Spec: v1beta1.TemporalClusterSpec{
			Version: version.MustNewVersionFromString("1.28.2"),
		},
		Status: v1beta1.TemporalClusterStatus{
			Version: "1.25.2", // pods are still at 1.25.2
			Persistence: &v1beta1.TemporalPersistenceStatus{
				DefaultStore: &v1beta1.DatastoreStatus{
					SchemaVersion: version.MustNewVersionFromString("1.26.3"), // schema migrated ahead
				},
			},
		},
	}

	current := r.getCurrentVersion(cluster)
	// Must return status.version (1.25.2), NOT schema version (1.26.3).
	assert.Equal(t, "1.25.2", current.String())

	// Verify this produces the correct hop (1.26.3, not 1.27.4)
	effective, err := r.computeEffectiveVersion(cluster)
	require.NoError(t, err)
	assert.Equal(t, "1.26.3", effective.String())
}

func TestGetCurrentVersionEmptyStatusWithSchemaFallback(t *testing.T) {
	r := newReconciler()

	// When status.version is empty (never set) and schema version exists,
	// we fall back to schema version. This handles operator restart mid-migration.
	cluster := &v1beta1.TemporalCluster{
		Spec: v1beta1.TemporalClusterSpec{
			Version: version.MustNewVersionFromString("1.28.2"),
		},
		Status: v1beta1.TemporalClusterStatus{
			Version: "", // never set
			Persistence: &v1beta1.TemporalPersistenceStatus{
				DefaultStore: &v1beta1.DatastoreStatus{
					SchemaVersion: version.MustNewVersionFromString("1.26.3"),
				},
			},
		},
	}

	current := r.getCurrentVersion(cluster)
	assert.Equal(t, "1.26.3", current.String())
}

func TestGetCurrentVersionFirstInstall(t *testing.T) {
	r := newReconciler()

	// First install: no status, no schema. Should use spec.version.
	cluster := &v1beta1.TemporalCluster{
		Spec: v1beta1.TemporalClusterSpec{
			Version: version.MustNewVersionFromString("1.25.2"),
		},
	}

	current := r.getCurrentVersion(cluster)
	assert.Equal(t, "1.25.2", current.String())
}

// --- Pause annotation tests ---

func TestIsUpgradePaused(t *testing.T) {
	r := newReconciler()

	tests := map[string]struct {
		annotations map[string]string
		expected    bool
	}{
		"no annotations": {
			annotations: nil,
			expected:    false,
		},
		"pause annotation set to true": {
			annotations: map[string]string{
				annotationPauseUpgrade: "true",
			},
			expected: true,
		},
		"pause annotation set to false": {
			annotations: map[string]string{
				annotationPauseUpgrade: "false",
			},
			expected: false,
		},
		"pause annotation set to empty": {
			annotations: map[string]string{
				annotationPauseUpgrade: "",
			},
			expected: false,
		},
		"other annotations only": {
			annotations: map[string]string{
				"some-other": "annotation",
			},
			expected: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(tt *testing.T) {
			cluster := &v1beta1.TemporalCluster{}
			if test.annotations != nil {
				cluster.Annotations = test.annotations
			}
			assert.Equal(tt, test.expected, r.isUpgradePaused(cluster))
		})
	}
}

// --- Hop timeout tests ---

func TestEnsureHopStartAnnotation(t *testing.T) {
	r := newReconciler()

	t.Run("sets annotation when absent", func(tt *testing.T) {
		cluster := &v1beta1.TemporalCluster{}
		r.ensureHopStartAnnotation(cluster)
		annotations := cluster.GetAnnotations()
		assert.NotEmpty(tt, annotations[annotationHopStartTime])

		startTime, err := time.Parse(time.RFC3339, annotations[annotationHopStartTime])
		require.NoError(tt, err)
		assert.WithinDuration(tt, time.Now(), startTime, 5*time.Second)
	})

	t.Run("does not overwrite existing annotation", func(tt *testing.T) {
		existingTime := time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)
		cluster := &v1beta1.TemporalCluster{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					annotationHopStartTime: existingTime,
				},
			},
		}
		r.ensureHopStartAnnotation(cluster)
		assert.Equal(tt, existingTime, cluster.GetAnnotations()[annotationHopStartTime])
	})
}

func TestIsHopTimedOut(t *testing.T) {
	r := newReconciler()

	tests := map[string]struct {
		annotations map[string]string
		expected    bool
	}{
		"no annotations - not timed out": {
			annotations: nil,
			expected:    false,
		},
		"recent start - not timed out": {
			annotations: map[string]string{
				annotationHopStartTime: time.Now().UTC().Format(time.RFC3339),
			},
			expected: false,
		},
		"old start - timed out": {
			annotations: map[string]string{
				annotationHopStartTime: time.Now().Add(-31 * time.Minute).UTC().Format(time.RFC3339),
			},
			expected: true,
		},
		"exactly at timeout - timed out": {
			annotations: map[string]string{
				annotationHopStartTime: time.Now().Add(-30 * time.Minute).UTC().Format(time.RFC3339),
			},
			expected: true,
		},
		"invalid time - not timed out": {
			annotations: map[string]string{
				annotationHopStartTime: "not-a-time",
			},
			expected: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(tt *testing.T) {
			cluster := &v1beta1.TemporalCluster{}
			if test.annotations != nil {
				cluster.Annotations = test.annotations
			}
			assert.Equal(tt, test.expected, r.isHopTimedOut(cluster))
		})
	}
}

func TestClearHopStartAnnotation(t *testing.T) {
	r := newReconciler()

	t.Run("removes hop start annotation", func(tt *testing.T) {
		cluster := &v1beta1.TemporalCluster{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					annotationHopStartTime: time.Now().UTC().Format(time.RFC3339),
					"other":                "keep",
				},
			},
		}
		r.clearHopStartAnnotation(cluster)
		assert.Equal(tt, "keep", cluster.GetAnnotations()["other"])
		_, has := cluster.GetAnnotations()[annotationHopStartTime]
		assert.False(tt, has)
	})

	t.Run("no-op when no annotation", func(_ *testing.T) {
		cluster := &v1beta1.TemporalCluster{}
		r.clearHopStartAnnotation(cluster) // should not panic
	})
}

func TestSetAutoPause(t *testing.T) {
	r := newReconciler()
	r.Recorder = &fakeRecorder{}

	cluster := &v1beta1.TemporalCluster{}
	v := version.MustNewVersionFromString("1.26.3")
	r.setAutoPause(cluster, v)

	annotations := cluster.GetAnnotations()
	assert.Equal(t, "true", annotations[annotationPauseUpgrade])
	assert.Equal(t, "1.26.3", annotations["temporal.io/auto-paused-at-version"])
}

// --- Current hop target annotation tests ---

func TestCurrentHopTarget(t *testing.T) {
	r := newReconciler()

	t.Run("get returns empty when no annotation", func(tt *testing.T) {
		cluster := &v1beta1.TemporalCluster{}
		assert.Equal(tt, "", r.getCurrentHopTarget(cluster))
	})

	t.Run("set and get", func(tt *testing.T) {
		cluster := &v1beta1.TemporalCluster{}
		v := version.MustNewVersionFromString("1.26.3")
		r.setCurrentHopTarget(cluster, v)
		assert.Equal(tt, "1.26.3", r.getCurrentHopTarget(cluster))
	})

	t.Run("set preserves other annotations", func(tt *testing.T) {
		cluster := &v1beta1.TemporalCluster{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{"existing": "value"},
			},
		}
		v := version.MustNewVersionFromString("1.27.4")
		r.setCurrentHopTarget(cluster, v)
		assert.Equal(tt, "1.27.4", r.getCurrentHopTarget(cluster))
		assert.Equal(tt, "value", cluster.GetAnnotations()["existing"])
	})

	t.Run("clear removes annotation", func(tt *testing.T) {
		cluster := &v1beta1.TemporalCluster{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					annotationCurrentHopTarget: "1.26.3",
					"other":                    "keep",
				},
			},
		}
		r.clearCurrentHopTarget(cluster)
		assert.Equal(tt, "", r.getCurrentHopTarget(cluster))
		assert.Equal(tt, "keep", cluster.GetAnnotations()["other"])
	})

	t.Run("clear no-op when no annotation", func(_ *testing.T) {
		cluster := &v1beta1.TemporalCluster{}
		r.clearCurrentHopTarget(cluster) // should not panic
	})
}

// --- End-to-end multi-hop integration tests ---
// These simulate the full cross-reconcile flow that exposed the stability wait bug.
// The key insight: between reconcile cycles, status.version gets updated but the
// next reconcile must NOT skip ahead to the next hop without the stability wait.

// simulateReconcileCycle simulates what the Reconcile method does in terms of
// version computation, hop target tracking, and stability annotation management.
// It returns (effectiveVersion, multiHopInProgress, stabilityWaitTriggered).
// This is a faithful reproduction of the Reconcile logic for multi-hop handling.
func simulateReconcileCycle(r *TemporalClusterReconciler, cluster *v1beta1.TemporalCluster, servicesReady bool) (effectiveVersion string, multiHopInProgress bool, stabilityWaitTriggered bool) {
	// Step 1: Stability wait gate
	if _, waiting := r.remainingStabilityWait(cluster); waiting {
		return "", false, true
	}

	targetVersion := cluster.Spec.Version

	// Step 2: Check for in-flight hop target
	var effective *version.Version
	if hopTarget := r.getCurrentHopTarget(cluster); hopTarget != "" {
		hopTargetVersion, err := version.NewVersionFromString(hopTarget)
		if err == nil && targetVersion != nil && hopTarget != targetVersion.String() {
			effective = hopTargetVersion
			multiHopInProgress = true
		}
	}

	// Step 3: Compute effective version if no in-flight hop
	if !multiHopInProgress {
		var err error
		effective, err = r.computeEffectiveVersion(cluster)
		if err != nil {
			return "", false, false
		}
		if effective == nil {
			effective = targetVersion
		}
		multiHopInProgress = targetVersion != nil && effective.String() != targetVersion.String()
	}

	if multiHopInProgress {
		// Record hop target
		r.setCurrentHopTarget(cluster, effective)
		r.ensureHopStartAnnotation(cluster)

		// Temporarily set spec.version (simulating the defer pattern)
		origSpec := cluster.Spec.Version
		cluster.Spec.Version = effective
		defer func() { cluster.Spec.Version = origSpec }()

		// Clear old hop completion annotations
		r.clearHopAnnotations(cluster)

		// Simulate reconcileResources: update service statuses based on effective version
		// In real code, this comes from deployment labels. Here we simulate it.
		for i := range cluster.Status.Services {
			cluster.Status.Services[i].Version = effective.String()
			cluster.Status.Services[i].Ready = servicesReady
		}

		// Simulate ObservedVersionMatchesDesiredVersion setting status.version
		allVersionsMatch := true
		for _, svc := range cluster.Status.Services {
			if svc.Version != cluster.Spec.Version.String() {
				allVersionsMatch = false
				break
			}
		}
		if allVersionsMatch && len(cluster.Status.Services) > 0 {
			cluster.Status.Version = cluster.Spec.Version.String()
		}

		// Check readiness (same as IsClusterReady)
		clusterReady := len(cluster.Status.Services) > 0
		for _, svc := range cluster.Status.Services {
			if !svc.Ready || svc.Version != cluster.Spec.Version.String() {
				clusterReady = false
				break
			}
		}

		if !clusterReady {
			// Not ready yet, will requeue
			return effective.String(), true, false
		}

		// Hop completed! Set stability annotation and clear hop tracking.
		r.setHopCompletionAnnotation(cluster, effective)
		r.clearHopStartAnnotation(cluster)
		r.clearCurrentHopTarget(cluster)
		return effective.String(), true, true
	}

	// Not a multi-hop or final version
	r.clearHopAnnotations(cluster)
	r.clearHopStartAnnotation(cluster)
	r.clearCurrentHopTarget(cluster)
	return effective.String(), false, false
}

func TestE2EMultiHopStabilityWaitRespected(t *testing.T) {
	// This test reproduces the exact bug scenario:
	// 1. Hop 1 starts (1.25.2 → 1.26.3), schema migrates, deployments update
	// 2. Pods aren't ready yet → reconcile returns RequeueAfter
	// 3. Next reconcile: pods are now ready at 1.26.3
	// 4. BUG (old code): computeEffectiveVersion returns 1.27.4, skipping stability wait
	// 4. FIX (new code): in-flight hop target is still 1.26.3, stability wait triggers

	r := newReconciler()

	cluster := &v1beta1.TemporalCluster{
		Spec: v1beta1.TemporalClusterSpec{
			Version: version.MustNewVersionFromString("1.28.2"),
			VersionUpgrade: &v1beta1.VersionUpgradeSpec{
				StabilityDuration:    &metav1.Duration{Duration: 5 * time.Minute},
				IntermediateVersions: []string{"1.26.3", "1.27.4"},
			},
		},
		Status: v1beta1.TemporalClusterStatus{
			Version: "1.25.2",
			Services: []v1beta1.ServiceStatus{
				{Name: "frontend", Ready: true, Version: "1.25.2"},
				{Name: "history", Ready: true, Version: "1.25.2"},
				{Name: "matching", Ready: true, Version: "1.25.2"},
				{Name: "worker", Ready: true, Version: "1.25.2"},
				{Name: "internal-frontend", Ready: true, Version: "1.25.2"},
			},
		},
	}

	// === Reconcile 1: Start hop 1, pods not ready yet ===
	effective, multiHop, stabilityWait := simulateReconcileCycle(r, cluster, false)
	assert.Equal(t, "1.26.3", effective, "Reconcile 1: should target 1.26.3")
	assert.True(t, multiHop, "Reconcile 1: multi-hop should be in progress")
	assert.False(t, stabilityWait, "Reconcile 1: stability wait should NOT trigger (pods not ready)")

	// Verify hop target annotation was set
	assert.Equal(t, "1.26.3", r.getCurrentHopTarget(cluster), "Reconcile 1: hop target should be recorded")

	// Verify status.version was updated (simulating ObservedVersionMatchesDesiredVersion)
	assert.Equal(t, "1.26.3", cluster.Status.Version, "Reconcile 1: status.version should be 1.26.3")

	// Verify spec.version was restored by defer
	assert.Equal(t, "1.28.2", cluster.Spec.Version.String(), "Reconcile 1: spec.version should be restored to target")

	// === Reconcile 2: Pods are now ready at 1.26.3 ===
	// This is the critical test: the old code would compute effectiveVersion=1.27.4
	// because status.version=1.26.3 and no stability annotation exists.
	// The fix: getCurrentHopTarget returns "1.26.3", so we continue with that hop.
	effective, multiHop, stabilityWait = simulateReconcileCycle(r, cluster, true)
	assert.Equal(t, "1.26.3", effective, "Reconcile 2: should still target 1.26.3 (not 1.27.4!)")
	assert.True(t, multiHop, "Reconcile 2: multi-hop should be in progress")
	assert.True(t, stabilityWait, "Reconcile 2: stability wait MUST trigger (hop completed)")

	// Verify hop completion annotation was set
	annotations := cluster.GetAnnotations()
	assert.NotEmpty(t, annotations[annotationLastHopTime], "Reconcile 2: hop completion time should be set")
	assert.Equal(t, "1.26.3", annotations[annotationLastHopVersion], "Reconcile 2: last hop version should be 1.26.3")

	// Verify hop target was cleared (hop is done)
	assert.Equal(t, "", r.getCurrentHopTarget(cluster), "Reconcile 2: hop target should be cleared after completion")

	// Verify hop start time was cleared
	_, hasHopStart := annotations[annotationHopStartTime]
	assert.False(t, hasHopStart, "Reconcile 2: hop start time should be cleared")

	// === Reconcile 3: During stability wait, should not advance ===
	effective, multiHop, stabilityWait = simulateReconcileCycle(r, cluster, true)
	assert.Equal(t, "", effective, "Reconcile 3: no effective version during stability wait")
	assert.False(t, multiHop, "Reconcile 3: multi-hop should not be computed during stability wait")
	assert.True(t, stabilityWait, "Reconcile 3: should still be in stability wait")

	// === Simulate stability wait expiry ===
	// Backdate the hop completion time to make it expire
	cluster.Annotations[annotationLastHopTime] = time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)

	// === Reconcile 4: Stability wait expired, start hop 2 ===
	effective, multiHop, stabilityWait = simulateReconcileCycle(r, cluster, false)
	assert.Equal(t, "1.27.4", effective, "Reconcile 4: should now target 1.27.4")
	assert.True(t, multiHop, "Reconcile 4: multi-hop should be in progress")
	assert.False(t, stabilityWait, "Reconcile 4: stability wait should NOT trigger (new hop, pods not ready)")
	assert.Equal(t, "1.27.4", r.getCurrentHopTarget(cluster), "Reconcile 4: hop target should be 1.27.4")

	// === Reconcile 5: Pods ready at 1.27.4 ===
	effective, multiHop, stabilityWait = simulateReconcileCycle(r, cluster, true)
	assert.Equal(t, "1.27.4", effective, "Reconcile 5: should still target 1.27.4")
	assert.True(t, multiHop, "Reconcile 5: multi-hop should be in progress")
	assert.True(t, stabilityWait, "Reconcile 5: stability wait MUST trigger (hop 2 completed)")
	assert.Equal(t, "1.27.4", cluster.GetAnnotations()[annotationLastHopVersion])
	assert.Equal(t, "", r.getCurrentHopTarget(cluster), "Reconcile 5: hop target should be cleared")

	// === Simulate stability wait expiry for hop 2 ===
	cluster.Annotations[annotationLastHopTime] = time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)

	// === Reconcile 6: Final hop to 1.28.2 ===
	effective, multiHop, stabilityWait = simulateReconcileCycle(r, cluster, true)
	assert.Equal(t, "1.28.2", effective, "Reconcile 6: should target 1.28.2 (final)")
	assert.False(t, multiHop, "Reconcile 6: multi-hop should be false (effective == target)")
	assert.False(t, stabilityWait, "Reconcile 6: no stability wait for final version")

	// Verify all hop annotations are cleaned up
	annotations = cluster.GetAnnotations()
	_, hasHopTime := annotations[annotationLastHopTime]
	_, hasHopVersion := annotations[annotationLastHopVersion]
	_, hasHopStart = annotations[annotationHopStartTime]
	_, hasHopTarget := annotations[annotationCurrentHopTarget]
	assert.False(t, hasHopTime, "Final: hop time annotation should be cleaned up")
	assert.False(t, hasHopVersion, "Final: hop version annotation should be cleaned up")
	assert.False(t, hasHopStart, "Final: hop start annotation should be cleaned up")
	assert.False(t, hasHopTarget, "Final: hop target annotation should be cleaned up")
}

func TestE2EMultiHopNeverSkipsVersion(t *testing.T) {
	// Verify that across multiple reconcile cycles, we never see an effective version
	// that skips a minor version. This catches the root cause bug where status.version
	// advancing caused computeEffectiveVersion to jump ahead.

	r := newReconciler()

	cluster := &v1beta1.TemporalCluster{
		Spec: v1beta1.TemporalClusterSpec{
			Version: version.MustNewVersionFromString("1.28.2"),
			VersionUpgrade: &v1beta1.VersionUpgradeSpec{
				StabilityDuration:    &metav1.Duration{Duration: 5 * time.Minute},
				IntermediateVersions: []string{"1.26.3", "1.27.4"},
			},
		},
		Status: v1beta1.TemporalClusterStatus{
			Version: "1.25.2",
			Services: []v1beta1.ServiceStatus{
				{Name: "frontend", Ready: true, Version: "1.25.2"},
				{Name: "history", Ready: true, Version: "1.25.2"},
				{Name: "matching", Ready: true, Version: "1.25.2"},
				{Name: "worker", Ready: true, Version: "1.25.2"},
				{Name: "internal-frontend", Ready: true, Version: "1.25.2"},
			},
		},
	}

	// Track all effective versions seen across reconcile cycles
	var seenVersions []string
	prevMinor := uint64(25) // starting minor

	// Run through the full upgrade with mixed ready/not-ready cycles
	for cycle := 0; cycle < 20; cycle++ {
		// Check if we're done
		if cluster.Status.Version == "1.28.2" && r.getCurrentHopTarget(cluster) == "" {
			_, waiting := r.remainingStabilityWait(cluster)
			if !waiting {
				break
			}
		}

		// Alternate between not-ready and ready to simulate real rollout timing
		servicesReady := cycle%2 == 1

		effective, _, stabilityWait := simulateReconcileCycle(r, cluster, servicesReady)

		if stabilityWait && effective == "" {
			// In stability wait, expire it for next cycle
			if ann, ok := cluster.Annotations[annotationLastHopTime]; ok && ann != "" {
				cluster.Annotations[annotationLastHopTime] = time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)
			}
			continue
		}

		if effective != "" {
			seenVersions = append(seenVersions, effective)

			// Verify no minor version was skipped
			v, err := version.NewVersionFromString(effective)
			require.NoError(t, err, "cycle %d: invalid version %s", cycle, effective)
			if v.Minor() > prevMinor {
				assert.Equal(t, prevMinor+1, v.Minor(),
					"cycle %d: version %s skips minor from %d (versions seen: %v)",
					cycle, effective, prevMinor, seenVersions)
				prevMinor = v.Minor()
			}
		}
	}

	// Verify we saw all intermediate versions
	assert.Contains(t, seenVersions, "1.26.3", "should have seen 1.26.3")
	assert.Contains(t, seenVersions, "1.27.4", "should have seen 1.27.4")
	assert.Contains(t, seenVersions, "1.28.2", "should have seen 1.28.2")
}

func TestE2EHopTargetPersistsAcrossReconciles(t *testing.T) {
	// Verify that the hop target annotation prevents effective version from changing
	// between reconcile cycles even when status.version advances.

	r := newReconciler()

	cluster := &v1beta1.TemporalCluster{
		Spec: v1beta1.TemporalClusterSpec{
			Version: version.MustNewVersionFromString("1.28.2"),
			VersionUpgrade: &v1beta1.VersionUpgradeSpec{
				StabilityDuration:    &metav1.Duration{Duration: 5 * time.Minute},
				IntermediateVersions: []string{"1.26.3", "1.27.4"},
			},
		},
		Status: v1beta1.TemporalClusterStatus{
			Version: "1.25.2",
			Services: []v1beta1.ServiceStatus{
				{Name: "frontend", Ready: true, Version: "1.25.2"},
				{Name: "history", Ready: true, Version: "1.25.2"},
			},
		},
	}

	// Start hop 1 (pods not ready)
	effective1, _, _ := simulateReconcileCycle(r, cluster, false)
	assert.Equal(t, "1.26.3", effective1)
	assert.Equal(t, "1.26.3", r.getCurrentHopTarget(cluster))

	// Simulate what the patch helper does: status.version is now 1.26.3
	// (set by ObservedVersionMatchesDesiredVersion inside reconcileResources)
	// but spec.version is restored to 1.28.2 by the defer
	assert.Equal(t, "1.26.3", cluster.Status.Version)
	assert.Equal(t, "1.28.2", cluster.Spec.Version.String())

	// Without the fix, computeEffectiveVersion(1.26.3, 1.28.2) would return 1.27.4
	// With the fix, getCurrentHopTarget returns "1.26.3" and we stick with it
	effective2, _, _ := simulateReconcileCycle(r, cluster, false)
	assert.Equal(t, "1.26.3", effective2, "hop target should prevent advancing to 1.27.4")

	// Run it 5 more times — should always be 1.26.3 until pods are ready
	for i := 0; i < 5; i++ {
		effective, _, _ := simulateReconcileCycle(r, cluster, false)
		assert.Equal(t, "1.26.3", effective, "iteration %d: should still target 1.26.3", i)
	}

	// Now pods are ready — should trigger stability wait and clear hop target
	effective3, _, stabilityWait := simulateReconcileCycle(r, cluster, true)
	assert.Equal(t, "1.26.3", effective3)
	assert.True(t, stabilityWait, "should trigger stability wait when pods become ready")
	assert.Equal(t, "", r.getCurrentHopTarget(cluster), "hop target should be cleared after completion")
}

func TestE2EPauseAnnotationDuringHop(t *testing.T) {
	// Verify that pausing during an in-flight hop clears the hop target
	// so that when unpaused, it recomputes from the current state.

	r := newReconciler()

	cluster := &v1beta1.TemporalCluster{
		Spec: v1beta1.TemporalClusterSpec{
			Version: version.MustNewVersionFromString("1.28.2"),
			VersionUpgrade: &v1beta1.VersionUpgradeSpec{
				StabilityDuration:    &metav1.Duration{Duration: 5 * time.Minute},
				IntermediateVersions: []string{"1.26.3", "1.27.4"},
			},
		},
		Status: v1beta1.TemporalClusterStatus{
			Version: "1.25.2",
			Services: []v1beta1.ServiceStatus{
				{Name: "frontend", Ready: true, Version: "1.25.2"},
				{Name: "history", Ready: true, Version: "1.25.2"},
			},
		},
	}

	// Start hop 1
	simulateReconcileCycle(r, cluster, false)
	assert.Equal(t, "1.26.3", r.getCurrentHopTarget(cluster))

	// Now simulate the pause annotation being set by an operator
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[annotationPauseUpgrade] = "true"
	cluster.SetAnnotations(annotations)

	// Simulate what the Reconcile method does with pause:
	// It checks isUpgradePaused, then clears hop target and falls through
	// at the current running version.
	if r.isUpgradePaused(cluster) {
		r.clearCurrentHopTarget(cluster)
	}

	assert.Equal(t, "", r.getCurrentHopTarget(cluster), "hop target should be cleared when paused")

	// Remove pause and continue.
	// Note: the first simulateReconcileCycle set status.version=1.26.3
	// (via ObservedVersionMatchesDesiredVersion), so after unpause the
	// next hop from 1.26.3 is correctly 1.27.4.
	delete(cluster.Annotations, annotationPauseUpgrade)

	// Should recompute from current state (status.version=1.26.3 → next hop is 1.27.4)
	effective, multiHop, _ := simulateReconcileCycle(r, cluster, false)
	assert.Equal(t, "1.27.4", effective, "should recompute hop from current state after pause")
	assert.True(t, multiHop)
}

// fakeRecorder implements record.EventRecorder for tests.
type fakeRecorder struct{}

func (f *fakeRecorder) Event(_ runtime.Object, _, _, _ string)                       {}
func (f *fakeRecorder) Eventf(_ runtime.Object, _, _, _ string, _ ...interface{})     {}
func (f *fakeRecorder) AnnotatedEventf(_ runtime.Object, _ map[string]string, _, _, _ string, _ ...interface{}) {
}
