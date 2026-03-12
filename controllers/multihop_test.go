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

// fakeRecorder implements record.EventRecorder for tests.
type fakeRecorder struct{}

func (f *fakeRecorder) Event(_ runtime.Object, _, _, _ string)                       {}
func (f *fakeRecorder) Eventf(_ runtime.Object, _, _, _ string, _ ...interface{})     {}
func (f *fakeRecorder) AnnotatedEventf(_ runtime.Object, _ map[string]string, _, _, _ string, _ ...interface{}) {
}
