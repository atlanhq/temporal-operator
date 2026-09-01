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

package controllers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	"github.com/alexandrevilain/temporal-operator/api/v1beta1"
	"github.com/alexandrevilain/temporal-operator/pkg/version"
)

// visibilityCluster builds a cluster whose visibility store is at schemaVersion
// while the spec targets specVersion.
func visibilityCluster(specVersion, schemaVersion string) *v1beta1.TemporalCluster {
	cluster := &v1beta1.TemporalCluster{
		Spec: v1beta1.TemporalClusterSpec{
			Version: version.MustNewVersionFromString(specVersion),
			Persistence: v1beta1.TemporalPersistenceSpec{
				VisibilityStore: &v1beta1.DatastoreSpec{
					SQL: &v1beta1.SQLSpec{PluginName: "postgres12"},
				},
			},
		},
	}

	cluster.Status.Persistence = &v1beta1.TemporalPersistenceStatus{
		VisibilityStore: &v1beta1.DatastoreStatus{
			Type: cluster.Spec.Persistence.VisibilityStore.GetType(),
		},
	}
	if schemaVersion != "" {
		cluster.Status.Persistence.VisibilityStore.SchemaVersion = version.MustNewVersionFromString(schemaVersion)
	}

	return cluster
}

func target(v string) *version.Version {
	return version.MustNewVersionFromString(v)
}

// The gate must engage only where a migration will rewrite live data. Gating a
// fresh install, or a cluster already at its target, would hold a cluster that
// has no rewrite to protect.
func TestVisibilitySchemaUpgradePending(t *testing.T) {
	tests := map[string]struct {
		cluster *v1beta1.TemporalCluster
		target  string
		want    bool
	}{
		"schema behind the target is an upgrade": {
			cluster: visibilityCluster("1.30.6", "1.29.4"),
			target:  "1.30.6",
			want:    true,
		},
		"schema at the target has nothing pending": {
			cluster: visibilityCluster("1.30.6", "1.30.6"),
			target:  "1.30.6",
			want:    false,
		},
		"schema ahead of the target has nothing pending": {
			cluster: visibilityCluster("1.29.4", "1.30.6"),
			target:  "1.29.4",
			want:    false,
		},
		"fresh install is a setup, not a rewrite": {
			cluster: visibilityCluster("1.30.6", ""),
			target:  "1.30.6",
			want:    false,
		},
		"an intermediate hop short of the final target still rewrites": {
			cluster: visibilityCluster("1.31.0", "1.29.4"),
			target:  "1.30.6",
			want:    true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.want, visibilitySchemaUpgradePending(tt.cluster, target(tt.target)))
		})
	}
}

// A datastore plugin change re-runs update-schema against the same populated
// table, so the Job runs and the gate has to be in front of it. Treating this as
// "nothing pending" would let exactly the migration this gate exists to hold run
// unguarded.
func TestVisibilitySchemaUpgradePendingCoversDatastoreTypeChange(t *testing.T) {
	cluster := visibilityCluster("1.30.6", "1.30.6")
	cluster.Status.Persistence.VisibilityStore.Type = v1beta1.PostgresSQLDatastore
	require.NotEqual(t, cluster.Status.Persistence.VisibilityStore.Type,
		cluster.Spec.Persistence.VisibilityStore.GetType(), "fixture must represent a plugin change")

	assert.True(t, visibilitySchemaUpgradePending(cluster, target("1.30.6")),
		"a plugin change runs update-schema over live data and must be gated")
}

func TestIsSchemaPreflightBlocked(t *testing.T) {
	tests := map[string]struct {
		reason string
		status metav1.ConditionStatus
		want   bool
	}{
		"blocked on headroom": {
			reason: v1beta1.SchemaPreflightBlockedReason,
			status: metav1.ConditionTrue,
			want:   true,
		},
		"blocked because the inputs could not be trusted": {
			reason: v1beta1.SchemaPreflightInputInvalidReason,
			status: metav1.ConditionTrue,
			want:   true,
		},
		"a different reconcile error is not a headroom block": {
			reason: v1beta1.PersistenceReconciliationFailedReason,
			status: metav1.ConditionTrue,
			want:   false,
		},
		"a cleared condition is not a block": {
			reason: v1beta1.SchemaPreflightBlockedReason,
			status: metav1.ConditionFalse,
			want:   false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			cluster := visibilityCluster("1.30.6", "1.29.4")
			v1beta1.SetTemporalClusterReconcileError(cluster, tt.status, tt.reason, "message")

			assert.Equal(t, tt.want, isSchemaPreflightBlocked(cluster))
		})
	}
}

// The hold must clear itself. Left set, it would misreport a healthy cluster and,
// worse, would be read by anything that consults it as a live signal.
func TestSchemaPreflightBlockClears(t *testing.T) {
	r := newReconciler()
	r.Recorder = record.NewFakeRecorder(10)

	cluster := visibilityCluster("1.30.6", "1.29.4")

	r.recordSchemaPreflightBlock(cluster, shortfallResult())
	require.True(t, isSchemaPreflightBlocked(cluster))

	r.clearSchemaPreflightBlock(cluster)
	assert.False(t, isSchemaPreflightBlocked(cluster),
		"a hold must not outlive the measurement that caused it")
}

// The check runs every reconcile and its message carries live byte counts, so an
// event per pass would be thousands of distinct, un-aggregatable events a day for
// one held cluster.
func TestSchemaPreflightEventsOnlyOnTransition(t *testing.T) {
	r := newReconciler()
	recorder := record.NewFakeRecorder(10)
	r.Recorder = recorder

	cluster := visibilityCluster("1.30.6", "1.29.4")

	for i := 0; i < 5; i++ {
		r.recordSchemaPreflightBlock(cluster, shortfallResult())
	}

	assert.Len(t, recorder.Events, 1, "the hold should announce itself once, not every reconcile")
}

// A hold must not advance any version status. Those are what the version logic
// trusts, and reporting a version that is not running would let the migration be
// treated as already done.
func TestSchemaPreflightBlockLeavesVersionStatusAlone(t *testing.T) {
	r := newReconciler()
	r.Recorder = record.NewFakeRecorder(10)

	cluster := visibilityCluster("1.30.6", "1.29.4")
	cluster.Status.Version = "1.29.4"

	for i := 0; i < 5; i++ {
		r.recordSchemaPreflightBlock(cluster, shortfallResult())
	}

	assert.Equal(t, "1.29.4", cluster.Status.Version,
		"status.version must not move while the migration is held")
	assert.Equal(t, "1.29.4", cluster.Status.Persistence.VisibilityStore.SchemaVersion.String(),
		"the recorded schema version must only advance when a migration Job succeeds")
}

// A held cluster never starts a hop, so the hop timeout cannot age a hold into an
// auto-pause. The timeout itself must still work for a genuinely stalled hop,
// which is what the second half asserts.
func TestHopTimeoutStillWorksAndIsNotReachedWhileHeld(t *testing.T) {
	r := newReconciler()

	stalled := visibilityCluster("1.30.6", "1.29.4")
	stalled.SetAnnotations(map[string]string{
		annotationHopStartTime: staleHopStart(),
	})
	assert.True(t, r.isHopTimedOut(stalled),
		"a genuinely stalled hop must still time out and auto-pause")

	held := visibilityCluster("1.30.6", "1.29.4")
	held.SetAnnotations(map[string]string{
		annotationHopStartTime:     staleHopStart(),
		annotationCurrentHopTarget: "1.30.6",
	})

	// The reconcile clears the hop clock when it decides to hold, which is what
	// keeps the elapsed time from ever reaching the timeout.
	r.clearHopStartAnnotation(held)
	r.clearCurrentHopTarget(held)

	assert.False(t, r.isHopTimedOut(held),
		"a hold leaves no hop clock running, so it cannot time out")
	assert.NotContains(t, held.GetAnnotations(), annotationPauseUpgrade,
		"a headroom hold must never auto-pause the upgrade")
}

func staleHopStart() string {
	return metav1.NewTime(metav1.Now().Add(-2 * defaultHopTimeout)).UTC().Format("2006-01-02T15:04:05Z07:00")
}

// The reconcile that records a hold still succeeds: the migration is skipped, the
// remaining resources reconcile normally, and so the success path runs and
// rewrites the same condition the hold was recorded on. If it clears the hold, a
// held cluster reports itself healthy and the only sign left that an upgrade is
// stuck is a log line.
func TestSchemaPreflightHoldSurvivesASuccessfulReconcile(t *testing.T) {
	r := newReconciler()
	r.Recorder = record.NewFakeRecorder(10)

	cluster := visibilityCluster("1.30.6", "1.29.4")
	r.recordSchemaPreflightBlock(cluster, shortfallResult())
	require.True(t, isSchemaPreflightBlocked(cluster))

	_, err := r.handleSuccessWithRequeue(cluster, 0)
	require.NoError(t, err)

	assert.True(t, isSchemaPreflightBlocked(cluster),
		"a successful reconcile must not clear a hold it did not resolve")
}

// The converse: with no hold recorded, a successful reconcile must still clear the
// error condition, or every cluster would keep the last error it ever saw.
func TestSuccessfulReconcileStillClearsAnOrdinaryError(t *testing.T) {
	r := newReconciler()
	r.Recorder = record.NewFakeRecorder(10)

	cluster := visibilityCluster("1.30.6", "1.29.4")
	v1beta1.SetTemporalClusterReconcileError(cluster, metav1.ConditionTrue,
		v1beta1.PersistenceReconciliationFailedReason, "something else failed")

	_, err := r.handleSuccessWithRequeue(cluster, 0)
	require.NoError(t, err)

	for _, c := range cluster.Status.Conditions {
		if c.Type == v1beta1.ReconcileErrorCondition {
			assert.Equal(t, metav1.ConditionFalse, c.Status,
				"an unrelated error must still be cleared on success")
		}
	}
}
