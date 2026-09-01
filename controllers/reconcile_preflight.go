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
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/alexandrevilain/temporal-operator/api/v1beta1"
	"github.com/alexandrevilain/temporal-operator/internal/preflight"
	"github.com/alexandrevilain/temporal-operator/pkg/version"
)

// evaluateSchemaPreflight measures whether the datastore's volume can absorb the
// rewriting migration that moving to target would run. It reports a result
// rather than acting on it; the caller holds the version transition by
// reconciling at the version already running, never by returning.
func (r *TemporalClusterReconciler) evaluateSchemaPreflight(ctx context.Context, cluster *v1beta1.TemporalCluster, target *version.Version) preflight.Result {
	logger := log.FromContext(ctx)

	spec := cluster.Spec.Persistence.Preflight
	if spec == nil || spec.Enabled == nil || !*spec.Enabled {
		preflight.Forget(cluster.GetName(), cluster.GetNamespace())
		return preflight.Result{Skipped: true}
	}

	// Only a migration that rewrites an existing table needs headroom. A first
	// install creates an empty schema, and a cluster already at its target has
	// nothing pending. Gating either would hold a cluster that has no rewrite to
	// protect, which on a tenant short of disk means holding it indefinitely.
	if !visibilitySchemaUpgradePending(cluster, target) {
		preflight.Forget(cluster.GetName(), cluster.GetNamespace())
		return preflight.Result{Skipped: true}
	}

	if r.Preflight == nil {
		// The checker is built from the manager's REST config at startup. Without
		// it we cannot measure, and an unmeasured gate must not approve.
		return r.publish(cluster, preflight.Result{
			Cause:   preflight.CauseUnavailable,
			Message: "the headroom checker is not configured, so headroom cannot be measured",
		})
	}

	cfg, err := resolvePreflightConfig(spec)
	if err != nil {
		// Configuration outside the supported range is a mistake, not a disk
		// problem. Refuse loudly rather than fall back to a default nobody asked
		// for.
		return r.publish(cluster, preflight.Result{
			Cause:   preflight.CauseFactorInvalid,
			Message: err.Error(),
		})
	}

	if spec.PostgresCluster == nil {
		return r.publish(cluster, preflight.Result{
			Cause:   preflight.CauseTargetNotFound,
			Message: "no postgresCluster reference is configured, so there is no instance to measure",
		})
	}

	result := r.publish(cluster, r.Preflight.Check(ctx, cfg, preflight.Target{
		Namespace:   spec.PostgresCluster.Namespace,
		ClusterName: spec.PostgresCluster.Name,
	}))

	if result.Blocked() {
		logger.Info("Holding the schema migration for lack of disk headroom",
			"target", target.String(),
			"relation", result.Relation,
			"tableBytes", result.TableBytes,
			"freeBytes", result.FreeBytes,
			"requiredBytes", result.RequiredBytes,
			"shortfallBytes", result.ShortfallBytes,
			"cause", string(result.Cause))
	}

	return result
}

// publish records the result as metrics and returns it unchanged, so callers can
// both report and act in one expression.
func (r *TemporalClusterReconciler) publish(cluster *v1beta1.TemporalCluster, result preflight.Result) preflight.Result {
	preflight.Publish(cluster.GetName(), cluster.GetNamespace(), result)
	return result
}

// recordSchemaPreflightBlock sets the condition describing why the migration is
// held. The event fires only on transition: the message carries live byte
// counts, so one per reconcile would be thousands of un-aggregatable events a
// day for a single held cluster.
func (r *TemporalClusterReconciler) recordSchemaPreflightBlock(cluster *v1beta1.TemporalCluster, result preflight.Result) {
	reason := v1beta1.SchemaPreflightBlockedReason
	if result.InputInvalid() {
		reason = v1beta1.SchemaPreflightInputInvalidReason
	}

	changed := !isSchemaPreflightBlocked(cluster)

	v1beta1.SetTemporalClusterReconcileError(cluster, metav1.ConditionTrue, reason, result.Message)

	if changed {
		r.Recorder.Event(cluster, corev1.EventTypeWarning, reason, result.Message)
	}
}

// clearSchemaPreflightBlock retires a previously recorded hold once the check
// passes, so the condition reflects the current measurement rather than the
// worst one ever seen.
func (r *TemporalClusterReconciler) clearSchemaPreflightBlock(cluster *v1beta1.TemporalCluster) {
	if !isSchemaPreflightBlocked(cluster) {
		return
	}

	v1beta1.SetTemporalClusterReconcileError(cluster, metav1.ConditionFalse,
		v1beta1.SchemaPreflightBlockedReason, "the volume now has room for the schema migration")
	r.Recorder.Event(cluster, corev1.EventTypeNormal, v1beta1.SchemaPreflightBlockedReason,
		"Disk headroom is sufficient, releasing the schema migration")
}

// isSchemaPreflightBlocked reports whether a hold is currently recorded on the
// cluster.
func isSchemaPreflightBlocked(cluster *v1beta1.TemporalCluster) bool {
	for _, condition := range cluster.Status.Conditions {
		if condition.Type != v1beta1.ReconcileErrorCondition {
			continue
		}
		if condition.Status != metav1.ConditionTrue {
			return false
		}

		return condition.Reason == v1beta1.SchemaPreflightBlockedReason ||
			condition.Reason == v1beta1.SchemaPreflightInputInvalidReason
	}

	return false
}

// resolvePreflightConfig translates the API spec into a validated check config.
func resolvePreflightConfig(spec *v1beta1.SchemaPreflightSpec) (preflight.Config, error) {
	var (
		safetyFactor  string
		minTableBytes int64
	)

	if spec.SafetyFactor != nil {
		safetyFactor = *spec.SafetyFactor
	}
	if spec.MinTableBytes != nil {
		minTableBytes = *spec.MinTableBytes
	}

	return preflight.ResolveConfig(safetyFactor, spec.Relations, minTableBytes, spec.MinFreeBytes)
}

// visibilitySchemaUpgradePending reports whether moving to target would run a
// migration over an existing, populated visibility schema.
//
// This mirrors the Skip predicate of the update-visibility-schema Job in
// reconcile_persistence.go, including its datastore-type branch: a plugin change
// such as postgres to postgres12 re-runs update-schema against the same
// populated table, which is exactly the rewrite this gate exists to hold. If
// that predicate changes, this must change with it, or the gate will guard a Job
// that no longer runs, or miss one that does.
func visibilitySchemaUpgradePending(cluster *v1beta1.TemporalCluster, target *version.Version) bool {
	status := cluster.Status.Persistence
	if status == nil || status.VisibilityStore == nil || target == nil {
		return false
	}

	// No recorded schema version means a fresh setup rather than an upgrade. A
	// new schema starts empty, so there is no table to copy.
	if status.VisibilityStore.SchemaVersion == nil {
		return false
	}

	// A datastore type change re-runs the update against the existing table, so
	// the Job runs and the gate must be in front of it.
	if cluster.Spec.Persistence.VisibilityStore != nil &&
		status.VisibilityStore.Type != cluster.Spec.Persistence.VisibilityStore.GetType() {
		return true
	}

	return !status.VisibilityStore.SchemaVersion.GreaterOrEqual(target)
}
