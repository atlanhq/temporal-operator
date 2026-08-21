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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	ctdiscovery "github.com/alexandrevilain/controller-tools/pkg/discovery"
	"github.com/alexandrevilain/temporal-operator/api/v1beta1"
	"github.com/alexandrevilain/temporal-operator/internal/discovery"
	"github.com/alexandrevilain/temporal-operator/internal/preflight"
	"github.com/alexandrevilain/temporal-operator/pkg/version"
	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	istionetworkingv1beta1 "istio.io/client-go/pkg/apis/networking/v1beta1"
	istiosecurityv1beta1 "istio.io/client-go/pkg/apis/security/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
)

// These run against a real API server, so the CRD schema, the object writes and
// the absence or presence of the schema Job are all real. Only the two
// measurements are stubbed: there is no kubelet to ask and no database to read.

// stubChecker returns a canned result and counts how often it was consulted.
type stubChecker struct {
	result preflight.Result
	calls  int
}

func (s *stubChecker) Check(context.Context, preflight.Config, preflight.Target) preflight.Result {
	s.calls++
	return s.result
}

func shortfallResult() preflight.Result {
	return preflight.Result{
		Relation:       preflight.DefaultRelation,
		TableBytes:     1298595840,
		FreeBytes:      1500000000,
		RequiredBytes:  3895787520,
		ShortfallBytes: 2395787520,
		Message:        "rewriting executions_visibility needs 3895787520 bytes but only 1500000000 are free",
	}
}

func passingResult() preflight.Result {
	return preflight.Result{
		OK:            true,
		Relation:      preflight.DefaultRelation,
		TableBytes:    1298595840,
		FreeBytes:     11798802432,
		RequiredBytes: 3895787520,
		Message:       "11798802432 bytes free covers the 3895787520 bytes the rewrite needs",
	}
}

func inputInvalidResult(cause preflight.Cause) preflight.Result {
	return preflight.Result{
		Cause:   cause,
		Message: fmt.Sprintf("cannot measure headroom: %s", cause),
	}
}

// newIntegrationReconciler wires a reconciler against the envtest API server.
func newIntegrationReconciler(t *testing.T, checker SchemaHeadroomChecker) *TemporalClusterReconciler {
	t.Helper()

	// The shared reconciler consults discovery to decide which optional resource
	// kinds exist, so it needs a real manager rather than a nil one.
	discoveryMgr, err := ctdiscovery.NewManager(cfg, scheme.Scheme)
	require.NoError(t, err)

	return &TemporalClusterReconciler{
		Base:          New(k8sClient, scheme.Scheme, record.NewFakeRecorder(64), discoveryMgr),
		AvailableAPIs: &discovery.AvailableAPIs{},
		Preflight:     checker,
	}
}

// upgradingCluster is a cluster mid-upgrade: its visibility schema is behind the
// version the spec asks for, which is when a rewriting migration would run.
func upgradingCluster(name, namespace string, enabled bool) *v1beta1.TemporalCluster {
	factor := "3"
	cluster := &v1beta1.TemporalCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1beta1.TemporalClusterSpec{
			Version:          version.MustNewVersionFromString("1.30.6"),
			NumHistoryShards: 1,
			Persistence: v1beta1.TemporalPersistenceSpec{
				DefaultStore: &v1beta1.DatastoreSpec{
					SQL: &v1beta1.SQLSpec{
						User:            "temporal",
						PluginName:      "postgres12",
						DatabaseName:    "temporal",
						ConnectAddr:     "postgres:5432",
						ConnectProtocol: "tcp",
					},
					PasswordSecretRef: &v1beta1.SecretKeyReference{Name: "pg", Key: "PASSWORD"},
				},
				VisibilityStore: &v1beta1.DatastoreSpec{
					SQL: &v1beta1.SQLSpec{
						User:            "temporal",
						PluginName:      "postgres12",
						DatabaseName:    "temporal_visibility",
						ConnectAddr:     "postgres:5432",
						ConnectProtocol: "tcp",
					},
					PasswordSecretRef: &v1beta1.SecretKeyReference{Name: "pg", Key: "PASSWORD"},
				},
				Preflight: &v1beta1.SchemaPreflightSpec{
					Enabled:      &enabled,
					SafetyFactor: &factor,
					Relations:    []string{preflight.DefaultRelation},
					PostgresCluster: &v1beta1.PostgresClusterRef{
						Namespace: "cloudnative-postgres",
						Name:      "postgres-cluster",
					},
				},
			},
		},
	}

	// In a live cluster the mutating webhook defaults the spec before the
	// controller ever sees the object. envtest runs no webhook, so apply the same
	// defaulting here; without it the resource builders dereference nil sub-specs.
	cluster.Default()

	// The CRD requires status.conditions to be present.
	cluster.Status.Conditions = []metav1.Condition{}

	return cluster
}

// createUpgradingCluster persists the cluster and marks its visibility schema as
// behind the target, which is what makes a migration pending.
func createUpgradingCluster(t *testing.T, ctx context.Context, name string, enabled bool) *v1beta1.TemporalCluster {
	t.Helper()

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	require.NoError(t, client.IgnoreAlreadyExists(k8sClient.Create(ctx, ns)))

	cluster := upgradingCluster(name, name, enabled)
	require.NoError(t, k8sClient.Create(ctx, cluster), "the CRD must accept the preflight spec")

	// Round-trip through the API server so the test asserts on what the schema
	// actually stored, not on what was sent.
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: name}, cluster))

	cluster.Status.Persistence = &v1beta1.TemporalPersistenceStatus{
		DefaultStore: &v1beta1.DatastoreStatus{
			Created: true, Setup: true,
			SchemaVersion: version.MustNewVersionFromString("1.29.4"),
			Type:          cluster.Spec.Persistence.DefaultStore.GetType(),
		},
		VisibilityStore: &v1beta1.DatastoreStatus{
			Created: true, Setup: true,
			SchemaVersion: version.MustNewVersionFromString("1.29.4"),
			Type:          cluster.Spec.Persistence.VisibilityStore.GetType(),
		},
	}
	cluster.Status.Version = "1.29.4"
	// The API server returns an empty conditions list as nil, and the schema
	// requires the field to be present on write.
	if cluster.Status.Conditions == nil {
		cluster.Status.Conditions = []metav1.Condition{}
	}
	require.NoError(t, k8sClient.Status().Update(ctx, cluster))

	t.Cleanup(func() {
		_ = k8sClient.Delete(context.Background(), cluster)
	})

	return cluster
}

func reconcileOnce(t *testing.T, ctx context.Context, r *TemporalClusterReconciler, name string) ctrl.Result {
	t.Helper()

	result, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: name, Namespace: name},
	})
	require.NoError(t, err)

	return result
}

func schemaJobs(t *testing.T, ctx context.Context, namespace string) []batchv1.Job {
	t.Helper()

	jobs := &batchv1.JobList{}
	require.NoError(t, k8sClient.List(ctx, jobs, client.InNamespace(namespace)))

	return jobs.Items
}

func reload(t *testing.T, ctx context.Context, name string) *v1beta1.TemporalCluster {
	t.Helper()

	cluster := &v1beta1.TemporalCluster{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: name}, cluster))

	return cluster
}

// The CRD must round-trip the whole preflight spec. If the schema were missing
// or wrong, the API server would prune these fields and the gate would report
// nothing while appearing configured.
func TestIntegrationCRDStoresPreflightSpec(t *testing.T) {
	ctx := context.Background()
	createUpgradingCluster(t, ctx, "preflight-crd", true)

	stored := reload(t, ctx, "preflight-crd").Spec.Persistence.Preflight
	require.NotNil(t, stored, "the CRD pruned the preflight spec")

	assert.True(t, *stored.Enabled)
	assert.Equal(t, "3", *stored.SafetyFactor)
	assert.Equal(t, []string{preflight.DefaultRelation}, stored.Relations)
	require.NotNil(t, stored.PostgresCluster)
	assert.Equal(t, "postgres-cluster", stored.PostgresCluster.Name)
}

// No schema Job may exist while the volume lacks room. This is the behaviour the
// whole change exists for, and it can only be asserted against a real API server.
func TestIntegrationHeldMigrationCreatesNoSchemaJob(t *testing.T) {
	ctx := context.Background()
	createUpgradingCluster(t, ctx, "preflight-held", true)

	checker := &stubChecker{result: shortfallResult()}
	r := newIntegrationReconciler(t, checker)

	reconcileOnce(t, ctx, r, "preflight-held")

	assert.Positive(t, checker.calls, "the gate must have been consulted")
	assert.Empty(t, schemaJobs(t, ctx, "preflight-held"),
		"a held migration must not create any Job")

	cluster := reload(t, ctx, "preflight-held")
	assert.True(t, isSchemaPreflightBlocked(cluster), "the hold must be recorded on the cluster")
	assert.NotContains(t, cluster.GetAnnotations(), annotationPauseUpgrade,
		"a hold must never auto-pause the upgrade")
}

// A hold must not advance any version status, because downstream version logic
// trusts those and would treat the migration as already done.
func TestIntegrationHoldDoesNotAdvanceVersionStatus(t *testing.T) {
	ctx := context.Background()
	createUpgradingCluster(t, ctx, "preflight-status", true)

	r := newIntegrationReconciler(t, &stubChecker{result: shortfallResult()})

	for i := 0; i < 3; i++ {
		reconcileOnce(t, ctx, r, "preflight-status")
	}

	cluster := reload(t, ctx, "preflight-status")
	assert.Equal(t, "1.29.4", cluster.Status.Version)
	assert.Equal(t, "1.29.4",
		cluster.Status.Persistence.VisibilityStore.SchemaVersion.String())
}

// Every way the check can go blind must hold the migration, because each would
// otherwise present as "there is room".
func TestIntegrationEveryBlindCauseHolds(t *testing.T) {
	causes := []preflight.Cause{
		preflight.CauseAbsent,
		preflight.CauseZero,
		preflight.CauseBelowFloor,
		preflight.CauseUnavailable,
		preflight.CauseTargetNotFound,
		preflight.CauseFactorInvalid,
	}

	for _, cause := range causes {
		t.Run(string(cause), func(t *testing.T) {
			ctx := context.Background()
			// Kubernetes names must be lowercase RFC 1123 labels.
			name := "preflight-blind-" + strings.ToLower(string(cause))
			createUpgradingCluster(t, ctx, name, true)

			r := newIntegrationReconciler(t, &stubChecker{result: inputInvalidResult(cause)})
			reconcileOnce(t, ctx, r, name)

			assert.Empty(t, schemaJobs(t, ctx, name),
				"an unmeasurable volume must not get a migration")

			cluster := reload(t, ctx, name)
			assert.True(t, isSchemaPreflightBlocked(cluster))

			condition := reconcileErrorCondition(cluster)
			require.NotNil(t, condition)
			assert.Equal(t, v1beta1.SchemaPreflightInputInvalidReason, condition.Reason,
				"a blind gate is reported separately from a genuine shortfall")
		})
	}
}

// A genuine shortfall carries the blocked reason rather than the invalid-input
// one, since the two are alerted with different urgency.
func TestIntegrationShortfallUsesBlockedReason(t *testing.T) {
	ctx := context.Background()
	createUpgradingCluster(t, ctx, "preflight-reason", true)

	r := newIntegrationReconciler(t, &stubChecker{result: shortfallResult()})
	reconcileOnce(t, ctx, r, "preflight-reason")

	condition := reconcileErrorCondition(reload(t, ctx, "preflight-reason"))
	require.NotNil(t, condition)
	assert.Equal(t, v1beta1.SchemaPreflightBlockedReason, condition.Reason)
	assert.Contains(t, condition.Message, "3895787520",
		"the condition must carry the arithmetic so triage needs no second lookup")
}

// The hold must clear itself once the volume grows: the operator re-measures each
// reconcile, so nobody has to re-merge or clear an annotation by hand.
func TestIntegrationHoldSelfClearsWhenRoomAppears(t *testing.T) {
	ctx := context.Background()
	createUpgradingCluster(t, ctx, "preflight-clears", true)

	checker := &stubChecker{result: shortfallResult()}
	r := newIntegrationReconciler(t, checker)

	reconcileOnce(t, ctx, r, "preflight-clears")
	require.True(t, isSchemaPreflightBlocked(reload(t, ctx, "preflight-clears")))

	// The volume grew.
	checker.result = passingResult()
	reconcileOnce(t, ctx, r, "preflight-clears")

	assert.False(t, isSchemaPreflightBlocked(reload(t, ctx, "preflight-clears")),
		"the hold must retire on its own once the measurement passes")
}

// A disabled gate must not consult the checker or hold anything. This is the
// shipped default, so it is the path most clusters take.
func TestIntegrationDisabledGateIsInert(t *testing.T) {
	ctx := context.Background()
	createUpgradingCluster(t, ctx, "preflight-off", false)

	checker := &stubChecker{result: shortfallResult()}
	r := newIntegrationReconciler(t, checker)

	reconcileOnce(t, ctx, r, "preflight-off")

	assert.Zero(t, checker.calls, "a disabled gate must not measure")
	assert.False(t, isSchemaPreflightBlocked(reload(t, ctx, "preflight-off")))
}

// A cluster already at its target has no rewrite pending, so the gate must not
// engage even when the volume is short. Otherwise a tenant low on disk would be
// held forever with nothing to wait for.
func TestIntegrationNoUpgradePendingIsNotHeld(t *testing.T) {
	ctx := context.Background()
	cluster := createUpgradingCluster(t, ctx, "preflight-current", true)

	cluster = reload(t, ctx, "preflight-current")
	cluster.Status.Persistence.VisibilityStore.SchemaVersion =
		version.MustNewVersionFromString("1.30.6")
	require.NoError(t, k8sClient.Status().Update(ctx, cluster))

	checker := &stubChecker{result: shortfallResult()}
	r := newIntegrationReconciler(t, checker)

	reconcileOnce(t, ctx, r, "preflight-current")

	assert.Zero(t, checker.calls,
		"with no migration pending there is nothing to measure")
	assert.False(t, isSchemaPreflightBlocked(reload(t, ctx, "preflight-current")))
}

// A hold lasts until someone grows the volume, so the operator must keep
// reconciling the cluster's own resources throughout. Returning early instead
// would leave a tenant unmanaged for hours.
func TestIntegrationClusterStaysManagedWhileHeld(t *testing.T) {
	ctx := context.Background()
	createUpgradingCluster(t, ctx, "preflight-managed", true)

	r := newIntegrationReconciler(t, &stubChecker{result: shortfallResult()})
	reconcileOnce(t, ctx, r, "preflight-managed")

	// The reconcile proceeded past persistence into resources, which is what
	// keeps a held cluster managed. Config is the first thing built for a
	// cluster, so its presence is the marker that the reconcile did not stop.
	configMaps := &corev1.ConfigMapList{}
	require.NoError(t, k8sClient.List(ctx, configMaps, client.InNamespace("preflight-managed")))

	assert.NotEmpty(t, configMaps.Items,
		"a held cluster must still have its resources reconciled")
}

// A held cluster must be requeued, or it would only re-measure when something
// else happened to trigger a reconcile.
func TestIntegrationHoldRequeues(t *testing.T) {
	ctx := context.Background()
	createUpgradingCluster(t, ctx, "preflight-requeue", true)

	r := newIntegrationReconciler(t, &stubChecker{result: shortfallResult()})
	result := reconcileOnce(t, ctx, r, "preflight-requeue")

	assert.True(t, result.RequeueAfter > 0 || result.Requeue,
		"a held cluster must come back to re-measure on its own")
	assert.Less(t, result.RequeueAfter, 10*time.Minute,
		"the retry interval should notice a grown volume promptly")
}

func reconcileErrorCondition(cluster *v1beta1.TemporalCluster) *metav1.Condition {
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == v1beta1.ReconcileErrorCondition {
			return &cluster.Status.Conditions[i]
		}
	}

	return nil
}

// TestMain starts one API server for the package's plain Go tests. The Ginkgo
// suite in suite_test.go reuses it rather than starting a second.
func TestMain(m *testing.M) {
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}

	restConfig, err := env.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot start the test environment: %v\n", err)
		os.Exit(1)
	}

	// Register everything main.go does. The optional kinds have no CRDs installed
	// here, so discovery reports them unsupported and their builders are skipped,
	// but the scheme still has to know the types to build them at all.
	for _, add := range []func(*runtime.Scheme) error{
		v1beta1.AddToScheme,
		certmanagerv1.AddToScheme,
		istiosecurityv1beta1.AddToScheme,
		istionetworkingv1beta1.AddToScheme,
		monitoringv1.AddToScheme,
	} {
		if err := add(scheme.Scheme); err != nil {
			fmt.Fprintf(os.Stderr, "cannot register the API types: %v\n", err)
			os.Exit(1)
		}
	}

	cfg = restConfig
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot build a client: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	if err := env.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "cannot stop the test environment: %v\n", err)
	}

	os.Exit(code)
}
