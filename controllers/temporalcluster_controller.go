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
	"errors"
	"fmt"
	"time"

	"go.temporal.io/server/common/primitives"
	kerrors "k8s.io/apimachinery/pkg/util/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/alexandrevilain/temporal-operator/api/v1beta1"
	"github.com/alexandrevilain/temporal-operator/internal/discovery"
	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	istionetworkingv1beta1 "istio.io/client-go/pkg/apis/networking/v1beta1"
	istiosecurityv1beta1 "istio.io/client-go/pkg/apis/security/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/alexandrevilain/controller-tools/pkg/hash"
	"github.com/alexandrevilain/controller-tools/pkg/patch"
	"github.com/alexandrevilain/controller-tools/pkg/resource"
	"github.com/alexandrevilain/temporal-operator/internal/resource/admintools"
	"github.com/alexandrevilain/temporal-operator/internal/resource/base"
	"github.com/alexandrevilain/temporal-operator/internal/resource/config"
	"github.com/alexandrevilain/temporal-operator/internal/resource/mtls/certmanager"
	"github.com/alexandrevilain/temporal-operator/internal/resource/mtls/istio"
	"github.com/alexandrevilain/temporal-operator/internal/resource/prometheus"
	"github.com/alexandrevilain/temporal-operator/internal/resource/ui"
	"github.com/alexandrevilain/temporal-operator/pkg/status"
	"github.com/alexandrevilain/temporal-operator/pkg/version"
)

const (
	ownerKey = ".metadata.controller"

	// Annotations used to track multi-hop upgrade stability timing.
	// These are set on the TemporalCluster object to persist hop completion time
	// across reconcile cycles (including watch-triggered reconciles that bypass RequeueAfter).
	annotationLastHopTime    = "temporal.io/last-intermediate-hop-time"
	annotationLastHopVersion = "temporal.io/last-intermediate-hop-version"

	// annotationHopStartTime tracks when the current hop started (schema migration + deployment).
	// Used to detect stuck hops and auto-pause after hopTimeout.
	annotationHopStartTime = "temporal.io/hop-start-time"

	// annotationPauseUpgrade can be set to "true" on a TemporalCluster to pause
	// multi-hop version upgrades. The operator will skip hop advancement while
	// this annotation is present, allowing operators to inspect/fix issues mid-upgrade.
	annotationPauseUpgrade = "temporal.io/pause-upgrade"

	// annotationCurrentHopTarget tracks the version being targeted by the in-flight hop.
	// This prevents the operator from computing a new effective version on each reconcile,
	// which would skip stability waits when a hop completes across reconcile cycles.
	annotationCurrentHopTarget = "temporal.io/current-hop-target"

	// defaultHopTimeout is the maximum time a single hop can take before auto-pausing.
	// Covers schema migration + deployment rollout + readiness checks.
	defaultHopTimeout = 30 * time.Minute
)

// TemporalClusterReconciler reconciles a Cluster object.
type TemporalClusterReconciler struct {
	Base

	AvailableAPIs *discovery.AvailableAPIs
}

//+kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;delete
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;delete
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;delete
//+kubebuilder:rbac:groups="",resources=events,verbs=get;create;patch
//+kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;delete
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;delete
//+kubebuilder:rbac:groups="networking.k8s.io",resources=ingresses,verbs=get;list;watch;create;update;delete
//+kubebuilder:rbac:groups="cert-manager.io",resources=certificates;issuers,verbs=get;list;watch;create;update;delete
//+kubebuilder:rbac:groups="security.istio.io",resources=peerauthentications,verbs=get;list;watch;create;update;delete
//+kubebuilder:rbac:groups="networking.istio.io",resources=destinationrules,verbs=get;list;watch;create;update;delete
//+kubebuilder:rbac:groups="monitoring.coreos.com",resources=servicemonitors,verbs=get;list;watch;create;update;delete
//+kubebuilder:rbac:groups=temporal.io,resources=temporalclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=temporal.io,resources=temporalclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=temporal.io,resources=temporalclusters/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *TemporalClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, reterr error) {
	logger := log.FromContext(ctx)

	logger.Info("Starting reconciliation")

	cluster := &v1beta1.TemporalCluster{}
	err := r.Get(ctx, req.NamespacedName, cluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	// Check if the resource has been marked for deletion
	if !cluster.ObjectMeta.DeletionTimestamp.IsZero() {
		logger.Info("Deleting temporal cluster", "name", cluster.Name)
		return reconcile.Result{}, nil
	}

	// DEBUG: Dump full multi-hop state at reconcile entry
	{
		ann := cluster.GetAnnotations()
		schemaVer := ""
		if cluster.Status.Persistence != nil && cluster.Status.Persistence.DefaultStore != nil && cluster.Status.Persistence.DefaultStore.SchemaVersion != nil {
			schemaVer = cluster.Status.Persistence.DefaultStore.SchemaVersion.String()
		}
		svcVersions := make(map[string]string)
		svcReady := make(map[string]bool)
		for _, svc := range cluster.Status.Services {
			svcVersions[svc.Name] = svc.Version
			svcReady[svc.Name] = svc.Ready
		}
		logger.Info("DEBUG MULTIHOP: Reconcile entry state",
			"spec.version", cluster.Spec.Version.String(),
			"status.version", cluster.Status.Version,
			"schemaVersion", schemaVer,
			"serviceVersions", fmt.Sprintf("%v", svcVersions),
			"serviceReady", fmt.Sprintf("%v", svcReady),
			"ann.lastHopTime", ann[annotationLastHopTime],
			"ann.lastHopVersion", ann[annotationLastHopVersion],
			"ann.hopStartTime", ann[annotationHopStartTime],
			"ann.currentHopTarget", ann[annotationCurrentHopTarget],
			"ann.pauseUpgrade", ann[annotationPauseUpgrade],
			"generation", cluster.Generation,
			"resourceVersion", cluster.ResourceVersion,
		)
	}

	patchHelper, err := patch.NewHelper(cluster, r.Client)
	if err != nil {
		return reconcile.Result{}, err
	}

	defer func() {
		// DEBUG: Log annotation state just before patching
		{
			ann := cluster.GetAnnotations()
			logger.Info("DEBUG MULTIHOP: Pre-patch annotation state",
				"spec.version", cluster.Spec.Version.String(),
				"status.version", cluster.Status.Version,
				"ann.lastHopTime", ann[annotationLastHopTime],
				"ann.lastHopVersion", ann[annotationLastHopVersion],
				"ann.hopStartTime", ann[annotationHopStartTime],
				"ann.currentHopTarget", ann[annotationCurrentHopTarget],
				"ann.pauseUpgrade", ann[annotationPauseUpgrade],
			)
		}
		// Always attempt to Patch the Cluster object and status after each reconciliation.
		err := patchHelper.Patch(ctx, cluster)
		if err != nil {
			logger.Error(err, "DEBUG MULTIHOP: Patch failed")
			reterr = kerrors.NewAggregate([]error{reterr, err})
		} else {
			logger.Info("DEBUG MULTIHOP: Patch succeeded", "resourceVersion", cluster.ResourceVersion)
		}
	}()

	// Multi-hop stability gate: if we recently completed an intermediate hop,
	// wait for the stability duration before proceeding to the next hop.
	// This check runs BEFORE computing effectiveVersion to short-circuit
	// watch-triggered reconciles (deployment changes, job completions) that
	// would otherwise bypass the RequeueAfter timer.
	if remaining, waiting := r.remainingStabilityWait(cluster); waiting {
		logger.Info("DEBUG MULTIHOP: Stability wait gate ACTIVE - returning early",
			"lastHopVersion", cluster.GetAnnotations()[annotationLastHopVersion],
			"lastHopTime", cluster.GetAnnotations()[annotationLastHopTime],
			"remaining", remaining,
			"stabilityDuration", r.getStabilityDuration(cluster))
		return reconcile.Result{RequeueAfter: remaining}, nil
	} else {
		logger.Info("DEBUG MULTIHOP: Stability wait gate PASSED (not waiting)",
			"hasLastHopTime", cluster.GetAnnotations()[annotationLastHopTime] != "",
			"lastHopVersion", cluster.GetAnnotations()[annotationLastHopVersion],
			"stabilityDuration", r.getStabilityDuration(cluster))
	}

	targetVersion := cluster.Spec.Version

	// Determine effective version for this reconcile cycle.
	// If a hop is already in flight (recorded in annotation from a previous reconcile),
	// continue with that target instead of computing a new one. This prevents the
	// operator from skipping stability waits: without this, when a hop completes
	// across reconcile cycles, computeEffectiveVersion would return the NEXT hop
	// before the stability annotation for the completed hop is ever set.
	var effectiveVersion *version.Version
	multiHopInProgress := false

	if hopTarget := r.getCurrentHopTarget(cluster); hopTarget != "" {
		logger.Info("DEBUG MULTIHOP: Found in-flight hop target annotation",
			"hopTarget", hopTarget,
			"targetVersion", targetVersion.String())
		hopTargetVersion, parseErr := version.NewVersionFromString(hopTarget)
		if parseErr == nil && targetVersion != nil && hopTarget != targetVersion.String() {
			effectiveVersion = hopTargetVersion
			multiHopInProgress = true
			logger.Info("DEBUG MULTIHOP: Using in-flight hop target as effective version",
				"effectiveVersion", effectiveVersion.String())
		} else {
			logger.Info("DEBUG MULTIHOP: In-flight hop target NOT used",
				"parseErr", parseErr,
				"hopTargetEqualsTarget", hopTarget == targetVersion.String())
		}
	} else {
		logger.Info("DEBUG MULTIHOP: No in-flight hop target annotation found")
	}

	if !multiHopInProgress {
		currentVer := r.getCurrentVersion(cluster)
		logger.Info("DEBUG MULTIHOP: Computing effective version (no in-flight hop)",
			"currentVersion", currentVer.String(),
			"targetVersion", targetVersion.String())
		var computeErr error
		effectiveVersion, computeErr = r.computeEffectiveVersion(cluster)
		if computeErr != nil {
			logger.Error(computeErr, "Can't compute effective upgrade version")
			return r.handleErrorWithRequeue(cluster, v1beta1.ReconcileErrorReason, computeErr, 5*time.Second)
		}
		if effectiveVersion == nil {
			effectiveVersion = targetVersion
			logger.Info("DEBUG MULTIHOP: computeEffectiveVersion returned nil, using target")
		}
		multiHopInProgress = targetVersion != nil && effectiveVersion.String() != targetVersion.String()
		logger.Info("DEBUG MULTIHOP: Computed effective version",
			"effectiveVersion", effectiveVersion.String(),
			"multiHopInProgress", multiHopInProgress)
	}

	// Pause gate: if the pause-upgrade annotation is set, do not advance the
	// multi-hop upgrade. The cluster continues to reconcile its current state
	// (persistence, services, config) so pods are managed and can recover from
	// crashes — only hop advancement is blocked.
	if multiHopInProgress && r.isUpgradePaused(cluster) {
		logger.Info("DEBUG MULTIHOP: Upgrade PAUSED via annotation",
			"annotation", annotationPauseUpgrade,
			"currentVersion", r.getCurrentVersion(cluster).String(),
			"targetVersion", targetVersion.String())
		// Do NOT return early — fall through to reconcile persistence and resources
		// at the current running version so the cluster remains managed.
		multiHopInProgress = false
		effectiveVersion = r.getCurrentVersion(cluster)
		r.clearCurrentHopTarget(cluster)
	}

	if multiHopInProgress {
		logger.Info("DEBUG MULTIHOP: Multi-hop active, preparing hop",
			"current", r.getCurrentVersion(cluster).String(),
			"effectiveHop", effectiveVersion.String(),
			"target", targetVersion.String(),
			"currentAnnotations", fmt.Sprintf("%v", cluster.GetAnnotations()))

		// Record the hop target so subsequent reconciles continue with this hop
		// instead of computing a new effective version.
		r.setCurrentHopTarget(cluster, effectiveVersion)
		logger.Info("DEBUG MULTIHOP: Set current hop target annotation",
			"hopTarget", effectiveVersion.String())

		// Track hop start time for timeout detection.
		r.ensureHopStartAnnotation(cluster)

		// Check if this hop has exceeded the timeout. If so, auto-pause to prevent
		// the operator from retrying a stuck hop forever and alert operators.
		if r.isHopTimedOut(cluster) {
			logger.Error(nil, "Hop exceeded timeout, auto-pausing upgrade",
				"effectiveVersion", effectiveVersion.String(),
				"timeout", defaultHopTimeout)
			r.setAutoPause(cluster, effectiveVersion)
			v1beta1.SetTemporalClusterReconcileError(cluster, metav1.ConditionTrue,
				v1beta1.MultiHopUpgradeHopStuckReason,
				fmt.Sprintf("Hop to %s exceeded %s timeout. Upgrade auto-paused. Investigate and remove %s annotation to resume.",
					effectiveVersion.String(), defaultHopTimeout, annotationPauseUpgrade))
			return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
		}

		// IMPORTANT: Temporarily set spec.version to the intermediate hop so all builders
		// (schema migration, deployments) use the correct intermediate version.
		// Restored before the patch helper runs (defers execute LIFO).
		logger.Info("DEBUG MULTIHOP: Temporarily setting spec.version to intermediate hop",
			"from", cluster.Spec.Version.String(),
			"to", effectiveVersion.String())
		cluster.Spec.Version = effectiveVersion
		defer func() {
			logger.Info("DEBUG MULTIHOP: Restoring spec.version from intermediate to target",
				"from", effectiveVersion.String(),
				"to", targetVersion.String())
			cluster.Spec.Version = targetVersion
		}()
	}

	// Clear hop completion annotations if we're past the stability wait (they were checked above).
	// This keeps annotations clean once we've moved on to the next hop.
	logger.Info("DEBUG MULTIHOP: About to clearHopAnnotations (post-stability-gate cleanup)",
		"hasLastHopTime", cluster.GetAnnotations()[annotationLastHopTime] != "",
		"lastHopVersion", cluster.GetAnnotations()[annotationLastHopVersion])
	r.clearHopAnnotations(cluster)

	// Check the ready condition
	cond, exists := v1beta1.GetTemporalClusterReadyCondition(cluster)
	if !exists || cond.ObservedGeneration != cluster.GetGeneration() {
		v1beta1.SetTemporalClusterReady(cluster, metav1.ConditionUnknown, v1beta1.ProgressingReason, "")
	}

	logger.Info("DEBUG MULTIHOP: About to reconcile persistence",
		"spec.version", cluster.Spec.Version.String(),
		"multiHopInProgress", multiHopInProgress)
	if requeueAfter, err := r.reconcilePersistence(ctx, cluster); err != nil || requeueAfter > 0 {
		if err != nil {
			logger.Error(err, "Can't reconcile persistence")
			if requeueAfter == 0 {
				requeueAfter = 2 * time.Second
			}
			return r.handleErrorWithRequeue(cluster, v1beta1.PersistenceReconciliationFailedReason, err, requeueAfter)
		}
		if requeueAfter > 0 {
			return reconcile.Result{RequeueAfter: requeueAfter}, nil
		}
	}

	logger.Info("DEBUG MULTIHOP: About to reconcile resources",
		"spec.version", cluster.Spec.Version.String(),
		"multiHopInProgress", multiHopInProgress)
	if err := r.reconcileResources(ctx, cluster); err != nil {
		logger.Error(err, "Can't reconcile resources")
		return r.handleErrorWithRequeue(cluster, v1beta1.ResourcesReconciliationFailedReason, err, 2*time.Second)
	}

	// DEBUG: Log service status after reconcileResources
	{
		svcVersions := make(map[string]string)
		svcReady := make(map[string]bool)
		for _, svc := range cluster.Status.Services {
			svcVersions[svc.Name] = svc.Version
			svcReady[svc.Name] = svc.Ready
		}
		schemaVer := ""
		if cluster.Status.Persistence != nil && cluster.Status.Persistence.DefaultStore != nil && cluster.Status.Persistence.DefaultStore.SchemaVersion != nil {
			schemaVer = cluster.Status.Persistence.DefaultStore.SchemaVersion.String()
		}
		logger.Info("DEBUG MULTIHOP: Post-reconcileResources state",
			"status.version", cluster.Status.Version,
			"spec.version", cluster.Spec.Version.String(),
			"schemaVersion", schemaVer,
			"serviceVersions", fmt.Sprintf("%v", svcVersions),
			"serviceReady", fmt.Sprintf("%v", svcReady),
			"isClusterReady", status.IsClusterReady(cluster),
			"observedVersionMatches", status.ObservedVersionMatchesDesiredVersion(cluster),
			"multiHopInProgress", multiHopInProgress)
	}

	if multiHopInProgress {
		// Guard: do not advance to the next hop until the cluster is fully ready
		// at the current intermediate version. This prevents version gaps where
		// schema is ahead of running pods, which can cause crashes or data corruption.
		clusterReady := status.IsClusterReady(cluster)
		logger.Info("DEBUG MULTIHOP: Checking cluster readiness for hop completion",
			"isClusterReady", clusterReady,
			"effectiveVersion", effectiveVersion.String(),
			"spec.version", cluster.Spec.Version.String(),
			"status.version", cluster.Status.Version)
		if !clusterReady {
			logger.Info("DEBUG MULTIHOP: Cluster NOT ready, requeuing hop",
				"effectiveVersion", effectiveVersion.String(),
				"targetVersion", targetVersion.String())
			v1beta1.SetTemporalClusterReconcileSuccess(cluster, metav1.ConditionTrue,
				v1beta1.MultiHopUpgradeInProgressReason,
				fmt.Sprintf("Upgrading to %s (hop %s, waiting for services to be ready)",
					targetVersion.String(), effectiveVersion.String()))
			return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
		}

		// Cluster is ready at intermediate version. Record the hop completion time
		// in annotations so it persists across watch-triggered reconciles.
		// Clear the hop start and target annotations since this hop completed successfully.
		stabilityDuration := r.getStabilityDuration(cluster)
		logger.Info("DEBUG MULTIHOP: HOP COMPLETED! Setting hop completion annotation",
			"completedVersion", effectiveVersion.String(),
			"targetVersion", targetVersion.String(),
			"stabilityDuration", stabilityDuration,
			"annotationsBefore", fmt.Sprintf("%v", cluster.GetAnnotations()))
		r.setHopCompletionAnnotation(cluster, effectiveVersion)
		r.clearHopStartAnnotation(cluster)
		r.clearCurrentHopTarget(cluster)
		logger.Info("DEBUG MULTIHOP: Annotations after hop completion",
			"annotationsAfter", fmt.Sprintf("%v", cluster.GetAnnotations()))
		v1beta1.SetTemporalClusterReconcileSuccess(cluster, metav1.ConditionTrue,
			v1beta1.MultiHopUpgradeInProgressReason,
			fmt.Sprintf("Upgrading to %s (hop %s completed, stability wait %s)",
				targetVersion.String(), effectiveVersion.String(), stabilityDuration))
		return reconcile.Result{RequeueAfter: stabilityDuration}, nil
	}

	// Final version reached or single-hop upgrade.
	// Guard: wait for all services to be ready at the target version before declaring success.
	// Without this, schema may have migrated to the target version while pods are still
	// running the previous version — especially dangerous during the last hop of a multi-hop
	// upgrade where multiHopInProgress is false.
	clusterReady := status.IsClusterReady(cluster)
	logger.Info("DEBUG MULTIHOP: Final version or single-hop, checking readiness before success",
		"effectiveVersion", effectiveVersion.String(),
		"targetVersion", targetVersion.String(),
		"status.version", cluster.Status.Version,
		"isClusterReady", clusterReady)
	if !clusterReady {
		v1beta1.SetTemporalClusterReconcileSuccess(cluster, metav1.ConditionTrue,
			v1beta1.ProgressingReason,
			fmt.Sprintf("Waiting for cluster to be ready at %s", effectiveVersion.String()))
		return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
	}

	r.clearHopAnnotations(cluster)
	r.clearHopStartAnnotation(cluster)
	r.clearCurrentHopTarget(cluster)
	return r.handleSuccess(cluster)
}

func (r *TemporalClusterReconciler) reconcileResources(ctx context.Context, temporalCluster *v1beta1.TemporalCluster) error {
	// reconcile configmap first, then compute its hash.
	configMapObject, err := r.Reconciler.ReconcileBuilder(ctx,
		temporalCluster,
		config.NewConfigmapBuilder(temporalCluster, r.Scheme))
	if err != nil {
		return fmt.Errorf("can't reconcile configmap: %w", err)
	}

	configMap, ok := configMapObject.(*corev1.ConfigMap)
	if !ok {
		return errors.New("can't cast configmap object to *corev1.ConfigMap")
	}

	configHash, err := hash.Sha256(configMap.Data)
	if err != nil {
		return fmt.Errorf("can't compute configmap hash: %w", err)
	}

	builders, err := r.resourceBuilders(temporalCluster, configHash)
	if err != nil {
		return err
	}

	objects, err := r.Reconciler.ReconcileBuilders(ctx, temporalCluster, builders)
	if err != nil {
		return err
	}

	logger := log.FromContext(ctx)
	statuses, err := status.ReconciledObjectsToServiceStatuses(temporalCluster, objects)
	if err != nil {
		return err
	}

	for _, s := range statuses {
		logger.Info("DEBUG MULTIHOP: reconcileResources: service status from deployment",
			"service", s.Name,
			"version", s.Version,
			"ready", s.Ready)
		temporalCluster.Status.AddServiceStatus(s)
	}

	observedMatch := status.ObservedVersionMatchesDesiredVersion(temporalCluster)
	logger.Info("DEBUG MULTIHOP: reconcileResources: ObservedVersionMatchesDesiredVersion",
		"result", observedMatch,
		"spec.version", temporalCluster.Spec.Version.String(),
		"status.version.before", temporalCluster.Status.Version)
	if observedMatch {
		temporalCluster.Status.Version = temporalCluster.Spec.Version.String()
		logger.Info("DEBUG MULTIHOP: reconcileResources: Updated status.version",
			"newStatusVersion", temporalCluster.Status.Version)
	}

	clusterReady := status.IsClusterReady(temporalCluster)
	logger.Info("DEBUG MULTIHOP: reconcileResources: IsClusterReady",
		"result", clusterReady,
		"spec.version", temporalCluster.Spec.Version.String())
	if clusterReady {
		v1beta1.SetTemporalClusterReady(temporalCluster, metav1.ConditionTrue, v1beta1.ServicesReadyReason, "")
	} else {
		v1beta1.SetTemporalClusterReady(temporalCluster, metav1.ConditionFalse, v1beta1.ServicesNotReadyReason, "")
	}

	return nil
}

func (r *TemporalClusterReconciler) resourceBuilders(temporalCluster *v1beta1.TemporalCluster, configHash string) ([]resource.Builder, error) {
	builders := []resource.Builder{
		base.NewFrontendServiceBuilder(temporalCluster, r.Scheme),
	}

	services := []primitives.ServiceName{
		primitives.FrontendService,
		primitives.HistoryService,
		primitives.MatchingService,
		primitives.WorkerService,
		primitives.InternalFrontendService,
	}

	for _, service := range services {
		specs, err := temporalCluster.Spec.Services.GetServiceSpec(service)
		if err != nil {
			return nil, err
		}

		serviceName := string(service)

		builders = append(builders, base.NewServiceAccountBuilder(serviceName, temporalCluster, r.Scheme))
		builders = append(builders, base.NewDeploymentBuilder(serviceName, temporalCluster, r.Scheme, specs, configHash))
		builders = append(builders, base.NewHeadlessServiceBuilder(serviceName, temporalCluster, r.Scheme, specs))

		builders = append(builders, istio.NewPeerAuthenticationBuilder(serviceName, temporalCluster, r.Scheme, specs))
		builders = append(builders, istio.NewDestinationRuleBuilder(serviceName, temporalCluster, r.Scheme, specs))
		builders = append(builders, prometheus.NewServiceMonitorBuilder(serviceName, temporalCluster, r.Scheme, specs))
	}

	builders = append(builders,
		base.NewDynamicConfigmapBuilder(temporalCluster, r.Scheme),
		// mTLS
		certmanager.NewMTLSBootstrapIssuerBuilder(temporalCluster, r.Scheme),
		certmanager.NewMTLSRootCACertificateBuilder(temporalCluster, r.Scheme),
		certmanager.NewMTLSRootCAIssuerBuilder(temporalCluster, r.Scheme),
		certmanager.NewMTLSInternodeIntermediateCACertificateBuilder(temporalCluster, r.Scheme),
		certmanager.NewMTLSInternodeIntermediateCAIssuerBuilder(temporalCluster, r.Scheme),
		certmanager.NewMTLSInternodeCertificateBuilder(temporalCluster, r.Scheme),
		certmanager.NewMTLSFrontendIntermediateCACertificateBuilder(temporalCluster, r.Scheme),
		certmanager.NewMTLSFrontendIntermediateCAIssuerBuilder(temporalCluster, r.Scheme),
		certmanager.NewMTLSFrontendCertificateBuilder(temporalCluster, r.Scheme),
		certmanager.NewWorkerFrontendClientCertificateBuilder(temporalCluster, r.Scheme),
		// UI:
		ui.NewDeploymentBuilder(temporalCluster, r.Scheme, configHash),
		ui.NewServiceBuilder(temporalCluster, r.Scheme),
		ui.NewIngressBuilder(temporalCluster, r.Scheme),
		ui.NewFrontendClientCertificateBuilder(temporalCluster, r.Scheme),
		// Admin tools:
		admintools.NewDeploymentBuilder(temporalCluster, r.Scheme, configHash),
		admintools.NewFrontendClientCertificateBuilder(temporalCluster, r.Scheme),
	)

	return builders, nil
}

func (r *TemporalClusterReconciler) handleSuccess(cluster *v1beta1.TemporalCluster) (ctrl.Result, error) {
	return r.handleSuccessWithRequeue(cluster, 0)
}

func (r *TemporalClusterReconciler) handleSuccessWithRequeue(cluster *v1beta1.TemporalCluster, requeueAfter time.Duration) (ctrl.Result, error) {
	v1beta1.SetTemporalClusterReconcileSuccess(cluster, metav1.ConditionTrue, v1beta1.ReconcileSuccessReason, "")
	v1beta1.SetTemporalClusterReconcileError(cluster, metav1.ConditionFalse, v1beta1.ReconcileSuccessReason, "")
	return reconcile.Result{RequeueAfter: requeueAfter}, nil
}

func (r *TemporalClusterReconciler) handleErrorWithRequeue(cluster *v1beta1.TemporalCluster, reason string, err error, requeueAfter time.Duration) (ctrl.Result, error) {
	r.Recorder.Event(cluster, corev1.EventTypeWarning, "ProcessingError", err.Error())
	if reason == "" {
		reason = v1beta1.ReconcileErrorReason
	}
	v1beta1.SetTemporalClusterReconcileError(cluster, metav1.ConditionTrue, reason, err.Error())
	return reconcile.Result{RequeueAfter: requeueAfter}, err
}

// getCurrentVersion returns the current running version of the cluster.
// It uses status.version (the version all services are reporting) as the
// authoritative source. This ensures multi-hop upgrades do not advance to
// the next hop until the server is actually deployed and running at the
// intermediate version.
//
// Falls back to schema version ONLY when status.version is empty AND the
// schema version does not exceed status.version's minor. This prevents a
// dangerous scenario where schema migrations complete (advancing schemaVersion)
// but services haven't deployed yet — using the advanced schemaVersion would
// cause hop computation to skip ahead while pods run an older version.
func (r *TemporalClusterReconciler) getCurrentVersion(cluster *v1beta1.TemporalCluster) *version.Version {
	// Prefer status.version: only set when ObservedVersionMatchesDesiredVersion
	// returns true, meaning all service deployments report the correct version.
	if cluster.Status.Version != "" {
		v, err := version.NewVersionFromString(cluster.Status.Version)
		if err == nil {
			return v
		}
	}

	// Fallback to schema version for clusters that have completed schema migrations
	// but haven't yet had status.version set (e.g., operator restart mid-upgrade).
	// Safety: only use schema version if status.version was never set (empty).
	// If status.version IS set but unparseable, we skip this fallback to avoid
	// advancing past what services are actually running.
	if cluster.Status.Version == "" &&
		cluster.Status.Persistence != nil &&
		cluster.Status.Persistence.DefaultStore != nil &&
		cluster.Status.Persistence.DefaultStore.SchemaVersion != nil {
		return cluster.Status.Persistence.DefaultStore.SchemaVersion
	}

	// No current version known (first install), use spec version directly.
	// This is safe: on first install there's no upgrade path to compute.
	return cluster.Spec.Version
}

// getCurrentVersionDebug is like getCurrentVersion but also logs which path was taken.
func (r *TemporalClusterReconciler) getCurrentVersionDebug(ctx context.Context, cluster *v1beta1.TemporalCluster) *version.Version {
	logger := log.FromContext(ctx)
	schemaVer := ""
	if cluster.Status.Persistence != nil && cluster.Status.Persistence.DefaultStore != nil && cluster.Status.Persistence.DefaultStore.SchemaVersion != nil {
		schemaVer = cluster.Status.Persistence.DefaultStore.SchemaVersion.String()
	}

	if cluster.Status.Version != "" {
		v, err := version.NewVersionFromString(cluster.Status.Version)
		if err == nil {
			logger.Info("DEBUG MULTIHOP: getCurrentVersion chose STATUS.VERSION",
				"status.version", cluster.Status.Version,
				"schemaVersion", schemaVer,
				"spec.version", cluster.Spec.Version.String())
			return v
		}
		logger.Info("DEBUG MULTIHOP: getCurrentVersion status.version unparseable",
			"status.version", cluster.Status.Version,
			"err", err)
	}

	if cluster.Status.Version == "" &&
		cluster.Status.Persistence != nil &&
		cluster.Status.Persistence.DefaultStore != nil &&
		cluster.Status.Persistence.DefaultStore.SchemaVersion != nil {
		logger.Info("DEBUG MULTIHOP: getCurrentVersion chose SCHEMA VERSION (status.version empty)",
			"schemaVersion", schemaVer,
			"spec.version", cluster.Spec.Version.String())
		return cluster.Status.Persistence.DefaultStore.SchemaVersion
	}

	logger.Info("DEBUG MULTIHOP: getCurrentVersion chose SPEC.VERSION (no status, no schema)",
		"spec.version", cluster.Spec.Version.String())
	return cluster.Spec.Version
}

// computeEffectiveVersion returns the version to target for this reconcile cycle.
// For single-hop upgrades, this is just spec.version.
// For multi-hop upgrades, this is the next intermediate version.
func (r *TemporalClusterReconciler) computeEffectiveVersion(cluster *v1beta1.TemporalCluster) (*version.Version, error) {
	current := r.getCurrentVersion(cluster)
	target := cluster.Spec.Version

	// If user provided explicit intermediate versions, use those
	if cluster.Spec.VersionUpgrade != nil && len(cluster.Spec.VersionUpgrade.IntermediateVersions) > 0 {
		return version.NextUpgradeHopFromPath(current, target, cluster.Spec.VersionUpgrade.IntermediateVersions)
	}

	// Otherwise use the built-in registry
	return version.NextUpgradeHop(current, target)
}

// getStabilityDuration returns the configured stability duration between upgrade hops.
func (r *TemporalClusterReconciler) getStabilityDuration(cluster *v1beta1.TemporalCluster) time.Duration {
	if cluster.Spec.VersionUpgrade != nil &&
		cluster.Spec.VersionUpgrade.StabilityDuration != nil {
		return cluster.Spec.VersionUpgrade.StabilityDuration.Duration
	}
	// Default to 5 minutes even if not configured, as a safety net to ensure
	// intermediate versions are stable before proceeding to the next hop.
	return 5 * time.Minute
}

// remainingStabilityWait checks if the cluster is in a stability wait period
// from a recently completed intermediate hop. Returns the remaining duration
// and true if still waiting, or (0, false) if not.
// Uses annotations to persist timing across watch-triggered reconciles.
func (r *TemporalClusterReconciler) remainingStabilityWait(cluster *v1beta1.TemporalCluster) (time.Duration, bool) {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		return 0, false
	}

	hopTimeStr, ok := annotations[annotationLastHopTime]
	if !ok {
		return 0, false
	}

	hopTime, err := time.Parse(time.RFC3339, hopTimeStr)
	if err != nil {
		return 0, false
	}

	stabilityDuration := r.getStabilityDuration(cluster)
	if stabilityDuration == 0 {
		return 0, false
	}

	elapsed := time.Since(hopTime)
	if elapsed >= stabilityDuration {
		return 0, false
	}

	return stabilityDuration - elapsed, true
}

// remainingStabilityWaitDebug is like remainingStabilityWait but logs every branch.
func (r *TemporalClusterReconciler) remainingStabilityWaitDebug(ctx context.Context, cluster *v1beta1.TemporalCluster) (time.Duration, bool) {
	logger := log.FromContext(ctx)
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		logger.Info("DEBUG MULTIHOP: remainingStabilityWait: no annotations at all")
		return 0, false
	}

	hopTimeStr, ok := annotations[annotationLastHopTime]
	if !ok {
		logger.Info("DEBUG MULTIHOP: remainingStabilityWait: no lastHopTime annotation")
		return 0, false
	}

	hopTime, err := time.Parse(time.RFC3339, hopTimeStr)
	if err != nil {
		logger.Info("DEBUG MULTIHOP: remainingStabilityWait: lastHopTime unparseable",
			"value", hopTimeStr, "err", err)
		return 0, false
	}

	stabilityDuration := r.getStabilityDuration(cluster)
	if stabilityDuration == 0 {
		logger.Info("DEBUG MULTIHOP: remainingStabilityWait: stabilityDuration is 0, skipping wait")
		return 0, false
	}

	elapsed := time.Since(hopTime)
	if elapsed >= stabilityDuration {
		logger.Info("DEBUG MULTIHOP: remainingStabilityWait: stability period EXPIRED",
			"hopTime", hopTimeStr,
			"elapsed", elapsed,
			"stabilityDuration", stabilityDuration)
		return 0, false
	}

	remaining := stabilityDuration - elapsed
	logger.Info("DEBUG MULTIHOP: remainingStabilityWait: STILL WAITING",
		"hopTime", hopTimeStr,
		"elapsed", elapsed,
		"remaining", remaining,
		"stabilityDuration", stabilityDuration)
	return remaining, true
}

// setHopCompletionAnnotation records the completion of an intermediate hop
// so the stability timer persists across watch-triggered reconciles.
func (r *TemporalClusterReconciler) setHopCompletionAnnotation(cluster *v1beta1.TemporalCluster, v *version.Version) {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[annotationLastHopTime] = time.Now().UTC().Format(time.RFC3339)
	annotations[annotationLastHopVersion] = v.String()
	cluster.SetAnnotations(annotations)
}

// clearHopAnnotations removes the intermediate hop tracking annotations.
func (r *TemporalClusterReconciler) clearHopAnnotations(cluster *v1beta1.TemporalCluster) {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		return
	}
	hopTime, hasTime := annotations[annotationLastHopTime]
	hopVer, hasVersion := annotations[annotationLastHopVersion]
	if !hasTime && !hasVersion {
		return
	}
	// Log stack-trace-like info to track who's clearing these annotations
	fmt.Printf("DEBUG MULTIHOP: clearHopAnnotations CLEARING lastHopTime=%s lastHopVersion=%s\n", hopTime, hopVer)
	delete(annotations, annotationLastHopTime)
	delete(annotations, annotationLastHopVersion)
	cluster.SetAnnotations(annotations)
}

// isUpgradePaused returns true if the pause-upgrade annotation is set to "true".
func (r *TemporalClusterReconciler) isUpgradePaused(cluster *v1beta1.TemporalCluster) bool {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		return false
	}
	return annotations[annotationPauseUpgrade] == "true"
}

// ensureHopStartAnnotation sets the hop start time annotation if not already present.
// This tracks when the current hop began, used for timeout detection.
func (r *TemporalClusterReconciler) ensureHopStartAnnotation(cluster *v1beta1.TemporalCluster) {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	if _, ok := annotations[annotationHopStartTime]; !ok {
		annotations[annotationHopStartTime] = time.Now().UTC().Format(time.RFC3339)
		cluster.SetAnnotations(annotations)
	}
}

// clearHopStartAnnotation removes the hop start time annotation.
func (r *TemporalClusterReconciler) clearHopStartAnnotation(cluster *v1beta1.TemporalCluster) {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		return
	}
	if _, ok := annotations[annotationHopStartTime]; !ok {
		return
	}
	delete(annotations, annotationHopStartTime)
	cluster.SetAnnotations(annotations)
}

// isHopTimedOut returns true if the current hop has exceeded the timeout.
func (r *TemporalClusterReconciler) isHopTimedOut(cluster *v1beta1.TemporalCluster) bool {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		return false
	}
	startStr, ok := annotations[annotationHopStartTime]
	if !ok {
		return false
	}
	startTime, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		return false
	}
	return time.Since(startTime) >= defaultHopTimeout
}

// setAutoPause sets the pause annotation and records it was auto-triggered.
func (r *TemporalClusterReconciler) setAutoPause(cluster *v1beta1.TemporalCluster, stuckVersion *version.Version) {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[annotationPauseUpgrade] = "true"
	annotations["temporal.io/auto-paused-at-version"] = stuckVersion.String()
	cluster.SetAnnotations(annotations)
	r.Recorder.Event(cluster, corev1.EventTypeWarning, "HopTimeout",
		fmt.Sprintf("Hop to %s exceeded %s timeout, upgrade auto-paused", stuckVersion.String(), defaultHopTimeout))
}

// getCurrentHopTarget returns the in-flight hop target version from the annotation, or empty string if none.
func (r *TemporalClusterReconciler) getCurrentHopTarget(cluster *v1beta1.TemporalCluster) string {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		return ""
	}
	return annotations[annotationCurrentHopTarget]
}

// setCurrentHopTarget records the hop target version in an annotation so subsequent
// reconciles continue with the same hop instead of computing a new effective version.
func (r *TemporalClusterReconciler) setCurrentHopTarget(cluster *v1beta1.TemporalCluster, v *version.Version) {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[annotationCurrentHopTarget] = v.String()
	cluster.SetAnnotations(annotations)
}

// clearCurrentHopTarget removes the in-flight hop target annotation.
func (r *TemporalClusterReconciler) clearCurrentHopTarget(cluster *v1beta1.TemporalCluster) {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		return
	}
	if _, ok := annotations[annotationCurrentHopTarget]; !ok {
		return
	}
	delete(annotations, annotationCurrentHopTarget)
	cluster.SetAnnotations(annotations)
}

// SetupWithManager sets up the controller with the Manager.
func (r *TemporalClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	for _, resource := range []client.Object{&appsv1.Deployment{}, &corev1.ConfigMap{}, &corev1.Service{}, &corev1.ServiceAccount{}, &networkingv1.Ingress{}, &batchv1.Job{}} {
		if err := mgr.GetFieldIndexer().IndexField(context.Background(), resource, ownerKey, addResourceToIndex); err != nil {
			return err
		}
	}

	controller := ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.TemporalCluster{}, builder.WithPredicates(predicate.Or(
			predicate.GenerationChangedPredicate{},
			predicate.LabelChangedPredicate{},
			predicate.AnnotationChangedPredicate{},
		))).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&networkingv1.Ingress{}).
		Owns(&batchv1.Job{})

	if r.AvailableAPIs.CertManager {
		controller = controller.
			Owns(&certmanagerv1.Issuer{}).
			Owns(&certmanagerv1.Certificate{})

		for _, resource := range []client.Object{&certmanagerv1.Issuer{}, &certmanagerv1.Certificate{}} {
			if err := mgr.GetFieldIndexer().IndexField(context.Background(), resource, ownerKey, addCertManagerResourceToIndex); err != nil {
				return err
			}
		}
	}

	if r.AvailableAPIs.Istio {
		controller = controller.
			Owns(&istiosecurityv1beta1.PeerAuthentication{}).
			Owns(&istionetworkingv1beta1.DestinationRule{})

		for _, resource := range []client.Object{&istiosecurityv1beta1.PeerAuthentication{}, &istionetworkingv1beta1.DestinationRule{}} {
			if err := mgr.GetFieldIndexer().IndexField(context.Background(), resource, ownerKey, addIstioResourceToIndex); err != nil {
				return err
			}
		}
	}

	if r.AvailableAPIs.PrometheusOperator {
		controller = controller.Owns(&monitoringv1.ServiceMonitor{})

		for _, resource := range []client.Object{&monitoringv1.ServiceMonitor{}} {
			if err := mgr.GetFieldIndexer().IndexField(context.Background(), resource, ownerKey, addPromtheusOperatorResourceToIndex); err != nil {
				return err
			}
		}
	}

	return controller.Complete(r)
}

func addResourceToIndex(rawObj client.Object) []string {
	switch resourceObject := rawObj.(type) {
	case *appsv1.Deployment,
		*corev1.ConfigMap,
		*corev1.Service,
		*corev1.ServiceAccount,
		*networkingv1.Ingress,
		*batchv1.Job:
		owner := metav1.GetControllerOf(resourceObject)
		return validateAndGetOwner(owner)
	default:
		return nil
	}
}

func addIstioResourceToIndex(rawObj client.Object) []string {
	switch resourceObject := rawObj.(type) {
	case *istiosecurityv1beta1.PeerAuthentication,
		*istionetworkingv1beta1.DestinationRule:
		owner := metav1.GetControllerOf(resourceObject)
		return validateAndGetOwner(owner)
	default:
		return nil
	}
}

func addCertManagerResourceToIndex(rawObj client.Object) []string {
	switch resourceObject := rawObj.(type) {
	case *certmanagerv1.Issuer,
		*certmanagerv1.Certificate:
		owner := metav1.GetControllerOf(resourceObject)
		return validateAndGetOwner(owner)
	default:
		return nil
	}
}

func addPromtheusOperatorResourceToIndex(rawObj client.Object) []string {
	switch resourceObject := rawObj.(type) {
	case *monitoringv1.ServiceMonitor:
		owner := metav1.GetControllerOf(resourceObject)
		return validateAndGetOwner(owner)
	default:
		return nil
	}
}

func validateAndGetOwner(owner *metav1.OwnerReference) []string {
	if owner == nil {
		return nil
	}
	if owner.APIVersion != v1beta1.GroupVersion.String() || owner.Kind != v1beta1.TemporalClusterTypeMeta.Kind {
		return nil
	}
	return []string{owner.Name}
}
