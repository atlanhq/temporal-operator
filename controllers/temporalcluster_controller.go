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

	patchHelper, err := patch.NewHelper(cluster, r.Client)
	if err != nil {
		return reconcile.Result{}, err
	}

	defer func() {
		// Always attempt to Patch the Cluster object and status after each reconciliation.
		err := patchHelper.Patch(ctx, cluster)
		if err != nil {
			reterr = kerrors.NewAggregate([]error{reterr, err})
		}
	}()

	// Multi-hop stability gate: if we recently completed an intermediate hop,
	// wait for the stability duration before proceeding to the next hop.
	// This check runs BEFORE computing effectiveVersion to short-circuit
	// watch-triggered reconciles (deployment changes, job completions) that
	// would otherwise bypass the RequeueAfter timer.
	if remaining, waiting := r.remainingStabilityWait(cluster); waiting {
		logger.Info("In stability wait period between hops",
			"lastHopVersion", cluster.GetAnnotations()[annotationLastHopVersion],
			"remaining", remaining)
		return reconcile.Result{RequeueAfter: remaining}, nil
	}

	// Multi-hop version upgrade: compute the effective version for this reconcile cycle.
	// If the target is multiple minor versions ahead, we step through one minor at a time.
	targetVersion := cluster.Spec.Version
	effectiveVersion, err := r.computeEffectiveVersion(cluster)
	if err != nil {
		logger.Error(err, "Can't compute effective upgrade version")
		return r.handleErrorWithRequeue(cluster, v1beta1.ReconcileErrorReason, err, 5*time.Second)
	}
	if effectiveVersion == nil {
		effectiveVersion = targetVersion
	}

	multiHopInProgress := targetVersion != nil && effectiveVersion.String() != targetVersion.String()
	if multiHopInProgress {
		logger.Info("Multi-hop upgrade in progress",
			"current", r.getCurrentVersion(cluster).String(),
			"effectiveHop", effectiveVersion.String(),
			"target", targetVersion.String())
		// IMPORTANT: Temporarily set spec.version to the intermediate hop so all builders
		// (schema migration, deployments) use the correct intermediate version.
		// Restored before the patch helper runs (defers execute LIFO).
		//
		// CONTRACT: During reconciliation, do NOT:
		// - Read cluster.Spec.Version in a goroutine (it will see the intermediate version)
		// - Add defers between this block and the patch helper defer above
		// - Cache cluster.Spec.Version before this point and use it after
		cluster.Spec.Version = effectiveVersion
		defer func() {
			cluster.Spec.Version = targetVersion
		}()
	}

	// Clear hop annotations if we're past the stability wait (they were checked above).
	// This keeps annotations clean once we've moved on to the next hop.
	r.clearHopAnnotations(cluster)

	// Check the ready condition
	cond, exists := v1beta1.GetTemporalClusterReadyCondition(cluster)
	if !exists || cond.ObservedGeneration != cluster.GetGeneration() {
		v1beta1.SetTemporalClusterReady(cluster, metav1.ConditionUnknown, v1beta1.ProgressingReason, "")
	}

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

	if err := r.reconcileResources(ctx, cluster); err != nil {
		logger.Error(err, "Can't reconcile resources")
		return r.handleErrorWithRequeue(cluster, v1beta1.ResourcesReconciliationFailedReason, err, 2*time.Second)
	}

	if multiHopInProgress {
		// Guard: do not advance to the next hop until the cluster is fully ready
		// at the current intermediate version. This prevents version gaps where
		// schema is ahead of running pods, which can cause crashes or data corruption.
		if !status.IsClusterReady(cluster) {
			logger.Info("Waiting for cluster to be ready at intermediate version before next hop",
				"effectiveVersion", effectiveVersion.String(),
				"targetVersion", targetVersion.String())
			return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
		}

		// Cluster is ready at intermediate version. Record the hop completion time
		// in annotations so it persists across watch-triggered reconciles.
		stabilityDuration := r.getStabilityDuration(cluster)
		r.setHopCompletionAnnotation(cluster, effectiveVersion)
		logger.Info("Intermediate hop completed and cluster ready, starting stability wait",
			"completedVersion", effectiveVersion.String(),
			"targetVersion", targetVersion.String(),
			"stabilityDuration", stabilityDuration)
		return r.handleSuccessWithRequeue(cluster, stabilityDuration)
	}

	// Final version reached or single-hop upgrade. Clean up any leftover hop annotations.
	r.clearHopAnnotations(cluster)
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

	statuses, err := status.ReconciledObjectsToServiceStatuses(temporalCluster, objects)
	if err != nil {
		return err
	}

	for _, status := range statuses {
		temporalCluster.Status.AddServiceStatus(status)
	}

	if status.ObservedVersionMatchesDesiredVersion(temporalCluster) {
		temporalCluster.Status.Version = temporalCluster.Spec.Version.String()
	}

	if status.IsClusterReady(temporalCluster) {
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
// It uses status.version (the version all services are reporting) as the authoritative
// source. This ensures multi-hop upgrades do not advance to the next hop until the
// server is actually deployed and running at the intermediate version.
// Falls back to schema version for clusters mid-migration, then to spec.version
// for first install.
func (r *TemporalClusterReconciler) getCurrentVersion(cluster *v1beta1.TemporalCluster) *version.Version {
	// Prefer status.version: this is only set when ObservedVersionMatchesDesiredVersion
	// returns true, meaning all service deployments report the correct version.
	if cluster.Status.Version != "" {
		v, err := version.NewVersionFromString(cluster.Status.Version)
		if err == nil {
			return v
		}
	}

	// Fallback to schema version for clusters that have completed schema migrations
	// but haven't yet had status.version set (e.g., operator restart mid-upgrade).
	if cluster.Status.Persistence != nil &&
		cluster.Status.Persistence.DefaultStore != nil &&
		cluster.Status.Persistence.DefaultStore.SchemaVersion != nil {
		return cluster.Status.Persistence.DefaultStore.SchemaVersion
	}

	// No current version known (first install), use spec version directly
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
	return 0
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
	_, hasTime := annotations[annotationLastHopTime]
	_, hasVersion := annotations[annotationLastHopVersion]
	if !hasTime && !hasVersion {
		return
	}
	delete(annotations, annotationLastHopTime)
	delete(annotations, annotationLastHopVersion)
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
