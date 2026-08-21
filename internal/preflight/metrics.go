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
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Metric names are suffixed with the unit they carry. Both inputs to the gate
// are byte counts, and a unit mismatch between them would be wrong by a factor
// of a million in the permissive direction, so the unit travels with the name.
var (
	blocked = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "temporal_operator_schema_preflight_blocked",
		Help: "1 when a schema migration is being held back for lack of disk headroom, 0 when it may proceed.",
	}, []string{"cluster", "namespace"})

	inputInvalid = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "temporal_operator_schema_preflight_input_invalid",
		Help: "1 when the headroom check could not obtain a trustworthy measurement, labelled with the cause.",
	}, []string{"cluster", "namespace", "cause"})

	requiredBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "temporal_operator_schema_preflight_required_bytes",
		Help: "Bytes the pending schema migration needs, being the total relation size multiplied by the safety factor.",
	}, []string{"cluster", "namespace", "relation"})

	freeBytesGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "temporal_operator_schema_preflight_free_bytes",
		Help: "Bytes free on the volume backing the datastore, as reported by the kubelet.",
	}, []string{"cluster", "namespace"})

	shortfallBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "temporal_operator_schema_preflight_shortfall_bytes",
		Help: "Bytes by which the volume falls short of what the migration needs. 0 when there is enough room.",
	}, []string{"cluster", "namespace", "relation"})
)

func init() {
	metrics.Registry.MustRegister(blocked, inputInvalid, requiredBytes, freeBytesGauge, shortfallBytes)
}

// Publish records the outcome of a check.
//
// Every series is written on every reconcile, including the zero values, so a
// gate that stops blocking visibly reports zero rather than going stale at its
// last non-zero reading. An alert built on a metric that simply disappears
// cannot tell recovery from a crashed controller.
func Publish(cluster, namespace string, result Result) {
	if result.Skipped {
		return
	}

	labels := prometheus.Labels{"cluster": cluster, "namespace": namespace}

	blocked.With(labels).Set(boolToFloat(result.Blocked()))
	freeBytesGauge.With(labels).Set(float64(result.FreeBytes))

	// The relation is part of the label set, and an early refusal reports an empty
	// one. Clear the cluster's previous series first, or a shortfall recorded
	// against a named relation stays pinned at its last value while later
	// refusals write a separate empty-relation series, and a shortfall alert
	// never resolves.
	requiredBytes.DeletePartialMatch(labels)
	shortfallBytes.DeletePartialMatch(labels)

	relationLabels := prometheus.Labels{"cluster": cluster, "namespace": namespace, "relation": result.Relation}
	requiredBytes.With(relationLabels).Set(float64(result.RequiredBytes))
	shortfallBytes.With(relationLabels).Set(float64(result.ShortfallBytes))

	// The cause is a label, so clear the previous cause before setting the
	// current one. Otherwise a cluster that moves from one failure to another
	// reports both at once.
	inputInvalid.DeletePartialMatch(labels)
	if result.InputInvalid() {
		causeLabels := prometheus.Labels{"cluster": cluster, "namespace": namespace, "cause": string(result.Cause)}
		inputInvalid.With(causeLabels).Set(1)
	}
}

// Forget drops every series for a cluster, so a deleted TemporalCluster does not
// leave a permanently blocked gauge behind for an alert to fire on.
func Forget(cluster, namespace string) {
	labels := prometheus.Labels{"cluster": cluster, "namespace": namespace}

	blocked.DeletePartialMatch(labels)
	inputInvalid.DeletePartialMatch(labels)
	requiredBytes.DeletePartialMatch(labels)
	freeBytesGauge.DeletePartialMatch(labels)
	shortfallBytes.DeletePartialMatch(labels)
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
