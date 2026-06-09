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

package metadata

// operatorInternalAnnotations are managed exclusively by the temporal-operator
// for upgrade state tracking. They must not propagate to child resources
// (Deployments, Pod templates) to prevent spurious diffs and rolling restarts.
var operatorInternalAnnotations = map[string]bool{
	"temporal.io/current-hop-target":             true,
	"temporal.io/hop-start-time":                 true,
	"temporal.io/last-intermediate-hop-time":     true,
	"temporal.io/last-intermediate-hop-version":  true,
	"temporal.io/pause-upgrade":                  true,
	"temporal.io/auto-paused-at-version":         true,
}

// GetAnnotations returns service annotations.
func GetAnnotations(_ string, annotations ...map[string]string) map[string]string {
	return Merge(annotations...)
}

// FilterAnnotations filters the provided annotations using fn(k,v).
// Maps elements are kept if fn returns true.
func FilterAnnotations(annotations map[string]string, fn func(k, v string) bool) map[string]string {
	result := make(map[string]string)
	for k, v := range annotations {
		if fn(k, v) {
			result[k] = v
		}
	}
	return result
}

// FilterOperatorAnnotations removes operator-internal state-tracking annotations
// so they are not propagated to child resources (Deployments, Pod templates).
func FilterOperatorAnnotations(annotations map[string]string) map[string]string {
	return FilterAnnotations(annotations, func(k, _ string) bool {
		return !operatorInternalAnnotations[k]
	})
}
