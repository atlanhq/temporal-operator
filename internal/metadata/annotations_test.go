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

package metadata_test

import (
	"testing"

	"github.com/alexandrevilain/temporal-operator/internal/metadata"
	"github.com/stretchr/testify/assert"
)

func TestFilterOperatorAnnotations(t *testing.T) {
	tests := map[string]struct {
		annotations map[string]string
		expected    map[string]string
	}{
		"nil annotations returns empty map": {
			annotations: nil,
			expected:    map[string]string{},
		},
		"no operator annotations passes through unchanged": {
			annotations: map[string]string{
				"user-annotation":          "value",
				"some.other.io/annotation": "value2",
			},
			expected: map[string]string{
				"user-annotation":          "value",
				"some.other.io/annotation": "value2",
			},
		},
		"all six operator-internal annotations are stripped": {
			annotations: map[string]string{
				"temporal.io/current-hop-target":            "1.27.4",
				"temporal.io/hop-start-time":                "2026-03-18T10:00:00Z",
				"temporal.io/last-intermediate-hop-time":    "2026-03-18T09:00:00Z",
				"temporal.io/last-intermediate-hop-version": "1.26.3",
				"temporal.io/pause-upgrade":                 "true",
				"temporal.io/auto-paused-at-version":        "1.27.4",
			},
			expected: map[string]string{},
		},
		"operator annotations stripped but user annotations preserved": {
			annotations: map[string]string{
				"temporal.io/current-hop-target": "1.27.4",
				"temporal.io/hop-start-time":     "2026-03-18T10:00:00Z",
				"user-defined":                   "keep",
				"another.io/annotation":           "also-keep",
			},
			expected: map[string]string{
				"user-defined":          "keep",
				"another.io/annotation": "also-keep",
			},
		},
		"non-temporal.io annotation with similar prefix is kept": {
			annotations: map[string]string{
				"temporal.io/current-hop-target": "1.27.4",
				"not-temporal.io/something":      "keep",
				"temporalcluster/annotation":     "also-keep",
			},
			expected: map[string]string{
				"not-temporal.io/something":  "keep",
				"temporalcluster/annotation": "also-keep",
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(tt *testing.T) {
			result := metadata.FilterOperatorAnnotations(test.annotations)
			assert.Equal(tt, test.expected, result)
		})
	}
}

func TestFilterAnnotations(t *testing.T) {
	tests := map[string]struct {
		annotations map[string]string
		filterFunc  func(k, v string) bool
		expected    map[string]string
	}{
		"filter func always returns true": {
			annotations: map[string]string{
				"a": "b",
				"c": "d",
			},
			filterFunc: func(_, _ string) bool {
				return true
			},
			expected: map[string]string{
				"a": "b",
				"c": "d",
			},
		},
		"filter func always returns false": {
			annotations: map[string]string{
				"a": "b",
				"c": "d",
			},
			filterFunc: func(_, _ string) bool {
				return false
			},
			expected: map[string]string{},
		},
		"filter func filtering on keys": {
			annotations: map[string]string{
				"a": "b",
				"c": "d",
			},
			filterFunc: func(k, _ string) bool {
				return k != "a"
			},
			expected: map[string]string{
				"c": "d",
			},
		},
		"filter func filtering on values": {
			annotations: map[string]string{
				"a": "b",
				"c": "d",
			},
			filterFunc: func(_, v string) bool {
				return v != "d"
			},
			expected: map[string]string{
				"a": "b",
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(tt *testing.T) {
			result := metadata.FilterAnnotations(test.annotations, test.filterFunc)
			assert.Equal(tt, test.expected, result)
		})
	}
}
