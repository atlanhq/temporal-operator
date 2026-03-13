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
package version_test

import (
	"testing"

	"github.com/alexandrevilain/temporal-operator/pkg/version"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNextUpgradeHop(t *testing.T) {
	tests := map[string]struct {
		current     string
		target      string
		expectedHop string
		expectError bool
	}{
		"same version": {
			current:     "1.25.2",
			target:      "1.25.2",
			expectedHop: "1.25.2",
		},
		"single hop (next minor)": {
			current:     "1.25.2",
			target:      "1.26.3",
			expectedHop: "1.26.3",
		},
		"multi-hop returns next intermediate from registry": {
			current:     "1.25.2",
			target:      "1.28.2",
			expectedHop: "1.26.3",
		},
		"multi-hop 3 minors ahead": {
			current:     "1.24.2",
			target:      "1.28.2",
			expectedHop: "1.25.2",
		},
		"current already at target minor": {
			current:     "1.28.0",
			target:      "1.28.2",
			expectedHop: "1.28.2",
		},
		"current past target": {
			current:     "1.29.0",
			target:      "1.28.2",
			expectedHop: "1.28.2",
		},
	}

	for name, test := range tests {
		t.Run(name, func(tt *testing.T) {
			current := version.MustNewVersionFromString(test.current)
			target := version.MustNewVersionFromString(test.target)

			hop, err := version.NextUpgradeHop(current, target)
			if test.expectError {
				assert.Error(tt, err)
				return
			}
			require.NoError(tt, err)
			assert.Equal(tt, test.expectedHop, hop.String())
		})
	}
}

func TestNextUpgradeHopFromPath(t *testing.T) {
	tests := map[string]struct {
		current              string
		target               string
		intermediateVersions []string
		expectedHop          string
		expectError          bool
	}{
		"uses first intermediate greater than current": {
			current:              "1.25.2",
			target:               "1.28.2",
			intermediateVersions: []string{"1.26.3", "1.27.4"},
			expectedHop:          "1.26.3",
		},
		"skips intermediates at or below current": {
			current:              "1.26.3",
			target:               "1.28.2",
			intermediateVersions: []string{"1.26.3", "1.27.4"},
			expectedHop:          "1.27.4",
		},
		"returns target when all intermediates are below current": {
			current:              "1.27.4",
			target:               "1.28.2",
			intermediateVersions: []string{"1.26.3", "1.27.4"},
			expectedHop:          "1.28.2",
		},
		"empty intermediate list with single minor hop returns target": {
			current:              "1.27.4",
			target:               "1.28.2",
			intermediateVersions: []string{},
			expectedHop:          "1.28.2",
		},
		"empty intermediate list with multi minor hop returns error": {
			current:              "1.25.2",
			target:               "1.28.2",
			intermediateVersions: []string{},
			expectError:          true,
		},
		"invalid intermediate version returns error": {
			current:              "1.25.2",
			target:               "1.28.2",
			intermediateVersions: []string{"not-a-version"},
			expectError:          true,
		},
		"rejects intermediate that skips a minor": {
			current:              "1.25.2",
			target:               "1.28.2",
			intermediateVersions: []string{"1.27.4"},
			expectError:          true,
		},
		"rejects when remaining hop to target skips a minor": {
			current:              "1.26.3",
			target:               "1.28.2",
			intermediateVersions: []string{"1.26.3"},
			expectError:          true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(tt *testing.T) {
			current := version.MustNewVersionFromString(test.current)
			target := version.MustNewVersionFromString(test.target)

			hop, err := version.NextUpgradeHopFromPath(current, target, test.intermediateVersions)
			if test.expectError {
				assert.Error(tt, err)
				return
			}
			require.NoError(tt, err)
			assert.Equal(tt, test.expectedHop, hop.String())
		})
	}
}
