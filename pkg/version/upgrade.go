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

package version

import "fmt"

// RecommendedVersions maps minor version numbers to the recommended patch version
// to use when upgrading through that minor version as an intermediate hop.
// These should be the latest stable patch release for each minor.
var RecommendedVersions = map[uint64]string{
	18: "1.18.5",
	19: "1.19.1",
	20: "1.20.4",
	21: "1.21.6",
	22: "1.22.7",
	23: "1.23.1",
	24: "1.24.2",
	25: "1.25.2",
	26: "1.26.3",
	27: "1.27.4",
	28: "1.28.2",
}

// NextUpgradeHop returns the next intermediate version to upgrade to,
// given the current running version and the final target version.
//
// If current and target are the same minor (or current >= target), returns target.
// If target is exactly one minor ahead, returns target.
// Otherwise returns the recommended version for current.Minor()+1.
func NextUpgradeHop(current, target *Version) (*Version, error) {
	if current == nil || target == nil {
		return target, nil
	}

	if current.Major() != target.Major() {
		return nil, fmt.Errorf("cross-major version upgrades are not supported (current: %s, target: %s)", current, target)
	}

	// No hop needed: same minor or current is already at/past target
	if current.Minor() >= target.Minor() {
		return target, nil
	}

	// Single hop: target is the next minor
	if target.Minor() == current.Minor()+1 {
		return target, nil
	}

	// Multi-hop: need an intermediate version for current.Minor()+1
	nextMinor := current.Minor() + 1
	return resolveIntermediateVersion(current.Major(), nextMinor)
}

// NextUpgradeHopFromPath returns the next intermediate version using the user-specified
// intermediate versions list. It finds the first version in the path that is strictly
// greater than current. If none qualify, returns target.
// The intermediateVersions list should be in ascending order.
// Returns an error if the next hop would skip more than one minor version.
func NextUpgradeHopFromPath(current, target *Version, intermediateVersions []string) (*Version, error) {
	if current == nil || target == nil {
		return target, nil
	}

	for _, vs := range intermediateVersions {
		v, err := NewVersionFromString(vs)
		if err != nil {
			return nil, fmt.Errorf("invalid intermediate version %q: %w", vs, err)
		}

		// Return the first intermediate version that is strictly greater than current
		if v.GreaterOrEqual(current) && !current.GreaterOrEqual(v) {
			if err := validateHopDistance(current, v); err != nil {
				return nil, err
			}
			return v, nil
		}
	}

	// All intermediates exhausted, hop to target
	if err := validateHopDistance(current, target); err != nil {
		return nil, err
	}
	return target, nil
}

// validateHopDistance ensures a version hop does not skip more than one minor version.
func validateHopDistance(current, next *Version) error {
	if current == nil || next == nil {
		return nil
	}
	if next.Minor() > current.Minor()+1 {
		return fmt.Errorf("intermediate versions skip minor version: hop from %s to %s skips %d minor version(s), add the missing intermediate versions", current, next, next.Minor()-current.Minor()-1)
	}
	return nil
}

// resolveIntermediateVersion returns the recommended version for the given major.minor,
// falling back to major.minor.0 if no recommendation exists.
// It checks ForbiddenBrokenReleases to avoid known-broken versions.
func resolveIntermediateVersion(major, minor uint64) (*Version, error) {
	// Check the recommended versions registry first
	if recommended, ok := RecommendedVersions[minor]; ok {
		v, err := NewVersionFromString(recommended)
		if err != nil {
			return nil, fmt.Errorf("invalid recommended version %q for minor %d: %w", recommended, minor, err)
		}
		// Verify recommended version is not broken
		for _, broken := range ForbiddenBrokenReleases {
			if v.Equal(broken.Version) {
				return nil, fmt.Errorf("recommended version %s for minor %d is marked as broken, please update RecommendedVersions", recommended, minor)
			}
		}
		return v, nil
	}

	// Fallback: try major.minor.0, then .1, .2 to avoid broken releases
	for patch := uint64(0); patch <= 2; patch++ {
		candidate := fmt.Sprintf("%d.%d.%d", major, minor, patch)
		v, err := NewVersionFromString(candidate)
		if err != nil {
			continue
		}

		isBroken := false
		for _, broken := range ForbiddenBrokenReleases {
			if v.Equal(broken.Version) {
				isBroken = true
				break
			}
		}
		if !isBroken {
			return v, nil
		}
	}

	return nil, fmt.Errorf("could not find a non-broken version for %d.%d (tried .0, .1, .2)", major, minor)
}
