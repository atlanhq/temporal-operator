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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveConfigDefaults(t *testing.T) {
	cfg, err := ResolveConfig("", nil, 0)
	require.NoError(t, err)

	assert.Equal(t, float64(3), cfg.SafetyFactor)
	assert.Equal(t, []string{DefaultRelation}, cfg.Relations)
	assert.Equal(t, DefaultMinTableBytes, cfg.MinTableBytes)
}

// A factor arriving from a chart or ArgoCD override is a string. Both "3" and
// "30" parse; only the range check distinguishes a value from a typo.
func TestResolveConfigRejectsOutOfRangeFactors(t *testing.T) {
	for _, factor := range []string{"30", "0", "1", "1.4", "1.5", "1.9", "6.1", "100", "-3", "three", "3,5"} {
		t.Run(factor, func(t *testing.T) {
			_, err := ResolveConfig(factor, nil, 0)
			assert.Error(t, err, "factor %q must be rejected", factor)
		})
	}
}

func TestResolveConfigAcceptsInRangeFactors(t *testing.T) {
	for _, factor := range []string{"2", "2.5", "3", "4.75", "6"} {
		t.Run(factor, func(t *testing.T) {
			cfg, err := ResolveConfig(factor, nil, 0)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, cfg.SafetyFactor, MinSafetyFactor)
			assert.LessOrEqual(t, cfg.SafetyFactor, MaxSafetyFactor)
		})
	}
}

func TestDecide(t *testing.T) {
	cfg, err := ResolveConfig("3", nil, 0)
	require.NoError(t, err)

	tests := []struct {
		name          string
		tableBytes    int64
		freeBytes     int64
		wantOK        bool
		wantCause     Cause
		wantShortfall int64
	}{
		{
			// A healthy shape: a 1.21 GiB table against 10.99 GiB free.
			name:       "comfortable margin passes",
			tableBytes: 1298595840,
			freeBytes:  11798802432,
			wantOK:     true,
		},
		{
			name:       "exactly enough passes",
			tableBytes: 1_000_000_000,
			freeBytes:  3_000_000_000,
			wantOK:     true,
		},
		{
			name:          "one byte short blocks",
			tableBytes:    1_000_000_000,
			freeBytes:     2_999_999_999,
			wantOK:        false,
			wantShortfall: 1,
		},
		{
			// The dangerous shape: the rewrite needs more than the volume has free.
			name:          "genuine shortfall reports the exact gap",
			tableBytes:    1298595840,
			freeBytes:     1500000000,
			wantOK:        false,
			wantShortfall: 2395787520,
		},
		{
			name:       "zero table size blocks rather than passing",
			tableBytes: 0,
			freeBytes:  11798802432,
			wantOK:     false,
			wantCause:  CauseZero,
		},
		{
			name:       "implausibly small table blocks",
			tableBytes: 4096,
			freeBytes:  11798802432,
			wantOK:     false,
			wantCause:  CauseBelowFloor,
		},
		{
			name:          "no free space at all blocks",
			tableBytes:    1298595840,
			freeBytes:     0,
			wantOK:        false,
			wantShortfall: 3895787520,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Decide(cfg, DefaultRelation, tt.tableBytes, tt.freeBytes)

			assert.Equal(t, tt.wantOK, got.OK)
			assert.Equal(t, tt.wantCause, got.Cause)
			assert.Equal(t, tt.wantShortfall, got.ShortfallBytes)
			assert.NotEmpty(t, got.Message, "every outcome must explain itself")
		})
	}
}

// The single most dangerous failure: a missing or zero measurement makes the
// requirement zero, which makes "free >= required" trivially true. The gate
// would report healthy on every tenant while protecting none of them.
func TestZeroTableBytesNeverApproves(t *testing.T) {
	cfg, err := ResolveConfig("3", nil, 0)
	require.NoError(t, err)

	for _, freeBytes := range []int64{0, 1, 1 << 30, 1 << 40} {
		result := Decide(cfg, DefaultRelation, 0, freeBytes)

		assert.False(t, result.OK, "a zero measurement must never approve")
		assert.True(t, result.Blocked())
		assert.True(t, result.InputInvalid(), "a zero measurement is an input failure, not a shortfall")
		assert.Equal(t, CauseZero, result.Cause)
	}
}

// A genuine shortfall and a blind gate are alerted differently, so the result
// must distinguish them.
func TestShortfallIsNotAnInputFailure(t *testing.T) {
	cfg, err := ResolveConfig("3", nil, 0)
	require.NoError(t, err)

	result := Decide(cfg, DefaultRelation, 1298595840, 1000)

	assert.True(t, result.Blocked())
	assert.False(t, result.InputInvalid())
	assert.Equal(t, CauseNone, result.Cause)
	assert.Positive(t, result.ShortfallBytes)
}

// Required bytes must scale with the configured factor, since the factor is the
// knob operators tune per tenant.
func TestRequiredScalesWithFactor(t *testing.T) {
	const tableBytes int64 = 1_000_000_000

	two, err := ResolveConfig("2", nil, 0)
	require.NoError(t, err)
	three, err := ResolveConfig("3", nil, 0)
	require.NoError(t, err)

	assert.Equal(t, int64(2_000_000_000), Decide(two, DefaultRelation, tableBytes, 0).RequiredBytes)
	assert.Equal(t, int64(3_000_000_000), Decide(three, DefaultRelation, tableBytes, 0).RequiredBytes)
}

// The plausibility floor is a guard, so it may only be raised. Allowing it to be
// lowered would let a MetricBelowFloor refusal be dismissed by shrinking the
// floor, which is the same fail-open hole the floor exists to close.
func TestResolveConfigRejectsAWeakenedFloor(t *testing.T) {
	for _, floor := range []int64{1, 4096, DefaultMinTableBytes - 1} {
		_, err := ResolveConfig("3", nil, floor)
		assert.Error(t, err, "floor %d is below the default and must be rejected", floor)
	}

	cfg, err := ResolveConfig("3", nil, DefaultMinTableBytes*4)
	require.NoError(t, err, "raising the floor is allowed")
	assert.Equal(t, DefaultMinTableBytes*4, cfg.MinTableBytes)
}
