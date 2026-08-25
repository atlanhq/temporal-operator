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

// Package preflight decides whether a schema migration has enough disk headroom
// to run. A rewriting ALTER TABLE holds a second copy of the table and its
// indexes at once, and because Postgres is shared, filling the volume costs the
// whole tenant its database.
//
// Every failure mode refuses. An unreadable, absent or implausible measurement
// never approves, because the permissive direction is indistinguishable from
// success until a tenant is down.
package preflight

import (
	"fmt"
	"strconv"
)

const (
	// DefaultRelation is the only table the 1.29 -> 1.30 visibility migration
	// rewrites. Configuration accepts a list so covering another table later is
	// a values change rather than a release.
	DefaultRelation = "executions_visibility"

	// DefaultSafetyFactor covers the old copy, the new copy, and the WAL the
	// rewrite generates. Two copies is the floor: WAL shares the same volume
	// because no separate WAL storage is configured, and the rewrite is fully
	// WAL-logged at the replication levels in use.
	DefaultSafetyFactor = "3"

	// Bounds on the safety factor. A factor arriving from a chart override is a
	// string, so "30" parses as readily as "3"; the range is what stands between
	// a typo and a request to grow a volume tenfold.
	//
	// The lower bound is 2.0 because two copies is the arithmetic floor: the
	// rewrite holds the old and new table at once. Anything below that approves a
	// migration with less space than it provably needs.
	MinSafetyFactor = 2.0
	MaxSafetyFactor = 6.0

	// DefaultMinFreeBytes is an absolute headroom floor, applied alongside the
	// ratio and independent of table size. The ratio alone scales with the table,
	// so a small table permits a small margin, while the rewrite also needs room
	// for WAL and for the writes still arriving during it. Ten gibibytes is the
	// threshold the shared-volume outage was remediated at.
	//
	// It may be lowered, including to zero to apply the ratio alone, because a
	// tenant on a volume smaller than the floor could otherwise never pass.
	DefaultMinFreeBytes int64 = 10 << 30

	// DefaultMinTableBytes is the plausibility floor. A live tenant's
	// executions_visibility carries index and catalog structure even when empty,
	// so a measurement below this means the query measured the wrong thing.
	DefaultMinTableBytes int64 = 1 << 20
)

// Cause names why a check could not be trusted. It is surfaced as a condition
// reason and as a metric label so "we could not measure" is never reported as
// "there is room".
type Cause string

const (
	CauseNone           Cause = ""
	CauseUnavailable    Cause = "MetricsUnavailable"
	CauseAbsent         Cause = "MetricAbsent"
	CauseZero           Cause = "MetricZero"
	CauseBelowFloor     Cause = "MetricBelowFloor"
	CauseFactorInvalid  Cause = "SafetyFactorInvalid"
	CauseTargetNotFound Cause = "PostgresTargetNotFound"
)

// Config is the resolved, validated gate configuration for one cluster.
type Config struct {
	SafetyFactor  float64
	Relations     []string
	MinTableBytes int64
	MinFreeBytes  int64
}

// Result is the outcome of one check.
type Result struct {
	// Skipped is true when the gate is disabled and no measurement was taken.
	Skipped bool
	// OK is true only when both inputs were trusted and the headroom test passed.
	OK bool

	Relation       string
	TableBytes     int64
	FreeBytes      int64
	RequiredBytes  int64
	ShortfallBytes int64

	// Cause is set when the check could not be completed. An unset Cause with
	// OK false means the measurement succeeded and there is genuinely not
	// enough room.
	Cause   Cause
	Message string
}

// Blocked reports whether the migration must not proceed.
func (r Result) Blocked() bool {
	return !r.Skipped && !r.OK
}

// InputInvalid reports whether the refusal was caused by an untrustworthy
// measurement rather than by a genuine shortfall. These are alerted separately:
// a shortfall is expected and self-resolving, a blind gate is not.
func (r Result) InputInvalid() bool {
	return r.Cause != CauseNone
}

// ResolveConfig validates raw configuration and fills in defaults.
//
// safetyFactor is a string because chart and ArgoCD parameter overrides arrive
// as strings; parsing and range-checking one string is safer than a numeric
// field that silently accepts an absurd value.
func ResolveConfig(safetyFactor string, relations []string, minTableBytes int64, minFreeBytes *int64) (Config, error) {
	cfg := Config{
		Relations:     relations,
		MinTableBytes: minTableBytes,
		MinFreeBytes:  DefaultMinFreeBytes,
	}

	// Distinguished from unset by the pointer, so an explicit zero can disable
	// the absolute floor while an omitted field still gets the default.
	if minFreeBytes != nil {
		if *minFreeBytes < 0 {
			return cfg, fmt.Errorf("minimum free bytes %d cannot be negative", *minFreeBytes)
		}
		cfg.MinFreeBytes = *minFreeBytes
	}

	if safetyFactor == "" {
		safetyFactor = DefaultSafetyFactor
	}

	factor, err := strconv.ParseFloat(safetyFactor, 64)
	if err != nil {
		return cfg, fmt.Errorf("safety factor %q is not a number", safetyFactor)
	}
	if factor < MinSafetyFactor || factor > MaxSafetyFactor {
		return cfg, fmt.Errorf("safety factor %v is outside the supported range %v-%v", factor, MinSafetyFactor, MaxSafetyFactor)
	}
	cfg.SafetyFactor = factor

	if len(cfg.Relations) == 0 {
		cfg.Relations = []string{DefaultRelation}
	}
	// The floor is a guard, so it may only be raised. Allowing it to be lowered
	// would let an operator dismiss a MetricBelowFloor refusal by making the
	// floor smaller, which defeats the check it exists to perform.
	if cfg.MinTableBytes <= 0 {
		cfg.MinTableBytes = DefaultMinTableBytes
	}
	if cfg.MinTableBytes < DefaultMinTableBytes {
		return cfg, fmt.Errorf("minimum table bytes %d is below the %d byte floor, which would weaken the plausibility check", cfg.MinTableBytes, DefaultMinTableBytes)
	}

	return cfg, nil
}

// Decide applies the headroom test to measurements already taken.
//
// tableBytes must be the total relation size, meaning heap plus indexes plus
// TOAST. Measuring the heap alone understates the requirement by more than an
// order of magnitude on this table, because it is almost entirely index, and a
// rewrite rebuilds every index.
func Decide(cfg Config, relation string, tableBytes, freeBytes int64) Result {
	result := Result{
		Relation:   relation,
		TableBytes: tableBytes,
		FreeBytes:  freeBytes,
	}

	switch {
	case tableBytes == 0:
		result.Cause = CauseZero
		result.Message = fmt.Sprintf(
			"measured %s at 0 bytes, which a live table cannot be; treating the measurement as failed rather than as unlimited headroom",
			relation)
		return result
	case tableBytes < cfg.MinTableBytes:
		result.Cause = CauseBelowFloor
		result.Message = fmt.Sprintf(
			"measured %s at %d bytes, below the %d byte plausibility floor; the query is measuring the wrong thing",
			relation, tableBytes, cfg.MinTableBytes)
		return result
	}

	// Whichever bound is larger governs. The ratio scales with the table and the
	// floor does not, so on a small table the floor is the binding one and on a
	// large table the ratio is.
	result.RequiredBytes = int64(float64(tableBytes) * cfg.SafetyFactor)
	basis := fmt.Sprintf("%d x %v", tableBytes, cfg.SafetyFactor)
	if cfg.MinFreeBytes > result.RequiredBytes {
		result.RequiredBytes = cfg.MinFreeBytes
		basis = fmt.Sprintf("an absolute minimum of %d, above %d x %v", cfg.MinFreeBytes, tableBytes, cfg.SafetyFactor)
	}

	if freeBytes >= result.RequiredBytes {
		result.OK = true
		result.Message = fmt.Sprintf(
			"%d bytes free covers the %d bytes the %s rewrite needs (%s)",
			freeBytes, result.RequiredBytes, relation, basis)
		return result
	}

	result.ShortfallBytes = result.RequiredBytes - freeBytes
	result.Message = fmt.Sprintf(
		"rewriting %s needs %d bytes (%s) but only %d are free; short by %d",
		relation, result.RequiredBytes, basis, freeBytes, result.ShortfallBytes)

	return result
}
