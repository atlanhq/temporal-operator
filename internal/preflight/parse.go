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
	"bufio"
	"math"
	"strconv"
	"strings"
)

// findSample scans a Prometheus text-format exposition body for the first sample
// of the named metric family whose labels are a superset of match, and returns
// its value.
//
// Hand-rolled to read two known gauge families without promoting a transitive
// module to a direct dependency. Values that cannot be a byte count (NaN, ±Inf,
// negative) are rejected rather than returned, so a malformed endpoint cannot
// produce a requirement of zero.
func findSample(body, family string, match map[string]string) (float64, bool) {
	scanner := bufio.NewScanner(strings.NewReader(body))
	// Exposition lines are short, but a large label set on a single line can
	// exceed bufio's default 64KiB limit and silently truncate the scan.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		name, labels, value, ok := splitSample(line)
		if !ok || name != family {
			continue
		}

		if !labelsMatch(labels, match) {
			continue
		}

		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			continue
		}
		if math.IsNaN(parsed) || math.IsInf(parsed, 0) || parsed < 0 {
			continue
		}

		return parsed, true
	}

	return 0, false
}

// splitSample breaks a single exposition line into its family name, raw label
// block (without braces) and value token. A trailing timestamp, which is
// optional in the text format, is discarded.
func splitSample(line string) (name, labels, value string, ok bool) {
	open := strings.IndexByte(line, '{')
	if open == -1 {
		// Unlabelled sample: "name value [timestamp]".
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return "", "", "", false
		}
		return fields[0], "", fields[1], true
	}

	close := strings.LastIndexByte(line, '}')
	if close < open {
		return "", "", "", false
	}

	rest := strings.Fields(strings.TrimSpace(line[close+1:]))
	if len(rest) == 0 {
		return "", "", "", false
	}

	return strings.TrimSpace(line[:open]), line[open+1 : close], rest[0], true
}

// labelsMatch reports whether every wanted label is present in the raw label
// block with the expected value. Labels present on the sample but absent from
// want are ignored, so callers match on identity without having to enumerate
// every label the exporter happens to attach.
func labelsMatch(raw string, want map[string]string) bool {
	if len(want) == 0 {
		return true
	}

	found := 0
	for _, pair := range splitLabelPairs(raw) {
		eq := strings.IndexByte(pair, '=')
		if eq == -1 {
			continue
		}

		key := strings.TrimSpace(pair[:eq])
		expected, wanted := want[key]
		if !wanted {
			continue
		}

		if strings.Trim(strings.TrimSpace(pair[eq+1:]), `"`) != expected {
			return false
		}
		found++
	}

	return found == len(want)
}

// splitLabelPairs splits a raw label block on commas that sit outside quoted
// values, so a label value containing a comma does not break the split.
func splitLabelPairs(raw string) []string {
	var (
		pairs   []string
		current strings.Builder
		quoted  bool
		escaped bool
	)

	for i := 0; i < len(raw); i++ {
		c := raw[i]

		switch {
		case escaped:
			escaped = false
		case c == '\\' && quoted:
			escaped = true
		case c == '"':
			quoted = !quoted
		case c == ',' && !quoted:
			pairs = append(pairs, current.String())
			current.Reset()
			continue
		}

		current.WriteByte(c)
	}

	if current.Len() > 0 {
		pairs = append(pairs, current.String())
	}

	return pairs
}
