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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// kubeletBody mirrors the shape of a kubelet /metrics response: many volumes on
// the node, only one of which belongs to the cluster under test.
const kubeletBody = `
# HELP kubelet_volume_stats_available_bytes Number of available bytes in the volume
# TYPE kubelet_volume_stats_available_bytes gauge
kubelet_volume_stats_available_bytes{namespace="kafka",persistentvolumeclaim="kafka-0"} 5.36870912e+09
kubelet_volume_stats_available_bytes{namespace="cloudnative-postgres",persistentvolumeclaim="postgres-cluster-3"} 1.1798802432e+10
kubelet_volume_stats_capacity_bytes{namespace="cloudnative-postgres",persistentvolumeclaim="postgres-cluster-3"} 2.0957446144e+10
`

// cnpgBody mirrors a CNPG instance /metrics response carrying a custom query
// series alongside the built-in ones.
const cnpgBody = `
# TYPE cnpg_backends_total gauge
cnpg_backends_total{application_name="",datname="argo",state="idle",usename="argo"} 6
cnpg_backends_total{application_name="",datname="temporal_visibility",state="idle",usename="temporal"} 15
# TYPE cnpg_temporal_visibility_table_bytes gauge
cnpg_temporal_visibility_table_bytes{relation="executions_visibility"} 1.29859584e+09
`

func TestFindSample(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		family string
		match  map[string]string
		want   float64
		wantOK bool
	}{
		{
			name:   "kubelet available bytes for the right pvc",
			body:   kubeletBody,
			family: "kubelet_volume_stats_available_bytes",
			match:  map[string]string{"namespace": "cloudnative-postgres", "persistentvolumeclaim": "postgres-cluster-3"},
			want:   11798802432,
			wantOK: true,
		},
		{
			name:   "does not bleed across volumes on the same node",
			body:   kubeletBody,
			family: "kubelet_volume_stats_available_bytes",
			match:  map[string]string{"namespace": "kafka", "persistentvolumeclaim": "kafka-0"},
			want:   5368709120,
			wantOK: true,
		},
		{
			name:   "unknown pvc is not found rather than defaulted",
			body:   kubeletBody,
			family: "kubelet_volume_stats_available_bytes",
			match:  map[string]string{"namespace": "cloudnative-postgres", "persistentvolumeclaim": "postgres-cluster-9"},
			wantOK: false,
		},
		{
			name:   "custom query table bytes",
			body:   cnpgBody,
			family: "cnpg_temporal_visibility_table_bytes",
			match:  map[string]string{"relation": "executions_visibility"},
			want:   1298595840,
			wantOK: true,
		},
		{
			name:   "absent family is not found",
			body:   cnpgBody,
			family: "cnpg_temporal_visibility_table_bytes_missing",
			wantOK: false,
		},
		{
			name:   "family present but relation label differs",
			body:   cnpgBody,
			family: "cnpg_temporal_visibility_table_bytes",
			match:  map[string]string{"relation": "executions"},
			wantOK: false,
		},
		{
			name:   "unlabelled sample",
			body:   "some_metric_bytes 4096\n",
			family: "some_metric_bytes",
			want:   4096,
			wantOK: true,
		},
		{
			name:   "trailing timestamp is discarded",
			body:   `some_metric_bytes{a="b"} 4096 1700000000000` + "\n",
			family: "some_metric_bytes",
			match:  map[string]string{"a": "b"},
			want:   4096,
			wantOK: true,
		},
		{
			name:   "label value containing a comma does not break matching",
			body:   `some_metric_bytes{note="a,b",relation="executions_visibility"} 2048` + "\n",
			family: "some_metric_bytes",
			match:  map[string]string{"relation": "executions_visibility"},
			want:   2048,
			wantOK: true,
		},
		{
			name:   "comment lines are skipped even when they mention the family",
			body:   "# HELP some_metric_bytes 999999\nsome_metric_bytes 7\n",
			family: "some_metric_bytes",
			want:   7,
			wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := findSample(tt.body, tt.family, tt.match)
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

// A value that cannot represent a byte count must be rejected, never returned.
// Returning zero here would make the requirement zero, which makes
// "free > required" trivially true and silently disables the gate.
func TestFindSampleRejectsNonByteValues(t *testing.T) {
	for _, value := range []string{"NaN", "+Inf", "-Inf", "-1", "not-a-number"} {
		t.Run(value, func(t *testing.T) {
			body := `table_bytes{relation="executions_visibility"} ` + value + "\n"
			_, ok := findSample(body, "table_bytes", map[string]string{"relation": "executions_visibility"})
			assert.False(t, ok, "value %q must not be accepted as a byte count", value)
		})
	}
}

// A single very long line must not truncate the scan and hide later samples.
func TestFindSampleHandlesLongLines(t *testing.T) {
	padding := strings.Repeat("x", 200*1024)
	body := `noise{pad="` + padding + `"} 1` + "\n" +
		`table_bytes{relation="executions_visibility"} 1298595840` + "\n"

	got, ok := findSample(body, "table_bytes", map[string]string{"relation": "executions_visibility"})
	assert.True(t, ok)
	assert.Equal(t, float64(1298595840), got)
}
