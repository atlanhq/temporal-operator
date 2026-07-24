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
package config

import (
	"testing"

	"github.com/alexandrevilain/temporal-operator/api/v1beta1"
	"github.com/alexandrevilain/temporal-operator/pkg/version"
	"github.com/stretchr/testify/assert"
)

// builderForVersion returns a ConfigmapBuilder whose cluster targets the given
// Temporal server version, enough to exercise the version-gated template helpers.
func builderForVersion(v string) *ConfigmapBuilder {
	return &ConfigmapBuilder{
		instance: &v1beta1.TemporalCluster{
			Spec: v1beta1.TemporalClusterSpec{
				Version: version.MustNewVersionFromString(v),
			},
		},
	}
}

func TestEnvPlaceholderSyntaxByVersion(t *testing.T) {
	// Pre-1.30 images render config with dockerize (.Env syntax); 1.30+ renders
	// with the server binary's embedded sprig (env function).
	assert.Equal(t, `{{ .Env.POSTGRES_PWD }}`, builderForVersion("1.29.4").envPlaceholder("POSTGRES_PWD"))
	assert.Equal(t, `{{ env "POSTGRES_PWD" }}`, builderForVersion("1.30.6").envPlaceholder("POSTGRES_PWD"))
	// Boundary: 1.30.0 itself is on the sprig side of the gate.
	assert.Equal(t, `{{ env "SERVICES" }}`, builderForVersion("1.30.0").envPlaceholder("SERVICES"))
}

func TestBroadcastAddressPlaceholderByVersion(t *testing.T) {
	assert.Equal(t, `{{ default .Env.POD_IP "0.0.0.0" }}`, builderForVersion("1.29.4").broadcastAddressPlaceholder())
	assert.Equal(t, `{{ default "0.0.0.0" (env "POD_IP") }}`, builderForVersion("1.30.6").broadcastAddressPlaceholder())
}
