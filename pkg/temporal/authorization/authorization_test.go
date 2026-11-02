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

package authorization_test

import (
	"testing"
	"time"

	"github.com/alexandrevilain/temporal-operator/api/v1beta1"
	"github.com/alexandrevilain/temporal-operator/pkg/temporal/authorization"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestToTemporalAuthorizationPassesPermissionsRegexThrough(t *testing.T) {
	result := authorization.ToTemporalAuthorization(&v1beta1.AuthorizationSpec{
		PermissionsClaimName: "aud",
		PermissionsRegex:     `^(?P<namespace>[a-z-]+):(?P<role>write)$`,
		JWTKeyProvider: v1beta1.AuthorizationSpecJWTKeyProvider{
			KeySourceURIs:   []string{"https://keys.example/jwks"},
			RefreshInterval: &metav1.Duration{Duration: time.Minute},
		},
	})

	assert.Equal(t, `^(?P<namespace>[a-z-]+):(?P<role>write)$`, result.PermissionsRegex)
	assert.Equal(t, "aud", result.PermissionsClaimName)
	assert.Equal(t, time.Minute, result.JWTKeyProvider.RefreshInterval)
}

// Temporal starts its key refresh only for a positive interval, so an unset one means load once.
func TestToTemporalAuthorizationWithoutRefreshInterval(t *testing.T) {
	result := authorization.ToTemporalAuthorization(&v1beta1.AuthorizationSpec{
		JWTKeyProvider: v1beta1.AuthorizationSpecJWTKeyProvider{
			KeySourceURIs: []string{"https://keys.example/jwks"},
		},
	})

	assert.Zero(t, result.JWTKeyProvider.RefreshInterval)
}
