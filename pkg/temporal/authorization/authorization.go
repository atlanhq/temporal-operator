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

package authorization

import (
	"time"

	"github.com/alexandrevilain/temporal-operator/api/v1beta1"
	"github.com/alexandrevilain/temporal-operator/pkg/temporal/config"
	temporalconfig "go.temporal.io/server/common/config"
)

// ToTemporalAuthorization transforms v1beta1.AuthorizationSpec to temporal's authorization config.
func ToTemporalAuthorization(authorization *v1beta1.AuthorizationSpec) config.Authorization {
	if authorization == nil {
		return config.Authorization{}
	}

	// Temporal refreshes keys only for a positive interval, so an unset one loads them once.
	var refreshInterval time.Duration
	if authorization.JWTKeyProvider.RefreshInterval != nil {
		refreshInterval = authorization.JWTKeyProvider.RefreshInterval.Duration
	}

	return config.Authorization{
		Authorization: temporalconfig.Authorization{
			JWTKeyProvider: temporalconfig.JWTKeyProvider{
				KeySourceURIs:   authorization.JWTKeyProvider.KeySourceURIs,
				RefreshInterval: refreshInterval,
			},
			PermissionsClaimName: authorization.PermissionsClaimName,
			Authorizer:           authorization.Authorizer,
			ClaimMapper:          authorization.ClaimMapper,
		},
		PermissionsRegex: authorization.PermissionsRegex,
	}
}
