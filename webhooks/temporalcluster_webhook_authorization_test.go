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

package webhooks_test

import (
	"context"
	"strings"
	"testing"

	"github.com/alexandrevilain/temporal-operator/api/v1beta1"
	"github.com/alexandrevilain/temporal-operator/internal/discovery"
	"github.com/alexandrevilain/temporal-operator/pkg/version"
	"github.com/alexandrevilain/temporal-operator/webhooks"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func clusterWithPermissionsRegex(v, regex string) *v1beta1.TemporalCluster {
	return &v1beta1.TemporalCluster{
		TypeMeta:   v1beta1.TemporalClusterTypeMeta,
		ObjectMeta: metav1.ObjectMeta{Name: "fake"},
		Spec: v1beta1.TemporalClusterSpec{
			Version: version.MustNewVersionFromString(v),
			Authorization: &v1beta1.AuthorizationSpec{
				Authorizer:       "default",
				ClaimMapper:      "default",
				PermissionsRegex: regex,
			},
		},
	}
}

func TestValidatePermissionsRegex(t *testing.T) {
	const valid = `^(?P<namespace>[a-z0-9-]+):(?P<role>read|write|admin)$`
	withoutClaimMapper := clusterWithPermissionsRegex("1.30.6", valid)
	withoutClaimMapper.Spec.Authorization.ClaimMapper = ""
	withoutAuthorizer := clusterWithPermissionsRegex("1.30.6", valid)
	withoutAuthorizer.Spec.Authorization.Authorizer = ""
	tests := map[string]struct {
		cluster     *v1beta1.TemporalCluster
		expectedErr string
	}{
		"accepted on 1.29": {
			cluster: clusterWithPermissionsRegex("1.29.0", valid),
		},
		"accepted on 1.30": {
			cluster: clusterWithPermissionsRegex("1.30.6", valid),
		},
		"unset on 1.28 is fine": {
			cluster: clusterWithPermissionsRegex("1.28.1", ""),
		},
		"refused before 1.29, which ignores it": {
			cluster:     clusterWithPermissionsRegex("1.28.1", valid),
			expectedErr: "spec.authorization.permissionsRegex",
		},
		"refused without the default claim mapper, the only one that reads it": {
			cluster:     withoutClaimMapper,
			expectedErr: "spec.authorization.claimMapper",
		},
		"refused without the default authorizer, which ignores the claims": {
			cluster:     withoutAuthorizer,
			expectedErr: "spec.authorization.authorizer",
		},
		"refused when it does not compile": {
			cluster:     clusterWithPermissionsRegex("1.30.6", `^(?P<namespace>[a-z`),
			expectedErr: "spec.authorization.permissionsRegex",
		},
		"refused without the role group": {
			cluster:     clusterWithPermissionsRegex("1.30.6", `^(?P<namespace>[a-z-]+):write$`),
			expectedErr: "role",
		},
		"refused without the namespace group": {
			cluster:     clusterWithPermissionsRegex("1.30.6", `^[a-z-]+:(?P<role>write)$`),
			expectedErr: "namespace",
		},
	}
	wh := &webhooks.TemporalClusterWebhook{AvailableAPIs: &discovery.AvailableAPIs{}}
	for name, test := range tests {
		t.Run(name, func(tt *testing.T) {
			_, err := wh.ValidateCreate(context.Background(), test.cluster)
			if test.expectedErr == "" {
				assert.NoError(tt, err)
				return
			}
			assert.ErrorContains(tt, err, test.expectedErr)
		})
	}
}

func clusterWithFrontendTLS(requireClientAuth *bool, authorization *v1beta1.AuthorizationSpec) *v1beta1.TemporalCluster {
	return &v1beta1.TemporalCluster{
		TypeMeta:   v1beta1.TemporalClusterTypeMeta,
		ObjectMeta: metav1.ObjectMeta{Name: "fake"},
		Spec: v1beta1.TemporalClusterSpec{
			Version: version.MustNewVersionFromString("1.30.6"),
			MTLS: &v1beta1.MTLSSpec{
				Provider: v1beta1.CertManagerMTLSProvider,
				Frontend: &v1beta1.FrontendMTLSSpec{Enabled: true, RequireClientAuth: requireClientAuth},
			},
			Authorization: authorization,
		},
	}
}

func TestValidateFrontendWithoutClientCertificates(t *testing.T) {
	const permissionsRegex = `^(?P<namespace>[a-z0-9-]+):(?P<role>read|write|admin)$`
	authorizerNotRequired := clusterWithFrontendTLS(ptr.To(false), nil)
	authorizerNotRequired.Spec.MTLS.Frontend.RequireAuthorizer = ptr.To(false)
	authorizerRequired := clusterWithFrontendTLS(ptr.To(false), nil)
	authorizerRequired.Spec.MTLS.Frontend.RequireAuthorizer = ptr.To(true)
	permissionsRegexWithoutAuthorizer := clusterWithFrontendTLS(ptr.To(false), &v1beta1.AuthorizationSpec{PermissionsRegex: permissionsRegex})
	permissionsRegexWithoutAuthorizer.Spec.MTLS.Frontend.RequireAuthorizer = ptr.To(false)
	tests := map[string]struct {
		cluster         *v1beta1.TemporalCluster
		expectedErr     string
		expectedWarning string
	}{
		"client certificates required, no authorization needed": {
			cluster: clusterWithFrontendTLS(nil, nil),
		},
		"refused without authorization, which makes every caller system admin": {
			cluster:     clusterWithFrontendTLS(ptr.To(false), nil),
			expectedErr: "spec.authorization: Required value",
		},
		"refused with the noop claim mapper": {
			cluster:     clusterWithFrontendTLS(ptr.To(false), &v1beta1.AuthorizationSpec{Authorizer: "default"}),
			expectedErr: "spec.authorization.claimMapper",
		},
		"accepted with the default authorizer and claim mapper, in any case": {
			cluster: clusterWithFrontendTLS(ptr.To(false), &v1beta1.AuthorizationSpec{Authorizer: "default", ClaimMapper: "Default"}),
		},
		"accepted without authorization when requireAuthorizer is false, with a warning": {
			cluster:         authorizerNotRequired,
			expectedWarning: "spec.mTLS.frontend.requireAuthorizer is false",
		},
		"refused without authorization when requireAuthorizer is true": {
			cluster:     authorizerRequired,
			expectedErr: "spec.authorization: Required value",
		},
		"refused with permissionsRegex without the default authorizer, even when requireAuthorizer is false": {
			cluster:         permissionsRegexWithoutAuthorizer,
			expectedErr:     "spec.authorization.authorizer",
			expectedWarning: "spec.mTLS.frontend.requireAuthorizer is false",
		},
	}
	wh := &webhooks.TemporalClusterWebhook{AvailableAPIs: &discovery.AvailableAPIs{CertManager: true}}
	for name, test := range tests {
		t.Run(name, func(tt *testing.T) {
			warns, err := wh.ValidateCreate(context.Background(), test.cluster)
			if test.expectedWarning == "" {
				assert.Empty(tt, warns)
			} else {
				assert.Contains(tt, strings.Join(warns, "\n"), test.expectedWarning)
			}
			if test.expectedErr == "" {
				assert.NoError(tt, err)
				return
			}
			assert.ErrorContains(tt, err, test.expectedErr)
		})
	}
}
