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
	"testing"

	"github.com/alexandrevilain/temporal-operator/api/v1beta1"
	"github.com/alexandrevilain/temporal-operator/internal/discovery"
	"github.com/alexandrevilain/temporal-operator/pkg/version"
	"github.com/alexandrevilain/temporal-operator/webhooks"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func clusterWithPermissionsRegex(v, regex string) *v1beta1.TemporalCluster {
	return &v1beta1.TemporalCluster{
		TypeMeta:   v1beta1.TemporalClusterTypeMeta,
		ObjectMeta: metav1.ObjectMeta{Name: "fake"},
		Spec: v1beta1.TemporalClusterSpec{
			Version:       version.MustNewVersionFromString(v),
			Authorization: &v1beta1.AuthorizationSpec{PermissionsRegex: regex},
		},
	}
}

func TestValidatePermissionsRegex(t *testing.T) {
	const valid = `^(?P<namespace>[a-z0-9-]+):(?P<role>read|write|admin)$`
	tests := map[string]struct {
		cluster     *v1beta1.TemporalCluster
		expectedErr string
	}{
		"accepted on 1.30": {
			cluster: clusterWithPermissionsRegex("1.30.6", valid),
		},
		"unset on 1.29 is fine": {
			cluster: clusterWithPermissionsRegex("1.29.4", ""),
		},
		"refused before 1.30, which ignores it": {
			cluster:     clusterWithPermissionsRegex("1.29.4", valid),
			expectedErr: "spec.authorization.permissionsRegex",
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
