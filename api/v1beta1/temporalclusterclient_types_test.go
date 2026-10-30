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

package v1beta1_test

import (
	"testing"

	"github.com/alexandrevilain/temporal-operator/api/v1beta1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func clusterWithMTLS(internode, frontend bool) *v1beta1.TemporalCluster {
	return &v1beta1.TemporalCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "temporal", Namespace: "temporal"},
		Spec: v1beta1.TemporalClusterSpec{
			MTLS: &v1beta1.MTLSSpec{
				Provider:  v1beta1.CertManagerMTLSProvider,
				Internode: &v1beta1.InternodeMTLSSpec{Enabled: internode},
				Frontend:  &v1beta1.FrontendMTLSSpec{Enabled: frontend},
			},
		},
	}
}

func TestClientCertificateServerName(t *testing.T) {
	tests := map[string]struct {
		cluster    *v1beta1.TemporalCluster
		authority  v1beta1.ClientCertificateAuthority
		serverName string
		err        string
	}{
		"frontend by default": {
			cluster:    clusterWithMTLS(false, true),
			serverName: "temporal-frontend.temporal.svc.cluster.local",
		},
		"internode with only internode mTLS": {
			cluster:    clusterWithMTLS(true, false),
			authority:  v1beta1.InternodeCertificateAuthority,
			serverName: "temporal-internode.temporal.svc.cluster.local",
		},
		"frontend needs frontend mTLS": {
			cluster: clusterWithMTLS(true, false),
			err:     "frontend",
		},
		"internode needs internode mTLS": {
			cluster:   clusterWithMTLS(false, true),
			authority: v1beta1.InternodeCertificateAuthority,
			err:       "internode",
		},
		"either needs cert-manager": {
			cluster: func() *v1beta1.TemporalCluster {
				c := clusterWithMTLS(true, true)
				c.Spec.MTLS.Provider = v1beta1.IstioMTLSProvider
				return c
			}(),
			authority: v1beta1.InternodeCertificateAuthority,
			err:       "cert-manager",
		},
	}
	for name, test := range tests {
		t.Run(name, func(tt *testing.T) {
			serverName, err := test.cluster.ClientCertificateServerName(test.authority)
			if test.err != "" {
				assert.ErrorContains(tt, err, test.err)
				return
			}
			assert.NoError(tt, err)
			assert.Equal(tt, test.serverName, serverName)
		})
	}
}
