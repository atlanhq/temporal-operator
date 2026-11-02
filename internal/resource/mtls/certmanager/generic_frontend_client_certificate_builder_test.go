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

package certmanager_test

import (
	"testing"

	"github.com/alexandrevilain/temporal-operator/api/v1beta1"
	"github.com/alexandrevilain/temporal-operator/internal/resource/mtls/certmanager"
	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestClientCertificateIssuerFollowsCertificateAuthority(t *testing.T) {
	cluster := &v1beta1.TemporalCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "temporal", Namespace: "temporal"},
		Spec: v1beta1.TemporalClusterSpec{
			MTLS: &v1beta1.MTLSSpec{
				Provider:  v1beta1.CertManagerMTLSProvider,
				Internode: &v1beta1.InternodeMTLSSpec{Enabled: true},
				Frontend:  &v1beta1.FrontendMTLSSpec{Enabled: true},
			},
		},
	}
	cluster.Default()
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))

	tests := map[string]struct {
		authority v1beta1.ClientCertificateAuthority
		issuer    string
	}{
		"unset issues from the frontend CA": {authority: "", issuer: "temporal-frontend-intermediate-ca-issuer"},
		"frontend":                          {authority: v1beta1.FrontendCertificateAuthority, issuer: "temporal-frontend-intermediate-ca-issuer"},
		"internode":                         {authority: v1beta1.InternodeCertificateAuthority, issuer: "temporal-internode-intermediate-ca-issuer"},
	}
	for name, test := range tests {
		t.Run(name, func(tt *testing.T) {
			builder := certmanager.NewGenericFrontendClientCertificateBuilder(cluster, scheme, "keda", test.authority)
			object := builder.Build()
			require.NoError(tt, builder.Update(object))

			certificate := object.(*certmanagerv1.Certificate)
			assert.Equal(tt, test.issuer, certificate.Spec.IssuerRef.Name)
			assert.Equal(tt, "temporal-keda-mtls-certificate", certificate.Spec.SecretName)
		})
	}
}
