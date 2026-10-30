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

// Internal-frontend serves with the internode certificate, so a client that dials its Service and
// cannot set a server name (KEDA's Temporal scaler) must find that name on the certificate.
func TestInternodeCertificateNamesTheInternalFrontendService(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))

	tests := map[string]struct {
		internalFrontend bool
		dnsNames         []string
	}{
		"without internal-frontend": {
			dnsNames: []string{"temporal-internode.temporal.svc.cluster.local"},
		},
		"with internal-frontend": {
			internalFrontend: true,
			dnsNames: []string{
				"temporal-internode.temporal.svc.cluster.local",
				"temporal-internal-frontend-headless.temporal.svc.cluster.local",
				"temporal-internal-frontend-headless.temporal",
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(tt *testing.T) {
			cluster := &v1beta1.TemporalCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "temporal", Namespace: "temporal"},
				Spec: v1beta1.TemporalClusterSpec{
					Services: &v1beta1.ServicesSpec{
						InternalFrontend: &v1beta1.InternalFrontendServiceSpec{Enabled: test.internalFrontend},
					},
					MTLS: &v1beta1.MTLSSpec{
						Provider:  v1beta1.CertManagerMTLSProvider,
						Internode: &v1beta1.InternodeMTLSSpec{Enabled: true},
					},
				},
			}
			cluster.Default()
			builder := certmanager.NewMTLSInternodeCertificateBuilder(cluster, scheme)
			object := builder.Build()
			require.NoError(tt, builder.Update(object))

			assert.Equal(tt, test.dnsNames, object.(*certmanagerv1.Certificate).Spec.DNSNames)
		})
	}
}
