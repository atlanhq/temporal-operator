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

package temporal

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/alexandrevilain/temporal-operator/api/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// certificateSecret returns a cert-manager style secret holding a throwaway self-signed certificate.
func certificateSecret(t *testing.T, name string) *corev1.Secret {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "temporal"},
		Data: map[string][]byte{
			"ca.crt":  cert,
			"tls.crt": cert,
			"tls.key": pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		},
	}
}

func cluster(internalFrontend, internodeMTLS, frontendMTLS bool) *v1beta1.TemporalCluster {
	c := &v1beta1.TemporalCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "temporal", Namespace: "temporal"},
		Spec: v1beta1.TemporalClusterSpec{
			Services: &v1beta1.ServicesSpec{
				InternalFrontend: &v1beta1.InternalFrontendServiceSpec{Enabled: internalFrontend},
			},
		},
	}
	if internodeMTLS || frontendMTLS {
		c.Spec.MTLS = &v1beta1.MTLSSpec{
			Provider:  v1beta1.CertManagerMTLSProvider,
			Internode: &v1beta1.InternodeMTLSSpec{Enabled: internodeMTLS},
			Frontend:  &v1beta1.FrontendMTLSSpec{Enabled: frontendMTLS},
		}
	}
	c.Default()
	return c
}

func TestClusterClientOptions(t *testing.T) {
	k8s := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(
		certificateSecret(t, "temporal-internode-certificate"),
		certificateSecret(t, "temporal-frontend-certificate"),
	).Build()

	tests := map[string]struct {
		cluster     *v1beta1.TemporalCluster
		hostPort    string
		serverName  string
		certificate string
		plaintext   bool
	}{
		"internal-frontend with internode mTLS uses the internode certificate": {
			cluster:     cluster(true, true, false),
			hostPort:    "temporal-internal-frontend-headless.temporal:7236",
			serverName:  "temporal-internode.temporal.svc.cluster.local",
			certificate: "temporal-internode-certificate",
		},
		"internal-frontend is preferred over frontend mTLS": {
			cluster:     cluster(true, true, true),
			hostPort:    "temporal-internal-frontend-headless.temporal:7236",
			serverName:  "temporal-internode.temporal.svc.cluster.local",
			certificate: "temporal-internode-certificate",
		},
		"internal-frontend without mTLS is plaintext": {
			cluster:   cluster(true, false, false),
			hostPort:  "temporal-internal-frontend-headless.temporal:7236",
			plaintext: true,
		},
		"frontend mTLS without internal-frontend uses the frontend certificate": {
			cluster:     cluster(false, false, true),
			hostPort:    "temporal-frontend.temporal:7233",
			serverName:  "temporal-frontend.temporal.svc.cluster.local",
			certificate: "temporal-frontend-certificate",
		},
	}
	for name, test := range tests {
		t.Run(name, func(tt *testing.T) {
			opts, err := buildClusterClientOptions(context.Background(), k8s, test.cluster)
			require.NoError(tt, err)
			assert.Equal(tt, test.hostPort, opts.HostPort)
			if test.plaintext {
				assert.Nil(tt, opts.ConnectionOptions.TLS)
				return
			}
			require.NotNil(tt, opts.ConnectionOptions.TLS)
			assert.Equal(tt, test.serverName, opts.ConnectionOptions.TLS.ServerName)
			require.Len(tt, opts.ConnectionOptions.TLS.Certificates, 1)
			leaf, err := x509.ParseCertificate(opts.ConnectionOptions.TLS.Certificates[0].Certificate[0])
			require.NoError(tt, err)
			assert.Equal(tt, test.certificate, leaf.Subject.CommonName)
		})
	}
}
