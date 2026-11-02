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

package admintools_test

import (
	"testing"

	"github.com/alexandrevilain/temporal-operator/api/v1beta1"
	"github.com/alexandrevilain/temporal-operator/internal/resource/admintools"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func admintoolsCluster(internalFrontend, internodeMTLS, frontendMTLS bool) *v1beta1.TemporalCluster {
	c := &v1beta1.TemporalCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "temporal", Namespace: "temporal"},
		Spec: v1beta1.TemporalClusterSpec{
			AdminTools: &v1beta1.TemporalAdminToolsSpec{Enabled: true},
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

func renderAdmintools(t *testing.T, cluster *v1beta1.TemporalCluster) (map[string]string, []corev1.Volume) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	builder := admintools.NewDeploymentBuilder(cluster, scheme, "hash")
	object := builder.Build()
	require.NoError(t, builder.Update(object))
	pod := object.(*appsv1.Deployment).Spec.Template.Spec
	env := map[string]string{}
	for _, e := range pod.Containers[0].Env {
		env[e.Name] = e.Value
	}
	return env, pod.Volumes
}

func TestAdmintoolsCallsInternalFrontendWithTheInternodeCertificate(t *testing.T) {
	env, volumes := renderAdmintools(t, admintoolsCluster(true, true, true))

	assert.Equal(t, "temporal-internal-frontend-headless.temporal:7236", env["TEMPORAL_ADDRESS"])
	assert.Equal(t, "temporal-internal-frontend-headless.temporal:7236", env["TEMPORAL_CLI_ADDRESS"])
	assert.Equal(t, "temporal-internode.temporal.svc.cluster.local", env["TEMPORAL_TLS_SERVER_NAME"])
	assert.Equal(t, "temporal-internode.temporal.svc.cluster.local", env["TEMPORAL_CLI_TLS_SERVER_NAME"])
	require.Len(t, volumes, 1)
	assert.Equal(t, "temporal-internode-certificate", volumes[0].Secret.SecretName)
}

func TestAdmintoolsCallsInternalFrontendInPlaintextWithoutMTLS(t *testing.T) {
	env, volumes := renderAdmintools(t, admintoolsCluster(true, false, false))

	assert.Equal(t, "temporal-internal-frontend-headless.temporal:7236", env["TEMPORAL_ADDRESS"])
	assert.NotContains(t, env, "TEMPORAL_TLS_CERT")
	assert.Empty(t, volumes)
}

func TestAdmintoolsKeepsTheFrontendWithoutInternalFrontend(t *testing.T) {
	env, volumes := renderAdmintools(t, admintoolsCluster(false, false, true))

	assert.Equal(t, "temporal-frontend:7233", env["TEMPORAL_ADDRESS"])
	assert.Equal(t, "temporal-frontend.temporal.svc.cluster.local", env["TEMPORAL_TLS_SERVER_NAME"])
	require.Len(t, volumes, 1)
	assert.Equal(t, "temporal-admintools-mtls-certificate", volumes[0].Secret.SecretName)
}

// The frontend client certificate is only issued while admintools calls the frontend.
func TestAdmintoolsFrontendCertificateOnlyWithoutInternalFrontend(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))

	assert.False(t, admintools.NewFrontendClientCertificateBuilder(admintoolsCluster(true, true, true), scheme).Enabled())
	assert.True(t, admintools.NewFrontendClientCertificateBuilder(admintoolsCluster(false, false, true), scheme).Enabled())
}
