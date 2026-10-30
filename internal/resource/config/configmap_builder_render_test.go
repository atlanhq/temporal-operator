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

package config_test

import (
	"strings"
	"testing"

	"github.com/alexandrevilain/temporal-operator/api/v1beta1"
	"github.com/alexandrevilain/temporal-operator/internal/resource/config"
	"github.com/alexandrevilain/temporal-operator/pkg/version"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

func datastore(database string) *v1beta1.DatastoreSpec {
	return &v1beta1.DatastoreSpec{
		SQL: &v1beta1.SQLSpec{
			User:            "temporal",
			PluginName:      "postgres12",
			DatabaseName:    database,
			ConnectAddr:     "postgres:5432",
			ConnectProtocol: "tcp",
		},
		PasswordSecretRef: &v1beta1.SecretKeyReference{Name: "postgres-password", Key: "PASSWORD"},
	}
}

func renderableCluster(frontend *v1beta1.FrontendMTLSSpec, authorization *v1beta1.AuthorizationSpec) *v1beta1.TemporalCluster {
	c := &v1beta1.TemporalCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "temporal", Namespace: "temporal"},
		Spec: v1beta1.TemporalClusterSpec{
			Version:          version.MustNewVersionFromString("1.30.6"),
			NumHistoryShards: 1,
			Persistence: v1beta1.TemporalPersistenceSpec{
				DefaultStore:    datastore("temporal"),
				VisibilityStore: datastore("temporal_visibility"),
			},
			MTLS: &v1beta1.MTLSSpec{
				Provider:  v1beta1.CertManagerMTLSProvider,
				Internode: &v1beta1.InternodeMTLSSpec{Enabled: true},
				Frontend:  frontend,
			},
			Authorization: authorization,
		},
	}
	c.Default()
	return c
}

// renderedGlobal returns the "global" section of the server config the builder renders.
func renderedGlobal(t *testing.T, cluster *v1beta1.TemporalCluster) map[string]any {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	builder := config.NewConfigmapBuilder(cluster, scheme)
	object := builder.Build()
	require.NoError(t, builder.Update(object))

	rendered := object.(*corev1.ConfigMap).Data["config_template.yaml"]
	var cfg struct {
		Global map[string]any `yaml:"global"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(strings.TrimPrefix(rendered, "# enable-template\n")), &cfg))
	return cfg.Global
}

func lookup(t *testing.T, node map[string]any, keys ...string) any {
	t.Helper()
	var value any = node
	for _, key := range keys {
		m, ok := value.(map[string]any)
		require.True(t, ok, "%s is not a map", key)
		value, ok = m[key]
		require.True(t, ok, "missing %s", key)
	}
	return value
}

func TestFrontendRequiresClientCertificatesByDefault(t *testing.T) {
	global := renderedGlobal(t, renderableCluster(&v1beta1.FrontendMTLSSpec{Enabled: true}, nil))

	assert.Equal(t, true, lookup(t, global, "tls", "frontend", "server", "requireClientAuth"))
}

func TestFrontendCanServeTLSWithoutClientCertificates(t *testing.T) {
	frontend := &v1beta1.FrontendMTLSSpec{Enabled: true, RequireClientAuth: ptr.To(false)}
	global := renderedGlobal(t, renderableCluster(frontend, nil))

	assert.Equal(t, false, lookup(t, global, "tls", "frontend", "server", "requireClientAuth"))
	assert.Equal(t, true, lookup(t, global, "tls", "internode", "server", "requireClientAuth"))
}

func TestAuthorizationRendersPermissionsRegex(t *testing.T) {
	authorization := &v1beta1.AuthorizationSpec{
		Authorizer:           "default",
		ClaimMapper:          "default",
		PermissionsClaimName: "aud",
		PermissionsRegex:     `^(?P<namespace>[a-z0-9-]+):(?P<role>read|write|admin)$`,
	}
	global := renderedGlobal(t, renderableCluster(&v1beta1.FrontendMTLSSpec{Enabled: true}, authorization))

	assert.Equal(t, authorization.PermissionsRegex, lookup(t, global, "authorization", "permissionsRegex"))
	assert.Equal(t, "aud", lookup(t, global, "authorization", "permissionsClaimName"))
}
