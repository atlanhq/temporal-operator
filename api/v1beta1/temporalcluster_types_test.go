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

package v1beta1_test

import (
	"encoding/json"
	"testing"

	"github.com/alexandrevilain/temporal-operator/api/v1beta1"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"
)

// A nil ServiceSpec.Replicas must serialize as omitted, not `"replicas":null`.
// The CRD field is a non-nullable integer, so a null (which is what a nil
// pointer without omitempty marshals to) is rejected on apply - this blocks
// leaving the frontend replicas unset for scale-to-zero.
func TestServiceSpecReplicasOmitEmpty(t *testing.T) {
	unset, err := json.Marshal(v1beta1.ServiceSpec{})
	assert.NoError(t, err)
	assert.NotContains(t, string(unset), "replicas", "nil replicas must be omitted, not null")

	zero, err := json.Marshal(v1beta1.ServiceSpec{Replicas: ptr.To[int32](0)})
	assert.NoError(t, err)
	assert.Contains(t, string(zero), `"replicas":0`, "explicit 0 must still be serialized")
}
