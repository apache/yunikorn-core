/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package configs

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestConfigMap(t *testing.T) {
	defer SetConfigMap(nil)

	configmap := make(map[string]string)
	configmap["key"] = "value"
	SetConfigMap(configmap)
	value, ok := GetConfigMap()["key"]
	assert.Assert(t, ok, "entry not found")
	assert.Equal(t, "value", value, "test value not found")

	SetConfigMap(nil)
	_, ok = GetConfigMap()["key"]
	assert.Assert(t, !ok, "test value still found")
}

func TestCallback(t *testing.T) {
	defer RemoveConfigMapCallback("test-callback")

	var callbackReceived bool
	AddConfigMapCallback("test-callback", func() {
		callbackReceived = true
	})

	SetConfigMap(nil)
	assert.Assert(t, callbackReceived, "callback not received")

	callbackReceived = false
	RemoveConfigMapCallback("test-callback")
	SetConfigMap(nil)
	assert.Assert(t, !callbackReceived, "callback still received")
}
