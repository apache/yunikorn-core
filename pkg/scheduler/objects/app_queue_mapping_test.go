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

package objects

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestNewAppQueueMapping(t *testing.T) {
	aqm := NewAppQueueMapping()
	assert.Assert(t, aqm != nil, "expected non-nil AppQueueMapping")
	assert.Equal(t, 0, len(aqm.byAppID), "expected empty byAppID map")
}
func TestAppQueueMappingOperations(t *testing.T) {
	aqm := NewAppQueueMapping()
	queue := &Queue{}
	appID := "app-1234"

	// Test AddAppQueueMapping
	aqm.AddAppQueueMapping(appID, queue)
	assert.Equal(t, 1, len(aqm.byAppID), "expected 1 entry in byAppID map")

	// Test FindQueueByAppID
	foundQueue := aqm.GetQueueByAppId(appID)
	assert.Equal(t, foundQueue, queue, "expected to find the correct queue for appID %s", appID)

	// Test RemoveAppQueueMapping
	aqm.RemoveAppQueueMapping(appID)
	assert.Equal(t, 0, len(aqm.byAppID), "expected 0 entries in byAppID map after removal")
}
