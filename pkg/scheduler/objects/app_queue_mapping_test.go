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
)

func TestNewAppQueueMapping(t *testing.T) {
	aqm := NewAppQueueMapping()
	if aqm == nil {
		t.Fatal("Expected non-nil AppQueueMapping")
	}
	if len(aqm.byAppID) != 0 {
		t.Errorf("Expected empty byAppID map, got %d entries", len(aqm.byAppID))
	}
}
func TestAppQueueMappingOperations(t *testing.T) {
	aqm := NewAppQueueMapping()
	queue := &Queue{}
	appID := "app-1234"

	// Test AddAppQueueMapping
	aqm.AddAppQueueMapping(appID, queue)
	if len(aqm.byAppID) != 1 {
		t.Errorf("Expected 1 entry in byAppID map, got %d", len(aqm.byAppID))
	}

	// Test FindQueueByAppID
	foundQueue := aqm.FindQueueByAppID(appID)
	if foundQueue != queue {
		t.Errorf("Expected to find the correct queue for appID %s", appID)
	}

	// Test RemoveAppQueueMapping
	aqm.RemoveAppQueueMapping(appID)
	if len(aqm.byAppID) != 0 {
		t.Errorf("Expected empty byAppID map after removal, got %d entries", len(aqm.byAppID))
	}
}
