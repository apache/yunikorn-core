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

	"github.com/apache/yunikorn-core/pkg/common/resources"
)

func TestNewReservation(t *testing.T) {
	// create the input objects
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	ask := newAllocationAsk("alloc-1", "app-1", res)
	app := newApplication("app-1", "default", "root.unknown")
	node := newNodeRes("node-1", res)

	tests := []struct {
		name     string
		node     *Node
		app      *Application
		ask      *Allocation
		appBased bool
		expected *reservation
	}{
		{"nil input", nil, nil, nil, true, nil},
		{"nil alloc", node, app, nil, true, nil},
		{"nil app", node, nil, ask, true, nil},
		{"nil node", nil, app, ask, true, nil},
		{"node based", node, app, ask, false, &reservation{"app-1", "", "alloc-1", app, node, ask}},
		{"app based", node, app, ask, true, &reservation{"", "node-1", "alloc-1", app, node, ask}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reserve := newReservation(tt.node, tt.app, tt.ask, tt.appBased)
			if tt.expected == nil {
				assert.Equal(t, tt.expected, reserve, "unexpected reservation")
			} else {
				assert.Equal(t, reserve.appID, tt.expected.appID, "incorrect appID")
				assert.Equal(t, reserve.nodeID, tt.expected.nodeID, "incorrect node ID")
				assert.Equal(t, reserve.allocKey, tt.expected.allocKey, "incorrect alloc key")
				if tt.appBased {
					assert.Equal(t, reserve.String(), "app-1 -> node-1|alloc-1", "incorrect string form")
				} else {
					assert.Equal(t, reserve.String(), "node-1 -> app-1|alloc-1", "incorrect string form")
				}
			}
		})
	}
}

func TestReservationString(t *testing.T) {
	var nilReserve *reservation
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil reservation in object test")
		}
	}()
	str := nilReserve.String()
	assert.Equal(t, "nil reservation", str, "nil reservation did not return correct string")
}

func TestGetObjects(t *testing.T) {
	// create the input objects
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	ask := newAllocationAsk("alloc-1", "app-1", res)
	app := newApplication("app-1", "default", "root.unknown")
	node := newNodeRes("node-1", res)
	reserve := newReservation(node, app, ask, true)
	if reserve == nil {
		t.Errorf("reservation should have been created")
	}

	node2, app2, ask2 := reserve.GetObjects()
	assert.Equal(t, node, node2, "node: expected same object back")
	assert.Equal(t, app, app2, "app: expected same object back")
	assert.Equal(t, ask, ask2, "ask: expected same object back")

	var nilReserve *reservation
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil reservation in object test")
		}
	}()
	node2, app2, ask2 = nilReserve.GetObjects()
	if node2 != nil || app2 != nil || ask2 != nil {
		t.Fatalf("nil reservation should return nil objects")
	}
}
