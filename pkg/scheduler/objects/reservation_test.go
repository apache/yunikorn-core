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

	// check the basics (failures)
	reserve := newReservation(nil, nil, nil, true)
	if reserve != nil {
		t.Errorf("reservation with nil objects should have returned nil: %v", reserve)
	}
	reserve = newReservation(node, app, nil, true)
	if reserve != nil {
		t.Errorf("reservation with nil ask set should have returned nil: '%v'", reserve)
	}
	reserve = newReservation(node, nil, ask, true)
	if reserve != nil {
		t.Errorf("reservation with nil app set should have returned nil: '%v'", reserve)
	}
	reserve = newReservation(nil, app, ask, true)
	if reserve != nil {
		t.Errorf("reservation with nil node set should have returned nil: '%v'", reserve)
	}

	// working cases
	reserve = newReservation(node, app, ask, true)
	if reserve == nil {
		t.Fatalf("reservation with all objects set should have returned nil: %v", reserve)
	}
	assert.Equal(t, reserve.getKey(), "node-1|alloc-1", "incorrect node reservation key")
	assert.Equal(t, reserve.String(), "app-1 -> node-1|alloc-1", "incorrect string form")

	reserve = newReservation(node, app, ask, false)
	if reserve == nil {
		t.Fatalf("reservation with all objects set should have returned nil: %v", reserve)
	}
	assert.Equal(t, reserve.getKey(), "app-1|alloc-1", "incorrect app reservation key")
	assert.Equal(t, reserve.String(), "node-1 -> app-1|alloc-1", "incorrect string form")
}

func TestReservationKey(t *testing.T) {
	// create the input objects
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	ask := newAllocationAsk("alloc-1", "app-1", res)
	app := newApplication("app-1", "default", "root.unknown")
	node := newNodeRes("node-1", res)

	// check the basics
	reserve := reservationKey(nil, nil, nil)
	assert.Equal(t, reserve, "", "reservation with nil objects should have  empty key")
	reserve = reservationKey(node, app, nil)
	assert.Equal(t, reserve, "", "reservation with nil ask set should have empty key")
	reserve = reservationKey(node, app, ask)
	assert.Equal(t, reserve, "", "reservation with all objects set should have empty key")

	// other cases
	reserve = reservationKey(node, nil, ask)
	assert.Equal(t, reserve, "node-1|alloc-1", "incorrect node reservation key")
	reserve = reservationKey(nil, app, ask)
	assert.Equal(t, reserve, "app-1|alloc-1", "incorrect app reservation key")
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
