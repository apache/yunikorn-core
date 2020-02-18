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

package scheduler

import (
	"testing"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
)

func TestNewReservation(t *testing.T) {
	// create the input objects
	q := map[string]resources.Quantity{"first": 1}
	res := resources.NewResourceFromMap(q)
	ask := newAllocationAsk("alloc-1", "app-1", res)
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "app-1"})
	node := newNode("node-1", q)

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
	q := map[string]resources.Quantity{"first": 1}
	res := resources.NewResourceFromMap(q)
	ask := newAllocationAsk("alloc-1", "app-1", res)
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "app-1"})
	node := newNode("node-1", q)

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

func TestUnReserve(t *testing.T) {
	// create the input objects
	q := map[string]resources.Quantity{"first": 0}
	res := resources.NewResourceFromMap(q)
	ask := newAllocationAsk("alloc-1", "app-1", res)
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "app-1"})
	node := newNode("node-1", q)

	// standalone reservation unreserve returns false as app is not reserved
	// appIDs list should be empty
	reserve := newReservation(node, app, ask, true)
	assert.Equal(t, reserve.getKey(), "node-1|alloc-1", "incorrect node reservation key")
	appID, ok, err := reserve.unReserve()
	if ok || err != nil || appID != "" {
		t.Fatalf("unreserve should have returned false and no error: %v, app %v", err, appID)
	}
	reserve = newReservation(node, app, ask, false)
	assert.Equal(t, reserve.getKey(), "app-1|alloc-1", "incorrect app reservation key")
	appID, ok, err = reserve.unReserve()
	if ok || err != nil || appID != "app-1" {
		t.Fatalf("unreserve should have returned false and no error: %v, app %v", err, appID)
	}

	// do a bogus reserve and unreserve: no errors and should be really removed
	app.requests[ask.AskProto.AllocationKey] = ask
	ok, err = app.reserve(node, ask)
	if !ok || err != nil || len(app.reservations) != 1 || len(node.reservations) != 1 {
		t.Fatalf("reserve should not have failed: %v", err)
	}
	appID, ok, err = reserve.unReserve()
	if !ok || err != nil || appID != "app-1" || len(app.reservations) != 0 || len(node.reservations) != 0 {
		t.Fatalf("unreserve should have returned true and no error: %v, app %v", err, appID)
	}
}
