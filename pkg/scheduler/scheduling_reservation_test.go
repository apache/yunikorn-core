/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

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

	"github.com/cloudera/yunikorn-core/pkg/cache"
	"github.com/cloudera/yunikorn-core/pkg/common/resources"
)

func TestNewReservation(t *testing.T) {
	// create the input objects
	q := map[string]resources.Quantity{"first": 1}
	res := resources.NewResourceFromMap(q)
	ask := newAllocationAsk("alloc-1", "app-1", res)
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "app-1"})
	node := newNode("node-1", q)

	// check the basics
	reserved := newReservation(nil, nil, nil)
	if reserved != nil {
		t.Errorf("reservation with nil objects should have returned nil: %v", reserved)
	}
	reserved = newReservation(node, app, nil)
	if reserved != nil {
		t.Errorf("reservation with nil ask set should have returned nil: '%v'", reserved)
	}
	reserved = newReservation(node, app, ask)
	if reserved != nil {
		t.Errorf("reservation with all objects set should have returned nil: '%v'", reserved)
	}

	// other cases
	reserved = newReservation(node, nil, ask)
	if reserved == nil {
		t.Fatal("node reservation was unexpectedly nil")
	}
	assert.Equal(t, reserved.getKey(), "node-1|alloc-1", "incorrect node reservation key")

	reserved = newReservation(nil, app, ask)
	if reserved == nil {
		t.Fatal("app reservation was unexpectedly nil")
	}
	assert.Equal(t, reserved.getKey(), "app-1|alloc-1", "incorrect app reservation key")
}

func TestReservationKey(t *testing.T) {
	// create the input objects
	q := map[string]resources.Quantity{"first": 1}
	res := resources.NewResourceFromMap(q)
	ask := newAllocationAsk("alloc-1", "app-1", res)
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "app-1"})
	node := newNode("node-1", q)

	// check the basics
	reserved := reservationKey(nil, nil, nil)
	assert.Equal(t, reserved, "", "reservation with nil objects should have  empty key")
	reserved = reservationKey(node, app, nil)
	assert.Equal(t, reserved, "", "reservation with nil ask set should have empty key")
	reserved = reservationKey(node, app, ask)
	assert.Equal(t, reserved, "", "reservation with all objects set should have empty key")

	// other cases
	reserved = reservationKey(node, nil, ask)
	assert.Equal(t, reserved, "node-1|alloc-1", "incorrect node reservation key")
	reserved = reservationKey(nil, app, ask)
	assert.Equal(t, reserved, "app-1|alloc-1", "incorrect app reservation key")
}
