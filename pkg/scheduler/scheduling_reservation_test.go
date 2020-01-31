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
	reserve := newReservation(nil, nil, nil)
	if reserve != nil {
		t.Errorf("reservation with nil objects should have returned nil: %v", reserve)
	}
	reserve = newReservation(node, app, nil)
	if reserve != nil {
		t.Errorf("reservation with nil ask set should have returned nil: '%v'", reserve)
	}
	reserve = newReservation(node, app, ask)
	if reserve != nil {
		t.Errorf("reservation with all objects set should have returned nil: '%v'", reserve)
	}

	// other cases
	reserve = newReservation(node, nil, ask)
	if reserve == nil {
		t.Fatal("node reservation was unexpectedly nil")
	}
	assert.Equal(t, reserve.getKey(), "node-1|alloc-1", "incorrect node reservation key")

	reserve = newReservation(nil, app, ask)
	if reserve == nil {
		t.Fatal("app reservation was unexpectedly nil")
	}
	assert.Equal(t, reserve.getKey(), "app-1|alloc-1", "incorrect app reservation key")
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
