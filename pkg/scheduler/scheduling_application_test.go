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

	"github.com/cloudera/yunikorn-core/pkg/cache"
	"github.com/cloudera/yunikorn-core/pkg/common/resources"
)

func TestSchedulingApplication(t *testing.T) {

}

func TestAppReservation(t *testing.T) {
	app := newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "app-1"})
	if app == nil || app.ApplicationInfo.ApplicationID != "app-1" {
		t.Fatalf("app create failed which should not have %v", app)
	}
	if app.hasReserved() {
		t.Fatal("new app should not have reservations")
	}
	if app.isReservedOnNode("unknown") {
		t.Error("new app should not have reservations for unknown node")
	}

	// reserve illegal request
	ok, err := app.reserve(nil, nil)
	if ok || err == nil {
		t.Errorf("illegal reservation requested but did not fail: status %t, error %v", ok, err)
	}

	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 15})
	ask := newAllocationAsk("alloc-1", "app-1", res)
	node := newNode("node-1", map[string]resources.Quantity{"first": 10})

	// too large for node
	ok, err = app.reserve(node, ask)
	if ok || err == nil {
		t.Errorf("requested reservation does not fit in node resource but did not fail: status %t, error %v", ok, err)
	}

	res = resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	ask = newAllocationAsk("alloc-1", "app-1", res)
	app = newSchedulingApplication(&cache.ApplicationInfo{ApplicationID: "app-1"})
	// reserve that works
	ok, err = app.reserve(node, ask)
	if !ok || err != nil {
		t.Errorf("reservation should not have failed: status %t, error %v", ok, err)
	}
	if app.isReservedOnNode("unknown") {
		t.Errorf("app should not have reservations for unknown node")
	}
	if app.hasReserved() && !app.isReservedOnNode("node-1") {
		t.Errorf("app should have reservations for node-1")
	}

	// unreserve unknown node/ask
	ok, err = app.unReserve(nil, nil)
	if ok || err == nil {
		t.Errorf("illegal reservation release but did not fail: status %t, error %v", ok, err)
	}

	// 2nd reservation for app
	ask2 := newAllocationAsk("alloc-2", "app-1", res)
	node2 := newNode("node-2", map[string]resources.Quantity{"first": 10})
	ok, err = app.reserve(node2, ask2)
	if !ok || err != nil {
		t.Errorf("reservation of 2nd node should not have failed: status %t, error %v", ok, err)
	}
	ok, err = app.unReserve(node2, ask2)
	if !ok || err != nil {
		t.Errorf("remove of reservation of 2nd node should not have failed: status %t, error %v", ok, err)
	}
	// unreserve the same should fail
	ok, err = app.unReserve(node2, ask2)
	if ok && err == nil {
		t.Errorf("remove of reservation of 2nd node should not have failed: status %t, error %v", ok, err)
	}

	// failure case: remove reservation from node
	ok, err = node.unReserve(app, ask)
	if !ok || err != nil {
		t.Fatalf("un-reserve on node should not have failed: status %t, error %v", ok, err)
	}
	ok, err = app.unReserve(node, ask)
	if ok || err != nil {
		t.Errorf("node does not have reservation removal of app reservation should have failed: status %t, error %v", ok, err)
	}
}
