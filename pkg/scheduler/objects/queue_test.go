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
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtu "github.com/prometheus/client_golang/prometheus/testutil"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects/template"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const ZeroResource string = "{\"resources\":{\"first\":{\"value\":0}}}"

// base test for creating a managed queue
func TestQueueBasics(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	// check the state of the queue
	if !root.IsManaged() && !root.IsLeafQueue() && !root.IsRunning() {
		t.Error("root queue status is incorrect")
	}
	// allocations should be nil
	if !resources.IsZero(root.pending) {
		t.Error("root queue must not have allocations set on create")
	}
}

func TestManagedSubQueues(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	// single parent under root
	var parent *Queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	if parent.IsLeafQueue() || !parent.IsManaged() || !parent.IsRunning() {
		t.Error("parent queue is not marked as running managed parent")
	}
	if len(root.children) == 0 {
		t.Error("parent queue is not added to the root queue")
	}
	if parent.isRoot() {
		t.Error("parent queue says it is the root queue which is incorrect")
	}
	if parent.RemoveQueue() || len(root.children) != 1 {
		t.Error("parent queue should not have been removed as it is running")
	}

	// add a leaf under the parent
	var leaf *Queue
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	if len(parent.children) == 0 {
		t.Error("leaf queue is not added to the parent queue")
	}
	if !leaf.IsLeafQueue() || !leaf.IsManaged() {
		t.Error("leaf queue is not marked as managed leaf")
	}

	// cannot remove child with app in it
	app := newApplication(appID1, "default", "root.parent.leaf")
	leaf.AddApplication(app)

	// both parent and leaf are marked for removal
	parent.MarkQueueForRemoval()
	if !leaf.IsDraining() || !parent.IsDraining() {
		t.Error("queues are not marked for removal (not in draining state)")
	}
	// try to remove the parent
	if parent.RemoveQueue() {
		t.Error("parent queue should not have been removed as it has a child")
	}
	// try to remove the child
	if leaf.RemoveQueue() {
		t.Error("leaf queue should not have been removed")
	}
	// remove the app (dirty way)
	delete(leaf.applications, appID1)
	if !leaf.RemoveQueue() && len(parent.children) != 0 {
		t.Error("leaf queue should have been removed and parent updated and was not")
	}
}

func TestDynamicSubQueues(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	// single parent under root
	var parent *Queue
	parent, err = createDynamicQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	if parent.IsLeafQueue() || parent.IsManaged() {
		t.Errorf("parent queue is not marked as parent")
	}
	if len(root.children) == 0 {
		t.Errorf("parent queue is not added to the root queue")
	}
	// add a leaf under the parent
	var leaf *Queue
	leaf, err = createDynamicQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	if len(parent.children) == 0 {
		t.Error("leaf queue is not added to the parent queue")
	}
	if !leaf.IsLeafQueue() || leaf.IsManaged() {
		t.Error("leaf queue is not marked as managed leaf")
	}

	// cannot remove child with app in it
	app := newApplication(appID1, "default", "root.parent.leaf")
	leaf.AddApplication(app)

	// try to mark parent and leaf for removal
	parent.MarkQueueForRemoval()
	if leaf.IsDraining() || parent.IsDraining() {
		t.Error("queues are marked for removal (draining state not for Dynamic queues)")
	}
	// try to remove the parent
	if parent.RemoveQueue() {
		t.Error("parent queue should not have been removed as it has a child")
	}
	// try to remove the child
	if leaf.RemoveQueue() {
		t.Error("leaf queue should not have been removed")
	}
	// remove the app (dirty way)
	delete(leaf.applications, appID1)
	if !leaf.RemoveQueue() && len(parent.children) != 0 {
		t.Error("leaf queue should have been removed and parent updated and was not")
	}
}

func TestPriorityCalcWithFencedQueue(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	// single parent under root
	var parent *Queue
	parent, err = createManagedQueueWithProps(root, "parent", true, nil, map[string]string{
		configs.PriorityOffset: "5",
	})
	assert.NilError(t, err, "failed to create parent queue")

	// add a leaf under the parent
	var leaf *Queue
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, map[string]string{
		configs.PriorityOffset: "3",
		configs.PriorityPolicy: policies.FencePriorityPolicy.String(),
	})
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, parent.GetCurrentPriority(), configs.MinPriority, "initial parent priority wrong")
	assert.Equal(t, leaf.GetCurrentPriority(), configs.MinPriority, "initial leaf priority wrong")

	app := newApplication(appID1, "default", "root.parent.leaf")
	app.SetQueue(leaf)
	leaf.AddApplication(app)

	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "failed to create basic resource")

	err = app.AddAllocationAsk(newAllocationAskPriority("alloc-1", appID1, res, 0))
	assert.NilError(t, err, "failed to add app")
	assert.Equal(t, parent.GetCurrentPriority(), int32(8), "parent priority wrong after alloc add")
	assert.Equal(t, leaf.GetCurrentPriority(), int32(3), "leaf priority wrong after alloc add")

	app2 := newApplication(appID2, "default", "root.parent.leaf")
	app2.SetQueue(leaf)
	leaf.AddApplication(app2)
	err = app2.AddAllocationAsk(newAllocationAskPriority("alloc-2", appID2, res, 5))
	assert.NilError(t, err, "failed to add app")
	assert.Equal(t, parent.GetCurrentPriority(), int32(8), "parent priority wrong after alloc add 2")
	assert.Equal(t, leaf.GetCurrentPriority(), int32(3), "leaf priority wrong after alloc add 2")

	leaf.RemoveApplication(app2)
	assert.Equal(t, parent.GetCurrentPriority(), int32(8), "parent priority wrong after app 2 removed")
	assert.Equal(t, leaf.GetCurrentPriority(), int32(3), "leaf priority wrong after app 2 removed")

	leaf.RemoveApplication(app)
	assert.Equal(t, parent.GetCurrentPriority(), configs.MinPriority, "final parent priority wrong")
	assert.Equal(t, leaf.GetCurrentPriority(), configs.MinPriority, "final leaf priority wrong")
}

func TestPriorityCalc(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	// single parent under root
	var parent *Queue
	parent, err = createManagedQueueWithProps(root, "parent", true, nil, map[string]string{
		configs.PriorityOffset: "5",
	})
	assert.NilError(t, err, "failed to create parent queue")

	// add a leaf under the parent
	var leaf *Queue
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, map[string]string{
		configs.PriorityOffset: "3",
	})
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, parent.GetCurrentPriority(), configs.MinPriority, "initial parent priority wrong")
	assert.Equal(t, leaf.GetCurrentPriority(), configs.MinPriority, "initial leaf priority wrong")

	app := newApplication(appID1, "default", "root.parent.leaf")
	app.SetQueue(leaf)
	leaf.AddApplication(app)

	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "failed to create basic resource")

	err = app.AddAllocationAsk(newAllocationAskPriority("alloc-1", appID1, res, 0))
	assert.NilError(t, err, "failed to add app")
	assert.Equal(t, parent.GetCurrentPriority(), int32(8), "parent priority wrong after alloc add")
	assert.Equal(t, leaf.GetCurrentPriority(), int32(3), "leaf priority wrong after alloc add")

	app2 := newApplication(appID2, "default", "root.parent.leaf")
	app2.SetQueue(leaf)
	leaf.AddApplication(app2)
	err = app2.AddAllocationAsk(newAllocationAskPriority("alloc-2", appID2, res, 5))
	assert.NilError(t, err, "failed to add app")
	assert.Equal(t, parent.GetCurrentPriority(), int32(13), "parent priority wrong after alloc add 2")
	assert.Equal(t, leaf.GetCurrentPriority(), int32(8), "leaf priority wrong after alloc add 2")

	leaf.RemoveApplication(app2)
	assert.Equal(t, parent.GetCurrentPriority(), int32(8), "parent priority wrong after app 2 removed")
	assert.Equal(t, leaf.GetCurrentPriority(), int32(3), "leaf priority wrong after app 2 removed")

	leaf.RemoveApplication(app)
	assert.Equal(t, parent.GetCurrentPriority(), configs.MinPriority, "final parent priority wrong")
	assert.Equal(t, leaf.GetCurrentPriority(), configs.MinPriority, "final leaf priority wrong")
}

func TestPendingCalc(t *testing.T) {
	// Reset existing metric storage; otherwise this unit test would get metrics populated by other UTs.
	// In long run, to make the metrics code more testable, we should pass instantiable Metrics obj to Queue
	// instead of using a global Metrics obj at pkg/metrics/init.go.
	metrics.Reset()

	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")

	res := map[string]string{"memory": "100", "vcores": "10"}
	var allocRes *resources.Resource
	allocRes, err = resources.NewResourceFromConf(res)
	assert.NilError(t, err, "failed to create basic resource")
	leaf.incPendingResource(allocRes)
	if !resources.Equals(root.pending, allocRes) {
		t.Errorf("root queue pending allocation failed to increment expected %v, got %v", allocRes, root.pending)
	}
	if !resources.Equals(leaf.pending, allocRes) {
		t.Errorf("leaf queue pending allocation failed to increment expected %v, got %v", allocRes, leaf.pending)
	}
	metrics := []string{"yunikorn_queue_resource"}
	want := concatQueueResourceMetric(metrics, []string{`
yunikorn_queue_resource{queue="root",resource="memory",state="pending"} 100
yunikorn_queue_resource{queue="root",resource="vcores",state="pending"} 10
yunikorn_queue_resource{queue="root",resource="apps",state="maxRunningApps"} 0
yunikorn_queue_resource{queue="root.leaf",resource="memory",state="pending"} 100
yunikorn_queue_resource{queue="root.leaf",resource="vcores",state="pending"} 10
yunikorn_queue_resource{queue="root.leaf",resource="apps",state="maxRunningApps"} 0
`},
	)
	assert.NilError(t, promtu.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(want), metrics...), "unexpected metrics")
	leaf.decPendingResource(allocRes)
	if !resources.IsZero(root.pending) {
		t.Errorf("root queue pending allocation failed to decrement expected 0, got %v", root.pending)
	}
	if !resources.IsZero(leaf.pending) {
		t.Errorf("leaf queue pending allocation failed to decrement expected 0, got %v", leaf.pending)
	}
	want = concatQueueResourceMetric(metrics, []string{`
yunikorn_queue_resource{queue="root",resource="memory",state="pending"} 0
yunikorn_queue_resource{queue="root",resource="vcores",state="pending"} 0
yunikorn_queue_resource{queue="root",resource="apps",state="maxRunningApps"} 0
yunikorn_queue_resource{queue="root.leaf",resource="memory",state="pending"} 0
yunikorn_queue_resource{queue="root.leaf",resource="vcores",state="pending"} 0
yunikorn_queue_resource{queue="root.leaf",resource="apps",state="maxRunningApps"} 0
`},
	)
	assert.NilError(t, promtu.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(want), metrics...), "unexpected metrics")
	// Not allowed to go negative: both will be zero after this
	newRes := resources.Multiply(allocRes, 2)
	root.pending = newRes
	leaf.decPendingResource(newRes)
	// using the get function to access the value
	if !resources.IsZero(root.GetPendingResource()) {
		t.Errorf("root queue pending allocation failed to decrement expected zero, got %v", root.pending)
	}
	if !resources.IsZero(leaf.GetPendingResource()) {
		t.Errorf("leaf queue pending allocation should have failed to decrement expected zero, got %v", leaf.pending)
	}
	want = concatQueueResourceMetric(metrics, []string{`
yunikorn_queue_resource{queue="root",resource="memory",state="pending"} 0
yunikorn_queue_resource{queue="root",resource="vcores",state="pending"} 0
yunikorn_queue_resource{queue="root",resource="apps",state="maxRunningApps"} 0
yunikorn_queue_resource{queue="root.leaf",resource="memory",state="pending"} 0
yunikorn_queue_resource{queue="root.leaf",resource="vcores",state="pending"} 0
yunikorn_queue_resource{queue="root.leaf",resource="apps",state="maxRunningApps"} 0
`},
	)
	assert.NilError(t, promtu.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(want), metrics...), "unexpected metrics")
}

const (
	QueueResourceMetricHelp = "# HELP %v Queue resource metrics. State of the resource includes `guaranteed`, `max`, `allocated`, `pending`, `preempting`, `maxRunningApps`."
	QueueResourceMetricType = "# TYPE %v gauge"
)

func concatQueueResourceMetric(metricNames, metricVals []string) string {
	var out string
	for i, metricName := range metricNames {
		out = out + fmt.Sprintf(QueueResourceMetricHelp, metricName) + "\n"
		out = out + fmt.Sprintf(QueueResourceMetricType, metricName) + "\n"
		out += strings.TrimLeft(metricVals[i], "\n")
	}
	return out
}

func TestGetChildQueueInfo(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var parent *Queue
	parent, err = createManagedQueue(root, "parent-man", true, nil)
	assert.NilError(t, err, "failed to create managed parent queue")
	for i := 0; i < 10; i++ {
		_, err = createManagedQueue(parent, "leaf-man"+strconv.Itoa(i), false, nil)
		if err != nil {
			t.Errorf("failed to create managed queue: %v", err)
		}
	}
	if len(parent.children) != 10 {
		t.Errorf("managed leaf queues are not added to the parent queue, expected 10 children got %d", len(parent.children))
	}

	parent, err = createDynamicQueue(root, "parent-un", true, nil)
	assert.NilError(t, err, "failed to create dynamic parent queue")
	for i := 0; i < 10; i++ {
		_, err = createDynamicQueue(parent, "leaf-un-"+strconv.Itoa(i), false, nil)
		if err != nil {
			t.Errorf("failed to create dynamic queue: %v", err)
		}
	}
	if len(parent.children) != 10 {
		t.Errorf("dynamic leaf queues are not added to the parent queue, expected 10 children got %d", len(parent.children))
	}

	// check the root queue
	if len(root.children) != 2 {
		t.Errorf("parent queues are not added to the root queue, expected 2 children got %d", len(root.children))
	}
}

func TestAddApplication(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf-man", false, nil)
	assert.NilError(t, err, "failed to create managed leaf queue")
	pending := resources.NewResourceFromMap(
		map[string]resources.Quantity{
			siCommon.Memory: 10,
		})
	app := newApplication(appID1, "default", "root.parent.leaf")
	app.pending = pending
	// adding the app must not update pending resources
	leaf.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pending), "leaf queue pending resource not zero")

	// add the same app again should not increase the number of apps
	leaf.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not replaced in the queue as expected")
}

func TestRemoveApplication(t *testing.T) {
	// create the root
	root, err := createRootQueue(map[string]string{"first": "100"})
	assert.NilError(t, err, "queue create failed")
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf-man", false, nil)
	assert.NilError(t, err, "failed to create managed leaf queue")
	// try removing a non existing app
	nonExist := newApplication("test", "", "")
	leaf.RemoveApplication(nonExist)
	assert.Equal(t, len(leaf.applications), 0, "Removal of non existing app updated unexpected")

	// add an app and remove it
	app := newApplication("exists", "default", "root.leaf-man")
	leaf.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pending), "leaf queue pending resource not zero")
	leaf.RemoveApplication(nonExist)
	assert.Equal(t, len(leaf.applications), 1, "Non existing application was removed from the queue")
	assert.Equal(t, len(leaf.GetCopyOfApps()), 1, "Non existing application was removed from the queue")
	leaf.RemoveApplication(app)
	assert.Equal(t, len(leaf.applications), 0, "Application was not removed from the queue as expected")
	assert.Equal(t, len(leaf.GetCopyOfApps()), 0, "Application was not removed from the queue as expected")

	// try the same again now with pending resources set
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	app.pending.AddTo(res)
	leaf.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pending), "leaf queue pending resource not zero")
	// update pending resources for the hierarchy
	leaf.incPendingResource(res)
	leaf.RemoveApplication(app)
	assert.Equal(t, len(leaf.applications), 0, "Application was not removed from the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.pending), "leaf queue pending resource not updated correctly")
	assert.Assert(t, resources.IsZero(root.pending), "root queue pending resource not updated correctly")

	app.allocatedResource.AddTo(res)
	app.pending = resources.NewResource()
	leaf.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.allocatedResource), "leaf queue allocated resource not zero")
	// update allocated resources for the hierarchy
	err = leaf.TryIncAllocatedResource(res)
	assert.NilError(t, err, "increment of allocated resource on queue should not have failed")
	leaf.RemoveApplication(app)
	assert.Equal(t, len(leaf.applications), 0, "Application was not removed from the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.allocatedResource), "leaf queue allocated resource not updated correctly")
	assert.Assert(t, resources.IsZero(root.allocatedResource), "root queue allocated resource not updated correctly")

	// remove an application with allocated placeholders
	app.allocatedPlaceholder.AddTo(res)
	app.allocatedResource.SubFrom(res)
	leaf.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "Application was not added to the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.allocatedResource), "leaf queue allocated resource not zero")
	err = leaf.TryIncAllocatedResource(res)
	assert.NilError(t, err, "increment of allocated resource on queue should not have failed")
	assert.Assert(t, resources.Equals(leaf.allocatedResource, res), "leaf queue allocated resource not updated as expected")
	leaf.RemoveApplication(app)
	assert.Equal(t, len(leaf.applications), 0, "Application was not removed from the queue as expected")
	assert.Assert(t, resources.IsZero(leaf.allocatedResource), "leaf queue allocated resource not updated correctly")
	assert.Assert(t, resources.IsZero(root.allocatedResource), "root queue allocated resource not updated correctly")
}

func TestQueueStates(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	// add a leaf under the root
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	err = leaf.handleQueueEvent(Stop)
	if err != nil || !leaf.IsStopped() {
		t.Errorf("leaf queue is not marked stopped: %v", err)
	}
	err = leaf.handleQueueEvent(Start)
	if err != nil || !leaf.IsRunning() {
		t.Errorf("leaf queue is not marked running: %v", err)
	}
	err = leaf.handleQueueEvent(Remove)
	if err != nil || !leaf.IsDraining() {
		t.Errorf("leaf queue is not marked draining: %v", err)
	}
	err = leaf.handleQueueEvent(Start)
	if err != nil || !leaf.IsRunning() {
		t.Errorf("leaf queue is not marked running: %v", err)
	}
}

// This test must not test the sorter that is underlying.
// It tests the queue specific parts of the code only.
func TestSortApplications(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf, parent *Queue
	// empty parent queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue: %v")
	if apps := parent.sortApplications(false); apps != nil {
		t.Errorf("parent queue should not return sorted apps: %v", apps)
	}

	// empty leaf queue
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	if len(leaf.sortApplications(false)) != 0 {
		t.Errorf("empty queue should return no app from sort: %v", leaf)
	}
	// new app does not have pending res, does not get returned
	app := newApplication(appID1, "default", leaf.QueuePath)
	app.queue = leaf
	leaf.AddApplication(app)
	if len(leaf.sortApplications(false)) != 0 {
		t.Errorf("app without ask should not be in sorted apps: %v", app)
	}
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	// add an ask app must be returned
	err = app.AddAllocationAsk(newAllocationAsk("alloc-1", appID1, res))
	assert.NilError(t, err, "failed to add allocation ask")
	sortedApp := leaf.sortApplications(false)
	if len(sortedApp) != 1 || sortedApp[0].ApplicationID != appID1 {
		t.Errorf("sorted application is missing expected app: %v", sortedApp)
	}
	// set allocated
	_, err = app.AllocateAsk("alloc-1")
	if err != nil || len(leaf.sortApplications(false)) != 0 {
		t.Errorf("app with ask but no pending resources should not be in sorted apps: %v (err = %v)", app, err)
	}
}

func TestSortAppsWithPlaceholderAllocations(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	leaf, err := createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")

	app1 := newApplication(appID1, "default", leaf.QueuePath)
	app1.queue = leaf
	leaf.AddApplication(app1)
	app2 := newApplication(appID2, "default", leaf.QueuePath)
	app2.queue = leaf
	leaf.AddApplication(app2)

	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	alloc := newAllocation(appID1, "node-0", res)
	alloc.placeholder = true
	// adding a placeholder allocation & pending request to "app1"
	app1.AddAllocation(alloc)
	err = app1.AddAllocationAsk(newAllocationAsk("ask-0", appID1, res))
	assert.NilError(t, err, "could not add ask")
	phApps := leaf.sortApplications(true)
	assert.Equal(t, 1, len(phApps))

	// adding a placeholder allocation & pending request to "app2"
	alloc2 := newAllocation(appID2, "node-1", res)
	alloc2.placeholder = true
	app2.AddAllocation(alloc2)
	err = app2.AddAllocationAsk(newAllocationAsk("ask-0", appID1, res))
	assert.NilError(t, err, "could not add ask")
	phApps = leaf.sortApplications(true)
	assert.Equal(t, 2, len(phApps))
}

// This test must not test the sorter that is underlying.
// It tests the queue specific parts of the code only.
func TestSortQueue(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	var leaf, parent *Queue
	// empty parent queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	if len(parent.sortQueues()) != 0 {
		t.Errorf("parent queue should return sorted queues: %v", parent)
	}

	// leaf queue must be nil
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	if queues := leaf.sortQueues(); queues != nil {
		t.Errorf("leaf queue should return sorted queues: %v", queues)
	}
	if queues := parent.sortQueues(); len(queues) != 0 {
		t.Errorf("parent queue with leaf returned unexpectd queues: %v", queues)
	}
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	leaf.incPendingResource(res)
	if queues := parent.sortQueues(); len(queues) != 1 {
		t.Errorf("parent queue did not return expected queues: %v", queues)
	}
	err = leaf.handleQueueEvent(Stop)
	assert.NilError(t, err, "failed to stop queue")
	if queues := parent.sortQueues(); len(queues) != 0 {
		t.Errorf("parent queue returned stopped queue: %v", queues)
	}
}

func TestHeadroom(t *testing.T) {
	// create the root: nil test
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	headRoom := root.getHeadRoom()
	if headRoom != nil {
		t.Errorf("empty cluster with root queue should not have headroom: %v", headRoom)
	}

	var parent *Queue
	// empty parent queue: nil test
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	headRoom = parent.getHeadRoom()
	if headRoom != nil {
		t.Errorf("empty cluster with parent queue should not have headroom: %v", headRoom)
	}

	// recreate the structure to pick up changes on max etc
	// structure is:
	// root			max resource 20,10;	alloc 10,6	head 10,4
	// - parent		max resource 20,8;	alloc 10,6	head 10,2
	//   - leaf1	max resource ---;	alloc 5,3	head 10,2 * parent headroom used
	//   - leaf2	max resource ---;	alloc 5,3	head 10,2 * parent headroom used
	// set the max on the root
	resMap := map[string]string{"first": "20", "second": "10"}
	root, err = createRootQueue(resMap)
	assert.NilError(t, err, "failed to create root queue with limit")
	// set the max on the parent
	resMap = map[string]string{"first": "20", "second": "8"}
	parent, err = createManagedQueue(root, "parent", true, resMap)
	assert.NilError(t, err, "failed to create parent queue")
	// leaf1 queue no limit
	var leaf1, leaf2 *Queue
	leaf1, err = createManagedQueue(parent, "leaf1", false, nil)
	assert.NilError(t, err, "failed to create leaf1 queue")
	// leaf2 queue no limit
	leaf2, err = createManagedQueue(parent, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")

	// allocating and allocated
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "3"})
	assert.NilError(t, err, "failed to create resource")
	leaf1.IncAllocatedResource(res)
	leaf2.IncAllocatedResource(res)

	// headRoom root should be this (max 20-10 - alloc 10-6)
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10", "second": "4"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = root.getHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "root queue head room not as expected %v, got: %v", res, headRoom)

	maxHeadRoom := root.getMaxHeadRoom()
	assert.Assert(t, maxHeadRoom == nil, "root queue max headroom should be nil")

	// headRoom parent should be this (max 20-10 - alloc 8-6)
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10", "second": "2"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = parent.getHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "parent queue head room not as expected %v, got: %v", res, headRoom)
	maxHeadRoom = parent.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, maxHeadRoom), "parent queue max head room not as expected %v, got: %v", res, maxHeadRoom)

	// headRoom for any leaves will be at most the parent headroom
	// since leaf1 does not have a max it will be the same
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10", "second": "2"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = leaf1.getHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "leaf1 queue head room not as expected %v, got: %v", res, headRoom)
	maxHeadRoom = leaf1.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, maxHeadRoom), "leaf1 queue max head room not as expected %v, got: %v", res, maxHeadRoom)
	// since leaf2 does not have a max it will be the same
	headRoom = leaf2.getHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "leaf2 queue head room not as expected %v, got: %v", res, headRoom)
	maxHeadRoom = leaf2.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, maxHeadRoom), "leaf2 queue max head room not as expected %v, got: %v", res, maxHeadRoom)
}

func TestHeadroomMerge(t *testing.T) {
	// recreate the structure, set max capacity in parent and a leaf queue
	// structure is:
	// root (max: nil)               (alloc a:5, b:5, c:10, d:5)  (headroom: nil)
	//   - parent (max: a:20, b:10)  (alloc a:5, b:5, c:10, d:5)  (headroom a:15 b:5)
	//     - leaf1 (max: a:10, c:10) (alloc a:5, b:5, c:5)        (headroom a:5 b:5 c:5)
	//     - leaf2 (max: d:10)       (alloc c:5 d:5)              (headroom a:15 b:5 d:5)
	resMap := map[string]string{"first": "20", "second": "10"}
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	var parent, leaf1, leaf2 *Queue
	parent, err = createManagedQueue(root, "parent", true, resMap)
	assert.NilError(t, err, "failed to create parent queue")
	resMap = map[string]string{"first": "10", "third": "10"}
	leaf1, err = createManagedQueue(parent, "leaf1", false, resMap)
	assert.NilError(t, err, "failed to create leaf1 queue")
	resMap = map[string]string{"fourth": "10"}
	leaf2, err = createManagedQueue(parent, "leaf2", false, resMap)
	assert.NilError(t, err, "failed to create leaf2 queue")

	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "5", "third": "5"})
	assert.NilError(t, err, "failed to create resource")
	leaf1.IncAllocatedResource(res)
	res, err = resources.NewResourceFromConf(map[string]string{"third": "5", "fourth": "5"})
	assert.NilError(t, err, "failed to create resource")
	leaf2.IncAllocatedResource(res)

	// root headroom should be nil
	headRoom := root.getHeadRoom()
	assert.Assert(t, headRoom == nil, "headRoom of root should be nil because no max set")
	maxHeadRoom := root.getMaxHeadRoom()
	assert.Assert(t, maxHeadRoom == nil, "maxHeadRoom of root should be nil because no max")

	res, err = resources.NewResourceFromConf(map[string]string{"first": "15", "second": "5"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = parent.getHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "parent queue max head room not as expected %v, got: %v", res, headRoom)
	maxHeadRoom = parent.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, maxHeadRoom), "parent queue max head room not as expected %v, got: %v", res, maxHeadRoom)

	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "5", "third": "5"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = leaf1.getHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "leaf1 queue head room not as expected %v, got: %v", res, headRoom)
	maxHeadRoom = leaf1.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, maxHeadRoom), "leaf1 queue max head room not as expected %v, got: %v", res, maxHeadRoom)

	res, err = resources.NewResourceFromConf(map[string]string{"first": "15", "second": "5", "fourth": "5"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = leaf2.getHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "leaf2 queue head room not as expected %v, got: %v", res, headRoom)
	maxHeadRoom = leaf2.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, maxHeadRoom), "leaf2 queue max head room not as expected %v, got: %v", res, maxHeadRoom)
}

func TestMaxHeadroomNoMax(t *testing.T) {
	// create the root: nil test
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	headRoom := root.getMaxHeadRoom()
	if headRoom != nil {
		t.Errorf("empty cluster with root queue should not have headroom: %v", headRoom)
	}

	var parent *Queue
	// empty parent queue: nil test
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	headRoom = parent.getMaxHeadRoom()
	if headRoom != nil {
		t.Errorf("empty cluster with parent queue should not have headroom: %v", headRoom)
	}

	// recreate the structure, all queues have no max capacity set
	// structure is:
	// root (max: nil)
	//   - parent (max: nil)
	//     - leaf1 (max: nil)  (alloc: 5,3)
	//     - leaf2 (max: nil)  (alloc: 5,3)
	root, err = createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	var leaf1, leaf2 *Queue
	leaf1, err = createManagedQueue(parent, "leaf1", false, nil)
	assert.NilError(t, err, "failed to create leaf1 queue")
	leaf2, err = createManagedQueue(parent, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")

	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "3"})
	assert.NilError(t, err, "failed to create resource")
	leaf1.IncAllocatedResource(res)
	leaf2.IncAllocatedResource(res)

	headRoom = root.getMaxHeadRoom()
	assert.Assert(t, headRoom == nil, "headRoom of root should be nil because no max set for all queues")

	headRoom = parent.getMaxHeadRoom()
	assert.Assert(t, headRoom == nil, "headRoom of parent should be nil because no max set for all queues")

	headRoom = leaf1.getMaxHeadRoom()
	assert.Assert(t, headRoom == nil, "headRoom of leaf1 should be nil because no max set for all queues")

	headRoom = leaf2.getMaxHeadRoom()
	assert.Assert(t, headRoom == nil, "headRoom of leaf2 should be nil because no max set for all queues")
}

func TestMaxHeadroomMax(t *testing.T) {
	// recreate the structure, set max capacity in parent and a leaf queue
	// structure is:
	// root (max: nil)
	//   - parent (max: 20,8)
	//     - leaf1 (max: 10, 8)  (alloc: 5,3)
	//     - leaf2 (max: nil)    (alloc: 6,4)
	resMap := map[string]string{"first": "20", "second": "8"}
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	var parent, leaf1, leaf2 *Queue
	parent, err = createManagedQueue(root, "parent", true, resMap)
	assert.NilError(t, err, "failed to create parent queue")
	resMap = map[string]string{"first": "10", "second": "8"}
	leaf1, err = createManagedQueue(parent, "leaf1", false, resMap)
	assert.NilError(t, err, "failed to create leaf1 queue")
	leaf2, err = createManagedQueue(parent, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")

	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "3"})
	assert.NilError(t, err, "failed to create resource")
	leaf1.IncAllocatedResource(res)
	leaf2.IncAllocatedResource(res)

	// root headroom should be nil
	headRoom := root.getMaxHeadRoom()
	assert.Assert(t, headRoom == nil, "headRoom of root should be nil because no max set for all queues")

	// parent headroom = parentMax - leaf1Allocated - leaf2Allocated
	// parent headroom = (20 - 5 - 5, 8 - 3 - 3) = (10, 2)
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10", "second": "2"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = parent.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "parent queue head room not as expected %v, got: %v", res, headRoom)

	// leaf1 headroom = MIN(parentHeadRoom, leaf1Max - leaf1Allocated)
	// leaf1 headroom = MIN((10,2), (10-5, 8-3)) = MIN((10,2), (5,5)) = MIN(5, 2)
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "2"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = leaf1.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "leaf1 queue head room not as expected %v, got: %v", res, headRoom)

	// leaf2 headroom = parentMax - leaf1Allocated - leaf2Allocated
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10", "second": "2"})
	assert.NilError(t, err, "failed to create resource")
	headRoom = leaf2.getMaxHeadRoom()
	assert.Assert(t, resources.Equals(res, headRoom), "leaf2 queue head room not as expected %v, got: %v", res, headRoom)
}

// nolint: funlen
func TestGetFairMaxResource(t *testing.T) {
	tests := []struct {
		name             string
		RootResource     map[string]string
		ParentResource   map[string]string
		Tier0Resource    map[string]string
		Tier0Expectation map[string]string
		Tier1Resource    map[string]string
		Tier1Expectation map[string]string
	}{
		{
			name:             "children provide overrides for resources types",
			RootResource:     map[string]string{"vcore": "1000m"},
			ParentResource:   map[string]string{},
			Tier0Resource:    map[string]string{"vcore": "800m"},
			Tier0Expectation: map[string]string{"vcore": "800m"},
			Tier1Resource:    map[string]string{"vcore": "1200m"},
			Tier1Expectation: map[string]string{"vcore": "1200m"},
		},
		{
			name:             "0's in the root are ommitted. there is no ephemeral-storage available on this cluster",
			RootResource:     map[string]string{"ephemeral-storage": "0", "memory": "1000", "pods": "1000", "vcore": "1000m"},
			ParentResource:   map[string]string{},
			Tier0Resource:    map[string]string{},
			Tier0Expectation: map[string]string{"memory": "1000", "pods": "1000", "vcore": "1000m"},
			Tier1Resource:    map[string]string{"vcore": "900m"},
			Tier1Expectation: map[string]string{"memory": "1000", "pods": "1000", "vcore": "900m"},
		},
		{
			name:             "children provide maximum for resource type that do NOT exist on root queue currently but may later because of autoscaling",
			RootResource:     map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "1000m"},
			ParentResource:   map[string]string{},
			Tier0Resource:    map[string]string{"nvidia.com/gpu": "100", "vcore": "800m"},
			Tier0Expectation: map[string]string{"nvidia.com/gpu": "100", "ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "800m"},
			Tier1Resource:    map[string]string{},
			Tier1Expectation: map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "1000m"},
		},
		{
			name:             "this is true even if they are on the root queue but are currently zero",
			RootResource:     map[string]string{"nvidia.com/gpu": "0", "ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "0m"},
			ParentResource:   map[string]string{},
			Tier0Resource:    map[string]string{"nvidia.com/gpu": "100", "vcore": "800m"},
			Tier0Expectation: map[string]string{"nvidia.com/gpu": "100", "ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "800m"},
			Tier1Resource:    map[string]string{},
			Tier1Expectation: map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000"},
		},
		{
			name:             "multiple level restrictions",
			RootResource:     map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "1000m"},
			ParentResource:   map[string]string{"vcore": "900m"},
			Tier0Resource:    map[string]string{"vcore": "800m"},
			Tier0Expectation: map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "800m"},
			Tier1Resource:    map[string]string{},
			Tier1Expectation: map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "900m"},
		},
		{
			name:             "explicity 0's are honored for non-root queues",
			RootResource:     map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "1000m"},
			ParentResource:   map[string]string{},
			Tier0Resource:    map[string]string{"ephemeral-storage": "1000", "vcore": "0m"},
			Tier0Expectation: map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "0m"},
			Tier1Resource:    map[string]string{},
			Tier1Expectation: map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "1000m"},
		},

		{
			name:             "nil root resources( no nodes in cluster)",
			RootResource:     nil,
			ParentResource:   map[string]string{},
			Tier0Resource:    map[string]string{},
			Tier0Expectation: nil,
			Tier1Resource:    map[string]string{},
			Tier1Expectation: nil,
		},
		{
			name:             "nil parent resources",
			RootResource:     map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "1000m"},
			ParentResource:   nil,
			Tier0Resource:    map[string]string{"ephemeral-storage": "1000", "vcore": "0m"},
			Tier0Expectation: map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "0m"},
			Tier1Resource:    map[string]string{},
			Tier1Expectation: map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "1000m"},
		},
		{
			name:             "nil leaf resources",
			RootResource:     map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "1000m"},
			ParentResource:   map[string]string{},
			Tier0Resource:    map[string]string{"ephemeral-storage": "1000", "vcore": "0m"},
			Tier0Expectation: map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "0m"},
			Tier1Resource:    nil,
			Tier1Expectation: map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "1000m"},
		},
		{
			name:             "parent max with type different than child",
			RootResource:     map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "1000m"},
			ParentResource:   map[string]string{"nvidia.com/gpu": "100"},
			Tier0Resource:    map[string]string{"vcore": "800m"},
			Tier0Expectation: map[string]string{"nvidia.com/gpu": "100", "ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "800m"},
			Tier1Resource:    map[string]string{},
			Tier1Expectation: map[string]string{"nvidia.com/gpu": "100", "ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "1000m"},
		},
		{
			name:             "parent explicit 0 limit for type not set in child",
			RootResource:     map[string]string{"ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "1000m"},
			ParentResource:   map[string]string{"nvidia.com/gpu": "0"},
			Tier0Resource:    map[string]string{"vcore": "800m"},
			Tier0Expectation: map[string]string{"nvidia.com/gpu": "0", "ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "800m"},
			Tier1Resource:    map[string]string{},
			Tier1Expectation: map[string]string{"nvidia.com/gpu": "0", "ephemeral-storage": "1000", "memory": "1000", "pods": "1000", "vcore": "1000m"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// create root
			root, err := createRootQueue(tc.RootResource)
			assert.NilError(t, err, "queue create failed")

			// create parent
			parent, err := createManagedQueue(root, "parent", true, tc.ParentResource)
			assert.NilError(t, err, "failed to create 'parent' queue")

			// create tier0
			tier0, err := createManagedQueue(parent, "tier0", true, tc.Tier0Resource)
			assert.NilError(t, err, "failed to create 'tier0' queue")

			// create tier1
			tier1, err := createManagedQueue(parent, "tier1", true, tc.Tier1Resource)
			assert.NilError(t, err, "failed to create 'tier1' queue")

			if tc.Tier0Expectation != nil {
				actualTier0Max := tier0.GetFairMaxResource()
				expectedTier0Max, err := resources.NewResourceFromConf(tc.Tier0Expectation)
				assert.NilError(t, err, "failed to create resource")

				if !resources.Equals(expectedTier0Max, actualTier0Max) {
					t.Errorf("root.parent.tier0 queue expected max %v, got: %v", expectedTier0Max, actualTier0Max)
				}
			}
			if tc.Tier1Expectation != nil {
				actualTier1Max := tier1.GetFairMaxResource()
				expectedTier1Max, err := resources.NewResourceFromConf(tc.Tier1Expectation)
				assert.NilError(t, err, "failed to create resource")

				if !resources.Equals(expectedTier1Max, actualTier1Max) {
					t.Errorf("root.parent.tier1 queue expected max %v, got: %v", expectedTier1Max, actualTier1Max)
				}
			}
		})
	}
}

func TestGetMaxResource(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	maxUsage := root.GetMaxResource()
	if maxUsage != nil {
		t.Errorf("empty cluster with root queue should not have max set: %v", maxUsage)
	}

	var parent *Queue
	// empty parent queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	maxUsage = parent.GetMaxResource()
	if maxUsage != nil {
		t.Errorf("empty cluster parent queue should not have max set: %v", maxUsage)
	}

	// set the max on the root: recreate the structure to pick up changes
	var res *resources.Resource
	resMap := map[string]string{"first": "10", "second": "5"}
	res, err = resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create resource")
	root, err = createRootQueue(resMap)
	assert.NilError(t, err, "failed to create root queue with limit")
	maxUsage = root.GetMaxResource()
	if !resources.Equals(res, maxUsage) {
		t.Errorf("root queue should have max set expected %v, got: %v", res, maxUsage)
	}
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	maxUsage = parent.GetMaxResource()
	if !resources.Equals(res, maxUsage) {
		t.Errorf("parent queue should have max from root set expected %v, got: %v", res, maxUsage)
	}

	// leaf queue with limit: contrary to root should get min from both merged
	var leaf *Queue
	resMap = map[string]string{"first": "5", "second": "10"}
	leaf, err = createManagedQueue(parent, "leaf", false, resMap)
	assert.NilError(t, err, "failed to create leaf queue")
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "5"})
	assert.NilError(t, err, "failed to create resource")
	maxUsage = leaf.GetMaxResource()
	assert.Assert(t, resources.Equals(res, maxUsage), "leaf queue should have merged max set expected %v, got: %v", res, maxUsage)

	// replace parent with one with limit on different resource
	resMap = map[string]string{"third": "2"}
	parent, err = createManagedQueue(root, "parent2", true, resMap)
	assert.NilError(t, err, "failed to create parent2 queue")
	res, err = resources.NewResourceFromConf(map[string]string{"first": "10", "second": "5", "third": "2"})
	assert.NilError(t, err, "failed to create resource")
	maxUsage = parent.GetMaxResource()
	assert.Assert(t, resources.Equals(res, maxUsage), "parent2 queue should have max from root set expected %v, got: %v", res, maxUsage)
	resMap = map[string]string{"first": "5", "second": "10"}
	leaf, err = createManagedQueue(parent, "leaf2", false, resMap)
	assert.NilError(t, err, "failed to create leaf2 queue")
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "5", "third": "2"})
	assert.NilError(t, err, "failed to create resource")
	maxUsage = leaf.GetMaxResource()
	assert.Assert(t, resources.Equals(res, maxUsage), "leaf2 queue should have reset merged max set expected %v, got: %v", res, maxUsage)
}

func TestGetMaxQueueSet(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var nilRes *resources.Resource
	assert.Equal(t, root.GetMaxQueueSet(), nilRes, "root queue should always return max set nil")

	var parent *Queue
	// empty parent queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Equal(t, parent.GetMaxQueueSet(), nilRes, "parent queue should not have max")

	// set the max on the root: recreate the structure to pick up changes
	resMap := map[string]string{"first": "10", "second": "10"}
	assert.NilError(t, err, "failed to create resource")
	root, err = createRootQueue(resMap)
	assert.NilError(t, err, "failed to create root queue with limit set")
	assert.Equal(t, root.GetMaxQueueSet(), nilRes, "root queue should always return max set nil")
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Equal(t, parent.GetMaxQueueSet(), nilRes, "parent queue should not have max even with root set")

	// leaf queue with limit
	// parent has no limit and root is ignored: expect the leaf limit returned
	var leaf *Queue
	resMap = map[string]string{"first": "5", "second": "5"}
	leaf, err = createManagedQueue(parent, "leaf", false, resMap)
	assert.NilError(t, err, "failed to create leaf queue")
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create resource")
	maxSet := leaf.GetMaxQueueSet()
	assert.Assert(t, resources.Equals(res, maxSet), "leaf queue should have max set expected %v, got: %v", res, maxSet)

	// replace parent with one with limit on multiple resource
	resMap = map[string]string{"second": "5", "third": "2"}
	parent, err = createManagedQueue(root, "parent2", true, resMap)
	assert.NilError(t, err, "failed to create parent2 queue")
	res, err = resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create resource")
	maxSet = parent.GetMaxQueueSet()
	assert.Assert(t, resources.Equals(res, maxSet), "parent2 queue should have max excluding root expected %v, got: %v", res, maxSet)

	// a leaf with max set on different resource than the parent.
	// The parent has limit and root is ignored: expect the merged parent and leaf to be returned
	resMap = map[string]string{"first": "5", "second": "10"}
	leaf, err = createManagedQueue(parent, "leaf2", false, resMap)
	assert.NilError(t, err, "failed to create leaf2 queue")
	res, err = resources.NewResourceFromConf(map[string]string{"first": "5", "second": "5", "third": "2"})
	assert.NilError(t, err, "failed to create resource")
	maxSet = leaf.GetMaxQueueSet()
	assert.Assert(t, resources.Equals(res, maxSet), "leaf2 queue should have reset merged max set expected %v, got: %v", res, maxSet)
}

func TestReserveApp(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, len(leaf.reservedApps), 0, "new queue should not have reserved apps")
	// no checks this works for everything
	appName := "something"
	leaf.Reserve(appName)
	assert.Equal(t, len(leaf.reservedApps), 1, "app should have been reserved")
	assert.Equal(t, leaf.reservedApps[appName], 1, "app should have one reservation")
	leaf.Reserve(appName)
	assert.Equal(t, leaf.reservedApps[appName], 2, "app should have two reservations")
	leaf.UnReserve(appName, 2)
	assert.Equal(t, len(leaf.reservedApps), 0, "queue should not have any reserved apps, all reservations were removed")

	leaf.UnReserve("unknown", 1)
	assert.Equal(t, len(leaf.reservedApps), 0, "unreserve of unknown app should not have changed count or added app")
}

func TestGetApp(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	// check for init of the map
	if unknown := leaf.GetApplication("unknown"); unknown != nil {
		t.Errorf("un registered app found using appID which should not happen: %v", unknown)
	}

	// add app and check proper returns
	app := newApplication(appID1, "default", leaf.QueuePath)
	leaf.AddApplication(app)
	assert.Equal(t, len(leaf.applications), 1, "queue should have one app registered")
	if leaf.GetApplication(appID1) == nil {
		t.Errorf("registered app not found using appID")
	}
	if unknown := leaf.GetApplication("unknown"); unknown != nil {
		t.Errorf("un registered app found using appID which should not happen: %v", unknown)
	}
}

func TestIsEmpty(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	assert.Equal(t, root.IsEmpty(), true, "new root queue should have been empty")
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, root.IsEmpty(), false, "root queue with child leaf should not have been empty")
	assert.Equal(t, leaf.IsEmpty(), true, "new leaf should have been empty")

	// add app and check proper returns
	app := newApplication(appID1, "default", leaf.QueuePath)
	leaf.AddApplication(app)
	assert.Equal(t, leaf.IsEmpty(), false, "queue with registered app should not be empty")
}

func TestGetOutstandingRequestMax(t *testing.T) {
	// queue structure:
	// root (max.cpu = 5)
	//   - queue1 (max.cpu = 10)
	//   - queue2 (max.cpu = 5)
	//
	// submit app1 to root.queue1, app2 to root.queue2
	// app1 asks for 20 1x1CPU requests, app2 asks for 20 1x1CPU requests
	// verify the outstanding requests for each of the queue is up to its max capacity
	// root max is irrelevant for the calculation
	// root: 15, queue1: 10 and queue2: 5
	// add an allocation for 5 CPU to queue1 and check the reduced numbers
	// root: 10, queue1: 5 and queue2: 5
	testOutstanding(t, map[string]string{"cpu": "1"}, map[string]string{"cpu": "5"})
}

func TestGetOutstandingUntracked(t *testing.T) {
	// same test as TestGetOutstandingRequestMax but adding an unlimited resource to the
	// allocations to make sure it does not affect the calculations
	// queue structure:
	// root (max.cpu = 5, pods = 10)
	//   - queue1 (max.cpu = 10)
	//   - queue2 (max.cpu = 5)
	//
	// submit app1 to root.queue1, app2 to root.queue2
	// app1 asks for 20 1x1CPU, 1xPOD requests, app2 asks for 20 1x1CPU, 1xPOD requests
	// verify the outstanding requests for each of the queue is up to its max capacity
	// root max is irrelevant for the calculation
	// root: 15, queue1: 10 and queue2: 5
	// add an allocation for 5 CPU to queue1 and check the reduced numbers
	// root: 10, queue1: 5 and queue2: 5
	testOutstanding(t, map[string]string{"cpu": "1", "pods": "2"}, map[string]string{"cpu": "5", "pods": "10"})
}

func testOutstanding(t *testing.T, allocMap, usedMap map[string]string) {
	root, err := createRootQueue(usedMap)
	var queue1, queue2 *Queue
	assert.NilError(t, err, "failed to create root queue with limit")
	queue1, err = createManagedQueue(root, "queue1", false, map[string]string{"cpu": "10"})
	assert.NilError(t, err, "failed to create queue1 queue")
	queue2, err = createManagedQueue(root, "queue2", false, map[string]string{"cpu": "5"})
	assert.NilError(t, err, "failed to create queue2 queue")

	var allocRes, usedRes *resources.Resource
	allocRes, err = resources.NewResourceFromConf(allocMap)
	assert.NilError(t, err, "failed to create basic resource")
	usedRes, err = resources.NewResourceFromConf(usedMap)
	assert.NilError(t, err, "failed to create basic resource")

	app1 := newApplication(appID1, "default", "root.queue1")
	app1.queue = queue1
	queue1.AddApplication(app1)
	for i := 0; i < 20; i++ {
		ask := newAllocationAsk(fmt.Sprintf("alloc-%d", i), appID1, allocRes)
		ask.SetSchedulingAttempted(true)
		err = app1.AddAllocationAsk(ask)
		assert.NilError(t, err, "failed to add allocation ask")
	}

	app2 := newApplication(appID2, "default", "root.queue2")
	app2.queue = queue2
	queue2.AddApplication(app2)
	for i := 0; i < 20; i++ {
		ask := newAllocationAsk(fmt.Sprintf("alloc-%d", i), appID2, allocRes)
		ask.SetSchedulingAttempted(true)
		err = app2.AddAllocationAsk(ask)
		assert.NilError(t, err, "failed to add allocation ask")
	}

	// verify get outstanding requests for root, and child queues
	rootTotal := root.GetOutstandingRequests()
	assert.Equal(t, len(rootTotal), 15)

	queue1Total := make([]*Allocation, 0)
	queue1.getOutStandingRequestsInternal(resources.NewResource(), &queue1Total)
	assert.Equal(t, len(queue1Total), 10)

	queue2Total := make([]*Allocation, 0)
	queue2.getOutStandingRequestsInternal(resources.NewResource(), &queue2Total)
	assert.Equal(t, len(queue2Total), 5)

	// simulate queue1 has some allocated resources
	err = queue1.TryIncAllocatedResource(usedRes)
	assert.NilError(t, err, "failed to increment allocated resources")

	queue1Total = make([]*Allocation, 0)
	queue1.getOutStandingRequestsInternal(resources.NewResource(), &queue1Total)
	assert.Equal(t, len(queue1Total), 5)

	queue2Total = make([]*Allocation, 0)
	queue2.getOutStandingRequestsInternal(resources.NewResource(), &queue2Total)
	assert.Equal(t, len(queue2Total), 5)

	rootTotal = root.GetOutstandingRequests()
	assert.Equal(t, len(rootTotal), 10)

	// remove app2 from queue2
	queue2.RemoveApplication(app2)
	queue2Total = make([]*Allocation, 0)
	queue2.getOutStandingRequestsInternal(resources.NewResource(), &queue2Total)
	assert.Equal(t, len(queue2Total), 0)

	rootTotal = root.GetOutstandingRequests()
	assert.Equal(t, len(rootTotal), 5)

	// test for non-root queue
	assert.Assert(t, queue1.GetOutstandingRequests() == nil)
}

func TestGetOutstandingOnlyUntracked(t *testing.T) {
	// all outstanding pods use only an unlimited resource type
	// max is set for a different resource type and fully allocated
	// queue structure:
	// root (max.cpu = 10, pods: 10)
	//   - queue1 (max.cpu = 10)
	//
	// submit app1 to root.queue1, app1 asks for 20 1xPOD requests
	// verify the outstanding requests of the queue is all outstanding requests
	// add an allocation that uses all limited resources
	// verify the outstanding requests of the queue is still all outstanding requests
	alloc, err := resources.NewResourceFromConf(map[string]string{"pods": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	var used *resources.Resource
	usedMap := map[string]string{"cpu": "10", "pods": "10"}
	used, err = resources.NewResourceFromConf(usedMap)
	assert.NilError(t, err, "failed to create basic resource")
	var root, queue1 *Queue
	root, err = createRootQueue(usedMap)
	assert.NilError(t, err, "failed to create root queue with limit")
	queue1, err = createManagedQueue(root, "queue1", false, map[string]string{"cpu": "10"})
	assert.NilError(t, err, "failed to create queue1 queue")

	app1 := newApplication(appID1, "default", "root.queue1")
	app1.queue = queue1
	queue1.AddApplication(app1)
	for i := 0; i < 20; i++ {
		ask := newAllocationAsk(fmt.Sprintf("alloc-%d", i), appID1, alloc)
		ask.SetSchedulingAttempted(true)
		err = app1.AddAllocationAsk(ask)
		assert.NilError(t, err, "failed to add allocation ask")
	}

	// verify get outstanding requests for root, and child queues
	rootTotal := root.GetOutstandingRequests()
	assert.Equal(t, len(rootTotal), 20)

	queue1Total := make([]*Allocation, 0)
	queue1.getOutStandingRequestsInternal(resources.NewResource(), &queue1Total)
	assert.Equal(t, len(queue1Total), 20)

	// simulate queue1 has some allocated resources
	// after allocation, the max available becomes to be 5
	err = queue1.TryIncAllocatedResource(used)
	assert.NilError(t, err, "failed to increment allocated resources")

	queue1Total = make([]*Allocation, 0)
	queue1.getOutStandingRequestsInternal(resources.NewResource(), &queue1Total)
	assert.Equal(t, len(queue1Total), 20)
	headRoom := queue1.getHeadRoom()
	assert.Assert(t, resources.IsZero(headRoom), "headroom should have been zero")

	rootTotal = root.GetOutstandingRequests()
	assert.Equal(t, len(rootTotal), 20)
	headRoom = root.getHeadRoom()
	assert.Assert(t, resources.IsZero(headRoom), "headroom should have been zero")
}

func TestGetOutstandingRequestNoMax(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	var queue1, queue2 *Queue
	queue1, err = createManagedQueue(root, "queue1", false, nil)
	assert.NilError(t, err, "failed to create queue1 queue")
	queue2, err = createManagedQueue(root, "queue2", false, nil)
	assert.NilError(t, err, "failed to create queue2 queue")

	app1 := newApplication(appID1, "default", "root.queue1")
	app1.queue = queue1
	queue1.AddApplication(app1)
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"cpu": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	for i := 0; i < 10; i++ {
		ask := newAllocationAsk(fmt.Sprintf("alloc-%d", i), appID1, res)
		ask.SetSchedulingAttempted(true)
		err = app1.AddAllocationAsk(ask)
		assert.NilError(t, err, "failed to add allocation ask")
	}

	app2 := newApplication(appID2, "default", "root.queue2")
	app2.queue = queue2
	queue2.AddApplication(app2)
	for i := 0; i < 20; i++ {
		ask := newAllocationAsk(fmt.Sprintf("alloc-%d", i), appID2, res)
		ask.SetSchedulingAttempted(true)
		err = app2.AddAllocationAsk(ask)
		assert.NilError(t, err, "failed to add allocation ask")
	}

	rootTotal := root.GetOutstandingRequests()
	assert.Equal(t, len(rootTotal), 30)

	queue1Total := make([]*Allocation, 0)
	queue1.getOutStandingRequestsInternal(resources.NewResource(), &queue1Total)
	assert.Equal(t, len(queue1Total), 10)

	queue2Total := make([]*Allocation, 0)
	queue2.getOutStandingRequestsInternal(resources.NewResource(), &queue2Total)
	assert.Equal(t, len(queue2Total), 20)
}

func TestOutstandingMultipleApps(t *testing.T) {
	root, err := createRootQueue(map[string]string{"memory": "10"})
	assert.NilError(t, err, "failed to create root queue with limit")
	leaf, err := createManagedQueue(root, "leaf", false, map[string]string{"memory": "10"})
	assert.NilError(t, err, "failed to create leaf")

	used, err := resources.NewResourceFromConf(map[string]string{"memory": "8"})
	assert.NilError(t, err, "failed to create basic resource")

	err = leaf.TryIncAllocatedResource(used)
	assert.NilError(t, err, "failed to increment allocated resources")
	headRoom := leaf.getMaxHeadRoom()
	assert.Assert(t, headRoom != nil, "headroom should not be nil")
	expectedHeadroom, err := resources.NewResourceFromConf(map[string]string{"memory": "2"})
	assert.NilError(t, err, "failed to create basic resource")
	assert.Assert(t, resources.Equals(headRoom, expectedHeadroom), "headRoom is %s instead of %s", headRoom, expectedHeadroom)

	appRes, err := resources.NewResourceFromConf(map[string]string{"memory": "1"})
	assert.NilError(t, err, "failed to create request resource")
	for i := 0; i < 10; i++ {
		appID := "app-" + strconv.Itoa(i)
		app := newApplication(appID, "default", "root.leaf")
		app.queue = leaf
		leaf.AddApplication(app)

		ask := newAllocationAsk(fmt.Sprintf("alloc-%d", i), appID, appRes)
		ask.SetSchedulingAttempted(true)
		err = app.AddAllocationAsk(ask)
		assert.NilError(t, err, "failed to add allocation ask")
	}

	rootTotal := root.GetOutstandingRequests()
	assert.Equal(t, len(rootTotal), 2)

	leafTotal := make([]*Allocation, 0)
	leaf.getOutStandingRequestsInternal(nil, &leafTotal)
	assert.Equal(t, len(rootTotal), 2)
}

// checks proper headroom calculation with multiple leafs
// with 3 pending asks, only 2 of them are expected to be collected
func TestOutStandingRequestMultipleChildrenWithMax(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue")

	parent, err := createManagedQueue(root, "parent", true, map[string]string{
		"memory": "6",
	})
	assert.NilError(t, err, "failed to create parent queue")

	leaf1, err := createManagedQueue(parent, "leaf1", false, map[string]string{
		"memory": "5",
	})
	assert.NilError(t, err, "failed to create leaf1 queue")
	leaf2, err := createManagedQueue(parent, "leaf2", false, map[string]string{
		"memory": "5",
	})
	assert.NilError(t, err, "failed to create leaf2 queue")

	allocatedRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 2,
	})
	askRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 1,
	})
	leaf1App := newApplication("app-leaf1", "default", "root.parent.leaf1")
	leaf1App.SetQueue(leaf1)
	leaf1.AddApplication(leaf1App)
	leaf1.IncAllocatedResource(allocatedRes)
	// use priority = 1000 for this ask to force ordering of queues when sorting
	askLeaf1 := newAllocationAskAll("ask-leaf1", "app-leaf1", "", askRes, false, 1000)
	askLeaf1.SetSchedulingAttempted(true)
	err = leaf1App.AddAllocationAsk(askLeaf1)
	assert.NilError(t, err, "could not add ask")

	leaf2App := newApplication("app-leaf2", "default", "root.parent.leaf2")
	leaf2App.SetQueue(leaf2)
	ask1Leaf2 := newAllocationAsk("ask1-leaf2", "app-leaf2", askRes)
	ask1Leaf2.SetSchedulingAttempted(true)
	err = leaf2App.AddAllocationAsk(ask1Leaf2)
	assert.NilError(t, err, "could not add ask")
	ask2Leaf2 := newAllocationAsk("ask1-leaf2", "app-leaf2", askRes)
	ask2Leaf2.SetSchedulingAttempted(true)
	err = leaf2App.AddAllocationAsk(ask2Leaf2)
	assert.NilError(t, err, "could not add ask")
	leaf2.AddApplication(leaf2App)
	leaf2.IncAllocatedResource(allocatedRes)

	outstanding := root.GetOutstandingRequests()
	assert.Equal(t, 2, len(outstanding), "expected 2 outstanding requests to be collected")
	assert.Equal(t, "ask-leaf1", outstanding[0].allocationKey)
	assert.Equal(t, "ask1-leaf2", outstanding[1].allocationKey)
}

func TestAllocationCalcRoot(t *testing.T) {
	resMap := map[string]string{"memory": "100", "vcores": "10"}
	// create the root: must set a max on the queue
	root, err := createRootQueue(resMap)
	assert.NilError(t, err, "failed to create basic root queue")
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create basic resource")
	err = root.TryIncAllocatedResource(res)
	assert.NilError(t, err, "root queue allocation failed on increment")
	// increment again should fail
	err = root.TryIncAllocatedResource(res)
	if err == nil {
		t.Error("root queue allocation should have failed to increment (max hit)")
	}
	err = root.DecAllocatedResource(res)
	assert.NilError(t, err, "root queue allocation failed on decrement")
	if !resources.IsZero(root.allocatedResource) {
		t.Errorf("root queue allocations are not zero: %v", root.allocatedResource)
	}
	err = root.DecAllocatedResource(res)
	if err == nil {
		t.Error("root queue allocation should have failed to decrement")
	}
}

func TestAllocationCalcSub(t *testing.T) {
	resMap := map[string]string{"memory": "100", "vcores": "10"}
	// create the root
	root, err := createRootQueue(resMap)
	assert.NilError(t, err, "failed to create basic root queue")
	var parent *Queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")

	var res *resources.Resource
	res, err = resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create basic resource")
	err = parent.TryIncAllocatedResource(res)
	assert.NilError(t, err, "parent queue allocation failed on increment")
	// increment again should fail
	err = parent.TryIncAllocatedResource(res)
	if err == nil {
		t.Error("parent queue allocation should have failed to increment (root max hit)")
	}
	err = parent.DecAllocatedResource(res)
	assert.NilError(t, err, "parent queue allocation failed on decrement")
	if !resources.IsZero(root.allocatedResource) {
		t.Errorf("root queue allocations are not zero: %v", root.allocatedResource)
	}
	err = root.DecAllocatedResource(res)
	if err == nil {
		t.Error("root queue allocation should have failed to decrement")
	}

	// add to the parent, remove from root and then try to remove from parent: root should complain
	err = parent.TryIncAllocatedResource(res)
	assert.NilError(t, err, "parent queue allocation failed on increment")
	err = root.DecAllocatedResource(res)
	assert.NilError(t, err, "root queue allocation failed on decrement")
	err = parent.DecAllocatedResource(res)
	if err == nil {
		t.Errorf("parent queue allocation should have failed on decrement %v, %v", root.allocatedResource, parent.allocatedResource)
	}
}

func TestQueueProps(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	var parent *Queue
	props := map[string]string{"first": "value", "second": "other value"}
	parent, err = createManagedQueueWithProps(root, "parent", true, nil, props)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Assert(t, !parent.isLeaf, "parent queue is not marked as a parent")
	assert.Equal(t, len(root.children), 1, "parent queue is not added to the root queue")
	assert.Equal(t, len(parent.properties), 2, "parent queue properties expected 2, got %v", parent.properties)

	var leaf *Queue
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, len(parent.children), 1, "leaf queue is not added to the parent queue")
	assert.Assert(t, leaf.isLeaf && leaf.isManaged, "leaf queue is not marked as managed leaf")
	assert.Equal(t, len(leaf.properties), 2, "leaf queue properties size incorrect")

	props = map[string]string{}
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, props)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.priorityPolicy, policies.DefaultPriorityPolicy)
	assert.Equal(t, leaf.priorityOffset, int32(0))
	assert.Equal(t, leaf.prioritySortEnabled, true)

	props = map[string]string{"priority.policy": "default", "priority.offset": "3", "application.sort.priority": "enabled"}
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, props)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.priorityPolicy, policies.DefaultPriorityPolicy)
	assert.Equal(t, leaf.priorityOffset, int32(3))
	assert.Equal(t, leaf.prioritySortEnabled, true)

	props = map[string]string{"priority.policy": "fence", "priority.offset": "-3", "application.sort.priority": "disabled"}
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, props)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.priorityPolicy, policies.FencePriorityPolicy)
	assert.Equal(t, leaf.priorityOffset, int32(-3))
	assert.Equal(t, leaf.prioritySortEnabled, false)

	props = map[string]string{"priority.policy": "invalid", "priority.offset": "invalid", "application.sort.priority": "invalid"}
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, props)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.priorityPolicy, policies.DefaultPriorityPolicy)
	assert.Equal(t, leaf.priorityOffset, int32(0))
	assert.Equal(t, leaf.prioritySortEnabled, true)

	props = map[string]string{"preemption.policy": "default", "preemption.delay": "10s"}
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, props)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.preemptionPolicy, policies.DefaultPreemptionPolicy)
	assert.Equal(t, leaf.preemptionDelay, time.Second*10)

	props = map[string]string{"preemption.policy": "disabled", "preemption.delay": "-1s"}
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, props)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.preemptionPolicy, policies.DisabledPreemptionPolicy)
	assert.Equal(t, leaf.preemptionDelay, time.Second*30)

	props = map[string]string{"preemption.policy": "fence", "preemption.delay": "xxxx"}
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, props)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.preemptionPolicy, policies.FencePreemptionPolicy)
	assert.Equal(t, leaf.preemptionDelay, time.Second*30)

	props = map[string]string{"preemption.policy": "invalid"}
	leaf, err = createManagedQueueWithProps(parent, "leaf", false, nil, props)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.preemptionPolicy, policies.DefaultPreemptionPolicy)
}

func TestInheritedQueueProps(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	var parent *Queue
	props := map[string]string{
		"key":               "value",
		"priority.policy":   "fence",
		"priority.offset":   "100",
		"preemption.policy": "fence",
	}
	parent, err = createManagedQueueWithProps(root, "parent", true, nil, props)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Equal(t, parent.properties["key"], "value")
	assert.Equal(t, parent.properties["priority.policy"], "fence")
	assert.Equal(t, parent.properties["priority.offset"], "100")
	assert.Equal(t, parent.properties["preemption.policy"], "fence")
	assert.Equal(t, parent.priorityPolicy, policies.FencePriorityPolicy)
	assert.Equal(t, parent.priorityOffset, int32(100))
	assert.Equal(t, parent.preemptionPolicy, policies.FencePreemptionPolicy)

	var leaf *Queue
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.properties["key"], "value")
	assert.Equal(t, leaf.properties["priority.policy"], "default")
	assert.Equal(t, leaf.properties["priority.offset"], "0")
	assert.Equal(t, leaf.priorityPolicy, policies.DefaultPriorityPolicy)
	assert.Equal(t, leaf.priorityOffset, int32(0))
	assert.Equal(t, leaf.preemptionPolicy, policies.DefaultPreemptionPolicy)

	props = map[string]string{
		"preemption.policy": "disabled",
	}

	parent, err = createManagedQueueWithProps(root, "parent", true, nil, props)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Equal(t, parent.preemptionPolicy, policies.DisabledPreemptionPolicy)

	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, leaf.preemptionPolicy, policies.DisabledPreemptionPolicy)
}

func TestMaxResource(t *testing.T) {
	resMap := map[string]string{"first": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create basic resource")
	// create the root
	var root, parent *Queue
	root, err = createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create basic managed parent queue")
	// Nothing set max should be nil
	if root.GetMaxResource() != nil || parent.GetMaxResource() != nil {
		t.Errorf("empty cluster should not have max set on root queue")
	}
	// try setting on the parent (nothing should change)
	parent.SetMaxResource(res)
	if parent.GetMaxResource() != nil {
		t.Errorf("parent queue change should have been rejected parent: %v", parent.GetMaxResource())
	}
	// Set on the root should change
	root.SetMaxResource(res)
	if !resources.Equals(res, root.GetMaxResource()) {
		t.Errorf("root max setting not picked up by parent queue expected %v, got %v", res, parent.GetMaxResource())
	}
}

func TestSupportTaskGroup(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue: %v", err)
	assert.Assert(t, !root.SupportTaskGroup(), "root queue should not support task group (parent)")

	// parent with sort policy set
	properties := map[string]string{configs.ApplicationSortPolicy: "fifo"}
	var parent *Queue
	parent, err = createManagedQueueWithProps(root, "parent", true, nil, properties)
	assert.NilError(t, err, "failed to create queue: %v", err)
	assert.Assert(t, !parent.SupportTaskGroup(), "parent queue (FIFO policy) should not support task group")

	var leaf *Queue
	leaf, err = createManagedQueueWithProps(parent, "leaf1", false, nil, properties)
	assert.NilError(t, err, "failed to create queue: %v", err)
	assert.Assert(t, leaf.SupportTaskGroup(), "leaf queue (FIFO policy) should support task group")

	properties = map[string]string{configs.ApplicationSortPolicy: "fair"}
	leaf, err = createManagedQueueWithProps(parent, "leaf3", false, nil, properties)
	assert.NilError(t, err, "failed to create queue: %v", err)
	assert.Assert(t, !leaf.SupportTaskGroup(), "leaf queue (FAIR policy) should not support task group")
}

func TestGetPartitionQueueDAOInfo(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue: %v", err)

	// test properties
	root.properties = getProperties()
	assert.DeepEqual(t, root.properties, root.GetPartitionQueueDAOInfo(true).Properties)

	// test template
	root.template, err = template.FromConf(&configs.ChildTemplate{
		MaxApplications: uint64(1),
		Properties:      getProperties(),
		Resources: configs.Resources{
			Max:        getResourceConf(),
			Guaranteed: getResourceConf(),
		},
	})
	assert.NilError(t, err)
	rootDAO := root.GetPartitionQueueDAOInfo(true)
	assert.Equal(t, root.template.GetMaxApplications(), rootDAO.TemplateInfo.MaxApplications)
	assert.DeepEqual(t, root.template.GetProperties(), rootDAO.TemplateInfo.Properties)
	assert.DeepEqual(t, root.template.GetMaxResource().DAOMap(), rootDAO.TemplateInfo.MaxResource)
	assert.DeepEqual(t, root.template.GetGuaranteedResource().DAOMap(), rootDAO.TemplateInfo.GuaranteedResource)

	// test resources
	root.maxResource = getResource(t)
	root.guaranteedResource = getResource(t)
	rootDAO = root.GetPartitionQueueDAOInfo(true)
	assert.DeepEqual(t, root.GetMaxResource().DAOMap(), rootDAO.MaxResource)
	assert.DeepEqual(t, root.GetGuaranteedResource().DAOMap(), rootDAO.GuaranteedResource)
	assert.DeepEqual(t, root.getHeadRoom().DAOMap(), rootDAO.HeadRoom)

	// test allocatingAcceptedApps
	root.allocatingAcceptedApps = getAllocatingAcceptedApps()
	rootDAO = root.GetPartitionQueueDAOInfo(true)
	assert.Equal(t, len(root.allocatingAcceptedApps), 2, "allocatingAcceptedApps size")
	assert.Equal(t, len(rootDAO.AllocatingAcceptedApps), 1, "AllocatingAcceptedApps size")
	assert.Equal(t, rootDAO.AllocatingAcceptedApps[0], appID1)

	// Test specific queue
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf-queue", false, nil)
	assert.NilError(t, err, "failed to create managed queue")
	rootDAO = root.GetPartitionQueueDAOInfo(false)
	assert.Equal(t, rootDAO.QueueName, "root")
	assert.Equal(t, len(rootDAO.Children), 0)
	assert.Equal(t, len(rootDAO.ChildNames), 1)
	assert.Equal(t, rootDAO.ChildNames[0], "root.leaf-queue")
	// Test hierarchy queue
	rootDAO = root.GetPartitionQueueDAOInfo(true)
	assert.Equal(t, rootDAO.QueueName, "root")
	assert.Equal(t, len(rootDAO.Children), 1)
	assert.Equal(t, len(rootDAO.ChildNames), 1)
	assert.Equal(t, rootDAO.Children[0].QueueName, "root.leaf-queue")
	assert.Equal(t, rootDAO.ChildNames[0], "root.leaf-queue")
	// special prop checks
	leaf.properties = map[string]string{
		configs.ApplicationSortPolicy: policies.FairSortPolicy.String(),
		configs.PreemptionDelay:       "3600s",
		configs.PreemptionPolicy:      policies.FencePreemptionPolicy.String(),
	}
	leaf.UpdateQueueProperties()
	leafDAO := leaf.GetPartitionQueueDAOInfo(false)
	assert.Equal(t, leafDAO.QueueName, "root.leaf-queue")
	assert.Equal(t, len(leafDAO.Children), 0, "leaf has no children")
	assert.Equal(t, len(leafDAO.ChildNames), 0, "leaf has no children (names)")
	assert.Equal(t, leafDAO.PreemptionEnabled, true, "preemption should be enabled")
	assert.Equal(t, leafDAO.IsPreemptionFence, true, "fence should have been set")
	assert.Equal(t, leafDAO.PreemptionDelay, "1h0m0s", "incorrect delay returned")
	assert.Equal(t, leafDAO.SortingPolicy, "fair", "incorrect policy returned")

	// special prop checks
	leaf.properties = map[string]string{
		configs.ApplicationSortPolicy: policies.FifoSortPolicy.String(),
		configs.PreemptionDelay:       "10s",
		configs.PreemptionPolicy:      policies.DisabledPreemptionPolicy.String(),
	}
	leaf.UpdateQueueProperties()
	leafDAO = leaf.GetPartitionQueueDAOInfo(false)
	assert.Equal(t, leafDAO.PreemptionEnabled, false, "preemption should not be enabled")
	assert.Equal(t, leafDAO.IsPreemptionFence, false, "queue should not be a fence")
	assert.Equal(t, leafDAO.PreemptionDelay, "10s", "incorrect delay returned")
	assert.Equal(t, leafDAO.SortingPolicy, "fifo", "incorrect policy returned")
}

func getAllocatingAcceptedApps() map[string]bool {
	allocatingAcceptedApps := make(map[string]bool)
	allocatingAcceptedApps[appID1] = true
	allocatingAcceptedApps[appID2] = false
	return allocatingAcceptedApps
}

func getResourceConf() map[string]string {
	resource := make(map[string]string)
	resource["memory"] = strconv.Itoa(rand.Intn(10000) + 100) //nolint:gosec
	return resource
}

func getResource(t *testing.T) *resources.Resource {
	r, err := resources.NewResourceFromConf(getResourceConf())
	assert.NilError(t, err, "failed to parse resource: %v", err)
	return r
}

func getZeroResourceConf() map[string]string {
	resource := make(map[string]string)
	resource["memory"] = "0"
	resource["vcore"] = "0"
	return resource
}

func getProperties() map[string]string {
	properties := make(map[string]string)
	properties[strconv.Itoa(rand.Intn(10000))] = strconv.Itoa(rand.Intn(10000)) //nolint:gosec
	return properties
}

func TestSetResources(t *testing.T) {
	queue, err := createManagedQueueWithProps(nil, "tmp", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue queue: %v", err)

	guaranteedResource := getResourceConf()
	maxResource := getResourceConf()

	// case 0: normal case
	err = queue.setResourcesFromConf(configs.Resources{
		Guaranteed: guaranteedResource,
		Max:        maxResource,
	})
	assert.NilError(t, err, "failed to set resources: %v", err)

	expectedGuaranteedResource, err := resources.NewResourceFromConf(guaranteedResource)
	assert.NilError(t, err, "failed to parse resource: %v", err)
	assert.DeepEqual(t, queue.guaranteedResource, expectedGuaranteedResource)

	expectedMaxResource, err := resources.NewResourceFromConf(maxResource)
	assert.NilError(t, err, "failed to parse resource: %v", err)
	assert.DeepEqual(t, queue.maxResource, expectedMaxResource)

	// case 1: empty resource would set the queue resources to 'nil' if it has been set already
	var nilResource *resources.Resource = nil
	err = queue.setResourcesFromConf(configs.Resources{
		Guaranteed: make(map[string]string),
		Max:        make(map[string]string),
	})
	assert.NilError(t, err, "failed to set resources: %v", err)
	assert.DeepEqual(t, queue.guaranteedResource, nilResource)
	assert.DeepEqual(t, queue.maxResource, nilResource)

	// case 2: zero resource won't change the queue resources as it is 'nil' already
	err = queue.setResourcesFromConf(configs.Resources{
		Guaranteed: getZeroResourceConf(),
		Max:        getZeroResourceConf(),
	})
	assert.NilError(t, err, "failed to set resources: %v", err)
	assert.DeepEqual(t, queue.guaranteedResource, nilResource)
	assert.DeepEqual(t, queue.maxResource, nilResource)
}

func TestPreemptingResource(t *testing.T) {
	one, err := resources.NewResourceFromConf(map[string]string{"memory": "10000000", "vcores": "1"})
	assert.NilError(t, err, "failed to create resource")
	two, err := resources.NewResourceFromConf(map[string]string{"memory": "20000000", "vcores": "2"})
	assert.NilError(t, err, "failed to create resource")
	three, err := resources.NewResourceFromConf(map[string]string{"memory": "30000000", "vcores": "3"})
	assert.NilError(t, err, "failed to create resource")

	queue, err := createManagedQueueWithProps(nil, "tmp", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue queue: %v", err)

	assert.Check(t, resources.IsZero(queue.GetPreemptingResource()), "initial value should be zero")
	queue.IncPreemptingResource(one)
	assert.Check(t, resources.Equals(one, queue.GetPreemptingResource()), "wrong value after increment")
	queue.IncPreemptingResource(two)
	assert.Check(t, resources.Equals(three, queue.GetPreemptingResource()), "wrong value after increment")
	queue.DecPreemptingResource(one)
	assert.Check(t, resources.Equals(two, queue.GetPreemptingResource()), "wrong value after decrement")
	queue.DecPreemptingResource(two)
	assert.Check(t, resources.IsZero(queue.GetPreemptingResource()), "final value should be zero")
}

func TestPreemptionDelay(t *testing.T) {
	queue, err := createManagedQueueWithProps(nil, "tmp", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue queue: %v", err)

	assert.Equal(t, configs.DefaultPreemptionDelay, queue.GetPreemptionDelay(), "initial preemption delay incorrect")

	twice := 2 * configs.DefaultPreemptionDelay
	queue.preemptionDelay = twice
	assert.Equal(t, twice, queue.GetPreemptionDelay(), "preemption delay not updated correctly")
}

func TestFindQueueByAppID(t *testing.T) {
	appQueueMapping := NewAppQueueMapping()
	root, err := createRootQueueWithAppQueueMapping(nil, appQueueMapping)
	assert.NilError(t, err, "failed to create queue")
	parent1, err := createManagedQueueWithAppQueueMapping(root, "parent1", true, nil, appQueueMapping)
	assert.NilError(t, err, "failed to create queue")
	parent2, err := createManagedQueueWithAppQueueMapping(root, "parent2", true, nil, appQueueMapping)
	assert.NilError(t, err, "failed to create queue")
	leaf1, err := createManagedQueueWithAppQueueMapping(parent1, "leaf1", false, nil, appQueueMapping)
	assert.NilError(t, err, "failed to create queue")
	leaf2, err := createManagedQueueWithAppQueueMapping(parent2, "leaf2", false, nil, appQueueMapping)
	assert.NilError(t, err, "failed to create queue")

	app := newApplication(appID1, "default", "root.parent.leaf")
	app.pending = resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 10})
	leaf1.AddApplication(app)
	appQueueMapping.AddAppQueueMapping(appID1, leaf1)

	// we should be able to find the queue from any other given the appID
	assert.Equal(t, leaf1, root.GetQueueByAppID(appID1), "failed to find queue from root")
	assert.Equal(t, leaf1, parent1.GetQueueByAppID(appID1), "failed to find queue from parent1")
	assert.Equal(t, leaf1, parent2.GetQueueByAppID(appID1), "failed to find queue from parent2")
	assert.Equal(t, leaf1, leaf1.GetQueueByAppID(appID1), "failed to find queue from leaf1")
	assert.Equal(t, leaf1, leaf2.GetQueueByAppID(appID1), "failed to find queue from leaf2")

	// non-existent queue should be nil
	var none *Queue = nil
	assert.Equal(t, none, root.GetQueueByAppID("missing"), "found queue reference in root")
	assert.Equal(t, none, parent1.GetQueueByAppID("missing"), "found queue reference in parent1")
	assert.Equal(t, none, parent2.GetQueueByAppID("missing"), "found queue reference in parent2")
	assert.Equal(t, none, leaf1.GetQueueByAppID("missing"), "found queue reference in leaf1")
	assert.Equal(t, none, leaf2.GetQueueByAppID("missing"), "found queue reference in leaf2")
}

// nolint: funlen
func TestFindEligiblePreemptionVictims(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 100})
	parentMax := map[string]string{siCommon.Memory: "200"}
	parentGuar := map[string]string{siCommon.Memory: "100"}
	ask := createAllocationAsk("ask1", appID1, true, true, 0, res)
	ask2 := createAllocationAsk("ask2", appID2, true, true, -1000, res)
	alloc2 := createAllocation("ask2", appID2, nodeID1, true, true, -1000, false, res)
	ask3 := createAllocationAsk("ask3", appID2, true, true, -1000, res)
	alloc3 := createAllocation("ask3", appID2, nodeID1, true, true, -1000, false, res)
	root, err := createRootQueue(map[string]string{siCommon.Memory: "1000"})
	assert.NilError(t, err, "failed to create queue")
	parent1, err := createManagedQueueGuaranteed(root, "parent1", true, parentMax, parentGuar, nil)
	assert.NilError(t, err, "failed to create queue")
	parent2, err := createManagedQueueGuaranteed(root, "parent2", true, parentMax, parentGuar, nil)
	assert.NilError(t, err, "failed to create queue")
	leaf1, err := createManagedQueueGuaranteed(parent1, "leaf1", false, nil, nil, nil)
	assert.NilError(t, err, "failed to create queue")
	leaf2, err := createManagedQueueGuaranteed(parent2, "leaf2", false, nil, nil, nil)
	assert.NilError(t, err, "failed to create queue")

	// verify no victims when no allocations exist
	snapshot := leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)

	assert.Equal(t, 5, len(snapshot), "wrong snapshot count") // root, root.parent1, root.parent1.leaf1, root.parent2, root.parent2.leaf2
	assert.Equal(t, 0, len(victims(snapshot)), "found victims")

	// add two lower-priority allocs in leaf2
	app2 := newApplication(appID2, "default", "root.parent.leaf")
	app2.pending = res
	leaf2.AddApplication(app2)
	app2.SetQueue(leaf2)
	err = app2.AddAllocationAsk(ask2)
	assert.NilError(t, err, "failed to add ask")
	err = app2.AddAllocationAsk(ask3)
	assert.NilError(t, err, "failed to add ask")
	app2.AddAllocation(alloc2)
	app2.AddAllocation(alloc3)
	err = leaf2.TryIncAllocatedResource(alloc2.GetAllocatedResource())
	assert.NilError(t, err, "failed to inc allocated resources")
	err = leaf2.TryIncAllocatedResource(alloc3.GetAllocatedResource())
	assert.NilError(t, err, "failed to inc allocated resources")

	// verify victims
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 5, len(snapshot), "wrong snapshot count") // leaf1, parent1, root
	assert.Equal(t, 2, len(victims(snapshot)), "wrong victim count")
	assert.Equal(t, alloc2.allocationKey, victims(snapshot)[0].allocationKey, "wrong alloc")
	assert.Equal(t, alloc3.allocationKey, victims(snapshot)[1].allocationKey, "wrong alloc")

	// disabling preemption on victim queue should remove victims from consideration
	leaf2.preemptionPolicy = policies.DisabledPreemptionPolicy
	assert.Equal(t, leaf1.findPreemptionFenceRoot(make(map[string]int64), int64(ask.priority)).QueuePath, "root")
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 0, len(victims(snapshot)), "found victims")
	leaf2.preemptionPolicy = policies.DefaultPreemptionPolicy

	// fencing parent1 queue should limit scope
	parent1.preemptionPolicy = policies.FencePreemptionPolicy
	assert.Equal(t, leaf1.findPreemptionFenceRoot(make(map[string]int64), int64(ask.priority)).QueuePath, "root.parent1")
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 0, len(victims(snapshot)), "found victims")
	parent1.preemptionPolicy = policies.DefaultPreemptionPolicy

	// fencing leaf1 queue should limit scope
	leaf1.preemptionPolicy = policies.FencePreemptionPolicy
	assert.Equal(t, leaf1.findPreemptionFenceRoot(make(map[string]int64), int64(ask.priority)).QueuePath, "root.parent1.leaf1")
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 0, len(victims(snapshot)), "found victims")
	leaf1.preemptionPolicy = policies.DefaultPreemptionPolicy

	// fencing parent2 queue should not limit scope
	parent2.preemptionPolicy = policies.FencePreemptionPolicy
	assert.Equal(t, leaf1.findPreemptionFenceRoot(make(map[string]int64), int64(ask.priority)).QueuePath, "root")
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 2, len(victims(snapshot)), "wrong victim count")
	parent2.preemptionPolicy = policies.DefaultPreemptionPolicy

	// fencing leaf2 queue should not limit scope
	leaf2.preemptionPolicy = policies.FencePreemptionPolicy
	assert.Equal(t, leaf1.findPreemptionFenceRoot(make(map[string]int64), int64(ask.priority)).QueuePath, "root")
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 5, len(snapshot), "wrong victim count")
	assert.Equal(t, 2, len(victims(snapshot)), "wrong victim count")
	leaf2.preemptionPolicy = policies.DefaultPreemptionPolicy

	// fencing using max resources and usage comparison check
	// parent1 queue is full. usage has reached max resources.
	// even though root.parent1 and root.parent1.leaf1 policy is DefaultPreemptionPolicy, still root.parent1 would be fenced
	used := parent1.allocatedResource
	parent1.allocatedResource = parent1.maxResource
	assert.Equal(t, parent1.preemptionPolicy, policies.DefaultPreemptionPolicy)
	assert.Equal(t, leaf1.preemptionPolicy, policies.DefaultPreemptionPolicy)
	assert.Equal(t, leaf1.findPreemptionFenceRoot(make(map[string]int64), int64(ask.priority)).QueuePath, parent1.QueuePath)
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 0, len(victims(snapshot)), "wrong victim count")
	parent1.allocatedResource = used

	// requiring a specific node take alloc out of consideration
	alloc2.SetRequiredNode(nodeID1)
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 1, len(victims(snapshot)), "wrong victim count")
	assert.Equal(t, alloc3.allocationKey, victims(snapshot)[0].allocationKey, "wrong alloc")
	alloc2.SetRequiredNode("")

	// placeholder which has been marked released should not be considered
	alloc2.released = true
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 1, len(victims(snapshot)), "wrong victim count")
	assert.Equal(t, alloc3.allocationKey, victims(snapshot)[0].allocationKey, "wrong alloc")
	alloc2.released = false

	// alloc2 has already been preempted and should not be considered a valid victim
	err = alloc2.MarkPreempted()
	assert.NilError(t, err, "failed to mark preempted node")
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 1, len(victims(snapshot)), "wrong victim count")
	assert.Equal(t, alloc3.allocationKey, victims(snapshot)[0].allocationKey, "wrong alloc")
	// recreate alloc2 to restore non-prempted state
	app2.RemoveAllocation(alloc2.GetAllocationKey(), si.TerminationType_STOPPED_BY_RM)
	alloc2 = createAllocation("ask2", appID2, nodeID1, true, true, -1000, false, res)
	app2.AddAllocation(alloc2)

	// setting priority offset on parent2 queue should remove leaf2 victims
	parent2.priorityOffset = 1001
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 0, len(victims(snapshot)), "found victims")
	parent2.priorityOffset = 0

	// priority-fencing parent2 with positive offset should remove leaf2 victims
	parent2.priorityOffset = 1
	parent2.priorityPolicy = policies.FencePriorityPolicy
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 0, len(victims(snapshot)), "found victims")
	parent2.priorityOffset = 0
	parent2.priorityPolicy = policies.DefaultPriorityPolicy

	// priority-fencing parent2 with negative offset should not affect leaf2 victims
	parent2.priorityOffset = -1
	parent2.priorityPolicy = policies.FencePriorityPolicy
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 2, len(victims(snapshot)), "wrong victim count")
	parent2.priorityOffset = 0
	parent2.priorityPolicy = policies.DefaultPriorityPolicy

	// priority-fencing parent1 with small negative offset should not affect leaf2 victims
	parent1.priorityOffset = -1000
	parent1.priorityPolicy = policies.FencePriorityPolicy
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 2, len(victims(snapshot)), "wrong victim count")
	parent1.priorityOffset = 0
	parent1.priorityPolicy = policies.DefaultPriorityPolicy

	// priority-fencing parent1 with larger negative offset should remove leaf2 victims
	parent1.priorityOffset = -1001
	parent1.priorityPolicy = policies.FencePriorityPolicy
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 0, len(victims(snapshot)), "found victims")
	parent1.priorityOffset = 0
	parent1.priorityPolicy = policies.DefaultPriorityPolicy

	// increasing parent2 guaranteed resources should remove leaf2 victims
	parent2.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 200})
	snapshot = leaf1.FindEligiblePreemptionVictims(leaf1.QueuePath, ask)
	assert.Equal(t, 0, len(victims(snapshot)), "found victims")
	parent2.guaranteedResource = resources.NewResourceFromMap(map[string]resources.Quantity{siCommon.Memory: 100})
}

func victims(snapshot map[string]*QueuePreemptionSnapshot) []*Allocation {
	results := make([]*Allocation, 0)
	for _, entry := range snapshot {
		results = append(results, entry.PotentialVictims...)
	}
	sort.SliceStable(results, func(i, j int) bool {
		return results[i].allocationKey < results[j].allocationKey
	})
	return results
}

func TestSetTemplate(t *testing.T) {
	queue, err := createManagedQueueWithProps(nil, "tmp", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue queue: %v", err)

	maxApplications := uint64(1)
	properties := getProperties()
	guaranteedResource := getResourceConf()
	expectedGuaranteedResource, err := resources.NewResourceFromConf(guaranteedResource)
	assert.NilError(t, err, "failed to parse resource: %v", err)
	maxResource := getResourceConf()
	expectedMaxResource, err := resources.NewResourceFromConf(maxResource)
	assert.NilError(t, err, "failed to parse resource: %v", err)

	checkTemplate := func(queue *Queue) {
		assert.Equal(t, queue.template.GetMaxApplications(), maxApplications)
		assert.DeepEqual(t, queue.template.GetProperties(), properties)
		assert.DeepEqual(t, queue.template.GetGuaranteedResource(), expectedGuaranteedResource)
		assert.DeepEqual(t, queue.template.GetMaxResource(), expectedMaxResource)
	}

	// case 0: normal case
	err = queue.setTemplate(configs.ChildTemplate{
		MaxApplications: maxApplications,
		Properties:      properties,
		Resources: configs.Resources{
			Guaranteed: guaranteedResource,
			Max:        maxResource,
		},
	})
	assert.NilError(t, err, "failed to set resources: %v", err)
	checkTemplate(queue)

	// case 1: empty config does nothing
	err = queue.setTemplate(configs.ChildTemplate{
		Properties: make(map[string]string),
		Resources: configs.Resources{
			Guaranteed: make(map[string]string),
			Max:        make(map[string]string),
		},
	})
	assert.NilError(t, err)
	assert.Assert(t, queue.template == nil)
}

func TestApplyTemplate(t *testing.T) {
	childTemplate, err := template.FromConf(&configs.ChildTemplate{
		MaxApplications: uint64(1),
		Properties:      getProperties(),
		Resources: configs.Resources{
			Max:        getResourceConf(),
			Guaranteed: getResourceConf(),
		},
	})
	assert.NilError(t, err)

	// case 0: leaf queue can apply template
	leaf, err := createManagedQueueWithProps(nil, "tmp", false, nil, nil)
	assert.NilError(t, err, "failed to create basic queue queue: %v", err)
	leaf.applyTemplate(childTemplate)
	assert.Assert(t, leaf.template == nil)
	assert.Equal(t, leaf.maxRunningApps, childTemplate.GetMaxApplications())
	assert.DeepEqual(t, leaf.properties, childTemplate.GetProperties())
	assert.DeepEqual(t, leaf.guaranteedResource, childTemplate.GetGuaranteedResource())
	assert.DeepEqual(t, leaf.maxResource, childTemplate.GetMaxResource())

	// case 1: zero resource template generates nil resource
	leaf2, err := createManagedQueueWithProps(nil, "tmp", false, nil, nil)
	assert.NilError(t, err, "failed to create basic queue queue: %v", err)
	zeroTemplate, err := template.FromConf(&configs.ChildTemplate{
		Properties: getProperties(),
		Resources: configs.Resources{
			Max:        getZeroResourceConf(),
			Guaranteed: getZeroResourceConf(),
		},
	})
	assert.NilError(t, err)
	leaf2.applyTemplate(zeroTemplate)
	assert.Assert(t, leaf2.maxRunningApps == 0)
	assert.Assert(t, leaf2.template == nil)
	assert.Assert(t, leaf2.maxResource == nil)
	assert.Assert(t, leaf2.guaranteedResource == nil)
}

func TestApplyConf(t *testing.T) {
	// cover error cases
	errQueue, err := createManagedQueueWithProps(nil, "errConf", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue: %v", err)

	// wrong submitACL
	errConf1 := configs.QueueConfig{
		SubmitACL: "error Submit ACL",
	}
	err = errQueue.ApplyConf(errConf1)
	assert.ErrorContains(t, err, "multiple spaces found in ACL")

	// wrong AdminACL
	errConf2 := configs.QueueConfig{
		AdminACL: "error Admin ACL",
	}
	err = errQueue.ApplyConf(errConf2)
	assert.ErrorContains(t, err, "multiple spaces found in ACL")

	// wrong ChildTemplate
	errConf3 := configs.QueueConfig{
		Parent: true,
		ChildTemplate: configs.ChildTemplate{
			Resources: configs.Resources{
				Guaranteed: map[string]string{"wrong template": "-100"},
			},
		},
	}
	err = errQueue.ApplyConf(errConf3)
	assert.ErrorContains(t, err, "invalid quantity")

	// isManaged is changed from unmanaged to managed
	childConf := configs.QueueConfig{}

	parent, err := createManagedQueueWithProps(nil, "parent", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue: %v", err)

	child, err := NewDynamicQueue("child", true, parent, nil)
	assert.NilError(t, err, "failed to create basic queue: %v", err)

	err = child.ApplyConf(childConf)
	assert.NilError(t, err, "failed to parse conf: %v", err)
	assert.Equal(t, child.IsManaged(), true)

	// isLeaf is set to false while Queues length > 0
	parentConf := configs.QueueConfig{
		Queues: []configs.QueueConfig{
			{
				Name:   "child",
				Parent: true,
				Queues: nil,
			},
		},
	}
	parent, err = createManagedQueueWithProps(nil, "parent", false, nil, nil)
	assert.NilError(t, err, "failed to create basic queue: %v", err)

	err = parent.ApplyConf(parentConf)
	assert.NilError(t, err, "failed to parse conf: %v", err)
	assert.Equal(t, parent.IsLeafQueue(), false)

	conf := configs.QueueConfig{
		SubmitACL: "",
		AdminACL:  "",
		Resources: configs.Resources{
			Max:        getResourceConf(),
			Guaranteed: getResourceConf(),
		},
		ChildTemplate: configs.ChildTemplate{
			Properties: make(map[string]string),
			Resources: configs.Resources{
				Max:        getResourceConf(),
				Guaranteed: getResourceConf(),
			},
		},
		MaxApplications: 100,
	}

	// case 0: leaf can't set template
	leaf, err := createManagedQueueWithProps(nil, "tmp", false, nil, nil)
	assert.NilError(t, err, "failed to create basic queue: %v", err)

	validTemplate, err := template.FromConf(&conf.ChildTemplate)
	assert.NilError(t, err, "failed to parse conf: %v", err)
	assert.Assert(t, validTemplate != nil)

	conf.Parent = false
	err = leaf.ApplyConf(conf)
	assert.NilError(t, err, "failed to apply conf: %v", err)
	assert.Assert(t, leaf.template == nil)
	assert.Assert(t, leaf.maxResource != nil)
	assert.Assert(t, leaf.guaranteedResource != nil)
	assert.Equal(t, leaf.maxRunningApps, uint64(100))

	// case 1-1: non-leaf queue can have template
	queue, err := createManagedQueueWithProps(nil, "tmp", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue: %v", err)

	conf.Parent = true
	err = queue.ApplyConf(conf)
	assert.NilError(t, err, "failed to apply conf: %v", err)
	assert.Assert(t, queue.template != nil)
	assert.Assert(t, queue.maxResource != nil)
	assert.Assert(t, queue.guaranteedResource != nil)
	assert.Equal(t, queue.maxRunningApps, uint64(100))

	// root can't set resources
	root, err := createManagedQueueWithProps(nil, "root", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue: %v", err)

	err = queue.ApplyConf(conf)
	assert.NilError(t, err, "failed to apply conf: %v", err)
	assert.Assert(t, root.maxResource == nil)
	assert.Assert(t, root.guaranteedResource == nil)
	assert.Equal(t, root.maxRunningApps, uint64(0))
}

func TestQuotaChangePreemptionSettings(t *testing.T) {
	root, err := createManagedQueueWithProps(nil, "root", true, nil, nil)
	assert.NilError(t, err, "failed to create basic queue: %v", err)

	parent, err := createManagedQueueWithProps(root, "parent", false, nil, nil)
	assert.NilError(t, err, "failed to create basic queue: %v", err)
	testCases := []struct {
		name          string
		conf          configs.QueueConfig
		expectedDelay uint64
	}{{"first time queue setup without delay", configs.QueueConfig{
		Resources: configs.Resources{
			Max:        getResourceConf(),
			Guaranteed: getResourceConf(),
		},
	}, 0},
		{"clearing max resources", configs.QueueConfig{
			Resources: configs.Resources{
				Max:        nil,
				Guaranteed: nil,
			},
		}, 0},
		{"first time queue setup with delay", configs.QueueConfig{
			Resources: configs.Resources{
				Max:        getResourceConf(),
				Guaranteed: getResourceConf(),
			},
			Preemption: configs.Preemption{
				Delay: 1,
			},
		}, 1},
		{"increase max with delay", configs.QueueConfig{
			Resources: configs.Resources{
				Max: map[string]string{"memory": "100000000"},
			},
			Preemption: configs.Preemption{
				Delay: 500,
			},
		}, 0},
		{"decrease max with delay", configs.QueueConfig{
			Resources: configs.Resources{
				Max: map[string]string{"memory": "100"},
			},
			Preemption: configs.Preemption{
				Delay: 2,
			},
		}, 2},
		{"max remains as is but delay changed", configs.QueueConfig{
			Resources: configs.Resources{
				Max: map[string]string{"memory": "100"},
			},
			Preemption: configs.Preemption{
				Delay: 2,
			},
		}, 2},
		{"unrelated config change, should not impact earlier set preemption settings", configs.QueueConfig{
			Resources: configs.Resources{
				Max:        map[string]string{"memory": "100"},
				Guaranteed: map[string]string{"memory": "50"},
			},
			Preemption: configs.Preemption{
				Delay: 2,
			},
		}, 2},
		{"increase max again with delay", configs.QueueConfig{
			Resources: configs.Resources{
				Max: map[string]string{"memory": "101"},
			},
			Preemption: configs.Preemption{
				Delay: 200,
			},
		}, 0}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err = parent.ApplyConf(tc.conf)
			assert.NilError(t, err, "failed to apply conf: %v", err)

			// assert the preemption settings
			delay, sTime := parent.getPreemptionSettings()
			assert.Equal(t, delay, tc.expectedDelay)
			if tc.expectedDelay != uint64(0) {
				assert.Equal(t, sTime.IsZero(), false)
			} else {
				assert.Equal(t, sTime.IsZero(), true)
			}
			assert.Equal(t, parent.shouldTriggerPreemption(), false)

			used, err := resources.NewResourceFromConf(tc.conf.Resources.Max)
			assert.NilError(t, err, "failed to set allocated resource: %v", err)
			parent.allocatedResource = resources.Multiply(used, 2)

			// Wait till delay expires to let trigger preemption automatically
			time.Sleep(time.Duration(int64(tc.expectedDelay)+1) * time.Second)
			if tc.expectedDelay != uint64(0) {
				assert.Equal(t, parent.shouldTriggerPreemption(), true)
			}
			parent.TryAllocate(nil, nil, nil, false)

			// preemption settings should be same as before even now as trigger is async process
			delay, sTime = parent.getPreemptionSettings()
			assert.Equal(t, delay, tc.expectedDelay)
			if tc.expectedDelay != uint64(0) {
				assert.Equal(t, sTime.IsZero(), false)
				assert.Equal(t, parent.shouldTriggerPreemption(), true)
			} else {
				assert.Equal(t, sTime.IsZero(), true)
			}

			time.Sleep(time.Millisecond * 100)

			// preemption should have been triggered by now, assert preemption settings to ensure values are reset
			if tc.expectedDelay != uint64(0) {
				delay, sTime = parent.getPreemptionSettings()
				assert.Equal(t, sTime.IsZero(), true)
				assert.Equal(t, delay, uint64(0))

				// since preemption settings are set, preemption should not be triggerred again during tryAllocate
				assert.Equal(t, parent.shouldTriggerPreemption(), false)
			}
		})
	}
}

func TestNewConfiguredQueue(t *testing.T) {
	// check variable assignment
	properties := getProperties()
	resourceConf := getResourceConf()
	// turn resouce config into resource struct
	resourceStruct, err := resources.NewResourceFromConf(resourceConf)
	assert.NilError(t, err, "failed to create new resource from config: %v", err)

	parentConfig := configs.QueueConfig{
		Name:            "PARENT_QUEUE",
		Parent:          true,
		MaxApplications: uint64(32),
		ChildTemplate: configs.ChildTemplate{
			Properties: properties,
			Resources: configs.Resources{
				Max:        resourceConf,
				Guaranteed: resourceConf,
			},
		},
	}
	parent, err := NewConfiguredQueue(parentConfig, nil, false, nil)
	assert.NilError(t, err, "failed to create queue: %v", err)
	assert.Equal(t, parent.Name, "parent_queue")
	assert.Equal(t, parent.QueuePath, "parent_queue")
	assert.Equal(t, parent.isManaged, true)
	assert.Equal(t, parent.maxRunningApps, uint64(32))
	assert.DeepEqual(t, properties, parent.template.GetProperties())
	assert.Assert(t, resources.Equals(resourceStruct, parent.template.GetMaxResource()))
	assert.Assert(t, resources.Equals(resourceStruct, parent.template.GetGuaranteedResource()))
	assert.Equal(t, parent.quotaChangePreemptionDelay, uint64(0))

	// case 0: managed leaf queue can't use template
	leafConfig := configs.QueueConfig{
		Name:       "leaf_queue",
		Parent:     false,
		Properties: getProperties(),
		Resources: configs.Resources{
			Max:        getResourceConf(),
			Guaranteed: getResourceConf(),
		},
		Preemption: configs.Preemption{
			Delay: 500,
		},
	}
	childLeaf, err := NewConfiguredQueue(leafConfig, parent, false, nil)
	assert.NilError(t, err, "failed to create queue: %v", err)
	assert.Equal(t, childLeaf.QueuePath, "parent_queue.leaf_queue")
	assert.Assert(t, childLeaf.template == nil)
	assert.Assert(t, reflect.DeepEqual(childLeaf.properties, leafConfig.Properties))
	childLeafMax, err := resources.NewResourceFromConf(leafConfig.Resources.Max)
	assert.NilError(t, err, "Resource creation failed")
	assert.Assert(t, resources.Equals(childLeaf.maxResource, childLeafMax))
	childLeafGuaranteed, err := resources.NewResourceFromConf(leafConfig.Resources.Guaranteed)
	assert.NilError(t, err, "Resource creation failed")
	assert.Assert(t, resources.Equals(childLeaf.guaranteedResource, childLeafGuaranteed))
	assert.Equal(t, childLeaf.quotaChangePreemptionDelay, uint64(500))

	// case 1: non-leaf can't use template but it can inherit template from parent
	NonLeafConfig := configs.QueueConfig{
		Name:   "nonleaf_queue",
		Parent: true,
		Preemption: configs.Preemption{
			Delay: 500,
		},
	}
	childNonLeaf, err := NewConfiguredQueue(NonLeafConfig, parent, false, nil)
	assert.NilError(t, err, "failed to create queue: %v", err)
	assert.Equal(t, childNonLeaf.QueuePath, "parent_queue.nonleaf_queue")
	assert.Assert(t, reflect.DeepEqual(childNonLeaf.template, parent.template))
	assert.Equal(t, len(childNonLeaf.properties), 0)
	assert.Assert(t, childNonLeaf.guaranteedResource == nil)
	assert.Assert(t, childNonLeaf.maxResource == nil)
	assert.Equal(t, childNonLeaf.quotaChangePreemptionDelay, uint64(0))

	// case 2: do not send queue event when silence flag is set to true
	events.Init()
	eventSystem := events.GetEventSystem().(*events.EventSystemImpl) //nolint:errcheck
	eventSystem.StartServiceWithPublisher(false)
	rootConfig := configs.QueueConfig{
		Name: "root",
		Preemption: configs.Preemption{
			Delay: 500,
		},
	}

	rootQ, err := NewConfiguredQueue(rootConfig, nil, true, nil)
	assert.NilError(t, err, "failed to create queue: %v", err)
	time.Sleep(time.Second)
	noEvents := eventSystem.Store.CountStoredEvents()
	assert.Equal(t, noEvents, uint64(0), "expected 0 event, got %d", noEvents)
	assert.Equal(t, rootQ.quotaChangePreemptionDelay, uint64(0))
}

func TestResetRunningState(t *testing.T) {
	emptyConf := configs.QueueConfig{
		Name: "not_used",
	}
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	// single parent under root
	var parent *Queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	var leaf *Queue
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	if len(parent.children) == 0 {
		t.Error("leaf queue is not added to the parent queue")
	}
	parent.MarkQueueForRemoval()
	assert.Assert(t, parent.IsDraining(), "parent should be marked as draining")
	assert.Assert(t, leaf.IsDraining(), "leaf should be marked as draining")
	err = parent.applyConf(emptyConf, false)
	assert.NilError(t, err, "failed to update parent")
	assert.Assert(t, parent.IsRunning(), "parent should be running again")
	assert.Assert(t, leaf.IsDraining(), "leaf should still be marked as draining")
	err = leaf.applyConf(emptyConf, false)
	assert.NilError(t, err, "failed to update leaf")
	assert.Assert(t, leaf.IsRunning(), "leaf should be running again")
}

func TestNewRecoveryQueue(t *testing.T) {
	var err error
	if _, err = NewRecoveryQueue(nil, nil); err == nil {
		t.Fatalf("recovery queue creation should fail with nil parent")
	}

	parent, err := createManagedQueueWithProps(nil, "parent", true, nil, nil)
	assert.NilError(t, err, "failed to create queue: %v", err)
	if _, err = NewRecoveryQueue(parent, nil); err == nil {
		t.Fatalf("recovery queue creation should fail with non-root parent")
	}

	parentConfig := configs.QueueConfig{
		Name:          "root",
		Parent:        true,
		Properties:    map[string]string{configs.ApplicationSortPolicy: "fair"},
		ChildTemplate: configs.ChildTemplate{Properties: map[string]string{configs.ApplicationSortPolicy: "fair"}},
	}
	parent, err = NewConfiguredQueue(parentConfig, nil, false, nil)
	assert.NilError(t, err, "failed to create queue: %v", err)
	recoveryQueue, err := NewRecoveryQueue(parent, nil)
	assert.NilError(t, err, "failed to create recovery queue: %v", err)
	assert.Equal(t, common.RecoveryQueueFull, recoveryQueue.GetQueuePath(), "wrong queue name")
	assert.Equal(t, policies.FifoSortPolicy, recoveryQueue.getSortType(), "wrong sort type")
}

func TestNewDynamicQueueDoesNotCreateRecovery(t *testing.T) {
	parent, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create queue: %v", err)
	if _, err := NewDynamicQueue(common.RecoveryQueue, true, parent, nil); err == nil {
		t.Fatalf("invalid recovery queue %s was created", common.RecoveryQueueFull)
	}
}

func TestNewDynamicQueue(t *testing.T) {
	parent, err := createManagedQueueWithProps(nil, "parent", true, nil, nil)
	assert.NilError(t, err, "failed to create queue: %v", err)
	parent.template, err = template.FromConf(&configs.ChildTemplate{
		MaxApplications: uint64(1),
		Properties:      getProperties(),
		Resources: configs.Resources{
			Max:        getResourceConf(),
			Guaranteed: getResourceConf(),
		},
	})
	assert.NilError(t, err)

	// case 0: leaf can use template
	childLeaf, err := NewDynamicQueue("leaf", true, parent, nil)
	assert.NilError(t, err, "failed to create dynamic queue: %v", err)
	assert.Assert(t, childLeaf.template == nil)
	assert.Equal(t, childLeaf.maxRunningApps, parent.template.GetMaxApplications())
	assert.DeepEqual(t, childLeaf.properties, parent.template.GetProperties())
	assert.DeepEqual(t, childLeaf.maxResource, parent.template.GetMaxResource())
	assert.DeepEqual(t, childLeaf.guaranteedResource, parent.template.GetGuaranteedResource())
	assert.Assert(t, childLeaf.prioritySortEnabled)
	assert.Equal(t, childLeaf.priorityPolicy, policies.DefaultPriorityPolicy)
	assert.Equal(t, childLeaf.preemptionPolicy, policies.DefaultPreemptionPolicy)

	// case 1: non-leaf can't use template but it can inherit template from parent
	childNonLeaf, err := NewDynamicQueue("nonleaf_Test-a_b_#_c_#_d_/_e@dom:ain", false, parent, nil)
	assert.NilError(t, err, "failed to create dynamic queue: %v", err)
	assert.Assert(t, reflect.DeepEqual(childNonLeaf.template, parent.template))
	assert.Equal(t, len(childNonLeaf.properties), 0)
	assert.Assert(t, childNonLeaf.guaranteedResource == nil)
	assert.Assert(t, childNonLeaf.maxResource == nil)
	assert.Assert(t, childNonLeaf.prioritySortEnabled)
	assert.Equal(t, childNonLeaf.priorityPolicy, policies.DefaultPriorityPolicy)
	assert.Equal(t, childNonLeaf.preemptionPolicy, policies.DefaultPreemptionPolicy)

	// case 2: invalid queue name
	_, err = NewDynamicQueue("invalid!queue", false, parent, nil)
	if err == nil {
		t.Errorf("new dynamic queue should have failed to create, err is %v", err)
	}
}

func TestTemplateIsNotOverrideByParent(t *testing.T) {
	parent, err := createManagedQueueWithProps(nil, "parent", true, nil, nil)
	assert.NilError(t, err)
	parent.template, err = template.FromConf(&configs.ChildTemplate{
		Properties: map[string]string{
			"k": "v",
		},
	})
	assert.NilError(t, err)

	leaf, err := createManagedQueueWithProps(nil, "leaf", false, nil, nil)
	assert.NilError(t, err)
	leaf.template, err = template.FromConf(&configs.ChildTemplate{
		Properties: map[string]string{
			"k0": "v0",
		},
	})
	assert.NilError(t, err)

	err = parent.addChildQueue(leaf)
	assert.NilError(t, err)

	assert.Assert(t, !reflect.DeepEqual(leaf.template, parent.template))
}

func TestGetHeadRoomFromTwoQueues(t *testing.T) {
	allocatedResource := &resources.Resource{
		Resources: map[string]resources.Quantity{
			"vcore":  2000,
			"memory": 2000,
		},
	}

	parent, err := createManagedQueueWithProps(nil, "parent", true, nil, nil)
	assert.NilError(t, err)
	parent.maxResource = &resources.Resource{
		Resources: map[string]resources.Quantity{
			"vcore":  3000,
			"memory": 3000,
		},
	}
	// make sure parent queue see all allocated resources
	parent.allocatedResource = allocatedResource

	// this child is not set with max memory, so it should follow parent max memory
	child, err := createManagedQueueWithProps(parent, "child", true, nil, nil)
	assert.NilError(t, err)
	child.maxResource = &resources.Resource{
		Resources: map[string]resources.Quantity{
			"vcore": 3000,
		},
	}
	child.allocatedResource = allocatedResource

	result := child.getHeadRoom()

	assert.Equal(t, resources.Quantity(1000), result.Resources["vcore"])
	assert.Equal(t, resources.Quantity(1000), result.Resources["memory"])
}

func TestGetHeadRoomFromThreeQueues(t *testing.T) {
	allocatedResource := &resources.Resource{
		Resources: map[string]resources.Quantity{
			"vcore":  2000,
			"memory": 2000,
			"pods":   1,
			"other":  1,
		},
	}

	rootQueue, err := createManagedQueueWithProps(nil, "rootQueue", true, nil, nil)
	assert.NilError(t, err)
	rootQueue.maxResource = &resources.Resource{
		Resources: map[string]resources.Quantity{
			"vcore":  3000,
			"memory": 3000,
			"other":  100,
		},
	}
	// make sure rootQueue queue see all allocated resources
	rootQueue.allocatedResource = allocatedResource

	parent, err := createManagedQueueWithProps(rootQueue, "parent", true, nil, nil)
	assert.NilError(t, err)
	// this parent has a limit for one specific resource
	parent.maxResource = &resources.Resource{
		Resources: make(map[string]resources.Quantity),
	}
	parent.maxResource = &resources.Resource{
		Resources: map[string]resources.Quantity{
			"pods": 100,
		},
	}
	// make sure parent queue see all allocated resources
	parent.allocatedResource = allocatedResource

	// this child is not set with max memory, so it should follow rootQueue max memory
	child, err := createManagedQueueWithProps(parent, "child", true, nil, nil)
	assert.NilError(t, err)
	child.maxResource = &resources.Resource{
		Resources: map[string]resources.Quantity{
			"vcore": 3000,
		},
	}
	child.allocatedResource = allocatedResource

	result := child.getHeadRoom()

	assert.Equal(t, resources.Quantity(1000), result.Resources["vcore"])
	assert.Equal(t, resources.Quantity(1000), result.Resources["memory"])
	assert.Equal(t, resources.Quantity(99), result.Resources["pods"])
	assert.Equal(t, resources.Quantity(99), result.Resources["other"])
}

func TestQueue_canRunApp(t *testing.T) {
	// create the root
	root, err := createManagedQueueMaxApps(nil, "root", true, nil, 1)
	assert.NilError(t, err, "queue create failed")
	var leaf, leaf2 *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	leaf2, err = createManagedQueue(root, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")

	// ignore allocatingAcceptedApps
	assert.Assert(t, leaf.canRunApp(""), "unlimited queue should be able to run app")
	assert.Assert(t, root.canRunApp(""), "queue should be able to run app (root max is 1)")
	root.incRunningApps("")
	assert.Assert(t, !leaf.canRunApp(""), "running apps max reached on root, should be denied")
	root.maxRunningApps = 2
	assert.Assert(t, leaf.canRunApp(""), "root and leave allowed")
	leaf.maxRunningApps = 1
	leaf.incRunningApps("")
	assert.Assert(t, !leaf.canRunApp(""), "leaf should not be able to run an application")

	leaf2.incRunningApps("")
	assert.Assert(t, !leaf2.canRunApp(""), "leaf2 should not be able to run an application (root max reached)")

	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil queue canRunApp")
		}
	}()
	var q *Queue
	q.canRunApp("")
}

func TestQueue_incRunningApps(t *testing.T) {
	// create the root
	root, err := createManagedQueueMaxApps(nil, "root", true, nil, 2)
	assert.NilError(t, err, "queue create failed")
	var leaf, leaf2 *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	leaf2, err = createManagedQueue(root, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")

	assert.Equal(t, leaf.runningApps, uint64(0), "default max running apps is 0")
	root.incRunningApps("")
	assert.Equal(t, root.runningApps, uint64(1), "root should have 1 app running")
	leaf.incRunningApps("")
	assert.Equal(t, leaf.runningApps, uint64(1), "leaf should have 1 app running")
	assert.Equal(t, root.runningApps, uint64(2), "root should have 2 apps running")
	leaf.incRunningApps("")
	assert.Equal(t, leaf.runningApps, uint64(2), "leaf should have 2 app running")
	assert.Equal(t, root.runningApps, uint64(2), "root should not have changed")

	leaf2.incRunningApps("")
	assert.Equal(t, leaf2.runningApps, uint64(1), "leaf2 should have 1 app running")
	assert.Equal(t, root.runningApps, uint64(2), "root should have 1 app running")

	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil queue incRunningApps")
		}
	}()
	var q *Queue
	q.incRunningApps("")
}

func TestQueue_decRunningApps(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")

	leaf.decRunningApps()
	assert.Equal(t, leaf.runningApps, uint64(0), "default running apps is 0, should not wrap")
	assert.Equal(t, root.runningApps, uint64(0), "default running apps is 0, should not wrap")
	root.runningApps = 2
	leaf.runningApps = 2
	leaf.decRunningApps()
	assert.Equal(t, leaf.runningApps, uint64(1), "leaf should have 1 app running")
	assert.Equal(t, root.runningApps, uint64(1), "root should have 1 apps running")
	root.decRunningApps()
	assert.Equal(t, leaf.runningApps, uint64(1), "leaf should have 1 app running")
	assert.Equal(t, root.runningApps, uint64(0), "root should have no apps")
	leaf.decRunningApps()
	assert.Equal(t, leaf.runningApps, uint64(0), "leaf should have 0 app running")
	assert.Equal(t, root.runningApps, uint64(0), "root running apps is 0, should not wrap")

	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil queue decRunningApps")
		}
	}()
	var q *Queue
	q.decRunningApps()
}

func TestQueue_setAllocatingAccepted(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf, leaf2 *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	leaf2, err = createManagedQueue(root, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")

	leaf.setAllocatingAccepted("test-1")
	assert.Equal(t, len(leaf.allocatingAcceptedApps), 1, "expected 1 app in leaf list")
	assert.Equal(t, len(root.allocatingAcceptedApps), 1, "expected 1 app in root list")
	leaf.setAllocatingAccepted("test-1")
	assert.Equal(t, len(leaf.allocatingAcceptedApps), 1, "expected no change after adding same app again")
	leaf.setAllocatingAccepted("test-2")
	assert.Equal(t, len(leaf.allocatingAcceptedApps), 2, "expected 2 apps in leaf list")
	assert.Equal(t, len(root.allocatingAcceptedApps), 2, "expected 2 apps in root list")

	leaf2.setAllocatingAccepted("test-3")
	assert.Equal(t, len(leaf2.allocatingAcceptedApps), 1, "expected 1 app in leaf2 list")
	assert.Equal(t, len(root.allocatingAcceptedApps), 3, "expected 3 apps in root list")

	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil queue setAllocatingAccepted")
		}
	}()
	var q *Queue
	q.setAllocatingAccepted("")
}

func TestQueueEvents(t *testing.T) {
	events.Init()
	eventSystem := events.GetEventSystem().(*events.EventSystemImpl) //nolint:errcheck
	eventSystem.StartServiceWithPublisher(false)
	queue, err := createRootQueue(nil)
	queue.Name = "testQueue"
	assert.NilError(t, err)

	app := newApplication(appID0, "default", "root")
	queue.AddApplication(app)
	queue.RemoveApplication(app)
	noEvents := uint64(0)
	err = common.WaitForCondition(10*time.Millisecond, time.Second, func() bool {
		noEvents = eventSystem.Store.CountStoredEvents()
		return noEvents == 5
	})
	assert.NilError(t, err, "expected 5 events, got %d", noEvents)
	records := eventSystem.Store.CollectEvents()
	assert.Equal(t, 5, len(records), "number of events")
	assert.Equal(t, si.EventRecord_QUEUE, records[2].Type)
	assert.Equal(t, si.EventRecord_ADD, records[2].EventChangeType)
	assert.Equal(t, si.EventRecord_QUEUE_APP, records[2].EventChangeDetail)
	assert.Equal(t, si.EventRecord_QUEUE, records[3].Type)
	assert.Equal(t, si.EventRecord_REMOVE, records[3].EventChangeType)
	assert.Equal(t, si.EventRecord_QUEUE_APP, records[3].EventChangeDetail)
	assert.Equal(t, si.EventRecord_APP, records[4].Type, "incorrect event type, expect app")
	assert.Equal(t, app.ApplicationID, records[4].ObjectID, "incorrect object ID, expected application ID")
	assert.Equal(t, si.EventRecord_REMOVE, records[4].EventChangeType, "incorrect change type, expected remove")
	assert.Equal(t, si.EventRecord_DETAILS_NONE, records[4].EventChangeDetail, "incorrect change detail, expected none")

	newConf := configs.QueueConfig{
		Parent: false,
		Name:   "testQueue",
		Resources: configs.Resources{
			Guaranteed: map[string]string{
				"memory": "1",
			},
			Max: map[string]string{
				"memory": "5",
			},
		},
	}
	err = queue.ApplyConf(newConf)
	assert.NilError(t, err)
	err = common.WaitForCondition(10*time.Millisecond, time.Second, func() bool {
		noEvents = eventSystem.Store.CountStoredEvents()
		return noEvents == 3
	})
	assert.NilError(t, err, "expected 3 events, got %d", noEvents)
	records = eventSystem.Store.CollectEvents()
	assert.Equal(t, 3, len(records), "number of events")
	assert.Equal(t, si.EventRecord_QUEUE, records[0].Type)
	assert.Equal(t, si.EventRecord_SET, records[0].EventChangeType)
	assert.Equal(t, si.EventRecord_QUEUE_TYPE, records[0].EventChangeDetail)
	assert.Equal(t, si.EventRecord_QUEUE, records[1].Type)
	assert.Equal(t, si.EventRecord_SET, records[1].EventChangeType)
	assert.Equal(t, si.EventRecord_QUEUE_MAX, records[1].EventChangeDetail)
	assert.Equal(t, si.EventRecord_QUEUE, records[2].Type)
	assert.Equal(t, si.EventRecord_SET, records[2].EventChangeType)
	assert.Equal(t, si.EventRecord_QUEUE_GUARANTEED, records[2].EventChangeDetail)
}

func TestQueueRunningAppsForSingleAllocationApp(t *testing.T) {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	// single leaf under root
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")

	app := newApplication(appID1, "default", "root.leaf")
	app.SetQueue(leaf)
	leaf.AddApplication(app)

	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "failed to create basic resource")

	ask := newAllocationAsk("ask-1", appID1, res)
	err = app.AddAllocationAsk(ask)
	assert.NilError(t, err, "failed to add ask")

	alloc := newAllocationWithKey("ask-1", appID1, nodeID1, res)
	app.AddAllocation(alloc)
	assert.Equal(t, app.CurrentState(), Running.String(), "app state should be running")
	assert.Equal(t, leaf.runningApps, uint64(1), "leaf should have 1 app running")

	_, err = app.allocateAsk(ask)
	assert.NilError(t, err, "failed to decrease pending resources")

	app.RemoveAllocation(alloc.GetAllocationKey(), si.TerminationType_STOPPED_BY_RM)
	assert.Equal(t, app.CurrentState(), Completing.String(), "app state should be completing")
	assert.Equal(t, leaf.runningApps, uint64(0), "leaf should have 0 app running")
}

func isNewApplicationEvent(t *testing.T, app *Application, record *si.EventRecord) {
	assert.Equal(t, si.EventRecord_APP, record.Type, "incorrect event type, expect app")
	assert.Equal(t, app.ApplicationID, record.ObjectID, "incorrect object ID, expected application ID")
	assert.Equal(t, si.EventRecord_ADD, record.EventChangeType, "incorrect change type, expected add")
	assert.Equal(t, si.EventRecord_APP_NEW, record.EventChangeDetail, "incorrect change detail, expected none")
}

func TestQueue_allocatedResFits_Root(t *testing.T) {
	const first = "first"
	const second = "second"
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	tests := []struct {
		name   string
		quota  map[string]string
		used   map[string]string
		change map[string]string
		want   bool
	}{
		{"all nil", nil, nil, nil, true},
		{"nil max no usage", nil, nil, map[string]string{first: "1"}, false},
		{"nil max set usage", nil, map[string]string{first: "1"}, map[string]string{second: "1"}, false},
		{"max = usage other in alloc", map[string]string{first: "1"}, map[string]string{first: "1"}, map[string]string{second: "1"}, false},
		{"max = usage same in alloc", map[string]string{first: "1"}, map[string]string{first: "1"}, map[string]string{first: "1"}, false},
		{"usage over undefined max other in alloc", map[string]string{first: "1"}, map[string]string{second: "1"}, map[string]string{first: "1"}, true},
		{"usage over undefined max same in alloc", map[string]string{first: "1"}, map[string]string{second: "1"}, map[string]string{second: "1"}, false},
		{"partial fit", map[string]string{first: "2"}, map[string]string{first: "1", second: "1"}, map[string]string{first: "1", second: "1"}, false},
		{"all fit no usage", map[string]string{first: "2", second: "2"}, nil, map[string]string{first: "1", second: "1"}, true},
		{"all fit with usage", map[string]string{first: "2", second: "2"}, map[string]string{first: "1", second: "1"}, map[string]string{first: "1", second: "1"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var quota, used, change *resources.Resource
			quota, err = resources.NewResourceFromConf(tt.quota)
			assert.NilError(t, err, "failed to create basic resource: quota")
			root.SetMaxResource(quota)
			used, err = resources.NewResourceFromConf(tt.used)
			assert.NilError(t, err, "failed to create basic resource: used")
			root.allocatedResource = used
			change, err = resources.NewResourceFromConf(tt.change)
			assert.NilError(t, err, "failed to create basic resource: diff")
			assert.Equal(t, root.allocatedResFits(change), tt.want, "allocatedResFits incorrect state returned")
		})
	}
}

func TestQueueSetMaxRunningApps(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil queue setMaxRunningApps")
		}
	}()
	queue := &Queue{}
	maxApps := uint64(10)

	queue.SetMaxRunningApps(maxApps)
	assert.Equal(t, maxApps, queue.maxRunningApps)

	queue = nil
	queue.SetMaxRunningApps(maxApps)
}

func TestQueue_allocatedResFits_Other(t *testing.T) {
	const first = "first"
	const second = "second"
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *Queue
	leaf, err = createManagedQueue(root, "leaf", false, nil)

	tests := []struct {
		name   string
		quota  map[string]string
		used   map[string]string
		change map[string]string
		want   bool
	}{
		{"all nil", nil, nil, nil, true},
		{"nil max no usage", nil, nil, map[string]string{first: "1"}, true},
		{"nil max set usage", nil, map[string]string{first: "1"}, map[string]string{second: "1"}, true},
		{"max = usage other in alloc", map[string]string{first: "1"}, map[string]string{first: "1"}, map[string]string{second: "1"}, true},
		{"max = usage same in alloc", map[string]string{first: "1"}, map[string]string{first: "1"}, map[string]string{first: "1"}, false},
		{"usage over zero max other in alloc", map[string]string{first: "1", second: "0"}, map[string]string{second: "1"}, map[string]string{first: "1"}, true},
		{"usage over zero max same in alloc", map[string]string{first: "1", second: "0"}, map[string]string{second: "1"}, map[string]string{second: "1"}, false},
		{"partial fit", map[string]string{first: "2", second: "1"}, map[string]string{first: "1", second: "1"}, map[string]string{first: "1", second: "1"}, false},
		{"all fit no usage", map[string]string{first: "2", second: "2"}, nil, map[string]string{first: "1", second: "1"}, true},
		{"all fit", map[string]string{first: "2", second: "2"}, map[string]string{first: "1", second: "1"}, map[string]string{first: "1", second: "1"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var quota, used, change *resources.Resource
			quota, err = resources.NewResourceFromConf(tt.quota)
			assert.NilError(t, err, "failed to create basic resource: quota")
			leaf.maxResource = quota
			used, err = resources.NewResourceFromConf(tt.used)
			assert.NilError(t, err, "failed to create basic resource: used")
			leaf.allocatedResource = used
			change, err = resources.NewResourceFromConf(tt.change)
			assert.NilError(t, err, "failed to create basic resource: diff")
			assert.Equal(t, leaf.allocatedResFits(change), tt.want, "allocatedResFits incorrect state returned")
		})
	}
}

func TestQueueBackoffProperties(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create basic root queue")
	parent, err := createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Equal(t, uint64(0), parent.GetMaxAppUnschedAskBackoff())
	assert.Equal(t, 30*time.Second, parent.GetBackoffDelay())

	leaf, err := createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	assert.Equal(t, uint64(0), leaf.GetMaxAppUnschedAskBackoff())
	assert.Equal(t, 30*time.Second, leaf.GetBackoffDelay())

	props := map[string]string{
		configs.ApplicationUnschedulableAsksBackoffDelay: "123s",
		configs.ApplicationUnschedulableAsksBackoff:      "12",
	}
	parent2, err := createManagedQueueWithProps(root, "parent2", true, nil, props)
	assert.NilError(t, err, "failed to create parent queue")
	assert.Equal(t, uint64(12), parent2.GetMaxAppUnschedAskBackoff())
	assert.Equal(t, 123*time.Second, parent2.GetBackoffDelay())

	leaf2, err := createManagedQueue(parent2, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")
	assert.Equal(t, uint64(12), leaf2.GetMaxAppUnschedAskBackoff())
	assert.Equal(t, 123*time.Second, leaf2.GetBackoffDelay())

	props = map[string]string{
		configs.ApplicationUnschedulableAsksBackoffDelay: "xyz",
		configs.ApplicationUnschedulableAsksBackoff:      "-4",
	}
	leaf3, err := createManagedQueueWithProps(root, "parent2", false, nil, props)
	assert.NilError(t, err, "failed to create leaf3 queue")
	assert.Equal(t, uint64(0), leaf3.GetMaxAppUnschedAskBackoff())
	assert.Equal(t, 30*time.Second, leaf3.GetBackoffDelay())
}

func TestQueue_IsQCPreemptionRunning(t *testing.T) {
	// create the root
	root, err := createManagedQueueMaxApps(nil, "root", true, nil, 1)
	assert.NilError(t, err, "queue create failed")
	parent, err := createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "failed to create parent queue")

	var leaf, leaf2, leaf11, leaf111 *Queue
	leaf, err = createManagedQueue(parent, "leaf", true, nil)
	assert.NilError(t, err, "failed to create leaf queue")
	leaf2, err = createManagedQueue(parent, "leaf2", false, nil)
	assert.NilError(t, err, "failed to create leaf2 queue")

	leaf11, err = createManagedQueue(leaf, "leaf11", true, nil)
	assert.NilError(t, err, "failed to create leaf11 queue")

	leaf111, err = createManagedQueue(leaf11, "leaf111", false, nil)
	assert.NilError(t, err, "failed to create leaf111 queue")

	// root.parent is running. any queue located in this hierarchy (both upwards and downwards) should return true. All branches of parent should return true.
	parent.isQuotaChangePreemptionRunning = true
	assert.Equal(t, parent.IsQCPreemptionRunning(), true)
	assert.Equal(t, root.IsQCPreemptionRunning(), true)
	assert.Equal(t, leaf111.IsQCPreemptionRunning(), true)
	assert.Equal(t, leaf11.IsQCPreemptionRunning(), true)
	assert.Equal(t, leaf.IsQCPreemptionRunning(), true)
	assert.Equal(t, leaf2.IsQCPreemptionRunning(), true)

	// reset
	parent.isQuotaChangePreemptionRunning = false

	// root.parent.leaf111 (leaf queue) is running. any queue located in this hierarchy (upwards) should return true. Other branches of parent should return false.
	leaf111.isQuotaChangePreemptionRunning = true
	assert.Equal(t, parent.IsQCPreemptionRunning(), true)
	assert.Equal(t, root.IsQCPreemptionRunning(), true)
	assert.Equal(t, leaf111.IsQCPreemptionRunning(), true)
	assert.Equal(t, leaf11.IsQCPreemptionRunning(), true)
	assert.Equal(t, leaf.IsQCPreemptionRunning(), true)
	assert.Equal(t, leaf2.IsQCPreemptionRunning(), false)

	// reset
	leaf111.isQuotaChangePreemptionRunning = false

	// root.parent.leaf2 (leaf queue) is running. any queue located in this hierarchy (upwards) should return true. Other branches of parent should return false.
	leaf2.isQuotaChangePreemptionRunning = true
	assert.Equal(t, parent.IsQCPreemptionRunning(), true)
	assert.Equal(t, root.IsQCPreemptionRunning(), true)
	assert.Equal(t, leaf111.IsQCPreemptionRunning(), false)
	assert.Equal(t, leaf11.IsQCPreemptionRunning(), false)
	assert.Equal(t, leaf.IsQCPreemptionRunning(), false)
	assert.Equal(t, leaf2.IsQCPreemptionRunning(), true)
}
