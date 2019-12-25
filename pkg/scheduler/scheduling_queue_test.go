/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

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
    "github.com/cloudera/yunikorn-core/pkg/cache"
    "github.com/cloudera/yunikorn-core/pkg/common/configs"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "strconv"
    "testing"
)

// create the root queue, base for all testing
func createRootQueue() (*SchedulingQueue, error) {
    rootConf := configs.QueueConfig{
        Name:  "root",
        Parent: true,
        Queues: nil,
        Properties: make(map[string]string, 0),
    }
    root, err := cache.NewManagedQueue(rootConf, nil)
    return NewSchedulingQueueInfo(root, nil), err
}

// wrapper around the create calls using the same syntax as an unmanaged queue
func createManagedQueue(parentQI *SchedulingQueue, name string, parent bool) (*SchedulingQueue, error) {
    childConf := configs.QueueConfig{
        Name:  name,
        Parent: parent,
        Queues: nil,
        Properties: make(map[string]string, 0),
    }
    child, err := cache.NewManagedQueue(childConf, parentQI.CachedQueueInfo)
    return NewSchedulingQueueInfo(child, parentQI), err
}

// wrapper around the create calls using the same syntax as a managed queue
func createUnManagedQueue(parentQI *SchedulingQueue, name string, parent bool) (*SchedulingQueue, error) {
    child, err := cache.NewUnmanagedQueue(name, !parent, parentQI.CachedQueueInfo)
    return NewSchedulingQueueInfo(child, parentQI), err
}

// base test for creating a managed queue
func TestQueueBasics(t *testing.T) {
    // create the root
    root, err := createRootQueue()
    if err != nil {
        t.Fatalf("failed to create basic root queue: %v", err)
    }
    // check the state of the queue
    if !root.isManaged() && !root.isLeafQueue() && !root.isRunning() {
        t.Errorf("root queue status is incorrect")
    }
    // allocations should be nil
    if !resources.IsZero(root.allocating) && !resources.IsZero(root.pendingResource) {
        t.Errorf("root queue must not have allocations set on create")
    }
}

func TestManagedSubQueues(t *testing.T) {
    // create the root
    root, err := createRootQueue()
    if err != nil {
        t.Fatalf("failed to create basic root queue: %v", err)
    }

    // single parent under root
    parent, err := createManagedQueue(root, "parent", true)
    if err != nil {
        t.Fatalf("failed to create parent queue: %v", err)
    }
    if parent.isLeafQueue() || !parent.isManaged() {
        t.Errorf("parent queue is not marked as managed parent")
    }
    if len(root.childrenQueues) == 0 {
        t.Errorf("parent queue is not added to the root queue")
    }
    // add a leaf under the parent
    leaf, err := createManagedQueue(parent, "leaf", false)
    if err != nil {
        t.Fatalf("failed to create leaf queue: %v", err)
    }
    if len(parent.childrenQueues) == 0 {
        t.Errorf("leaf queue is not added to the parent queue")
    }
    if !leaf.isLeafQueue() || !leaf.isManaged() {
        t.Errorf("leaf queue is not marked as managed leaf")
    }

    // cannot remove child with app in it
    app := NewSchedulingApplication(&cache.ApplicationInfo{ApplicationId:"test"})
    leaf.AddSchedulingApplication(app)

    // both parent and leaf are marked for removal
    parent.CachedQueueInfo.MarkQueueForRemoval()
    if !leaf.isDraining() || !parent.isDraining() {
        t.Errorf("queues are not marked for removal (not in draining state)")
    }
    // try to remove the parent
    if parent.RemoveQueue() {
        t.Errorf("parent queue should not have been removed as it has a child")
    }
    // try to remove the child
    if leaf.RemoveQueue() {
        t.Errorf("leaf queue should not have been removed")
    }
    // remove the app (dirty way)
    delete(leaf.applications, "test")
    if !leaf.RemoveQueue() && len(parent.childrenQueues) != 0 {
        t.Errorf("leaf queue should have been removed and parent updated and was not")
    }
}

func TestUnManagedSubQueues(t *testing.T) {
    // create the root
    root, err := createRootQueue()
    if err != nil {
        t.Fatalf("failed to create basic root queue: %v", err)
    }

    // single parent under root
    parent, err := createUnManagedQueue(root, "parent", true)
    if err != nil {
        t.Fatalf("failed to create parent queue: %v", err)
    }
    if parent.isLeafQueue() || parent.isManaged() {
        t.Errorf("parent queue is not marked as parent")
    }
    if len(root.childrenQueues) == 0 {
        t.Errorf("parent queue is not added to the root queue")
    }
    // add a leaf under the parent
    leaf, err := createUnManagedQueue(parent, "leaf", false)
    if err != nil {
        t.Fatalf("failed to create leaf queue: %v", err)
    }
    if len(parent.childrenQueues) == 0 {
        t.Errorf("leaf queue is not added to the parent queue")
    }
    if !leaf.isLeafQueue() || leaf.isManaged() {
        t.Errorf("leaf queue is not marked as managed leaf")
    }

    // cannot remove child with app in it
    app := NewSchedulingApplication(&cache.ApplicationInfo{ApplicationId:"test"})
    leaf.AddSchedulingApplication(app)

    // try to mark parent and leaf for removal
    parent.CachedQueueInfo.MarkQueueForRemoval()
    if leaf.isDraining() || parent.isDraining() {
        t.Errorf("queues are marked for removal (draining state not for unmanaged queues)")
    }
    // try to remove the parent
    if parent.RemoveQueue() {
        t.Errorf("parent queue should not have been removed as it has a child")
    }
    // try to remove the child
    if leaf.RemoveQueue() {
        t.Errorf("leaf queue should not have been removed")
    }
    // remove the app (dirty way)
    delete(leaf.applications, "test")
    if !leaf.RemoveQueue() && len(parent.childrenQueues) != 0 {
        t.Errorf("leaf queue should have been removed and parent updated and was not")
    }
}

func TestPendingCalc(t *testing.T) {
    // create the root
    root, err := createRootQueue()
    if err != nil {
        t.Fatalf("failed to create basic root queue: %v", err)
    }
    parent, err := createManagedQueue(root, "parent", true)
    if err != nil {
        t.Fatalf("failed to create parent queue: %v", err)
    }

    res := map[string]string{"memory":"100", "vcores":"10"}
    allocation, _ := resources.NewResourceFromConf(res)
    parent.IncPendingResource(allocation)
    if !resources.Equals(root.pendingResource, allocation) {
        t.Errorf("root queue pending allocation failed to increment expected %v, got %v", allocation, root.pendingResource)
    }
    parent.DecPendingResourceFromTheQueueAndParents(allocation)
    if !resources.IsZero(root.pendingResource) {
        t.Errorf("root queue pending allocation failed to decrement expected 0, got %v", root.pendingResource)
    }
    // Not allowed to go negative: both will be zero after this
    root.IncPendingResource(allocation)
    parent.DecPendingResourceFromTheQueueAndParents(allocation)
    if !resources.IsZero(root.pendingResource) {
        t.Errorf("root queue pending allocation failed to decrement expected zero, got %v", root.pendingResource)
    }
    if !resources.IsZero(parent.pendingResource) {
        t.Errorf("parent queue pending allocation should have failed to decrement expected zero, got %v", parent.pendingResource)
    }
}

func TestGetChildQueueInfos(t *testing.T) {
    // create the root
    root, err := createRootQueue()
    if err != nil {
        t.Fatalf("failed to create basic root queue: %v", err)
    }
    //
    parent, err := createManagedQueue(root, "parent-man", true)
    if err != nil {
        t.Fatalf("failed to create managed parent queue: %v", err)
    }
    for i := 0; i < 10; i++ {
        _, err := createManagedQueue(parent, "leaf-man" + strconv.Itoa(i), false)
        if err != nil {
            t.Errorf("failed to create managed queue: %v", err)
        }
    }
    if len(parent.childrenQueues) != 10 {
        t.Errorf("managed leaf queues are not added to the parent queue, expected 10 children got %d", len(parent.childrenQueues))
    }

    parent, err = createUnManagedQueue(root, "parent-un", true)
    if err != nil {
        t.Fatalf("failed to create unamanged parent queue: %v", err)
    }
    for i := 0; i < 10; i++ {
        _, err := createUnManagedQueue(parent, "leaf-un-" + strconv.Itoa(i), false)
        if err != nil {
            t.Errorf("failed to create unmanaged queue: %v", err)
        }
    }
    if len(parent.childrenQueues) != 10 {
        t.Errorf("unmanaged leaf queues are not added to the parent queue, expected 10 children got %d", len(parent.childrenQueues))
    }

    // check the root queue
    if len(root.childrenQueues) != 2 {
        t.Errorf("parent queues are not added to the root queue, expected 2 children got %d", len(root.childrenQueues))
    }
}
