/*
Copyright 2019 The Unity Scheduler Authors

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

package cache

import (
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/configs"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
    "testing"
)

func createRootQueue() (*QueueInfo, error) {
    rootConf := configs.QueueConfig{
        Name:  "root",
        Parent: true,
        Queues: nil,
        Properties: make(map[string]string, 0),
    }
    return NewManagedQueue(rootConf, nil)
}

func createManagedQueue(parentQI *QueueInfo, name string, parent bool) (*QueueInfo, error) {
    rootConf := configs.QueueConfig{
        Name:  name,
        Parent: parent,
        Queues: nil,
        Properties: make(map[string]string, 0),
    }
    return NewManagedQueue(rootConf, parentQI)
}

func createUnManagedQueue(parent *QueueInfo, name string, leaf bool) (*QueueInfo, error) {
    return NewUnmanagedQueue(name, leaf, parent)
}

// base test for creating a managed queue
func TestQueueInfo(t *testing.T) {
    // create the root
    root, err := createRootQueue()
    if err != nil {
        t.Errorf("failed to create basic root queue: %v", err)
        return
    }
    // check the state of the queue
    if !root.isManaged && !root.isLeaf && !root.IsRunning() {
        t.Errorf("root queue status is incorrect")
        return
    }
    // allocations should be nil
    if !resources.IsZero(root.GetAllocatedResource()) {
        t.Errorf("root queue must not have allocations set on create")
        return
    }
}

func TestAllocationCalcRoot(t *testing.T) {
    // create the root
    root, err := createRootQueue()
    if err != nil {
        t.Errorf("failed to create basic root queue: %v", err)
        return
    }
    res := map[string]string{"memory":"100", "vcores":"10"}
    allocation, _ := resources.NewResourceFromConf(res)
    err = root.IncAllocatedResource(allocation)
    if err != nil {
        t.Errorf("root queue allocation failed on increment %v", err)
    }
    err = root.DecAllocatedResource(allocation)
    if err != nil {
        t.Errorf("root queue allocation failed on decrement %v", err)
    }
    if !resources.IsZero(root.allocatedResource) {
        t.Errorf("root queue allocations are not zero: %v", root.allocatedResource)
    }
    err = root.DecAllocatedResource(allocation)
    if err == nil {
        t.Errorf("root queue allocation should have failed to decrement %v", err)
    }
}

func TestAllocationCalcSub(t *testing.T) {
    // create the root
    root, err := createRootQueue()
    if err != nil {
        t.Errorf("failed to create basic root queue: %v", err)
        return
    }
    parent, err := createManagedQueue(root, "parent", true)
    if err != nil {
        t.Errorf("failed to create parent queue: %v", err)
        return
    }

    res := map[string]string{"memory":"100", "vcores":"10"}
    allocation, _ := resources.NewResourceFromConf(res)
    err = parent.IncAllocatedResource(allocation)
    if err != nil {
        t.Errorf("parent queue allocation failed on increment %v", err)
    }
    err = parent.DecAllocatedResource(allocation)
    if err != nil {
        t.Errorf("parent queue allocation failed on decrement %v", err)
    }
    if !resources.IsZero(root.allocatedResource) {
        t.Errorf("root queue allocations are not zero: %v", root.allocatedResource)
    }
    err = root.DecAllocatedResource(allocation)
    if err == nil {
        t.Errorf("root queue allocation should have failed to decrement %v", root.allocatedResource)
    }

    // add to the parent, remove from root and then try to remove from parent: root should complain
    err = parent.IncAllocatedResource(allocation)
    if err != nil {
        t.Errorf("parent queue allocation failed on increment %v", err)
    }
    err = root.DecAllocatedResource(allocation)
    if err != nil {
        t.Errorf("root queue allocation failed on decrement %v", err)
    }
    err = parent.DecAllocatedResource(allocation)
    if err == nil {
        t.Errorf("parent queue allocation should have failed on decrement %v, %v", root.allocatedResource, parent.allocatedResource)
    }
}

func TestManagedSubQueues(t *testing.T) {
    // create the root
    root, err := createRootQueue()
    if err != nil {
        t.Errorf("failed to create basic root queue: %v", err)
        return
    }
    parent, err := createManagedQueue(root, "parent", true)
    if err != nil {
        t.Errorf("failed to create parent queue: %v", err)
        return
    }
    if parent.isLeaf {
        t.Errorf("parent queue is not marked as a parent")
    }
    if len(root.children) == 0 {
        t.Errorf("parent queue is not added to the root queue")
    }
    leaf, err := createManagedQueue(parent, "leaf", false)
    if err != nil {
        t.Errorf("failed to create leaf queue: %v", err)
        return
    }
    // both parent and leaf are marked for removal
    parent.MarkQueueForRemoval()
    if !leaf.IsDraining() || !parent.IsDraining() {
        t.Errorf("queues are not marked for removal (not in drianing state)")
    }
    // try to remove the parent
    if parent.RemoveQueue() {
        t.Errorf("parent queue should not have been removed as it has a child")
    }
    // remove the child
    if !leaf.RemoveQueue() && len(parent.children) != 0 {
        t.Errorf("leaf queue should have been removed and parent updated and was not")
    }
    // now set some allocation in the parent and try removal again
    res := map[string]string{"memory":"100", "vcores":"10"}
    allocation, _ := resources.NewResourceFromConf(res)
    err = parent.IncAllocatedResource(allocation)
    if err != nil {
        t.Errorf("allocation increase failed on parent: %v", err)
    }
    if parent.RemoveQueue() {
        t.Errorf("parent queue should not have been removed as it has an allocation")
    }
    err = parent.DecAllocatedResource(allocation)
    if err != nil {
        t.Errorf("parent queue allocation failed on decrement %v", err)
    }
    if !parent.RemoveQueue() {
        t.Errorf("parent queue should have been removed and was not")
    }
}

func TestMergeProperties(t *testing.T) {
    base := map[string]string{"first":"first value", "second":"second value"}
    // merge same values should not change anything
    merged := mergeProperties(base, base)
    if len(merged) != 2 {
        t.Errorf("merge failed not exactly 2 keys: %v", merged)
    }
    change := map[string]string{"third": "third value"}
    merged = mergeProperties(base, change)
    if len(merged) != 3 {
        t.Errorf("merge failed not exactly 3 keys: %v", merged)
    }
    change = map[string]string{"third": "changed"}
    merged = mergeProperties(base, change)
    if len(merged) != 3 {
        t.Errorf("merge failed not exactly 3 keys: %v", merged)
    }
}