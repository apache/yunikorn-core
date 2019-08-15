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

package cache

import (
    "github.com/cloudera/yunikorn-core/pkg/common/commonevents"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
    "testing"
    "time"
)

func createAllocation(queue, nodeID, allocID, appID string) *si.Allocation {
    resAlloc := &si.Resource{
        Resources: map[string]*si.Quantity{
            resources.MEMORY: {Value: 1},
        },
    }
    return &si.Allocation{
        AllocationKey: allocID,
        ResourcePerAlloc: resAlloc,
        QueueName: queue,
        NodeId: nodeID,
        ApplicationId: appID,
    }
}

func createAllocationProposal(queue, nodeID, allocID, appID string) *commonevents.AllocationProposal {
    resAlloc := resources.NewResourceFromMap(
        map[string]resources.Quantity{
            resources.MEMORY: 1,
        })

    return &commonevents.AllocationProposal{
        NodeId:            nodeID,
        ApplicationId:     appID,
        QueueName:         queue,
        AllocatedResource: resAlloc,
        AllocationKey:     allocID,
    }
}

func waitForPartitionState(t *testing.T, partition *PartitionInfo, state string, timeoutMs int) {
    for i:=0; i*100 < timeoutMs; i++ {
        if !partition.stateMachine.Is(state) {
            time.Sleep(time.Duration(100 * time.Millisecond))
        } else {
            return
        }
    }
    t.Fatalf("Failed to wait for partition %s state change: %s", partition.Name, partition.stateMachine.Current())
}

func TestLoadPartitionConfig(t *testing.T) {
    data := `
partitions:
  - name: default
    queues:
      - name: production
      - name: test
        queues:
          - name: admintest
`

    partition, err := CreatePartitionInfo([]byte(data))
    if err != nil {
        t.Error(err)
        return
    }
    // There is a queue setup as the config must be valid when we run
    root := partition.getQueue("root")
    if root == nil {
        t.Errorf("root queue not found in partition")
    }
    adminTest :=  partition.getQueue("root.test.admintest")
    if adminTest == nil {
        t.Errorf("root.test.adminTest queue not found in partition")
    }
}

func TestLoadDeepQueueConfig(t *testing.T) {
    data := `
partitions:
  - name: default
    queues:
      - name: root
        queues:
          - name: level1
            queues:
              - name: level2
                queues:
                  - name: level3
                    queues:
                      - name: level4
                        queues:
                          - name: level5
`

    partition, err := CreatePartitionInfo([]byte(data))
    if err != nil {
        t.Error(err)
        return
    }
    // There is a queue setup as the config must be valid when we run
    root := partition.getQueue("root")
    if root == nil {
        t.Errorf("root queue not found in partition")
    }
    adminTest :=  partition.getQueue("root.level1.level2.level3.level4.level5")
    if adminTest == nil {
        t.Errorf("root.level1.level2.level3.level4.level5 queue not found in partition")
    }
}

func TestAddNewNode(t *testing.T) {
    data := `
partitions:
  - name: default
    queues:
      - name: root
`

    partition, err := CreatePartitionInfo([]byte(data))
    if err != nil {
        t.Error(err)
        return
    }
    memVal := resources.Quantity(1000)
    nodeID := "node-1"
    node1 := newNodeInfoForTest(nodeID, resources.NewResourceFromMap(
        map[string]resources.Quantity{resources.MEMORY: memVal}), nil)
    // add a node this must work
    err = partition.addNewNode(node1, nil)
    if err != nil || partition.GetNode(nodeID) == nil {
        t.Errorf("add node to partition should not have failed: %v", err)
    }
    // check partition resources
    memRes := partition.totalPartitionResource.Resources[resources.MEMORY]
    if memRes != memVal {
        t.Errorf("add node to partition did not update total resources expected %d got %d", memVal, memRes)
    }

    // add the same node this must fail
    err = partition.addNewNode(node1, nil)
    if err == nil {
        t.Errorf("add same node to partition should have failed, node count is %d", partition.GetTotalNodeCount())
    }

    // mark partition stopped, no new node can be added
    if err = partition.HandlePartitionEvent(Stop); err != nil {
        t.Errorf("partition state change failed: %v", err)
        return
    }
    waitForPartitionState(t, partition, Stopped.String(), 1000)
    nodeID = "node-2"
    node2 := newNodeInfoForTest(nodeID, resources.NewResourceFromMap(
        map[string]resources.Quantity{resources.MEMORY: memVal}), nil)

    // add a second node to the test
    err = partition.addNewNode(node2, nil)
    if err == nil || partition.GetNode(nodeID) != nil {
        t.Errorf("add new node to stopped partition should have failed")
    }

    // mark partition active again, the new node can be added
    if err = partition.HandlePartitionEvent(Start); err != nil {
        t.Errorf("partition state change failed: %v", err)
        return
    }
    waitForPartitionState(t, partition, Active.String(), 1000)
    err = partition.addNewNode(node2, nil)

    if err != nil || partition.GetNode(nodeID) == nil {
        t.Errorf("add node to partition should not have failed: %v", err)
    }
    // check partition resources
    memRes = partition.totalPartitionResource.Resources[resources.MEMORY]
    if memRes !=  2 * memVal {
        t.Errorf("add node to partition did not update total resources expected %d got %d", memVal, memRes)
    }
    if partition.GetTotalNodeCount() != 2 {
        t.Errorf("node list was not updated, incorrect number of nodes registered expected 2 got %d", partition.GetTotalNodeCount())
    }

    // mark partition stopped, no new node can be added
    if err = partition.HandlePartitionEvent(Remove); err != nil {
        t.Errorf("partition state change failed: %v", err)
        return
    }
    waitForPartitionState(t, partition, Draining.String(), 1000)
    nodeID = "node-3"
    node3 := newNodeInfoForTest(nodeID, resources.NewResourceFromMap(
        map[string]resources.Quantity{resources.MEMORY: memVal}), nil)
    // add a third node to the test
    err = partition.addNewNode(node3, nil)
    if err == nil || partition.GetNode(nodeID) != nil {
        t.Errorf("add new node to removed partition should have failed")
    }

}

func TestRemoveNode(t *testing.T) {
    data := `
partitions:
  - name: default
    queues:
      - name: root
`

    partition, err := CreatePartitionInfo([]byte(data))
    if err != nil {
        t.Error(err)
        return
    }
    memVal := resources.Quantity(1000)
    node1 := newNodeInfoForTest("node-1", resources.NewResourceFromMap(
        map[string]resources.Quantity{resources.MEMORY: memVal}), nil)
    // add a node this must work
    err = partition.addNewNode(node1, nil)
    if err != nil {
        t.Errorf("add node to partition should not have failed: %v", err)
    }
    // check partition resources
    memRes := partition.totalPartitionResource.Resources[resources.MEMORY]
    if memRes != memVal {
        t.Errorf("add node to partition did not update total resources expected %d got %d", memVal, memRes)
    }

    // remove a bogus node should not do anything
    partition.RemoveNode("does-not-exist")
    if partition.GetTotalNodeCount() != 1 {
        t.Errorf("node list was updated, node was removed expected 1 nodes got %d", partition.GetTotalNodeCount())
    }

    // remove the node this cannot fail
    partition.RemoveNode(node1.NodeId)
    if partition.GetTotalNodeCount() != 0 {
        t.Errorf("node list was not updated, node was not removed expected 0 got %d", partition.GetTotalNodeCount())
    }
    // check partition resources
    memRes = partition.totalPartitionResource.Resources[resources.MEMORY]
    if memRes != 0 {
        t.Errorf("remove node from partition did not update total resources expected 0 got %d", memRes)
    }
}

func TestAddNewApplication(t *testing.T) {
    data := `
partitions:
  - name: default
    queues:
      - name: root
        queues:
        - name: default
`

    partition, err := CreatePartitionInfo([]byte(data))
    if err != nil {
        t.Error(err)
        return
    }

    // add a new app
    appInfo := newApplicationInfo("app-1", "default", "root.default")
    err = partition.addNewApplication(appInfo, true)
    if err != nil {
        t.Errorf("add application to partition should not have failed: %v", err)
    }
    // add the same app with failIfExist true should fail
    err = partition.addNewApplication(appInfo, true)
    if err == nil {
        t.Errorf("add same application to partition should have failed but did not")
    }
    // add the same app with failIfExist false should not fail
    err = partition.addNewApplication(appInfo, false)
    if err != nil {
        t.Errorf("add same application with failIfExist false should not have failed but did %v", err)
    }

    // add app to a parent queue should fail
    appInfo = newApplicationInfo("app-3", "default", "root")
    err = partition.addNewApplication(appInfo, true)
    if err == nil || partition.getApplication("app-3") != nil {
        t.Errorf("add application to parent queue should have failed")
    }

    // add app to a non existing queue should fail
    appInfo = newApplicationInfo("app-4", "default", "does-not-exist")
    err = partition.addNewApplication(appInfo, true)
    if err == nil || partition.getApplication("app-4") != nil {
        t.Errorf("add application to non existing queue should have failed")
    }

    // mark partition stopped, no new application can be added
    if err = partition.HandlePartitionEvent(Stop); err != nil {
        t.Errorf("partition state change failed: %v", err)
        return
    }
    waitForPartitionState(t, partition, Stopped.String(), 1000)

    appInfo = newApplicationInfo("app-2", "default", "root.default")
    err = partition.addNewApplication(appInfo, true)
    if err == nil || partition.getApplication("app-2") != nil {
        t.Errorf("add application on stopped partition should have failed but did not")
    }

    // mark partition for deletion, no new application can be added
    partition.stateMachine.SetState(Active.String())
    if err = partition.HandlePartitionEvent(Remove); err != nil {
        t.Errorf("partition state change failed: %v", err)
        return
    }
    waitForPartitionState(t, partition, Draining.String(), 1000)
    appInfo = newApplicationInfo("app-3", "default", "root.default")
    err = partition.addNewApplication(appInfo, true)
    if err == nil || partition.getApplication("app-3") != nil {
        t.Errorf("add application on draining partition should have failed but did not")
    }
}

func TestAddNodeWithAllocations(t *testing.T) {
    data := `
partitions:
  - name: default
    queues:
      - name: root
        queues:
        - name: default
`

    partition, err := CreatePartitionInfo([]byte(data))
    if err != nil {
        t.Error(err)
        return
    }

    // add a new app
    appID := "app-1"
    queueName := "root.default"
    appInfo := newApplicationInfo(appID, "default", queueName)
    err = partition.addNewApplication(appInfo, true)
    if err != nil {
        t.Errorf("add application to partition should not have failed: %v", err)
    }

    // add a node with allocations: must have the correct app added already
    memVal := resources.Quantity(1000)
    nodeID := "node-1"
    node1 := newNodeInfoForTest(nodeID, resources.NewResourceFromMap(
        map[string]resources.Quantity{resources.MEMORY: memVal}), nil)
    allocs := []*si.Allocation{createAllocation(queueName, nodeID, "alloc-1", appID)}
    // add a node this must work
    err = partition.addNewNode(node1, allocs)
    if err != nil || partition.GetNode(nodeID) == nil {
        t.Errorf("add node to partition should not have failed: %v", err)
    }
    // check partition resources
    memRes := partition.totalPartitionResource.Resources[resources.MEMORY]
    if memRes != memVal {
        t.Errorf("add node to partition did not update total resources expected %d got %d", memVal, memRes)
    }
    // check partition allocation count
    if partition.GetTotalAllocationCount() != 1 {
        t.Errorf("add node to partition did not add allocation expected 1 got %d", partition.GetTotalAllocationCount())
    }

    // check the leaf queue usage
    qi := partition.getQueue(queueName)
    if qi.allocatedResource.Resources[resources.MEMORY] != 1 {
        t.Errorf("add node to partition did not add queue %s allocation expected 1 got %d",
            qi.GetQueuePath(), qi.allocatedResource.Resources[resources.MEMORY])
    }
    // check the root queue usage
    qi = partition.getQueue("root")
    if qi.allocatedResource.Resources[resources.MEMORY] != 1 {
        t.Errorf("add node to partition did not add queue %s allocation expected 1 got %d",
            qi.GetQueuePath(), qi.allocatedResource.Resources[resources.MEMORY])
    }

    nodeID = "node-partial-fail"
    node2 := newNodeInfoForTest(nodeID, resources.NewResourceFromMap(
        map[string]resources.Quantity{resources.MEMORY: memVal}), nil)
    allocs = []*si.Allocation{createAllocation(queueName, nodeID, "alloc-2", "app-2")}
    // add a node this partially fails: node is added, allocation is not added and thus we have an error
    err = partition.addNewNode(node2, allocs)
    if err == nil {
        t.Errorf("add node to partition should have returned an error for allocations")
    }
    if partition.GetNode(nodeID) != nil {
        t.Errorf("node should not have been added to partition and was")
    }
}

func TestAddNewAllocation(t *testing.T) {
    data := `
partitions:
  - name: default
    queues:
      - name: root
        queues:
        - name: default
`

    partition, err := CreatePartitionInfo([]byte(data))
    if err != nil {
        t.Error(err)
        return
    }

    // add a new app
    appID := "app-1"
    queueName := "root.default"
    appInfo := newApplicationInfo(appID, "default", queueName)
    err = partition.addNewApplication(appInfo, true)
    if err != nil {
        t.Errorf("add application to partition should not have failed: %v", err)
    }

    memVal := resources.Quantity(1000)
    nodeID := "node-1"
    node1 := newNodeInfoForTest(nodeID, resources.NewResourceFromMap(
        map[string]resources.Quantity{resources.MEMORY: memVal}), nil)
    // add a node this must work
    err = partition.addNewNode(node1, nil)
    if err != nil || partition.GetNode(nodeID) == nil {
        t.Errorf("add node to partition should not have failed: %v", err)
    }

    alloc, err := partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc-1", appID))
    if err != nil {
        t.Errorf("adding allocation failed and should not have failed: %v", err)
    }
    if partition.allocations[alloc.AllocationProto.Uuid] == nil {
        t.Errorf("add allocation to partition not found in the allocation list")
    }
    // check the leaf queue usage
    qi := partition.getQueue(queueName)
    if qi.allocatedResource.Resources[resources.MEMORY] != 1 {
        t.Errorf("add allocation to partition did not add queue %s allocation expected 1 got %d",
            qi.GetQueuePath(), qi.allocatedResource.Resources[resources.MEMORY])
    }

    alloc, err = partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc-2", appID))
    if err != nil || partition.allocations[alloc.AllocationProto.Uuid] == nil {
        t.Errorf("adding allocation failed and should not have failed: %v", err)
    }
    // check the root queue usage
    qi = partition.getQueue(queueName)
    if qi.allocatedResource.Resources[resources.MEMORY] != 2 {
        t.Errorf("add allocation to partition did not add queue %s allocation expected 2 got %d",
            qi.GetQueuePath(), qi.allocatedResource.Resources[resources.MEMORY])
    }

    alloc, err = partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc-3", "does-not-exist"))
    if err == nil || alloc != nil || len(partition.allocations) != 2 {
        t.Errorf("adding allocation worked and should have failed: %v", alloc)
    }

    // mark partition stopped, no new application can be added
    if err = partition.HandlePartitionEvent(Stop); err != nil {
        t.Errorf("partition state change failed: %v", err)
        return
    }
    waitForPartitionState(t, partition, Stopped.String(), 1000)
    alloc, err = partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc-4", appID))
    if err == nil || alloc != nil || len(partition.allocations) != 2 {
        t.Errorf("adding allocation worked and should have failed: %v", alloc)
    }
    // mark partition for removal, no new application can be added
    partition.stateMachine.SetState(Active.String())
    if err = partition.HandlePartitionEvent(Remove); err != nil {
        t.Errorf("partition state change failed: %v", err)
        return
    }
    waitForPartitionState(t, partition, Draining.String(), 1000)
    alloc, err = partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc-4", appID))
    if err != nil || alloc == nil || len(partition.allocations) != 3 {
        t.Errorf("adding allocation did not work and should have (allocation length %d: %v", len(partition.allocations), err)
    }
}

func TestRemoveApp(t *testing.T) {
    data := `
partitions:
  - name: default
    queues:
      - name: root
        queues:
        - name: default
`

    partition, err := CreatePartitionInfo([]byte(data))
    if err != nil {
        t.Error(err)
        return
    }
    // add a new app that will just sit around to make sure we remove the right one
    appNotRemoved := "will_not_remove"
    queueName := "root.default"
    appInfo := newApplicationInfo(appNotRemoved, "default", queueName)
    err = partition.addNewApplication(appInfo, true)
    if err != nil {
        t.Errorf("add application to partition should not have failed: %v", err)
        return
    }
    // add a node to allow adding an allocation
    memVal := resources.Quantity(1000)
    nodeID := "node-1"
    node1 := newNodeInfoForTest(nodeID, resources.NewResourceFromMap(
        map[string]resources.Quantity{resources.MEMORY: memVal}), nil)
    // add a node this must work
    err = partition.addNewNode(node1, nil)
    if err != nil || partition.GetNode(nodeID) == nil {
        t.Errorf("add node to partition should not have failed: %v", err)
        return
    }
    alloc, err := partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc_not_removed", appNotRemoved))
    if err != nil {
        t.Errorf("add allocation to partition should not have failed: %v", err)
        return
    }
    uuid := alloc.AllocationProto.Uuid

    app, allocs := partition.RemoveApplication("does_not_exist")
    if app != nil && len(allocs) != 0 {
        t.Errorf("non existing application returned unexpected values: application info %v (allocs = %v)", app, allocs)
    }

    // add another new app
    appID := "app-1"
    appInfo = newApplicationInfo(appID, "default", queueName)
    err = partition.addNewApplication(appInfo, true)
    if err != nil {
        t.Errorf("add application to partition should not have failed: %v", err)
    }

    // remove the newly added app (no allocations)
    app, allocs = partition.RemoveApplication(appID)
    if app == nil && len(allocs) != 0 {
        t.Errorf("existing application without allocations returned allocations %v", allocs)
    }
    if len(partition.applications) != 1 {
        t.Errorf("existing application was not removed")
        return
    }

    // add the application again and then an allocation
    _ = partition.addNewApplication(appInfo, true)
    _, _ = partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc-1", appID))

    // remove the newly added app
    app, allocs = partition.RemoveApplication(appID)
    if app == nil && len(allocs) != 1 {
        t.Errorf("existing application with allocations returned unexpected allocations %v", allocs)
    }
    if len(partition.applications) != 1 {
        t.Errorf("existing application was not removed")
    }
    if partition.allocations[uuid] == nil {
        t.Errorf("allocation that should have been left was removed")
    }
}

func TestRemoveAppAllocs(t *testing.T) {
    data := `
partitions:
  - name: default
    queues:
      - name: root
        queues:
        - name: default
`

    partition, err := CreatePartitionInfo([]byte(data))
    if err != nil {
        t.Error(err)
        return
    }
    // add a new app that will just sit around to make sure we remove the right one
    appNotRemoved := "will_not_remove"
    queueName := "root.default"
    appInfo := newApplicationInfo(appNotRemoved, "default", queueName)
    err = partition.addNewApplication(appInfo, true)
    if err != nil {
        t.Errorf("add application to partition should not have failed: %v", err)
        return
    }
    // add a node to allow adding an allocation
    memVal := resources.Quantity(1000)
    nodeID := "node-1"
    node1 := newNodeInfoForTest(nodeID, resources.NewResourceFromMap(
        map[string]resources.Quantity{resources.MEMORY: memVal}), nil)
    // add a node this must work
    err = partition.addNewNode(node1, nil)
    if err != nil || partition.GetNode(nodeID) == nil {
        t.Errorf("add node to partition should not have failed: %v", err)
        return
    }
    alloc, err := partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc_not_removed", appNotRemoved))
    alloc, err = partition.addNewAllocation(createAllocationProposal(queueName, nodeID, "alloc-1", appNotRemoved))
    if err != nil {
        t.Errorf("add allocation to partition should not have failed: %v", err)
        return
    }
    uuid := alloc.AllocationProto.Uuid

    allocs := partition.releaseAllocationsForApplication(nil)
    if len(allocs) != 0 {
        t.Errorf("empty removal request returned allocations: %v", allocs)
    }
    // create a new release without app: should just return
    toRelease := commonevents.NewReleaseAllocation("", "", partition.Name, "", si.AllocationReleaseResponse_TerminationType(0))
    allocs = partition.releaseAllocationsForApplication(toRelease)
    if len(allocs) != 0 {
        t.Errorf("removal request for non existing application returned allocations: %v", allocs)
    }
    // create a new release with app, non existing allocation: should just return
    toRelease = commonevents.NewReleaseAllocation("does_not exist", appNotRemoved, partition.Name, "", si.AllocationReleaseResponse_TerminationType(0))
    allocs = partition.releaseAllocationsForApplication(toRelease)
    if len(allocs) != 0 {
        t.Errorf("removal request for non existing allocation returned allocations: %v", allocs)
    }
    // create a new release with app, existing allocation: should return 1 alloc
    toRelease = commonevents.NewReleaseAllocation(uuid, appNotRemoved, partition.Name, "", si.AllocationReleaseResponse_TerminationType(0))
    allocs = partition.releaseAllocationsForApplication(toRelease)
    if len(allocs) != 1 {
        t.Errorf("removal request for existing allocation returned wrong allocations: %v", allocs)
    }
    // create a new release with app, no uuid: should return last left alloc
    toRelease = commonevents.NewReleaseAllocation("", appNotRemoved, partition.Name, "", si.AllocationReleaseResponse_TerminationType(0))
    allocs = partition.releaseAllocationsForApplication(toRelease)
    if len(allocs) != 1 {
        t.Errorf("removal request for existing allocation returned wrong allocations: %v", allocs)
    }
    if len(partition.allocations) != 0 {
        t.Errorf("removal requests did not remove all allocations: %v", partition.allocations)
    }
}

func TestCreateQueues(t *testing.T) {
    data := `
partitions:
  - name: default
    queues:
      - name: root
        queues:
        - name: default
`

    partition, err := CreatePartitionInfo([]byte(data))
    if err != nil {
        t.Error(err)
        return
    }
    // top level should fail
    err = partition.CreateQueues("test")
    if err == nil {
        t.Errorf("top level queue creation did not fail")
    }

    // create below leaf
    err = partition.CreateQueues("root.default.test")
    if err == nil {
        t.Errorf("'root.default.test' queue creation did not")
    }

    // single level create
    err = partition.CreateQueues("root.test")
    if err != nil {
        t.Errorf("'root.test' queue creation failed")
    }
    queue := partition.getQueue("root.test")
    if queue == nil {
        t.Errorf("'root.test' queue creation failed without error")
    }
    if queue != nil && !queue.isLeaf && !queue.isManaged {
        t.Errorf("'root.test' queue creation failed not created with correct settings: %v", queue)
    }

    // multiple level create
    err = partition.CreateQueues("root.parent.test")
    if err != nil {
        t.Errorf("'root.parent.test' queue creation failed")
    }
    queue = partition.getQueue("root.parent.test")
    if queue == nil {
        t.Errorf("'root.parent.test' queue creation failed without error")
    }
    if queue != nil && !queue.isLeaf && !queue.isManaged {
        t.Errorf("'root.parent.test' queue not created with correct settings: %v", queue)
    }
    queue = queue.Parent
    if queue == nil {
        t.Errorf("'root.parent' queue creation failed: parent is not set correctly")
    }
    if queue != nil && queue.isLeaf && !queue.isManaged {
        t.Errorf("'root.parent' parent queue not created with correct settings: %v", queue)
    }

    // deep level create
    err = partition.CreateQueues("root.parent.next.level.test.leaf")
    if err != nil {
        t.Errorf("'root.parent.next.level.test.leaf' queue creation failed")
    }
    queue = partition.getQueue("root.parent.next.level.test.leaf")
    if queue == nil {
        t.Errorf("'root.parent.next.level.test.leaf' queue creation failed without error")
    }
    if queue != nil && !queue.isLeaf && !queue.isManaged {
        t.Errorf("'root.parent.next.level.test.leaf' queue not created with correct settings: %v", queue)
    }

}
