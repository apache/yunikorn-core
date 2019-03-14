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
    "gotest.tools/assert"
    "testing"
)

func createResource(memory, cpu int) *resources.Resource {
    return resources.NewResourceFromMap(map[string]resources.Quantity{"memory": resources.Quantity(memory), "vcore": resources.Quantity(cpu)})
}

// Test most basic, normal case
func TestBasicInitializer(t *testing.T) {
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        properties:
          x: 123
        children:
          - a
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 200
            vcore: 2
      -
        name: a
        properties:
          x: 345
          job.sort.policy: fair
  -
    name: gpu
    queues:
      -
        name: root
        properties:
          x: 123
        children:
          - b
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 400
            vcore: 2
      -
        name: b
        Properties:
          x: 345
`
    clusterInfo := NewClusterInfo()
    configs.MockSchedulerConfigByData([]byte(data))
    if _, err := UpdateClusterInfoFromConfigFile(clusterInfo, "rm-123", "default-policy-group"); err != nil {
        t.Errorf("Error when load clusterInfo from config %v", err)
        return
    }

    assert.Equal(t, 2, len(clusterInfo.partitions))

    // Check default partition
    defaultPartition := clusterInfo.partitions["[rm-123]default"]
    assert.Equal(t, 2, len(defaultPartition.queues))
    rootQueue := defaultPartition.queues["root"]
    assert.Equal(t, 1, len(rootQueue.children))
    assert.Assert(t, resources.Equals(createResource(100, 2), rootQueue.GuaranteedResource))
    assert.Assert(t, resources.Equals(createResource(200, 2), rootQueue.MaxResource))
    assert.Equal(t, rootQueue.children["a"], defaultPartition.queues["a"])
    assert.Equal(t, "345", rootQueue.children["a"].Properties["x"])
    assert.Equal(t, "fair", rootQueue.children["a"].Properties["job.sort.policy"])
    assert.Assert(t, resources.Equals(resources.NewResource(), defaultPartition.queues["a"].GuaranteedResource))
    assert.Assert(t, resources.Equals(createResource(200, 2), defaultPartition.queues["a"].MaxResource))
    assert.Assert(t, rootQueue == defaultPartition.queues["a"].Parent)

    // Check gpu partition
    gpuPartition := clusterInfo.partitions["[rm-123]gpu"]
    rootQueue = gpuPartition.queues["root"]
    assert.Assert(t, resources.Equals(createResource(400, 2), rootQueue.MaxResource))
}

func initializationAndCheck(t *testing.T, data string, expectFail bool) {
    configs.MockSchedulerConfigByData([]byte(data))

    clusterInfo := NewClusterInfo()
    _, err := UpdateClusterInfoFromConfigFile(clusterInfo, "rm-123", "default-policy-group")

    if expectFail && err == nil {
        t.Errorf("Expect fail, but suceeded")
    } else if !expectFail && err != nil {
        t.Errorf("Expect success, but failed, error = %s", err.Error())
    }

    if err != nil {
        t.Logf("Saw expected error=%s", err)
    }
}

func assertNoErrorFromInitialization(t *testing.T, data string) {
    initializationAndCheck(t, data, false)
}

func assertErrorFromInitialization(t *testing.T, data string) {
    initializationAndCheck(t, data, true)
}

// Should not have duplicated partitions defined in config
func TestDuplicatedPartition(t *testing.T) {
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        children:
          - a
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 200
            vcore: 2
      -
        name: a
        Properties:
          x: 345
  -
    name: default
    queues:
      -
        name: root
        children:
          - b
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 400
            vcore: 2
      -
        name: b
        Properties:
          x: 345
`
    assertErrorFromInitialization(t, data)
}

// Should not have duplicated queue definition in config
func TestDuplicatedQueueName(t *testing.T) {
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        children:
          - a
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 200
            vcore: 2
      -
        name: a
        Properties:
          x: 345
      -
        name: a
        Properties:
          x: 345
`
    assertErrorFromInitialization(t, data)
}

// Root queue should always exist
func TestRootQueueNotExist(t *testing.T) {
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: a
        Properties:
          x: 345
`
    assertErrorFromInitialization(t, data)
}

// Multi-top level queue should be avoided (only root is top level queue)
func TestMultiTopLevelQueue(t *testing.T) {
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: a
      -
        name: root
`
    assertErrorFromInitialization(t, data)
}

// Defined some queue, but not be able to traverse from root
func TestNonAccessibleQueues(t *testing.T) {
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: a
      -
        name: root
        children:
          - a
      -
        name: b
`
    // b is not accessible from root
    assertErrorFromInitialization(t, data)
}

// Don't allow define queue=x under multiple parents
func TestMultipleParents(t *testing.T) {
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: a
      -
        name: root
        children:
          - a
          - b
          - c
      -
        name: b
      -
        name: c
        children:
          - a
`
    // b is not accessible from root
    assertErrorFromInitialization(t, data)
}

func TestChildMaxGreaterThanParentMax(t *testing.T) {
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        children:
          - a
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 200
            vcore: 2
      -
        name: a
        Properties:
          x: 345
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 300
            vcore: 2
`
    assertErrorFromInitialization(t, data)
}

func TestQueueGuaranteedGreaterThanMax(t *testing.T) {
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        children:
          - a
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 200
            vcore: 2
      -
        name: a
        Properties:
          x: 345
        resources:
          guaranteed:
            memory: 80
            vcore: 2
          max:
            memory: 50
            vcore: 2
`
    assertErrorFromInitialization(t, data)
}

func TestChildrenSumGuaranteedGreaterThanParentGuaranteed(t *testing.T) {
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        children:
          - a
          - b
        resources:
          guaranteed:
            memory: 100
            vcore: 2
      -
        name: a
        resources:
          guaranteed:
            memory: 80
            vcore: 1
      -
        name: b
        resources:
          guaranteed:
            memory: 20
            vcore: 1
`
    assertNoErrorFromInitialization(t, data)

    data = `
partitions:
  -
    name: default
    queues:
      -
        name: root
        children:
          - a
          - b
        resources:
          guaranteed:
            memory: 100
            vcore: 2
      -
        name: a
        resources:
          guaranteed:
            memory: 80
            vcore: 1
      -
        name: b
        resources:
          guaranteed:
            memory: 30
            vcore: 1
`
    // Now a + b > root
    assertErrorFromInitialization(t, data)
}
