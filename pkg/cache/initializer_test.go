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
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 200
            vcore: 2
        queues:
          -
            name: a
            properties:
              x: 345
              application.sort.policy: fair
          -
            name: b
            properties:
              x: 345
              application.sort.policy: fifo
            resources:
              guaranteed:
                memory: 50
                vcore: 1
              max:
                memory: 100
                vcore: 1
  -
    name: gpu
    queues:
      -
        name: root
        properties:
          x: 123
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 400
            vcore: 2
        queues:
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
    assert.Equal(t, 3, len(defaultPartition.queues))
    rootQueue := defaultPartition.queues["root"]
    assert.Equal(t, 2, len(rootQueue.children))
    assert.Assert(t, resources.Equals(createResource(100, 2), rootQueue.GuaranteedResource))
    assert.Assert(t, resources.Equals(createResource(200, 2), rootQueue.MaxResource))
    assert.Equal(t, rootQueue.IsLeafQueue(), false)

    // check root.a queue in default partition
    assert.Equal(t, rootQueue.children["a"], defaultPartition.queues["root"].children["a"])
    assert.Equal(t, "345", rootQueue.children["a"].Properties["x"])
    assert.Equal(t, "fair", rootQueue.children["a"].Properties["application.sort.policy"])
    assert.Assert(t, resources.Equals(resources.NewResource(), defaultPartition.queues["root"].children["a"].GuaranteedResource))
    assert.Assert(t, resources.Equals(createResource(200, 2), defaultPartition.queues["root"].children["a"].MaxResource))
    assert.Assert(t, rootQueue == defaultPartition.queues["root"].children["a"].Parent)
    assert.Equal(t, rootQueue.children["a"].IsLeafQueue(), true)

    // check root.b queue in default partition
    assert.Equal(t, rootQueue.children["b"], defaultPartition.queues["root"].children["b"])
    assert.Equal(t, "345", rootQueue.children["b"].Properties["x"])
    assert.Equal(t, "fifo", rootQueue.children["b"].Properties["application.sort.policy"])
    assert.Assert(t, resources.Equals(createResource(50, 1), defaultPartition.queues["root"].children["b"].GuaranteedResource))
    assert.Assert(t, resources.Equals(createResource(100, 1), defaultPartition.queues["root"].children["b"].MaxResource))
    assert.Assert(t, rootQueue == defaultPartition.queues["root"].children["a"].Parent)
    assert.Equal(t, rootQueue.children["a"].IsLeafQueue(), true)

    // Check gpu partition
    gpuPartition := clusterInfo.partitions["[rm-123]gpu"]
    rootQueue = gpuPartition.queues["root"]
    assert.Assert(t, resources.Equals(createResource(400, 2), rootQueue.MaxResource))
    assert.Equal(t, rootQueue.IsLeafQueue(), false)
    assert.Equal(t, rootQueue.children["b"].IsLeafQueue(), true)
    assert.Equal(t, len(rootQueue.children), 1)

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
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 200
            vcore: 2
        queues:
          -
            name: a
            Properties:
              x: 345
  -
    name: default
    queues:
      -
        name: root
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 400
            vcore: 2
        queues:
         -
           name: b
           Properties:
             x: 345
`
    assertErrorFromInitialization(t, data)
}

// Should not have duplicated queue definition under same parent
func TestDuplicatedQueueName(t *testing.T) {
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        properties:
          x: 123
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 200
            vcore: 2
        queues:
          -
            name: a
            properties:
              x: 345
              application.sort.policy: fair
          -
            name: a
            properties:
              x: 345
              application.sort.policy: fifo
`
    assertErrorFromInitialization(t, data)
}

// Should allow to have duplicated queue names under different parent queues
func TestDuplicatedQueueNameUnderDifferentParent(t *testing.T) {
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 200
            vcore: 2
        queues:
          -
            name: a
            Properties:
              x: 345
            queues:
              -
                name: dup
                Properties:
                  x: 345
          -
            name: b
            Properties:
              x: 345
            queues:
              -
                name: dup
                Properties:
                  x: 345
`
    assertErrorFromInitialization(t, data)
}

//// Root queue should always exist
//func TestRootQueueNotExist(t *testing.T) {
//    data := `
//partitions:
//  -
//    name: default
//    queues:
//      -
//        name: a
//        Properties:
//          x: 345
//`
//    assertErrorFromInitialization(t, data)
//}

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
    // memory not fit
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 200
            vcore: 2
        queues:
          -
            name: a
            resources:
              max:
                memory: 300
                vcore: 2
`
    assertErrorFromInitialization(t, data)

    // cpu not fit
    data = `
partitions:
  -
    name: default
    queues:
      -
        name: root
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 200
            vcore: 2
        queues:
          -
            name: a
            resources:
              max:
                memory: 100
                vcore: 5
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
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 200
            vcore: 2
        queues:
          -
            name: a
            resources:
              max:
                memory: 100
                vcore: 2
              guaranteed:
                memory: 200
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
        resources:
          guaranteed:
            memory: 100
            vcore: 2
        queues:
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
        resources:
          guaranteed:
            memory: 100
            vcore: 2
        queues:
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

func TestQueuePropertiesInherit(t *testing.T) {
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        properties:
          application.default.priority: 1
          application.default.type: "batch"
        resources:
          guaranteed:
            memory: 100
            vcore: 2
        queues:
          -
            name: a
            properties:
              application.default.priority: 2
              a.self: 000
            resources:
              guaranteed:
                memory: 80
                vcore: 1
          -
            name: b
            properties:
              application.default.priority: 3
              b.self: 999
            resources:
              guaranteed:
                memory: 20
                vcore: 1
`
    clusterInfo := NewClusterInfo()
    configs.MockSchedulerConfigByData([]byte(data))
    if _, err := UpdateClusterInfoFromConfigFile(clusterInfo, "rm-123", "default-policy-group"); err != nil {
        t.Errorf("Error when load clusterInfo from config %v", err)
        return
    }

    assert.Equal(t, len(clusterInfo.partitions), 1)
    assert.Equal(t, len(clusterInfo.partitions["[rm-123]default"].queues), 3)

    root := clusterInfo.partitions["[rm-123]default"].queues["root"]
    a := root.children["a"]
    b := root.children["b"]

    assert.Equal(t, len(root.Properties), 2)
    assert.Equal(t, len(a.Properties), 3)
    assert.Equal(t, len(b.Properties), 3)

    // property not defined at child queue, overwrite the parent value
    assert.Equal(t, root.Properties["application.default.priority"], "1")
    assert.Equal(t, a.Properties["application.default.priority"], "2")
    assert.Equal(t, b.Properties["application.default.priority"], "3")

    // properties only defined in child queue
    assert.Equal(t, a.Properties["a.self"], "000")
    assert.Equal(t, b.Properties["b.self"], "999")

    // property not defined at child queue, directly inherited from parent
    assert.Equal(t, a.Properties["application.default.type"], "batch")
    assert.Equal(t, b.Properties["application.default.type"], "batch")
}

func TestQueueNameValidation(t *testing.T) {
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: root
        queues:
          -
            name: a.b.c
`
    assertErrorFromInitialization(t, data)

    data = `
partitions:
  -
    name: default
    queues:
      -
        name: root
        queues:
          -
            name: a%bc
`
    assertErrorFromInitialization(t, data)

    data = `
partitions:
  -
    name: default
    queues:
      -
        name: root
        queues:
          -
            name: this_is_a_too_long_queue_name
`
    assertErrorFromInitialization(t, data)

    data = `
partitions:
  -
    name: default
    queues:
      -
        name: root
        queues:
          -
            name: queue_abc_123
`
    assertNoErrorFromInitialization(t, data)

    data = `
partitions:
  -
    name: default
    queues:
      -
        name: root
        queues:
          -
            name: queue-a-b-c
`
    assertNoErrorFromInitialization(t, data)

    data = `
partitions:
  -
    name: default
    queues:
      -
        name: root
        queues:
          -
            name: level1
            queues:
              -
                name: level2
                queues:
                 -
                   name: level3
                   queues:
                    -
                      name: level4
`
    assertNoErrorFromInitialization(t, data)

    data = `
partitions:
  -
    name: default
    queues:
      -
        name: root
        queues:
          -
            name: level1
            queues:
              -
                name: level2
                queues:
                 -
                   name: level3
                   queues:
                    -
                      name: invalid$%)
`
    assertErrorFromInitialization(t, data)
}

func TestMultipleRootQueuesUnderOnePartition(t *testing.T) {
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: root1
        queues:
          -
            name: root1-queue
      -
        name: root2
        queues:
          -
            name: root2-queue
`
    assertErrorFromInitialization(t, data)
}