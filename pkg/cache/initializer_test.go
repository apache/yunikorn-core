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
	"testing"

	"gotest.tools/assert"

	"github.com/cloudera/yunikorn-core/pkg/common/configs"
	"github.com/cloudera/yunikorn-core/pkg/common/resources"
)

func createResource(memory, cpu int) *resources.Resource {
	return resources.NewResourceFromMap(map[string]resources.Quantity{"memory": resources.Quantity(memory), "vcore": resources.Quantity(cpu)})
}

// Test most basic, normal case
func TestBasicInitializer(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: root
        properties:
          x: 123
        queues:
          - name: production
            properties:
              x: 345
              application.sort.policy: fair
          - name: test
            properties:
              x: 345
              application.sort.policy: fifo
            resources:
              guaranteed: {memory: 50, vcore: 1}
              max: {memory: 100, vcore: 1}
  - name: gpu
    queues:
      - name: root
        properties:
          x: 123
        queues:
         - name: test
           Properties:
             x: 345
`
	clusterInfo := NewClusterInfo()
	configs.MockSchedulerConfigByData([]byte(data))
	if _, err := SetClusterInfoFromConfigFile(clusterInfo, "rm-123", "default-policy-group"); err != nil {
		t.Errorf("Error when load clusterInfo from config %v", err)
		return
	}

	assert.Equal(t, 2, len(clusterInfo.partitions))

	// Check default partition
	defaultPartition := clusterInfo.partitions["[rm-123]default"]
	rootQueue := defaultPartition.getQueue("root")
	assert.Equal(t, 2, len(rootQueue.children))
	assert.Equal(t, rootQueue.IsLeafQueue(), false)
	testQueue := defaultPartition.getQueue("root.test")
	if testQueue == nil {
		t.Errorf("Failed parsing the test queue in default partition")
		return
	}
	if len(testQueue.Properties) != 2 {
		t.Errorf("Failed parsing the properties on test queue in default partition")
	}

	// Check gpu partition
	gpuPartition := clusterInfo.partitions["[rm-123]gpu"]
	testQueue = gpuPartition.getQueue("root.test")
	if testQueue == nil {
		t.Errorf("Failed parsing the test queue in gpu partition")
		return
	}
	if len(testQueue.Properties) != 1 {
		t.Errorf("Failed parsing the properties on test queue in gpu partition")
	}
}

func initializationAndCheck(t *testing.T, data string, expectFail bool) {
	configs.MockSchedulerConfigByData([]byte(data))

	clusterInfo := NewClusterInfo()
	_, err := SetClusterInfoFromConfigFile(clusterInfo, "rm-123", "default-policy-group")

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
  - name: default
    queues:
      - name: root
  - name: default
    queues:
      - name: root
`
	assertErrorFromInitialization(t, data)
}

func TestChildMaxGreaterThanParentMax(t *testing.T) {
	// memory not fit
	data := `
partitions:
  - name: default
    queues:
      - name: test
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 200
            vcore: 2
        queues:
          - name: subtest
            resources:
              max:
                memory: 300
                vcore: 2
`
	assertErrorFromInitialization(t, data)

	// cpu not fit
	data = `
partitions:
  - name: default
    queues:
      - name: test
        resources:
          guaranteed:
            memory: 100
            vcore: 2
          max:
            memory: 200
            vcore: 2
        queues:
          - name: subtest
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
  - name: default
    queues:
      - name: test
        resources:
          guaranteed:
            memory: 300
            vcore: 2
          max:
            memory: 200
            vcore: 2
`
	assertErrorFromInitialization(t, data)
}

func TestChildrenSumGuaranteedGreaterThanParentGuaranteed(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: test
        resources:
          guaranteed:
            memory: 100
            vcore: 2
        queues:
          - name: subtest1
            resources:
              guaranteed:
                memory: 80
                vcore: 1
          - name: subtest2
            resources:
              guaranteed:
                memory: 20
                vcore: 1
`
	// children add up to exactly the parent size: no error
	assertNoErrorFromInitialization(t, data)

	data = `
partitions:
  - name: default
    queues:
      - name: test
        resources:
          guaranteed:
            memory: 100
            vcore: 2
        queues:
          - name: subtest1
            resources:
              guaranteed:
                memory: 80
                vcore: 1
          - name: subtest2
            resources:
              guaranteed:
                memory: 30
                vcore: 1
`
	// children are larger than parent: error
	assertErrorFromInitialization(t, data)
}

func TestQueuePropertiesInherit(t *testing.T) {
	data := `
partitions:
  -
    name: default
    queues:
      - name: root
        properties:
          application.default.priority: 1
          application.default.type: "batch"
        queues:
          - name: production
            properties:
              application.default.priority: 2
              production.self: 000
          - name: test
            properties:
              application.default.priority: 3
              test.self: 999
`
	clusterInfo := NewClusterInfo()
	configs.MockSchedulerConfigByData([]byte(data))
	if _, err := SetClusterInfoFromConfigFile(clusterInfo, "rm-123", "default-policy-group"); err != nil {
		t.Errorf("Error when load clusterInfo from config %v", err)
		return
	}

	assert.Equal(t, len(clusterInfo.partitions), 1)

	root := clusterInfo.partitions["[rm-123]default"].getQueue("root")
	production := root.children["production"]
	test := root.children["test"]

	assert.Equal(t, len(root.Properties), 2)
	assert.Equal(t, len(production.Properties), 3)
	assert.Equal(t, len(test.Properties), 3)

	// property not defined at child queue, overwrite the parent value
	assert.Equal(t, root.Properties["application.default.priority"], "1")
	assert.Equal(t, production.Properties["application.default.priority"], "2")
	assert.Equal(t, test.Properties["application.default.priority"], "3")

	// properties only defined in child queue
	assert.Equal(t, production.Properties["production.self"], "000")
	assert.Equal(t, test.Properties["test.self"], "999")

	// property not defined at child queue, directly inherited from parent
	assert.Equal(t, production.Properties["application.default.type"], "batch")
	assert.Equal(t, test.Properties["application.default.type"], "batch")
}

// Test most basic, normal case
func TestReloadInitializer(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: root
        properties:
          x: 123
        queues:
          - name: production
            properties:
              x: 345
              application.sort.policy: fair
          - name: test
            properties:
              x: 345
              application.sort.policy: fifo
            resources:
              guaranteed: {memory: 50, vcore: 1}
              max: {memory: 100, vcore: 1}
  - name: gpu
    queues:
      - name: root
        properties:
          x: 123
        queues:
         - name: test
           Properties:
             x: 345
`

	rmId := "rm-123"
	policyGroup := "default-policy-group"
	clusterInfo := NewClusterInfo()
	clusterInfo.policyGroup = policyGroup
	configs.MockSchedulerConfigByData([]byte(data))
	if _, err := SetClusterInfoFromConfigFile(clusterInfo, rmId, policyGroup); err != nil {
		t.Errorf("Error when load clusterInfo from config %v", err)
		return
	}

	assert.Equal(t, 2, len(clusterInfo.partitions))

	// Check default partition
	defaultPartition := clusterInfo.partitions["["+rmId+"]default"]
	rootQueue := defaultPartition.getQueue("root")
	assert.Equal(t, 2, len(rootQueue.children))
	assert.Equal(t, rootQueue.IsLeafQueue(), false)
	prodQueue := defaultPartition.getQueue("root.production")
	if prodQueue == nil {
		t.Errorf("Failed parsing the production queue in default partition")
		return
	}
	testQueue := defaultPartition.getQueue("root.test")
	if testQueue == nil {
		t.Errorf("Failed parsing the test queue in default partition")
		return
	}
	if len(testQueue.Properties) != 2 {
		t.Errorf("Failed parsing the properties on test queue in default partition")
	}

	// Check gpu partition
	gpuPartition := clusterInfo.partitions["["+rmId+"]gpu"]
	testQueue = gpuPartition.getQueue("root.test")
	if testQueue == nil {
		t.Errorf("Failed parsing the test queue in gpu partition")
		return
	}
	if len(testQueue.Properties) != 1 {
		t.Errorf("Failed parsing the properties on test queue in gpu partition")
	}

	data = `
partitions:
  - name: default
    queues:
      - name: root
        properties:
          x: 123
        queues:
          - name: test
            properties:
              x: 345
            resources:
              guaranteed: {memory: 100, vcore: 10}
              max: {memory: 200, vcore: 20}
          - name: new-queue
`
	configs.MockSchedulerConfigByData([]byte(data))
	if _, _, err := UpdateClusterInfoFromConfigFile(clusterInfo, rmId); err != nil {
		t.Errorf("Error when load clusterInfo from config %v", err)
		return
	}
	// Check the partitions: config update just marks as deleted does not do full remove
	assert.Equal(t, 2, len(clusterInfo.partitions))
	gpuPartition = clusterInfo.partitions["["+rmId+"]gpu"]
	if gpuPartition != nil && !gpuPartition.IsDraining() {
		t.Errorf("Failed removing the gpu partition")
		return
	}
	// Check default partition
	defaultPartition = clusterInfo.partitions["["+rmId+"]default"]
	rootQueue = defaultPartition.getQueue("root")
	assert.Equal(t, 3, len(rootQueue.children))
	assert.Equal(t, rootQueue.IsLeafQueue(), false)
	prodQueue = defaultPartition.getQueue("root.production")
	if !prodQueue.IsDraining() {
		t.Errorf("Failed removing the production queue in default partition")
		return
	}
	newQueue := defaultPartition.getQueue("root.new-queue")
	if newQueue == nil {
		t.Errorf("Failed parsing new-queue queue in default partition")
		return
	}
	testQueue = defaultPartition.getQueue("root.test")
	if testQueue == nil {
		t.Errorf("Failed parsing the test queue in default partition")
		return
	}
	if len(testQueue.Properties) != 1 {
		t.Errorf("Failed parsing the properties on test queue in default partition")
	}
	if testQueue.GuaranteedResource.Resources["memory"] != 100 {
		t.Errorf("Failed parsing GuaranteedResource settings on test queue in default partition")
	}
	if testQueue.MaxResource.Resources["vcore"] != 20 {
		t.Errorf("Failed parsing MaxResource settings on test queue in default partition")
	}
}
