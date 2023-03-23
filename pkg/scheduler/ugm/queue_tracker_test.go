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

package ugm

import (
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
)

func TestQTIncreaseTrackedResource(t *testing.T) {
	// Queue setup:
	// root->parent->child1->child12
	// root->parent->child2
	// root->parent->child12 (similar name like above leaf queue, but it is being treated differently as similar names are allowed)
	queueTracker := newQueueTracker("root")
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}

	err = queueTracker.increaseTrackedResource("", "", true, false, usage1)
	assert.Error(t, err, "mandatory parameters are missing. queuepath: , application id: , resource usage: "+usage1.String())

	err = queueTracker.increaseTrackedResource(queuePath1, TestApp1, true, false, usage1)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage1, err)
	}

	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	err = queueTracker.increaseTrackedResource(queuePath2, TestApp2, true, false, usage2)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath2, TestApp2, usage2, err)
	}

	usage3, err := resources.NewResourceFromConf(map[string]string{"mem": "30M", "vcore": "30"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	err = queueTracker.increaseTrackedResource(queuePath3, TestApp3, true, false, usage3)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath3, TestApp3, usage3, err)
	}

	usage4, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	err = queueTracker.increaseTrackedResource(queuePath4, TestApp4, true, false, usage4)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath4, TestApp4, usage4, err)
	}
	actualResources := getQTResource(queueTracker)

	assert.Equal(t, "map[mem:80000000 vcore:80000]", actualResources["root"].String(), "wrong resource")
	assert.Equal(t, "map[mem:80000000 vcore:80000]", actualResources["root.parent"].String(), "wrong resource")
	assert.Equal(t, "map[mem:40000000 vcore:40000]", actualResources["root.parent.child1"].String(), "wrong resource")
	assert.Equal(t, "map[mem:30000000 vcore:30000]", actualResources["root.parent.child1.child12"].String(), "wrong resource")
	assert.Equal(t, "map[mem:20000000 vcore:20000]", actualResources["root.parent.child2"].String(), "wrong resource")
	assert.Equal(t, "map[mem:20000000 vcore:20000]", actualResources["root.parent.child12"].String(), "wrong resource")
	assert.Equal(t, 4, len(queueTracker.runningApplications))
}

func TestQTDecreaseTrackedResource(t *testing.T) {
	// Queue setup:
	// root->parent->child1
	// root->parent->child2
	queueTracker := newQueueTracker("root")
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "70M", "vcore": "70"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}

	err = queueTracker.increaseTrackedResource(queuePath1, TestApp1, true, false, usage1)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage1, err)
	}
	assert.Equal(t, 1, len(queueTracker.runningApplications))

	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	err = queueTracker.increaseTrackedResource(queuePath2, TestApp2, true, false, usage2)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath2, TestApp2, usage2, err)
	}
	actualResources := getQTResource(queueTracker)

	assert.Equal(t, 2, len(queueTracker.runningApplications))
	assert.Equal(t, "map[mem:90000000 vcore:90000]", actualResources["root"].String(), "wrong resource")
	assert.Equal(t, "map[mem:90000000 vcore:90000]", actualResources["root.parent"].String(), "wrong resource")
	assert.Equal(t, "map[mem:70000000 vcore:70000]", actualResources["root.parent.child1"].String(), "wrong resource")
	assert.Equal(t, "map[mem:20000000 vcore:20000]", actualResources["root.parent.child2"].String(), "wrong resource")

	usage3, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}

	err = queueTracker.increaseTrackedResource("", "", true, false, usage3)
	assert.Error(t, err, "mandatory parameters are missing. queuepath: , application id: , resource usage: "+usage3.String())

	removeQT, err := queueTracker.decreaseTrackedResource(queuePath1, TestApp1, false, usage3, false)
	if err != nil {
		t.Fatalf("unable to decrease tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage3, err)
	}
	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")

	removeQT, err = queueTracker.decreaseTrackedResource(queuePath2, TestApp2, false, usage3, false)
	if err != nil {
		t.Fatalf("unable to decrease tracked resource: queuepath %s, app %s, res %v, error %t", queuePath2, TestApp2, usage3, err)
	}
	actualResources1 := getQTResource(queueTracker)

	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")
	assert.Equal(t, "map[mem:70000000 vcore:70000]", actualResources1["root"].String(), "wrong resource")
	assert.Equal(t, "map[mem:70000000 vcore:70000]", actualResources1["root.parent"].String(), "wrong resource")
	assert.Equal(t, "map[mem:60000000 vcore:60000]", actualResources1["root.parent.child1"].String(), "wrong resource")
	assert.Equal(t, "map[mem:10000000 vcore:10000]", actualResources1["root.parent.child2"].String(), "wrong resource")
	assert.Equal(t, len(queueTracker.childQueueTrackers["parent"].childQueueTrackers), 2)

	usage4, err := resources.NewResourceFromConf(map[string]string{"mem": "60M", "vcore": "60"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	removeQT, err = queueTracker.decreaseTrackedResource(queuePath1, TestApp1, false, usage4, true)
	if err != nil {
		t.Fatalf("unable to decrease tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage1, err)
	}
	assert.Equal(t, 1, len(queueTracker.runningApplications))
	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")
	// Make sure childQueueTracker cleaned
	assert.Equal(t, len(queueTracker.childQueueTrackers["parent"].childQueueTrackers), 1)
	usage5, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage5)
	}
	removeQT, err = queueTracker.decreaseTrackedResource(queuePath2, TestApp2, false, usage5, true)
	if err != nil {
		t.Fatalf("unable to decrease tracked resource: queuepath %s, app %s, res %v, error %t", queuePath2, TestApp2, usage2, err)
	}
	assert.Equal(t, 0, len(queueTracker.runningApplications))
	// Make sure all childQueueTracker cleaned
	assert.Equal(t, len(queueTracker.childQueueTrackers), 0)
	assert.Equal(t, removeQT, true, "wrong remove queue tracker value")

	// Test parent queueTracker has not zero usage, but child queueTrackers has all deleted
	err = queueTracker.increaseTrackedResource(queuePath1, TestApp1, true, false, usage1)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage1, err)
	}
	assert.Equal(t, 1, len(queueTracker.runningApplications))

	usage2, err = resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	err = queueTracker.increaseTrackedResource("root.parent", TestApp2, true, false, usage2)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", "root.parent", TestApp2, usage2, err)
	}
}

func TestGetChildQueuePath(t *testing.T) {
	childPath, immediateChildName := getChildQueuePath("root.parent.leaf")
	assert.Equal(t, childPath, "parent.leaf")
	assert.Equal(t, immediateChildName, "parent")

	childPath, immediateChildName = getChildQueuePath("parent.leaf")
	assert.Equal(t, childPath, "leaf")
	assert.Equal(t, immediateChildName, "leaf")

	childPath, immediateChildName = getChildQueuePath("leaf")
	assert.Equal(t, childPath, "")
	assert.Equal(t, immediateChildName, "")
}

func getQTResource(qt *QueueTracker) map[string]*resources.Resource {
	resources := make(map[string]*resources.Resource)
	usage := qt.getResourceUsageDAOInfo("")
	return internalGetResource(usage, resources)
}
