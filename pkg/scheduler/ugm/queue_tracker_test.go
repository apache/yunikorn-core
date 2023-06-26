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
	GetUserManager()
	queueTracker := newQueueTracker("", "root")
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}

	result := queueTracker.increaseTrackedResource(queuePath1, TestApp1, user, usage1)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, usage1)
	}

	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	result = queueTracker.increaseTrackedResource(queuePath2, TestApp2, user, usage2)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath2, TestApp2, usage2)
	}

	usage3, err := resources.NewResourceFromConf(map[string]string{"mem": "30M", "vcore": "30"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	result = queueTracker.increaseTrackedResource(queuePath3, TestApp3, user, usage3)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath3, TestApp3, usage3)
	}

	usage4, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	result = queueTracker.increaseTrackedResource(queuePath4, TestApp4, user, usage4)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath4, TestApp4, usage4)
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
	GetUserManager()
	queueTracker := newQueueTracker("", "root")
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "70M", "vcore": "70"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}

	result := queueTracker.increaseTrackedResource(queuePath1, TestApp1, user, usage1)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, usage1)
	}
	assert.Equal(t, 1, len(queueTracker.runningApplications))

	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	result = queueTracker.increaseTrackedResource(queuePath2, TestApp2, user, usage2)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath2, TestApp2, usage2)
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

	removeQT, decreased := queueTracker.decreaseTrackedResource(queuePath1, TestApp1, usage3, false)
	if !decreased {
		t.Fatalf("unable to decrease tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage3, err)
	}
	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")

	removeQT, decreased = queueTracker.decreaseTrackedResource(queuePath2, TestApp2, usage3, false)
	if !decreased {
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
	removeQT, decreased = queueTracker.decreaseTrackedResource(queuePath1, TestApp1, usage4, true)
	if !decreased {
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
	removeQT, decreased = queueTracker.decreaseTrackedResource(queuePath2, TestApp2, usage5, true)
	if !decreased {
		t.Fatalf("unable to decrease tracked resource: queuepath %s, app %s, res %v, error %t", queuePath2, TestApp2, usage2, err)
	}
	assert.Equal(t, 0, len(queueTracker.runningApplications))
	// Make sure all childQueueTracker cleaned
	assert.Equal(t, len(queueTracker.childQueueTrackers), 0)
	assert.Equal(t, removeQT, true, "wrong remove queue tracker value")

	// Test parent queueTracker has not zero usage, but child queueTrackers has all deleted
	result = queueTracker.increaseTrackedResource(queuePath1, TestApp1, user, usage1)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, usage1)
	}
	assert.Equal(t, 1, len(queueTracker.runningApplications))

	usage2, err = resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	result = queueTracker.increaseTrackedResource("root.parent", TestApp2, user, usage2)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", "root.parent", TestApp2, usage2)
	}
}

func TestQTQuotaEnforcement(t *testing.T) {
	// Queue setup:
	// root. max apps - 6 , max res - 60M, 60cores
	// root-> parent. max apps - 5 , max res - 50M, 50cores
	// root->parent->child1. max apps - 2 , max res - 20M, 20cores
	// root->parent->child1->child12. config not set
	// root->parent->child2. max apps - 2 , max res - 20M, 20cores
	// root->parent->child12 (similar name like above leaf queue, but it is being treated differently as similar names are allowed). config not set
	GetUserManager()
	queueTracker := newQueueTracker("", "root")

	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}

	queueTracker.maxResources = resources.Multiply(usage1, 6)
	queueTracker.maxRunningApps = 6

	parentQueueTracker := newQueueTracker("root", "parent")
	parentQueueTracker.maxResources = resources.Multiply(usage1, 5)
	parentQueueTracker.maxRunningApps = 5
	queueTracker.childQueueTrackers["parent"] = parentQueueTracker

	child1QueueTracker := newQueueTracker("root.parent", "child1")
	child1QueueTracker.maxResources = resources.Multiply(usage1, 2)
	child1QueueTracker.maxRunningApps = 2
	parentQueueTracker.childQueueTrackers["child1"] = child1QueueTracker

	child2QueueTracker := newQueueTracker("root.parent.child2", "child2")
	child2QueueTracker.maxResources = resources.Multiply(usage1, 2)
	child2QueueTracker.maxRunningApps = 2
	parentQueueTracker.childQueueTrackers["child2"] = child2QueueTracker

	result := queueTracker.increaseTrackedResource(queuePath1, TestApp1, user, usage1)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath1, TestApp1, usage1)
	}

	result = queueTracker.increaseTrackedResource(queuePath2, TestApp2, user, usage1)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath2, TestApp2, usage1)
	}

	result = queueTracker.increaseTrackedResource(queuePath2, TestApp2, user, usage1)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath2, TestApp2, usage1)
	}

	result = queueTracker.increaseTrackedResource(queuePath2, TestApp3, user, usage1)
	if result {
		t.Fatalf("Increasing resource usage should fail as child2's resource usage exceeded configured max resources limit. queuepath %s, app %s, res %v", queuePath2, TestApp3, usage1)
	}

	result = queueTracker.increaseTrackedResource(queuePath3, TestApp3, user, usage1)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath3, TestApp3, usage1)
	}

	result = queueTracker.increaseTrackedResource(queuePath4, TestApp4, user, usage1)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v", queuePath4, TestApp4, usage1)
	}

	result = queueTracker.increaseTrackedResource(queuePath4, TestApp4, user, usage1)
	if result {
		t.Fatalf("Increasing resource usage should fail as parent's resource usage exceeded configured max resources limit. queuepath %s, app %s, res %v", queuePath4, TestApp4, usage1)
	}
}

func getQTResource(qt *QueueTracker) map[string]*resources.Resource {
	resources := make(map[string]*resources.Resource)
	usage := qt.getResourceUsageDAOInfo("")
	return internalGetResource(usage, resources)
}
