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
	"github.com/apache/yunikorn-core/pkg/common/security"
)

func TestGTIncreaseTrackedResource(t *testing.T) {
	// Queue setup:
	// root->parent->child1->child12
	// root->parent->child2
	// root->parent->child12 (similar name like above leaf queue, but it is being treated differently as similar names are allowed)
	manager := GetUserManager()
	user := &security.UserGroup{User: "test", Groups: []string{"test"}}
	groupTracker := newGroupTracker(user.User)

	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}

	manager.Headroom(queuePath1, TestApp1, *user)
	result := groupTracker.increaseTrackedResource(hierarchy1, TestApp1, usage1, user.User)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v", hierarchy1, TestApp1, usage1)
	}

	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	result = groupTracker.increaseTrackedResource(hierarchy2, TestApp2, usage2, user.User)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v", hierarchy2, TestApp2, usage2)
	}

	usage3, err := resources.NewResourceFromConf(map[string]string{"mem": "30M", "vcore": "30"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	result = groupTracker.increaseTrackedResource(hierarchy3, TestApp3, usage3, user.User)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v", hierarchy3, TestApp3, usage3)
	}

	usage4, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	result = groupTracker.increaseTrackedResource(hierarchy4, TestApp4, usage4, user.User)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v", hierarchy4, TestApp4, usage4)
	}
	actualResources := getGroupResource(groupTracker)

	assert.Equal(t, "map[mem:80000000 vcore:80000]", actualResources["root"].String(), "wrong resource")
	assert.Equal(t, "map[mem:80000000 vcore:80000]", actualResources["root.parent"].String(), "wrong resource")
	assert.Equal(t, "map[mem:40000000 vcore:40000]", actualResources["root.parent.child1"].String(), "wrong resource")
	assert.Equal(t, "map[mem:30000000 vcore:30000]", actualResources["root.parent.child1.child12"].String(), "wrong resource")
	assert.Equal(t, "map[mem:20000000 vcore:20000]", actualResources["root.parent.child2"].String(), "wrong resource")
	assert.Equal(t, "map[mem:20000000 vcore:20000]", actualResources["root.parent.child12"].String(), "wrong resource")
	assert.Equal(t, 4, len(groupTracker.getTrackedApplications()))
}

func TestGTDecreaseTrackedResource(t *testing.T) {
	// Queue setup:
	// root->parent->child1
	// root->parent->child2
	// Initialize ugm
	GetUserManager()
	user := &security.UserGroup{User: "test", Groups: []string{"test"}}
	groupTracker := newGroupTracker(user.User)
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "70M", "vcore": "70"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}
	result := groupTracker.increaseTrackedResource(hierarchy1, TestApp1, usage1, user.User)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v", hierarchy1, TestApp1, usage1)
	}
	assert.Equal(t, 1, len(groupTracker.getTrackedApplications()))

	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	result = groupTracker.increaseTrackedResource(hierarchy2, TestApp2, usage2, user.User)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v", hierarchy2, TestApp2, usage2)
	}
	actualResources := getGroupResource(groupTracker)

	assert.Equal(t, 2, len(groupTracker.getTrackedApplications()))
	assert.Equal(t, "map[mem:90000000 vcore:90000]", actualResources["root"].String(), "wrong resource")
	assert.Equal(t, "map[mem:90000000 vcore:90000]", actualResources["root.parent"].String(), "wrong resource")
	assert.Equal(t, "map[mem:70000000 vcore:70000]", actualResources["root.parent.child1"].String(), "wrong resource")
	assert.Equal(t, "map[mem:20000000 vcore:20000]", actualResources["root.parent.child2"].String(), "wrong resource")

	usage3, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	removeQT, decreased := groupTracker.decreaseTrackedResource(hierarchy1, TestApp1, usage3, false)
	if !decreased {
		t.Fatalf("unable to decrease tracked resource: queuepath %+q, app %s, res %v, error %t", hierarchy1, TestApp1, usage3, err)
	}
	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")

	removeQT, decreased = groupTracker.decreaseTrackedResource(hierarchy2, TestApp2, usage3, false)
	if !decreased {
		t.Fatalf("unable to decrease tracked resource: queuepath %+q, app %s, res %v, error %t", hierarchy2, TestApp2, usage3, err)
	}
	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")

	actualResources1 := getGroupResource(groupTracker)

	assert.Equal(t, "map[mem:70000000 vcore:70000]", actualResources1["root"].String(), "wrong resource")
	assert.Equal(t, "map[mem:70000000 vcore:70000]", actualResources1["root.parent"].String(), "wrong resource")
	assert.Equal(t, "map[mem:60000000 vcore:60000]", actualResources1["root.parent.child1"].String(), "wrong resource")
	assert.Equal(t, "map[mem:10000000 vcore:10000]", actualResources1["root.parent.child2"].String(), "wrong resource")

	usage4, err := resources.NewResourceFromConf(map[string]string{"mem": "60M", "vcore": "60"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}

	removeQT, decreased = groupTracker.decreaseTrackedResource(hierarchy1, TestApp1, usage4, true)
	if !decreased {
		t.Fatalf("unable to decrease tracked resource: queuepath %+q, app %s, res %v, error %t", hierarchy1, TestApp1, usage1, err)
	}
	assert.Equal(t, 1, len(groupTracker.getTrackedApplications()))
	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")

	usage5, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}

	removeQT, decreased = groupTracker.decreaseTrackedResource(hierarchy2, TestApp2, usage5, true)
	if !decreased {
		t.Fatalf("unable to decrease tracked resource: queuepath %+q, app %s, res %v, error %t", hierarchy2, TestApp2, usage2, err)
	}
	assert.Equal(t, 0, len(groupTracker.getTrackedApplications()))
	assert.Equal(t, removeQT, true, "wrong remove queue tracker value")
}

func TestGTSetMaxLimits(t *testing.T) {
	// Queue setup:
	// root->parent->child1
	// Initialize ugm
	GetUserManager()
	user := security.UserGroup{User: "test", Groups: []string{"test"}}
	groupTracker := newGroupTracker(user.User)
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}

	result := groupTracker.increaseTrackedResource(hierarchy1, TestApp1, usage1, user.User)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v", hierarchy1, TestApp1, usage1)
	}

	groupTracker.setLimits(hierarchy1, resources.Multiply(usage1, 5), 5)
	groupTracker.setLimits(hierarchy5, resources.Multiply(usage1, 10), 10)

	result = groupTracker.increaseTrackedResource(hierarchy1, TestApp2, usage1, user.User)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v", hierarchy1, TestApp2, usage1)
	}
	result = groupTracker.increaseTrackedResource(hierarchy1, TestApp3, usage1, user.User)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v", hierarchy1, TestApp3, usage1)
	}
	groupTracker.setLimits(hierarchy1, usage1, 1)
	groupTracker.setLimits(hierarchy5, usage1, 1)
}

func TestGTCanRunApp(t *testing.T) {
	manager := GetUserManager()
	defer manager.ClearConfigLimits()
	user := security.UserGroup{User: "test", Groups: []string{"test"}}
	groupTracker := newGroupTracker(user.User)
	manager.userWildCardLimitsConfig["root.parent"] = &LimitConfig{
		maxResources: resources.NewResourceFromMap(map[string]resources.Quantity{
			"cpu": 1000,
		}),
		maxApplications: 3,
	} // manipulate map directly to avoid hierarchy creation

	assert.Assert(t, groupTracker.canRunApp(hierarchy1, TestApp1))
	// make sure wildcard limits are not applied due to the tracker type
	assert.Equal(t, uint64(0), groupTracker.queueTracker.childQueueTrackers["parent"].maxRunningApps)
	assert.Assert(t, groupTracker.queueTracker.childQueueTrackers["parent"].maxResources == nil)

	// limit hit
	groupTracker.setLimits(hierarchy1, resources.Zero, 1)
	groupTracker.increaseTrackedResource(hierarchy1, TestApp1, resources.NewResourceFromMap(map[string]resources.Quantity{
		"cpu": 1000,
	}), user.User)
	assert.Assert(t, groupTracker.canRunApp(hierarchy1, TestApp1))
	assert.Assert(t, !groupTracker.canRunApp(hierarchy1, TestApp2))
}

func getGroupResource(gt *GroupTracker) map[string]*resources.Resource {
	resources := make(map[string]*resources.Resource)
	usage := gt.GetGroupResourceUsageDAOInfo()
	return internalGetResource(usage.Queues, resources)
}
