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

const (
	TestApp1 = "test-app-1"
	TestApp2 = "test-app-2"
	TestApp3 = "test-app-3"
	TestApp4 = "test-app-4"

	queuePath1 = "root.parent.child1"
	queuePath2 = "root.parent.child2"
	queuePath3 = "root.parent.child1.child12"
	queuePath4 = "root.parent.child12"
)

var hierarchy1 = []string{"root", "parent", "child1"}
var hierarchy2 = []string{"root", "parent", "child2"}
var hierarchy3 = []string{"root", "parent", "child1", "child12"}
var hierarchy4 = []string{"root", "parent", "child12"}
var hierarchy5 = []string{"root", "parent"}

func TestIncreaseTrackedResource(t *testing.T) {
	// Queue setup:
	// root->parent->child1->child12
	// root->parent->child2
	// root->parent->child12 (similar name like above leaf queue, but it is being treated differently as similar names are allowed)
	// Initialize ugm
	GetUserManager()
	user := security.UserGroup{User: "test", Groups: []string{"test"}}
	userTracker := newUserTracker(user.User)
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}
	result := userTracker.increaseTrackedResource(hierarchy1, TestApp1, usage1)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v", hierarchy1, TestApp1, usage1)
	}
	groupTracker := newGroupTracker(user.User)
	userTracker.setGroupForApp(TestApp1, groupTracker)

	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	result = userTracker.increaseTrackedResource(hierarchy2, TestApp2, usage2)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v, error %t", hierarchy2, TestApp2, usage2, err)
	}
	userTracker.setGroupForApp(TestApp2, groupTracker)

	usage3, err := resources.NewResourceFromConf(map[string]string{"mem": "30M", "vcore": "30"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	result = userTracker.increaseTrackedResource(hierarchy3, TestApp3, usage3)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v, error %t", hierarchy3, TestApp3, usage3, err)
	}
	userTracker.setGroupForApp(TestApp3, groupTracker)

	usage4, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	result = userTracker.increaseTrackedResource(hierarchy4, TestApp4, usage4)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v, error %t", hierarchy4, TestApp4, usage4, err)
	}
	userTracker.setGroupForApp(TestApp4, groupTracker)

	actualResources := getUserResource(userTracker)

	assert.Equal(t, "map[mem:80000000 vcore:80000]", actualResources["root"].String(), "wrong resource")
	assert.Equal(t, "map[mem:80000000 vcore:80000]", actualResources["root.parent"].String(), "wrong resource")
	assert.Equal(t, "map[mem:40000000 vcore:40000]", actualResources["root.parent.child1"].String(), "wrong resource")
	assert.Equal(t, "map[mem:30000000 vcore:30000]", actualResources["root.parent.child1.child12"].String(), "wrong resource")
	assert.Equal(t, "map[mem:20000000 vcore:20000]", actualResources["root.parent.child2"].String(), "wrong resource")
	assert.Equal(t, "map[mem:20000000 vcore:20000]", actualResources["root.parent.child12"].String(), "wrong resource")
	assert.Equal(t, 4, len(userTracker.getTrackedApplications()))
}

func TestDecreaseTrackedResource(t *testing.T) {
	// Queue setup:
	// root->parent->child1
	// root->parent->child2
	// Initialize ugm
	GetUserManager()
	user := security.UserGroup{User: "test", Groups: []string{"test"}}
	userTracker := newUserTracker(user.User)

	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "70M", "vcore": "70"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}
	result := userTracker.increaseTrackedResource(hierarchy1, TestApp1, usage1)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v, error %t", hierarchy1, TestApp1, usage1, err)
	}
	groupTracker := newGroupTracker(user.User)
	userTracker.setGroupForApp(TestApp1, groupTracker)
	assert.Equal(t, 1, len(userTracker.getTrackedApplications()))

	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	result = userTracker.increaseTrackedResource(hierarchy2, TestApp2, usage2)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v, error %t", hierarchy2, TestApp2, usage2, err)
	}
	userTracker.setGroupForApp(TestApp2, groupTracker)
	actualResources := getUserResource(userTracker)

	assert.Equal(t, 2, len(userTracker.getTrackedApplications()))
	assert.Equal(t, "map[mem:90000000 vcore:90000]", actualResources["root"].String(), "wrong resource")
	assert.Equal(t, "map[mem:90000000 vcore:90000]", actualResources["root.parent"].String(), "wrong resource")
	assert.Equal(t, "map[mem:70000000 vcore:70000]", actualResources["root.parent.child1"].String(), "wrong resource")
	assert.Equal(t, "map[mem:20000000 vcore:20000]", actualResources["root.parent.child2"].String(), "wrong resource")

	usage3, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	removeQT, decreased := userTracker.decreaseTrackedResource(hierarchy1, TestApp1, usage3, false)
	if !decreased {
		t.Fatalf("unable to decrease tracked resource: queuepath %+q, app %s, res %v, error %t", hierarchy1, TestApp1, usage3, err)
	}
	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")

	removeQT, decreased = userTracker.decreaseTrackedResource(hierarchy2, TestApp2, usage3, false)
	if !decreased {
		t.Fatalf("unable to decrease tracked resource: queuepath %+q, app %s, res %v, error %t", hierarchy2, TestApp2, usage3, err)
	}
	actualResources1 := getUserResource(userTracker)

	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")
	assert.Equal(t, "map[mem:70000000 vcore:70000]", actualResources1["root"].String(), "wrong resource")
	assert.Equal(t, "map[mem:70000000 vcore:70000]", actualResources1["root.parent"].String(), "wrong resource")
	assert.Equal(t, "map[mem:60000000 vcore:60000]", actualResources1["root.parent.child1"].String(), "wrong resource")
	assert.Equal(t, "map[mem:10000000 vcore:10000]", actualResources1["root.parent.child2"].String(), "wrong resource")

	usage4, err := resources.NewResourceFromConf(map[string]string{"mem": "60M", "vcore": "60"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}

	removeQT, decreased = userTracker.decreaseTrackedResource(hierarchy1, TestApp1, usage4, true)
	if !decreased {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v, error %t", hierarchy1, TestApp1, usage1, err)
	}
	assert.Equal(t, 1, len(userTracker.getTrackedApplications()))
	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")

	usage5, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage5)
	}
	removeQT, decreased = userTracker.decreaseTrackedResource(hierarchy2, TestApp2, usage5, true)
	if !decreased {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v, error %t", hierarchy2, TestApp2, usage2, err)
	}
	assert.Equal(t, 0, len(userTracker.getTrackedApplications()))
	assert.Equal(t, removeQT, true, "wrong remove queue tracker value")
}

func TestSetMaxLimits(t *testing.T) {
	// Queue setup:
	// root->parent->child1
	// Initialize ugm
	GetUserManager()
	user := security.UserGroup{User: "test", Groups: []string{"test"}}
	userTracker := newUserTracker(user.User)
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}
	result := userTracker.increaseTrackedResource(hierarchy1, TestApp1, usage1)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v, error %t", hierarchy1, TestApp1, usage1, err)
	}

	userTracker.setLimits(hierarchy1, resources.Multiply(usage1, 5), 5, false, false)
	userTracker.setLimits(hierarchy5, resources.Multiply(usage1, 10), 10, false, false)

	result = userTracker.increaseTrackedResource(hierarchy1, TestApp1, usage1)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v", hierarchy1, TestApp1, usage1)
	}

	result = userTracker.increaseTrackedResource(hierarchy1, TestApp2, usage1)
	if !result {
		t.Fatalf("unable to increase tracked resource: queuepath %+q, app %s, res %v", hierarchy1, TestApp2, usage1)
	}
	userTracker.setLimits(hierarchy1, usage1, 1, false, false)
	userTracker.setLimits(hierarchy5, usage1, 1, false, false)
}

func getUserResource(ut *UserTracker) map[string]*resources.Resource {
	resources := make(map[string]*resources.Resource)
	usage := ut.GetUserResourceUsageDAOInfo()
	return internalGetResource(usage.Queues, resources)
}
