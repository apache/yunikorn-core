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
	user := &security.UserGroup{User: "test", Groups: []string{"test"}}
	groupTracker := newGroupTracker(user.User)

	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}
	err = groupTracker.increaseTrackedResource(queuePath1, TestApp1, usage1)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage1, err)
	}

	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	err = groupTracker.increaseTrackedResource(queuePath2, TestApp2, usage2)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath2, TestApp2, usage2, err)
	}

	usage3, err := resources.NewResourceFromConf(map[string]string{"mem": "30M", "vcore": "30"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	err = groupTracker.increaseTrackedResource(queuePath3, TestApp3, usage3)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath3, TestApp3, usage3, err)
	}

	usage4, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	err = groupTracker.increaseTrackedResource(queuePath4, TestApp4, usage4)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath4, TestApp4, usage4, err)
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
	user := &security.UserGroup{User: "test", Groups: []string{"test"}}
	groupTracker := newGroupTracker(user.User)
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "70M", "vcore": "70"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}
	err = groupTracker.increaseTrackedResource(queuePath1, TestApp1, usage1)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage1, err)
	}
	assert.Equal(t, 1, len(groupTracker.getTrackedApplications()))

	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	err = groupTracker.increaseTrackedResource(queuePath2, TestApp2, usage2)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath2, TestApp2, usage2, err)
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
	removeQT, err := groupTracker.decreaseTrackedResource(queuePath1, TestApp1, usage3, false)
	if err != nil {
		t.Fatalf("unable to decrease tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage3, err)
	}
	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")

	removeQT, err = groupTracker.decreaseTrackedResource(queuePath2, TestApp2, usage3, false)
	if err != nil {
		t.Fatalf("unable to decrease tracked resource: queuepath %s, app %s, res %v, error %t", queuePath2, TestApp2, usage3, err)
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

	removeQT, err = groupTracker.decreaseTrackedResource(queuePath1, TestApp1, usage4, true)
	if err != nil {
		t.Fatalf("unable to decrease tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage1, err)
	}
	assert.Equal(t, 1, len(groupTracker.getTrackedApplications()))
	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")

	usage5, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}

	removeQT, err = groupTracker.decreaseTrackedResource(queuePath2, TestApp2, usage5, true)
	if err != nil {
		t.Fatalf("unable to decrease tracked resource: queuepath %s, app %s, res %v, error %t", queuePath2, TestApp2, usage2, err)
	}
	assert.Equal(t, 0, len(groupTracker.getTrackedApplications()))
	assert.Equal(t, removeQT, true, "wrong remove queue tracker value")
}

func TestGTSetMaxLimits(t *testing.T) {
	// Queue setup:
	// root->parent->child1
	user := security.UserGroup{User: "test", Groups: []string{"test"}}
	groupTracker := newGroupTracker(user.User)
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}
	err = groupTracker.increaseTrackedResource(queuePath1, TestApp1, usage1)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage1, err)
	}

	setMaxAppsErr := groupTracker.setMaxApplications(1, queuePath1)
	assert.NilError(t, setMaxAppsErr)

	setMaxResourcesErr := groupTracker.setMaxResources(usage1, queuePath1)
	assert.NilError(t, setMaxResourcesErr)

	setParentMaxAppsErr := groupTracker.setMaxApplications(1, "root.parent")
	assert.NilError(t, setParentMaxAppsErr)

	setParentMaxResourcesErr := groupTracker.setMaxResources(usage1, "root.parent")
	assert.NilError(t, setParentMaxResourcesErr)

	err = groupTracker.increaseTrackedResource(queuePath1, TestApp2, usage1)
	if err != nil {
		t.Fatalf("unable to increase tracked resource: queuepath %s, app %s, res %v, error %t", queuePath1, TestApp1, usage1, err)
	}

	setMaxAppsErr1 := groupTracker.setMaxApplications(1, queuePath1)
	assert.Error(t, setMaxAppsErr1, "current running applications is greater than config max applications for "+queuePath1)

	setMaxResourcesErr1 := groupTracker.setMaxResources(usage1, queuePath1)
	assert.Error(t, setMaxResourcesErr1, "current resource usage is greater than config max resource for "+queuePath1)

	setParentMaxAppsErr1 := groupTracker.setMaxApplications(1, "root.parent")
	assert.Error(t, setParentMaxAppsErr1, "current running applications is greater than config max applications for root.parent")

	setParentMaxResourcesErr1 := groupTracker.setMaxResources(usage1, "root.parent")
	assert.Error(t, setParentMaxResourcesErr1, "current resource usage is greater than config max resource for root.parent")
}

func getGroupResource(gt *GroupTracker) map[string]*resources.Resource {
	resources := make(map[string]*resources.Resource)
	usage := gt.GetGroupResourceUsageDAOInfo()
	return internalGetResource(usage.Queues, resources)
}
