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
	"strings"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/events/mock"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestGTIncreaseTrackedResource(t *testing.T) {
	// Queue setup:
	// root->parent->child1->child12
	// root->parent->child2
	// root->parent->child12 (similar name like above leaf queue, but it is being treated differently as similar names are allowed)
	manager := GetUserManager()
	user := &security.UserGroup{User: "test", Groups: []string{"test"}}
	eventSystem := mock.NewEventSystem()
	groupTracker := newGroupTracker(user.User, newUGMEvents(eventSystem))

	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}

	manager.Headroom(queuePath1, TestApp1, *user)
	groupTracker.increaseTrackedResource(path1, TestApp1, usage1, user.User)
	assert.Equal(t, 1, len(eventSystem.Events))
	assert.Equal(t, si.EventRecord_UG_GROUP_RESOURCE, eventSystem.Events[0].EventChangeDetail)
	assert.Equal(t, si.EventRecord_ADD, eventSystem.Events[0].EventChangeType)

	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	groupTracker.increaseTrackedResource(path2, TestApp2, usage2, user.User)

	usage3, err := resources.NewResourceFromConf(map[string]string{"mem": "30M", "vcore": "30"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	groupTracker.increaseTrackedResource(path3, TestApp3, usage3, user.User)

	usage4, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	groupTracker.increaseTrackedResource(path4, TestApp4, usage4, user.User)
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
	eventSystem := mock.NewEventSystem()
	groupTracker := newGroupTracker(user.User, newUGMEvents(eventSystem))
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "70M", "vcore": "70"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}
	groupTracker.increaseTrackedResource(path1, TestApp1, usage1, user.User)
	assert.Equal(t, 1, len(groupTracker.getTrackedApplications()))

	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	groupTracker.increaseTrackedResource(path2, TestApp2, usage2, user.User)
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
	removeQT := groupTracker.decreaseTrackedResource(path1, TestApp1, usage3, false)
	eventSystem.Reset()
	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")
	assert.Equal(t, 1, len(eventSystem.Events))
	assert.Equal(t, si.EventRecord_UG_GROUP_RESOURCE, eventSystem.Events[0].EventChangeDetail)
	assert.Equal(t, si.EventRecord_REMOVE, eventSystem.Events[0].EventChangeType)

	removeQT = groupTracker.decreaseTrackedResource(path2, TestApp2, usage3, false)
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

	removeQT = groupTracker.decreaseTrackedResource(path1, TestApp1, usage4, true)
	assert.Equal(t, 1, len(groupTracker.getTrackedApplications()))
	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")

	usage5, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}

	removeQT = groupTracker.decreaseTrackedResource(path2, TestApp2, usage5, true)
	assert.Equal(t, 0, len(groupTracker.getTrackedApplications()))
	assert.Equal(t, removeQT, true, "wrong remove queue tracker value")
}

func TestGTSetAndClearMaxLimits(t *testing.T) {
	// Queue setup:
	// root->parent->child1
	// Initialize ugm
	GetUserManager()
	user := security.UserGroup{User: "test", Groups: []string{"test"}}
	eventSystem := mock.NewEventSystem()
	groupTracker := newGroupTracker(user.User, newUGMEvents(eventSystem))
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}

	groupTracker.increaseTrackedResource(path1, TestApp1, usage1, user.User)

	// higher limits - apps can run
	eventSystem.Reset()
	groupTracker.setLimits(path1, resources.Multiply(usage1, 5), 5)
	groupTracker.setLimits(path5, resources.Multiply(usage1, 10), 10)
	assert.Equal(t, 2, len(eventSystem.Events))
	assert.Equal(t, si.EventRecord_UG_GROUP_LIMIT, eventSystem.Events[0].EventChangeDetail)
	assert.Equal(t, si.EventRecord_SET, eventSystem.Events[0].EventChangeType)
	assert.Equal(t, path1, eventSystem.Events[0].ReferenceID)
	assert.Equal(t, si.EventRecord_UG_GROUP_LIMIT, eventSystem.Events[1].EventChangeDetail)
	assert.Equal(t, si.EventRecord_SET, eventSystem.Events[1].EventChangeType)
	assert.Equal(t, path5, eventSystem.Events[1].ReferenceID)
	groupTracker.increaseTrackedResource(path1, TestApp2, usage1, user.User)
	groupTracker.increaseTrackedResource(path1, TestApp3, usage1, user.User)
	path1expectedHeadroom := resources.NewResourceFromMap(map[string]resources.Quantity{
		"mem":   20000000,
		"vcore": 20000,
	})
	hierarchy1 := strings.Split(path1, configs.DOT)
	assert.Assert(t, resources.Equals(groupTracker.headroom(hierarchy1), path1expectedHeadroom))
	path5expectedHeadroom := resources.NewResourceFromMap(map[string]resources.Quantity{
		"mem":   70000000,
		"vcore": 70000,
	})
	hierarchy5 := strings.Split(path5, configs.DOT)
	assert.Assert(t, resources.Equals(groupTracker.headroom(hierarchy5), path5expectedHeadroom))
	assert.Assert(t, groupTracker.canRunApp(hierarchy1, TestApp4))

	// lower limits
	groupTracker.setLimits(path1, usage1, 1)
	groupTracker.setLimits(path5, resources.Multiply(usage1, 2), 1)
	lowerChildHeadroom := resources.NewResourceFromMap(map[string]resources.Quantity{
		"mem":   -20000000,
		"vcore": -20000,
	})
	assert.Assert(t, resources.Equals(groupTracker.headroom(hierarchy1), lowerChildHeadroom))
	lowerParentHeadroom := resources.NewResourceFromMap(map[string]resources.Quantity{
		"mem":   -10000000,
		"vcore": -10000,
	})
	assert.Assert(t, resources.Equals(groupTracker.headroom(hierarchy5), lowerParentHeadroom))
	assert.Assert(t, !groupTracker.canRunApp(hierarchy1, TestApp4))
	assert.Assert(t, !groupTracker.canRunApp(hierarchy5, TestApp4))

	// clear limits
	eventSystem.Reset()
	groupTracker.clearLimits(path1)
	assert.Assert(t, resources.Equals(groupTracker.headroom(hierarchy1), lowerParentHeadroom))
	assert.Assert(t, resources.Equals(groupTracker.headroom(hierarchy5), lowerParentHeadroom))
	assert.Assert(t, !groupTracker.canRunApp(hierarchy1, TestApp4))
	assert.Assert(t, !groupTracker.canRunApp(hierarchy5, TestApp4))
	groupTracker.clearLimits(path5)
	assert.Assert(t, groupTracker.headroom(hierarchy1) == nil)
	assert.Assert(t, groupTracker.headroom(hierarchy5) == nil)
	assert.Assert(t, groupTracker.canRunApp(hierarchy1, TestApp4))
	assert.Assert(t, groupTracker.canRunApp(hierarchy5, TestApp4))
	assert.Equal(t, 2, len(eventSystem.Events))
	assert.Equal(t, si.EventRecord_REMOVE, eventSystem.Events[0].EventChangeType)
	assert.Equal(t, si.EventRecord_UG_GROUP_LIMIT, eventSystem.Events[0].EventChangeDetail)
	assert.Equal(t, si.EventRecord_REMOVE, eventSystem.Events[1].EventChangeType)
	assert.Equal(t, si.EventRecord_UG_GROUP_LIMIT, eventSystem.Events[1].EventChangeDetail)
}

func TestGTCanRunApp(t *testing.T) {
	manager := GetUserManager()
	defer manager.ClearConfigLimits()
	user := security.UserGroup{User: "test", Groups: []string{"test"}}
	groupTracker := newGroupTracker(user.User, newUGMEvents(mock.NewEventSystemDisabled()))
	manager.userWildCardLimitsConfig["root.parent"] = &LimitConfig{
		maxResources: resources.NewResourceFromMap(map[string]resources.Quantity{
			"cpu": 1000,
		}),
		maxApplications: 3,
	} // manipulate map directly to avoid hierarchy creation

	hierarchy1 := strings.Split(path1, configs.DOT)
	assert.Assert(t, groupTracker.canRunApp(hierarchy1, TestApp1))
	// make sure wildcard limits are not applied due to the tracker type
	assert.Equal(t, uint64(0), groupTracker.queueTracker.childQueueTrackers["parent"].maxRunningApps)
	assert.Assert(t, groupTracker.queueTracker.childQueueTrackers["parent"].maxResources == nil)
	assert.Assert(t, !groupTracker.queueTracker.childQueueTrackers["parent"].useWildCard)

	// maxApps limit hit
	groupTracker.setLimits(path1, nil, 1)
	groupTracker.increaseTrackedResource(path1, TestApp1, resources.NewResourceFromMap(map[string]resources.Quantity{
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
