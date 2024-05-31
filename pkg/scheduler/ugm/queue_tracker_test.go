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
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

func TestQTIncreaseTrackedResource(t *testing.T) {
	// Queue setup:
	// root->parent->child1->child12
	// root->parent->child2
	// root->parent->child12 (similar name like above leaf queue, but it is being treated differently as similar names are allowed)
	GetUserManager()
	queueTracker := newQueueTracker("", "root", user)
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}

	queueTracker.increaseTrackedResource(strings.Split(queuePath1, configs.DOT), TestApp1, user, usage1)

	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	queueTracker.increaseTrackedResource(strings.Split(queuePath2, configs.DOT), TestApp2, user, usage2)

	usage3, err := resources.NewResourceFromConf(map[string]string{"mem": "30M", "vcore": "30"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	queueTracker.increaseTrackedResource(strings.Split(queuePath3, configs.DOT), TestApp3, user, usage3)

	usage4, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage3)
	}
	queueTracker.increaseTrackedResource(strings.Split(queuePath4, configs.DOT), TestApp4, user, usage4)
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
	queueTracker := newQueueTracker("", "root", user)
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "70M", "vcore": "70"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}

	queueTracker.increaseTrackedResource(strings.Split(queuePath1, configs.DOT), TestApp1, user, usage1)
	assert.Equal(t, 1, len(queueTracker.runningApplications))

	usage2, err := resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	queueTracker.increaseTrackedResource(strings.Split(queuePath2, configs.DOT), TestApp2, user, usage2)
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

	removeQT := queueTracker.decreaseTrackedResource(strings.Split(queuePath1, configs.DOT), TestApp1, usage3, false)
	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")

	removeQT = queueTracker.decreaseTrackedResource(strings.Split(queuePath2, configs.DOT), TestApp2, usage3, false)
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
	removeQT = queueTracker.decreaseTrackedResource(strings.Split(queuePath1, configs.DOT), TestApp1, usage4, true)
	assert.Equal(t, 1, len(queueTracker.runningApplications))
	assert.Equal(t, removeQT, false, "wrong remove queue tracker value")
	// Make sure childQueueTracker cleaned
	assert.Equal(t, len(queueTracker.childQueueTrackers["parent"].childQueueTrackers), 1)

	usage5, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage5)
	}
	removeQT = queueTracker.decreaseTrackedResource(strings.Split(queuePath2, configs.DOT), TestApp2, usage5, true)
	assert.Equal(t, 0, len(queueTracker.runningApplications))
	// Make sure all childQueueTracker cleaned
	assert.Equal(t, len(queueTracker.childQueueTrackers), 0)
	assert.Equal(t, removeQT, true, "wrong remove queue tracker value")

	// Test parent queueTracker has not zero usage, but child queueTrackers has all deleted
	queueTracker.increaseTrackedResource(strings.Split(queuePath1, configs.DOT), TestApp1, user, usage1)
	assert.Equal(t, 1, len(queueTracker.runningApplications))

	usage2, err = resources.NewResourceFromConf(map[string]string{"mem": "20M", "vcore": "20"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage2)
	}
	queueTracker.increaseTrackedResource([]string{"root", "parent"}, TestApp2, user, usage2)

	// no child queue
	removeQT = queueTracker.decreaseTrackedResource([]string{"root", "nonexisting"}, TestApp2, usage3, false)
	assert.Assert(t, !removeQT)
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
	queueTracker := newQueueTracker("", "root", user)

	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	if err != nil {
		t.Errorf("new resource create returned error or wrong resource: error %t, res %v", err, usage1)
	}

	queueTracker.maxResources = resources.Multiply(usage1, 6)
	queueTracker.maxRunningApps = 6

	parentQueueTracker := newQueueTracker("root", "parent", user)
	parentQueueTracker.maxResources = resources.Multiply(usage1, 5)
	parentQueueTracker.maxRunningApps = 5
	queueTracker.childQueueTrackers["parent"] = parentQueueTracker

	child1QueueTracker := newQueueTracker("root.parent", "child1", user)
	child1QueueTracker.maxResources = resources.Multiply(usage1, 2)
	child1QueueTracker.maxRunningApps = 2
	parentQueueTracker.childQueueTrackers["child1"] = child1QueueTracker

	child2QueueTracker := newQueueTracker("root.parent.child2", "child2", user)
	child2QueueTracker.maxResources = resources.Multiply(usage1, 2)
	child2QueueTracker.maxRunningApps = 2
	parentQueueTracker.childQueueTrackers["child2"] = child2QueueTracker

	queueTracker.increaseTrackedResource(strings.Split(queuePath1, configs.DOT), TestApp1, user, usage1)
	queueTracker.increaseTrackedResource(strings.Split(queuePath2, configs.DOT), TestApp2, user, usage1)
	queueTracker.increaseTrackedResource(strings.Split(queuePath2, configs.DOT), TestApp2, user, usage1)
	headroom := queueTracker.headroom(strings.Split(queuePath2, configs.DOT), user)
	assert.Equal(t, headroom.FitInMaxUndef(usage1), false)
	queueTracker.increaseTrackedResource(strings.Split(queuePath3, configs.DOT), TestApp3, user, usage1)
	queueTracker.increaseTrackedResource(strings.Split(queuePath4, configs.DOT), TestApp4, user, usage1)
	headroom = queueTracker.headroom(strings.Split(queuePath4, configs.DOT), user)
	assert.Equal(t, headroom.FitInMaxUndef(usage1), false)
}

func TestHeadroom(t *testing.T) {
	GetUserManager()
	var nilResource *resources.Resource
	path := "root.parent.leaf"
	hierarchy := strings.Split(path, configs.DOT)

	// validate that the hierarchy gets created
	root := newRootQueueTracker(user)
	headroom := root.headroom(hierarchy, user)
	assert.Equal(t, headroom, nilResource, "auto create: expected nil resource")
	parent := root.childQueueTrackers["parent"]
	assert.Assert(t, parent != nil, "parent queue tracker should have been created")
	leaf := parent.childQueueTrackers["leaf"]
	assert.Assert(t, leaf != nil, "leaf queue tracker should have been created")

	// prep resources to set as usage and max
	usage, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	assert.NilError(t, err, "usage: new resource create returned error")
	double := resources.Multiply(usage, 2)
	leaf.maxResources = double
	parent.maxResources = resources.Multiply(double, 2)

	// headroom should be equal to max cap of leaf queue as there is no usage so far
	headroom = root.headroom(hierarchy, none)
	assert.Assert(t, resources.Equals(headroom, double), "headroom not leaf max")

	// headroom should be equal to sub(max cap of leaf queue - resource usage) as there is some usage
	leaf.resourceUsage = usage
	headroom = root.headroom(hierarchy, none)
	assert.Assert(t, resources.Equals(headroom, usage), "headroom should be same as usage")

	// headroom should be equal to min headroom of parent and leaf: parent has none so zero
	parent.maxResources = double
	parent.resourceUsage = double
	headroom = root.headroom(hierarchy, none)
	assert.Assert(t, resources.IsZero(headroom), "leaf check: parent should have no headroom")

	headroom = root.headroom(hierarchy[:2], none)
	assert.Assert(t, resources.IsZero(headroom), "parent check: parent should have no headroom")

	// reset usage for the parent
	parent.resourceUsage = resources.NewResource()
	// set a different type in the parent max and check it is in the headroom
	var single, other *resources.Resource
	single, err = resources.NewResourceFromConf(map[string]string{"gpu": "1"})
	assert.NilError(t, err, "single: new resource create returned error")
	parent.maxResources = single
	single, err = resources.NewResourceFromConf(map[string]string{"gpu": "1"})
	assert.NilError(t, err, "single: new resource create returned error")
	combined := resources.Add(usage, single)
	headroom = root.headroom(hierarchy, none)
	assert.Assert(t, resources.Equals(headroom, combined), "headroom should be same as combined")

	// this "other" resource should be completely ignored as it has no limit
	other, err = resources.NewResourceFromConf(map[string]string{"unknown": "100"})
	assert.NilError(t, err, "single: new resource create returned error")
	parent.resourceUsage = other
	root.resourceUsage = other
	headroom = root.headroom(hierarchy, none)
	assert.Assert(t, resources.Equals(headroom, combined), "headroom should be same as combined")
}

func TestQTCanRunApp(t *testing.T) {
	GetUserManager()

	// validate that the hierarchy gets created
	root := newRootQueueTracker(user)
	hierarchy := strings.Split("root.parent.leaf", configs.DOT)
	assert.Assert(t, root.canRunApp(hierarchy, TestApp1, user))
	parent := root.childQueueTrackers["parent"]
	assert.Assert(t, parent != nil, "parent queue tracker should have been created")
	leaf := parent.childQueueTrackers["leaf"]
	assert.Assert(t, leaf != nil, "leaf queue tracker should have been created")

	// limit in the leaf queue
	leaf.maxRunningApps = 2
	leaf.runningApplications[TestApp1] = true
	leaf.runningApplications[TestApp2] = true
	parent.runningApplications[TestApp1] = true
	parent.runningApplications[TestApp2] = true
	root.runningApplications[TestApp1] = true
	root.runningApplications[TestApp2] = true
	assert.Assert(t, root.canRunApp(hierarchy, TestApp1, user))
	assert.Assert(t, root.canRunApp(hierarchy, TestApp2, user))
	assert.Assert(t, !root.canRunApp(hierarchy, TestApp3, user))

	// limit in the parent queue
	leaf.maxRunningApps = 0
	parent.maxRunningApps = 2
	assert.Assert(t, root.canRunApp(hierarchy, TestApp1, user))
	assert.Assert(t, root.canRunApp(hierarchy, TestApp2, user))
	assert.Assert(t, !root.canRunApp(hierarchy, TestApp3, user))
}

func TestNewQueueTracker(t *testing.T) {
	manager := GetUserManager()
	defer manager.ClearConfigLimits()

	maxRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"cpu": 100,
	})
	manager.userWildCardLimitsConfig = map[string]*LimitConfig{
		"root": {
			maxApplications: 3,
			maxResources:    maxRes.Clone(),
		},
	}

	root := newRootQueueTracker(user)
	assert.Equal(t, "root", root.queuePath)
	assert.Equal(t, "root", root.queueName)
	assert.Equal(t, uint64(3), root.maxRunningApps)
	assert.Equal(t, 0, len(root.runningApplications))
	assert.Assert(t, root.useWildCard)
	assert.Assert(t, resources.Equals(maxRes, root.maxResources))
	assert.Assert(t, resources.IsZero(root.resourceUsage))

	parent := newQueueTracker("root", "parent", user)
	assert.Equal(t, "root.parent", parent.queuePath)
	assert.Equal(t, "parent", parent.queueName)
	assert.Equal(t, uint64(0), parent.maxRunningApps)
	assert.Equal(t, 0, len(parent.runningApplications))
	assert.Assert(t, !parent.useWildCard)
	assert.Assert(t, resources.IsZero(parent.maxResources))
	assert.Assert(t, resources.IsZero(parent.resourceUsage))
}

func TestCanBeRemoved(t *testing.T) {
	GetUserManager()
	root := newRootQueueTracker(user)
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	assert.NilError(t, err)

	// create tracker hierarchy
	root.increaseTrackedResource(strings.Split(queuePath1, configs.DOT), TestApp1, user, usage1)
	parentQ := root.childQueueTrackers["parent"]
	childQ := parentQ.childQueueTrackers["child1"]
	assert.Assert(t, !root.canBeRemoved())
	assert.Assert(t, !parentQ.canBeRemoved())
	assert.Assert(t, !childQ.canBeRemoved())

	// remove app from "child1"
	childQ.runningApplications = make(map[string]bool)
	childQ.resourceUsage = resources.NewResource()
	assert.Assert(t, root.canBeRemoved())
	assert.Assert(t, parentQ.canBeRemoved())
	assert.Assert(t, childQ.canBeRemoved())
}

func TestGetResourceUsageDAOInfo(t *testing.T) {
	GetUserManager()
	root := newRootQueueTracker(user)
	usage1, err := resources.NewResourceFromConf(map[string]string{"mem": "10M", "vcore": "10"})
	assert.NilError(t, err)

	// create tracker hierarchy
	root.increaseTrackedResource(strings.Split(queuePath1, configs.DOT), TestApp1, user, usage1)

	// update settings on "parent" and "child1" directly
	parentQ := root.childQueueTrackers["parent"]
	childQ := parentQ.childQueueTrackers["child1"]
	childQ.maxRunningApps = 2
	maxRes := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": 123,
	})
	childQ.maxResources = maxRes.Clone()
	parentQ.maxRunningApps = 3

	rootDao := root.getResourceUsageDAOInfo("")
	assert.Assert(t, resources.Equals(usage1, rootDao.ResourceUsage))
	assert.Equal(t, "root", rootDao.QueuePath)
	assert.Equal(t, 1, len(rootDao.RunningApplications))
	assert.Equal(t, TestApp1, rootDao.RunningApplications[0])
	assert.Equal(t, 1, len(rootDao.Children))
	assert.Equal(t, uint64(0), rootDao.MaxApplications)
	assert.Assert(t, rootDao.MaxResources == nil)
	parentDao := rootDao.Children[0]
	assert.Assert(t, resources.Equals(usage1, parentDao.ResourceUsage))
	assert.Equal(t, "root.parent", parentDao.QueuePath)
	assert.Equal(t, 1, len(parentDao.RunningApplications))
	assert.Equal(t, TestApp1, parentDao.RunningApplications[0])
	assert.Equal(t, uint64(3), parentDao.MaxApplications)
	assert.Assert(t, parentDao.MaxResources == nil)
	assert.Equal(t, 1, len(parentDao.Children))
	childDao := parentDao.Children[0]
	assert.Assert(t, resources.Equals(usage1, childDao.ResourceUsage))
	assert.Equal(t, "root.parent.child1", childDao.QueuePath)
	assert.Equal(t, 1, len(childDao.RunningApplications))
	assert.Equal(t, TestApp1, childDao.RunningApplications[0])
	assert.Equal(t, uint64(2), childDao.MaxApplications)
	assert.Assert(t, resources.Equals(maxRes, childDao.MaxResources))
	assert.Equal(t, 0, len(childDao.Children))

	root = nil
	rootDao = root.getResourceUsageDAOInfo("")
	assert.DeepEqual(t, rootDao, &dao.ResourceUsageDAOInfo{})
}

func TestSetLimit(t *testing.T) {
	manager := GetUserManager()
	defer manager.ClearConfigLimits()
	manager.userWildCardLimitsConfig = map[string]*LimitConfig{
		path1: {
			maxApplications: 3,
			maxResources:    resources.NewResource(),
		},
	}
	root := newRootQueueTracker(user)
	assert.Assert(t, !root.useWildCard)
	assert.Equal(t, uint64(0), root.maxRunningApps)
	assert.Assert(t, root.maxResources == nil)

	// create tracker hierarchy
	limit := resources.NewResourceFromMap(map[string]resources.Quantity{
		"mem":   10,
		"vcore": 10})
	root.setLimit(strings.Split(queuePath1, configs.DOT), limit.Clone(), 9, true, user, true)

	// check settings
	parentQ := root.childQueueTrackers["parent"]
	assert.Assert(t, parentQ.maxResources == nil)
	assert.Equal(t, uint64(0), parentQ.maxRunningApps)
	childQ := parentQ.childQueueTrackers["child1"]
	assert.Assert(t, parentQ.maxResources == nil)
	assert.Equal(t, uint64(9), childQ.maxRunningApps)
	assert.Assert(t, resources.Equals(limit, childQ.maxResources))

	// check if settings are overridden
	newLimit := resources.NewResourceFromMap(map[string]resources.Quantity{
		"mem":   20,
		"vcore": 20})
	root.setLimit(strings.Split(queuePath1, configs.DOT), newLimit.Clone(), 3, false, user, true) // override
	assert.Assert(t, resources.Equals(newLimit, childQ.maxResources))
	assert.Assert(t, !childQ.useWildCard)
	newLimit2 := resources.NewResourceFromMap(map[string]resources.Quantity{
		"mem":   30,
		"vcore": 30})
	root.setLimit(strings.Split(queuePath1, configs.DOT), newLimit2.Clone(), 2, true, user, true) // no override
	assert.Assert(t, !childQ.useWildCard)
	assert.Assert(t, resources.Equals(newLimit, childQ.maxResources))
	assert.Equal(t, uint64(3), childQ.maxRunningApps)

	root.setLimit(strings.Split(queuePath1, configs.DOT), newLimit2.Clone(), 4, true, user, false) // override -> changes qt.doWildCardCheck
	assert.Assert(t, childQ.useWildCard)
	assert.Assert(t, resources.Equals(newLimit2, childQ.maxResources))
	assert.Equal(t, uint64(4), childQ.maxRunningApps)

	root.setLimit(strings.Split(queuePath1, configs.DOT), newLimit.Clone(), 5, false, user, false) // override
	assert.Assert(t, !childQ.useWildCard)
	assert.Assert(t, resources.Equals(newLimit, childQ.maxResources))
	assert.Equal(t, uint64(5), childQ.maxRunningApps)
}

func getQTResource(qt *QueueTracker) map[string]*resources.Resource {
	resources := make(map[string]*resources.Resource)
	usage := qt.getResourceUsageDAOInfo("")
	return internalGetResource(usage, resources)
}
