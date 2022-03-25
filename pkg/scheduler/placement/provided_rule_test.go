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

package placement

import (
	"testing"

	"gotest.tools/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/security"
)

func TestProvidedRulePlace(t *testing.T) {
	// Create the structure for the test
	data := `
partitions:
  - name: default
    queues:
      - name: testparent
        queues:
          - name: testchild
`
	err := initQueueStructure([]byte(data))
	assert.NilError(t, err, "setting up the queue config failed")

	tags := make(map[string]string)
	user := security.UserGroup{
		User:   "test",
		Groups: []string{},
	}

	conf := configs.PlacementRule{
		Name: "provided",
	}
	var pr rule
	pr, err = newRule(conf)
	if err != nil || pr == nil {
		t.Errorf("provided rule create failed, err %v", err)
	}
	// queue that does not exists directly under the root
	appInfo := newApplication("app1", "default", "unknown", user, tags, nil, "")
	var queue string
	queue, err = pr.placeApplication(appInfo, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("provided rule placed app in incorrect queue '%s', err %v", queue, err)
	}
	// trying to place when no queue provided in the app
	appInfo = newApplication("app1", "default", "", user, tags, nil, "")
	queue, err = pr.placeApplication(appInfo, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("provided rule placed app in incorrect queue '%s', error %v", queue, err)
	}
	// trying to place in a qualified queue that does not exist
	appInfo = newApplication("app1", "default", "root.unknown", user, tags, nil, "")
	queue, err = pr.placeApplication(appInfo, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("provided rule placed app in incorrect queue '%s', error %v", queue, err)
	}
	// same queue now with create flag
	conf = configs.PlacementRule{
		Name:   "provided",
		Create: true,
	}
	pr, err = newRule(conf)
	if err != nil || pr == nil {
		t.Errorf("provided rule create failed, err %v", err)
	}
	queue, err = pr.placeApplication(appInfo, queueFunc)
	if queue != "root.unknown" || err != nil {
		t.Errorf("provided rule placed app in incorrect queue '%s', error %v", queue, err)
	}

	conf = configs.PlacementRule{
		Name: "provided",
		Parent: &configs.PlacementRule{
			Name:  "fixed",
			Value: "testparent",
		},
	}
	pr, err = newRule(conf)
	if err != nil || pr == nil {
		t.Errorf("provided rule create failed with parent name, err %v", err)
	}

	// unqualified queue with parent rule that exists directly in hierarchy
	appInfo = newApplication("app1", "default", "testchild", user, tags, nil, "")
	queue, err = pr.placeApplication(appInfo, queueFunc)
	if queue != "root.testparent.testchild" || err != nil {
		t.Errorf("provided rule failed to place queue in correct queue '%s', err %v", queue, err)
	}

	// qualified queue with parent rule (parent rule ignored)
	appInfo = newApplication("app1", "default", "root.testparent", user, tags, nil, "")

	queue, err = pr.placeApplication(appInfo, queueFunc)
	if queue != "root.testparent" || err != nil {
		t.Errorf("provided rule placed in to be created queue with create false '%s', err %v", queue, err)
	}
}

func TestProvidedRuleParent(t *testing.T) {
	err := initQueueStructure([]byte(confParentChild))
	assert.NilError(t, err, "setting up the queue config failed")

	tags := make(map[string]string)
	user := security.UserGroup{
		User:   "test",
		Groups: []string{},
	}

	// trying to place in a child using a parent, fail to create child
	conf := configs.PlacementRule{
		Name:   "provided",
		Create: false,
		Parent: &configs.PlacementRule{
			Name:  "fixed",
			Value: "testparent",
		},
	}
	var pr rule
	pr, err = newRule(conf)
	if err != nil || pr == nil {
		t.Errorf("provided rule create failed, err %v", err)
	}

	appInfo := newApplication("app1", "default", "unknown", user, tags, nil, "")
	var queue string
	queue, err = pr.placeApplication(appInfo, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("provided rule placed app in incorrect queue '%s', err %v", queue, err)
	}

	// trying to place in a child using a non creatable parent
	conf = configs.PlacementRule{
		Name:   "provided",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:   "fixed",
			Value:  "testother",
			Create: false,
		},
	}
	pr, err = newRule(conf)
	if err != nil || pr == nil {
		t.Errorf("provided rule create failed, err %v", err)
	}

	appInfo = newApplication("app1", "default", "testchild", user, tags, nil, "")
	queue, err = pr.placeApplication(appInfo, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("provided rule placed app in incorrect queue '%s', err %v", queue, err)
	}

	// trying to place in a child using a creatable parent
	conf = configs.PlacementRule{
		Name:   "provided",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:   "fixed",
			Value:  "testparentnew",
			Create: true,
		},
	}
	pr, err = newRule(conf)
	if err != nil || pr == nil {
		t.Errorf("provided rule create failed, err %v", err)
	}
	queue, err = pr.placeApplication(appInfo, queueFunc)
	if queue != nameParentChild || err != nil {
		t.Errorf("provided rule with non existing parent queue should create '%s', error %v", queue, err)
	}

	// trying to place in a child using a parent which is defined as a leaf
	conf = configs.PlacementRule{
		Name:   "provided",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:  "fixed",
			Value: "testchild",
		},
	}
	pr, err = newRule(conf)
	if err != nil || pr == nil {
		t.Errorf("provided rule create failed, err %v", err)
	}

	appInfo = newApplication("app1", "default", "unknown", user, tags, nil, "")
	queue, err = pr.placeApplication(appInfo, queueFunc)
	if queue != "" || err == nil {
		t.Errorf("provided rule placed app in incorrect queue '%s', err %v", queue, err)
	}
}
