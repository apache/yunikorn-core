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

func TestFixedRule(t *testing.T) {
	conf := configs.PlacementRule{
		Name: "fixed",
	}
	fr, err := newRule(conf)
	if err == nil || fr != nil {
		t.Errorf("fixed rule create did not fail without queue name, err 'nil', rule: %v", fr)
	}
	conf = configs.PlacementRule{
		Name:  "fixed",
		Value: "testqueue",
	}
	fr, err = newRule(conf)
	if err != nil || fr == nil {
		t.Errorf("fixed rule create failed with queue name, err %v", err)
	}
	// trying to create using a parent with a fully qualified child
	conf = configs.PlacementRule{
		Name:  "fixed",
		Value: "root.testchild",
		Parent: &configs.PlacementRule{
			Name:  "fixed",
			Value: "testparent",
		},
	}
	fr, err = newRule(conf)
	if err == nil || fr != nil {
		t.Errorf("fixed rule create did not fail with parent rule and qualified child queue name, err 'nil', rule: %v", fr)
	}
}

func TestFixedRulePlace(t *testing.T) {
	// Create the structure for the test
	data := `
partitions:
  - name: default
    queues:
      - name: testqueue
      - name: testparent
        queues:
          - name: testchild
`
	err := initQueueStructure([]byte(data))
	assert.NilError(t, err, "setting up the queue config failed")

	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{},
	}
	tags := make(map[string]string)
	app := newApplication("app1", "default", "ignored", user, tags, nil, "")

	// fixed queue that exists directly under the root
	conf := configs.PlacementRule{
		Name:  "fixed",
		Value: "testqueue",
	}
	var fr rule
	fr, err = newRule(conf)
	if err != nil || fr == nil {
		t.Errorf("fixed rule create failed with queue name, err %v", err)
	}
	var queue string
	queue, err = fr.placeApplication(app, queueFunc)
	if queue != "root.testqueue" || err != nil {
		t.Errorf("fixed rule failed to place queue in correct queue '%s', err %v", queue, err)
	}

	// fixed queue that exists directly in hierarchy
	conf = configs.PlacementRule{
		Name:  "fixed",
		Value: "root.testparent.testchild",
	}
	fr, err = newRule(conf)
	if err != nil || fr == nil {
		t.Errorf("fixed rule create failed with queue name, err %v", err)
	}
	queue, err = fr.placeApplication(app, queueFunc)
	if queue != "root.testparent.testchild" || err != nil {
		t.Errorf("fixed rule failed to place queue in correct queue '%s', err %v", queue, err)
	}

	// fixed queue that does not exists
	conf = configs.PlacementRule{
		Name:   "fixed",
		Value:  "newqueue",
		Create: true,
	}
	fr, err = newRule(conf)
	if err != nil || fr == nil {
		t.Errorf("fixed rule create failed with queue name, err %v", err)
	}
	queue, err = fr.placeApplication(app, queueFunc)
	if queue != "root.newqueue" || err != nil {
		t.Errorf("fixed rule failed to place queue in to be created queue '%s', err %v", queue, err)
	}

	// trying to place in a parent queue should not fail: failure happens on create in this case
	conf = configs.PlacementRule{
		Name:  "fixed",
		Value: "root.testparent",
	}
	fr, err = newRule(conf)
	if err != nil || fr == nil {
		t.Errorf("fixed rule create failed with queue name, err %v", err)
	}
	queue, err = fr.placeApplication(app, queueFunc)
	if queue != "root.testparent" || err != nil {
		t.Errorf("fixed rule did fail with parent queue '%s', error %v", queue, err)
	}

	// trying to place in a child using a parent
	conf = configs.PlacementRule{
		Name:  "fixed",
		Value: "testchild",
		Parent: &configs.PlacementRule{
			Name:  "fixed",
			Value: "testparent",
		},
	}
	fr, err = newRule(conf)
	if err != nil || fr == nil {
		t.Errorf("fixed rule create failed with queue name, err %v", err)
	}
	queue, err = fr.placeApplication(app, queueFunc)
	if queue != "root.testparent.testchild" || err != nil {
		t.Errorf("fixed rule with parent queue should not have failed '%s', error %v", queue, err)
	}
}

func TestFixedRuleParent(t *testing.T) {
	err := initQueueStructure([]byte(confParentChild))
	assert.NilError(t, err, "setting up the queue config failed")

	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{},
	}
	tags := make(map[string]string)
	app := newApplication("app1", "default", "ignored", user, tags, nil, "")

	// trying to place in a child using a parent, fail to create child
	conf := configs.PlacementRule{
		Name:   "fixed",
		Value:  "nonexist",
		Create: false,
		Parent: &configs.PlacementRule{
			Name:  "fixed",
			Value: "testparent",
		},
	}
	var fr rule
	fr, err = newRule(conf)
	if err != nil || fr == nil {
		t.Errorf("fixed rule create failed with queue name, err %v", err)
	}
	var queue string
	queue, err = fr.placeApplication(app, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("fixed rule with create false for child should have failed and gave '%s', error %v", queue, err)
	}

	// trying to place in a child using a non creatable parent
	conf = configs.PlacementRule{
		Name:   "fixed",
		Value:  "testchild",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:   "fixed",
			Value:  "testparentnew",
			Create: false,
		},
	}
	fr, err = newRule(conf)
	if err != nil || fr == nil {
		t.Errorf("fixed rule create failed with queue name, err %v", err)
	}
	queue, err = fr.placeApplication(app, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("fixed rule with non existing parent queue should have failed '%s', error %v", queue, err)
	}

	// trying to place in a child using a creatable parent
	conf = configs.PlacementRule{
		Name:   "fixed",
		Value:  "testchild",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:   "fixed",
			Value:  "testparentnew",
			Create: true,
		},
	}
	fr, err = newRule(conf)
	if err != nil || fr == nil {
		t.Errorf("fixed rule create failed with queue name, err %v", err)
	}
	queue, err = fr.placeApplication(app, queueFunc)
	if queue != nameParentChild || err != nil {
		t.Errorf("fixed rule with non existing parent queue should created '%s', error %v", queue, err)
	}

	// trying to place in a child using a parent which is defined as a leaf
	conf = configs.PlacementRule{
		Name:   "fixed",
		Value:  "nonexist",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:  "fixed",
			Value: "testchild",
		},
	}
	fr, err = newRule(conf)
	if err != nil || fr == nil {
		t.Errorf("fixed rule create failed with queue name, err %v", err)
	}
	queue, err = fr.placeApplication(app, queueFunc)
	if queue != "" || err == nil {
		t.Errorf("fixed rule with parent declared as leaf should have failed '%s', error %v", queue, err)
	}
}
