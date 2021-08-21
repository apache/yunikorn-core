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

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
)

// basic test to check if no rules leave the manager unusable
func TestManagerNew(t *testing.T) {
	// basic info without rules, manager should not init
	man := NewPlacementManager(nil, nil)
	if man.initialised {
		t.Error("Placement manager marked initialised without rules")
	}
	if man.IsInitialised() {
		t.Error("Placement manager marked initialised without rules")
	}
	if len(man.rules) != 0 {
		t.Error("Placement manager marked initialised without rules")
	}
}

func TestManagerNoFunc(t *testing.T) {
	// basic info without rules, manager should not init
	rules := []configs.PlacementRule{
		{Name: "test"},
	}
	man := NewPlacementManager(rules, nil)
	if man.initialised {
		t.Error("Placement manager marked initialised without queue func")
	}
	if man.initialise(rules) == nil {
		t.Error("Placement manager should not initialise with nil function")
	}
	if man.UpdateRules(rules) == nil {
		t.Error("Placement manager should not update with nil function")
	}
}

func TestManagerInit(t *testing.T) {
	// basic info without rules, manager should not init no error
	man := NewPlacementManager(nil, queueFunc)
	if man.initialised {
		t.Error("Placement manager marked initialised without rules")
	}
	// try to init with empty list must error
	var rules []configs.PlacementRule
	err := man.initialise(rules)
	if err == nil || man.initialised {
		t.Error("initialise without rules should have failed")
	}
	rules = []configs.PlacementRule{
		{Name: "unknown"},
	}
	err = man.initialise(rules)
	if err == nil || man.initialised {
		t.Error("initialise with 'unknown' rule list should have failed")
	}

	// init the manager with one rule
	rules = []configs.PlacementRule{
		{Name: "test"},
	}
	err = man.initialise(rules)
	if err != nil || !man.initialised {
		t.Errorf("failed to init existing manager, init state: %t, error: %v", man.initialised, err)
	}
	// update the manager: remove rules init state is reverted
	rules = []configs.PlacementRule{}
	err = man.initialise(rules)
	if err == nil || !man.initialised {
		t.Errorf("init should have failed with empty list, init state: %t, error: %v", man.initialised, err)
	}
	// check if we handle a nil list
	err = man.initialise(nil)
	if err == nil || !man.initialised {
		t.Errorf("init should have failed with nil list, init state: %t, error: %v", man.initialised, err)
	}
}

func TestManagerUpdate(t *testing.T) {
	// basic info without rules, manager should not init
	man := NewPlacementManager(nil, queueFunc)
	// update the manager
	rules := []configs.PlacementRule{
		{Name: "test"},
	}
	err := man.UpdateRules(rules)
	if err != nil || !man.initialised {
		t.Errorf("failed to update existing manager, init state: %t, error: %v", man.initialised, err)
	}
	// update the manager: remove rules init state is reverted
	rules = []configs.PlacementRule{}
	err = man.UpdateRules(rules)
	if err != nil || man.initialised {
		t.Errorf("failed to update existing manager, init state: %t, error: %v", man.initialised, err)
	}
	// check if we handle a nil list
	err = man.UpdateRules(nil)
	if err != nil || man.initialised {
		t.Errorf("failed to update existing manager with nil list, init state: %t, error: %v", man.initialised, err)
	}
}

func TestManagerBuildRule(t *testing.T) {
	// basic with 1 rule
	man := NewPlacementManager(nil, queueFunc)
	rules := []configs.PlacementRule{
		{Name: "test"},
	}
	ruleObjs, err := man.buildRules(rules)
	if err != nil {
		t.Errorf("test rule build should not have failed, err: %v", err)
	}
	if len(ruleObjs) != 1 {
		t.Errorf("test rule build should have created 1 rule found: %d", len(ruleObjs))
	}

	// rule with a parent rule should only be 1 rule in the list
	rules = []configs.PlacementRule{
		{Name: "test",
			Parent: &configs.PlacementRule{
				Name: "test",
			},
		},
	}
	ruleObjs, err = man.buildRules(rules)
	if err != nil || len(ruleObjs) != 1 {
		t.Errorf("test rule build should not have failed and created 1 top level rule, err: %v, rules: %v", err, ruleObjs)
	} else {
		parent := ruleObjs[0].getParent()
		if parent == nil || parent.getName() != "test" {
			t.Error("test rule build should have created 2 rules: parent not found")
		}
	}

	// two rules in order: cannot use the same rule names as we can not check them
	rules = []configs.PlacementRule{
		{Name: "user"},
		{Name: "test"},
	}
	ruleObjs, err = man.buildRules(rules)
	if err != nil || len(ruleObjs) != 2 {
		t.Errorf("rule build should not have failed and created 2 rule, err: %v, rules: %v", err, ruleObjs)
	} else if ruleObjs[0].getName() != "user" || ruleObjs[1].getName() != "test" {
		t.Errorf("rule build order is not preserved: %v", ruleObjs)
	}
}

func TestManagerPlaceApp(t *testing.T) {
	// Create the structure for the test
	data := `
partitions:
  - name: default
    queues:
      - name: root
        queues:
          - name: testparent
            submitacl: "*"
            queues:
              - name: testchild
          - name: fixed
            submitacl: "other-user "
            parent: true
`
	err := initQueueStructure([]byte(data))
	assert.NilError(t, err, "setting up the queue config failed")
	// basic info without rules, manager should init
	man := NewPlacementManager(nil, queueFunc)
	if man == nil {
		t.Fatal("placement manager create failed")
	}
	// update the manager
	rules := []configs.PlacementRule{
		{Name: "user",
			Create: false,
			Parent: &configs.PlacementRule{
				Name:  "fixed",
				Value: "testparent"},
		},
		{Name: "provided",
			Create: true},
		{Name: "tag",
			Value:  "namespace",
			Create: true},
	}
	err = man.UpdateRules(rules)
	if err != nil || !man.initialised {
		t.Errorf("failed to update existing manager, init state: %t, error: %v", man.initialised, err)
	}
	tags := make(map[string]string)
	user := security.UserGroup{
		User:   "testchild",
		Groups: []string{},
	}
	app := newApplication("app1", "default", "", user, tags, nil, "")

	// user rule existing queue, acl allowed
	err = man.PlaceApplication(app)
	queueName := app.QueuePath
	if err != nil || queueName != "root.testparent.testchild" {
		t.Errorf("leaf exist: app should have been placed in user queue, queue: '%s', error: %v", queueName, err)
	}
	user = security.UserGroup{
		User:   "other-user",
		Groups: []string{},
	}

	// user rule new queue: fails on create flag
	app = newApplication("app1", "default", "", user, tags, nil, "")
	err = man.PlaceApplication(app)
	queueName = app.QueuePath
	if err == nil || queueName != "" {
		t.Errorf("leaf to create, no create flag: app should not have been placed, queue: '%s', error: %v", queueName, err)
	}

	// provided rule (2nd rule): queue acl allowed, anyone create
	app = newApplication("app1", "default", "root.fixed.leaf", user, tags, nil, "")
	err = man.PlaceApplication(app)
	queueName = app.QueuePath
	if err != nil || queueName != "root.fixed.leaf" {
		t.Errorf("leave create, acl allow: app should have been placed, queue: '%s', error: %v", queueName, err)
	}

	// provided rule (2rd): queue acl deny, queue does not exist
	user = security.UserGroup{
		User:   "unknown-user",
		Groups: []string{},
	}
	app = newApplication("app1", "default", "root.fixed.other", user, tags, nil, "")
	err = man.PlaceApplication(app)
	queueName = app.QueuePath
	if err == nil || queueName != "" {
		t.Errorf("leaf to create, acl deny: app should not have been placed, queue: '%s', error: %v", queueName, err)
	}

	// tag rule (3rd) check queue acl deny, queue was created above)
	tags = map[string]string{"namespace": "root.fixed.leaf"}
	app = newApplication("app1", "default", "", user, tags, nil, "")
	err = man.PlaceApplication(app)
	queueName = app.QueuePath
	if err == nil || queueName != "" {
		t.Errorf("existing leaf, acl deny: app should not have been placed, queue: '%s', error: %v", queueName, err)
	}

	// tag rule (3rd) queue acl allow, queue already exists
	user = security.UserGroup{
		User:   "other-user",
		Groups: []string{},
	}
	app = newApplication("app1", "default", "", user, tags, nil, "")
	err = man.PlaceApplication(app)
	queueName = app.QueuePath
	if err != nil || queueName != "root.fixed.leaf" {
		t.Errorf("existing leaf, acl allow: app should have been placed, queue: '%s', error: %v", queueName, err)
	}

	// provided rule (2nd): submit to parent
	app = newApplication("app1", "default", "root.fixed", user, nil, nil, "")
	err = man.PlaceApplication(app)
	queueName = app.QueuePath
	if err == nil || queueName != "" {
		t.Errorf("parent queue: app should not have been placed, queue: '%s', error: %v", queueName, err)
	}
}
