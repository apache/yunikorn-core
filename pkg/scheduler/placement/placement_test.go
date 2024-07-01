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
	"errors"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/scheduler/placement/types"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
)

// basic test to check if no rules leave the manager unusable
func TestManagerNew(t *testing.T) {
	// basic info without rules, manager should not init
	man := NewPlacementManager(nil, queueFunc)
	assert.Equal(t, 2, len(man.rules), "wrong rule count for new placement manager, no config")
	assert.Equal(t, types.Provided, man.rules[0].getName(), "wrong name for implicit provided rule")
	assert.Equal(t, types.Recovery, man.rules[1].getName(), "wrong name for implicit recovery rule")

	// fail update rules with unknown rule
	rules := []configs.PlacementRule{{Name: "unknown", Create: true}}
	man = NewPlacementManager(rules, queueFunc)
	assert.Equal(t, 0, len(man.rules), "wrong rule count for new placement manager, no config")
}

func TestManagerInit(t *testing.T) {
	// basic info without rules, manager should implicitly init
	man := NewPlacementManager(nil, queueFunc)
	assert.Equal(t, 2, len(man.rules), "wrong rule count for nil rules config")
	ruleDAOs := man.GetRulesDAO()
	assert.Equal(t, 2, len(ruleDAOs), "wrong DAO count for nil rules config")
	assert.Equal(t, ruleDAOs[0].Name, types.Provided, "expected provided rule as first rule")
	assert.Equal(t, ruleDAOs[1].Name, types.Recovery, "expected recovery rule as second rule")

	// try to init with empty list should do the same
	var rules []configs.PlacementRule
	err := man.initialise(rules)
	assert.NilError(t, err, "Failed to initialize empty placement rules")
	assert.Equal(t, 2, len(man.rules), "wrong rule count for empty rules config")
	ruleDAOs = man.GetRulesDAO()
	assert.Equal(t, 2, len(ruleDAOs), "wrong DAO count for empty rules config")
	assert.Equal(t, ruleDAOs[0].Name, types.Provided, "expected provided rule as first rule")
	assert.Equal(t, ruleDAOs[1].Name, types.Recovery, "expected recovery rule as second rule")

	rules = []configs.PlacementRule{
		{Name: "unknown"},
	}
	err = man.initialise(rules)
	if err == nil {
		t.Error("initialise with 'unknown' rule list should have failed")
	}
	// rules should not have changed
	ruleDAOs = man.GetRulesDAO()
	assert.Equal(t, 2, len(ruleDAOs), "unexpected change: wrong DAO count for failed reinit")
	assert.Equal(t, ruleDAOs[0].Name, types.Provided, "expected provided rule as first rule")
	assert.Equal(t, ruleDAOs[1].Name, types.Recovery, "expected recovery rule as second rule")

	// init the manager with one rule
	rules = []configs.PlacementRule{
		{Name: "test"},
	}
	err = man.initialise(rules)
	assert.NilError(t, err, "failed to init existing manager")
	ruleDAOs = man.GetRulesDAO()
	assert.Equal(t, 2, len(ruleDAOs), "wrong DAO count for manager with test rule")
	assert.Equal(t, ruleDAOs[0].Name, types.Test, "expected test rule as first rule")
	assert.Equal(t, ruleDAOs[1].Name, types.Recovery, "expected recovery rule as second rule")

	// update the manager: remove rules implicit state is reverted
	rules = []configs.PlacementRule{}
	err = man.initialise(rules)
	assert.NilError(t, err, "Failed to re-initialize empty placement rules")
	assert.Equal(t, 2, len(man.rules), "wrong rule count for empty rules config")
	ruleDAOs = man.GetRulesDAO()
	assert.Equal(t, 2, len(ruleDAOs), "wrong DAO count for empty rules list")
	assert.Equal(t, ruleDAOs[0].Name, types.Provided, "expected provided rule as first rule")
	assert.Equal(t, ruleDAOs[1].Name, types.Recovery, "expected recovery rule as second rule")

	// check if we handle a nil list
	err = man.initialise(nil)
	assert.NilError(t, err, "Failed to re-initialize nil placement rules")
	assert.Equal(t, 2, len(man.rules), "wrong rule count for nil rules config")
	ruleDAOs = man.GetRulesDAO()
	assert.Equal(t, 2, len(ruleDAOs), "wrong DAO count for nil rules config")
	assert.Equal(t, ruleDAOs[0].Name, types.Provided, "expected provided rule as first rule")
	assert.Equal(t, ruleDAOs[1].Name, types.Recovery, "expected recovery rule as second rule")

	// init the manager with one rule which has parent
	rules = []configs.PlacementRule{
		{
			Name:   "test",
			Create: true,
			Parent: &configs.PlacementRule{
				Name: "test",
			},
		},
	}
	err = man.initialise(rules)
	assert.NilError(t, err, "Failed to re-initialize nil placement rules")
	assert.Equal(t, 2, len(man.rules), "wrong rule count for nil rules config")
	ruleDAOs = man.GetRulesDAO()
	assert.Equal(t, 2, len(ruleDAOs), "wrong DAO count for nil rules config")
	assert.Equal(t, ruleDAOs[0].Name, types.Test, "expected test rule as first rule")
	assert.Equal(t, ruleDAOs[1].Name, types.Recovery, "expected recovery rule as second rule")
}

func TestManagerUpdate(t *testing.T) {
	// basic info without rules, manager should not init
	man := NewPlacementManager(nil, queueFunc)
	// update the manager
	rules := []configs.PlacementRule{
		{Name: "test"},
	}
	err := man.UpdateRules(rules)
	assert.NilError(t, err, "failed to update existing manager")
	ruleDAOs := man.GetRulesDAO()
	assert.Equal(t, 2, len(ruleDAOs), "wrong DAO count for manager with test rule")
	assert.Equal(t, ruleDAOs[0].Name, types.Test, "expected test rule as first rule")
	assert.Equal(t, ruleDAOs[1].Name, types.Recovery, "expected recovery rule as second rule")

	// update the manager: remove rules init state is reverted
	rules = []configs.PlacementRule{}
	err = man.UpdateRules(rules)
	assert.NilError(t, err, "Failed to re-initialize empty placement rules")
	assert.Equal(t, 2, len(man.rules), "wrong rule count for empty rules config")
	ruleDAOs = man.GetRulesDAO()
	assert.Equal(t, 2, len(ruleDAOs), "wrong DAO count for empty rules config")
	assert.Equal(t, ruleDAOs[0].Name, types.Provided, "expected provided rule as first rule")
	assert.Equal(t, ruleDAOs[1].Name, types.Recovery, "expected recovery rule as second rule")

	// check if we handle a nil list
	err = man.UpdateRules(nil)
	assert.NilError(t, err, "Failed to re-initialize nil placement rules")
	assert.Equal(t, 2, len(man.rules), "wrong rule count for nil rules config")
	ruleDAOs = man.GetRulesDAO()
	assert.Equal(t, 2, len(ruleDAOs), "wrong DAO count for nil rules config")
	assert.Equal(t, ruleDAOs[0].Name, types.Provided, "expected provided rule as first rule")
	assert.Equal(t, ruleDAOs[1].Name, types.Recovery, "expected recovery rule as second rule")

	// fail to update rules with unknown rule
	rules = []configs.PlacementRule{{Name: "unknown", Create: true}}
	err = man.UpdateRules(rules)
	if err == nil {
		t.Errorf("unknown rule should fail to update the rule.")
	}
}

func TestManagerBuildRule(t *testing.T) {
	// basic with 1 rule
	rules := []configs.PlacementRule{
		{Name: "test"},
	}
	ruleObjs, err := buildRules(rules)
	if err != nil {
		t.Errorf("test rule build should not have failed, err: %v", err)
	}
	if len(ruleObjs) != 2 {
		t.Errorf("test rule build should have created 2 rules found: %d", len(ruleObjs))
	}

	// rule with a parent rule should only be 1 rule in the list
	rules = []configs.PlacementRule{
		{Name: "test",
			Parent: &configs.PlacementRule{
				Name: "test",
			},
		},
	}
	ruleObjs, err = buildRules(rules)
	if err != nil || len(ruleObjs) != 2 {
		t.Errorf("test rule build should not have failed and created 2 top level rule, err: %v, rules: %v", err, ruleObjs)
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
	ruleObjs, err = buildRules(rules)
	if err != nil || len(ruleObjs) != 3 {
		t.Errorf("rule build should not have failed and created 3 rules, err: %v, rules: %v", err, ruleObjs)
	} else if ruleObjs[0].getName() != "user" || ruleObjs[1].getName() != "test" || ruleObjs[2].getName() != "recovery" {
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
	assert.NilError(t, err, "failed to update existing manager")
	tags := make(map[string]string)
	user := security.UserGroup{
		User:   "testchild",
		Groups: []string{},
	}
	app := newApplication("app1", "default", "", user, tags, nil, "")

	// user rule existing queue, acl allowed
	err = man.PlaceApplication(app)
	queueName := app.GetQueuePath()
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
	queueName = app.GetQueuePath()
	if err == nil || queueName != "" {
		t.Errorf("leaf to create, no create flag: app should not have been placed, queue: '%s', error: %v", queueName, err)
	}

	// provided rule (2nd rule): queue acl allowed, anyone create
	app = newApplication("app1", "default", "root.fixed.leaf", user, tags, nil, "")
	err = man.PlaceApplication(app)
	queueName = app.GetQueuePath()
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
	queueName = app.GetQueuePath()
	if err == nil || queueName != "" {
		t.Errorf("leaf to create, acl deny: app should not have been placed, queue: '%s', error: %v", queueName, err)
	}

	// tag rule (3rd) check queue acl deny, queue was created above)
	tags = map[string]string{"namespace": "root.fixed.leaf"}
	app = newApplication("app1", "default", "", user, tags, nil, "")
	err = man.PlaceApplication(app)
	queueName = app.GetQueuePath()
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
	queueName = app.GetQueuePath()
	if err != nil || queueName != "root.fixed.leaf" {
		t.Errorf("existing leaf, acl allow: app should have been placed, queue: '%s', error: %v", queueName, err)
	}

	// provided rule (2nd): submit to parent
	app = newApplication("app1", "default", "root.fixed", user, nil, nil, "")
	err = man.PlaceApplication(app)
	queueName = app.GetQueuePath()
	if err == nil || queueName != "" {
		t.Errorf("parent queue: app should not have been placed, queue: '%s', error: %v", queueName, err)
	}
}

func TestForcePlaceApp(t *testing.T) {
	const (
		def  = "default"
		defQ = "root.default"
	)

	// Create the structure for the test
	// specifically no acl to allow on root
	data := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "any-user"
        queues:
          - name: default
            submitacl: "*"
          - name: acldeny
            submitacl: " "
          - name: parent
            parent: true
            submitacl: "*"
`
	err := initQueueStructure([]byte(data))
	assert.NilError(t, err, "setting up the queue config failed")
	// update the manager
	rules := []configs.PlacementRule{
		{Name: "provided",
			Create: false},
		{Name: "tag",
			Value:  "namespace",
			Create: true},
	}
	man := NewPlacementManager(rules, queueFunc)
	if man == nil {
		t.Fatal("placement manager create failed")
	}

	tags := make(map[string]string)
	user := security.UserGroup{
		User:   "any-user",
		Groups: []string{},
	}
	deny := security.UserGroup{
		User:   "deny-user",
		Groups: []string{},
	}
	var tests = []struct {
		name   string
		queue  string
		placed string
		tags   map[string]string
		user   security.UserGroup
	}{
		{"empty", "", "root.default", tags, user},
		{"provided unqualified", def, defQ, tags, user},
		{"provided qualified", defQ, defQ, tags, user},
		{"provided not exist", "unknown", "root.default", tags, user},
		{"provided parent", "root.parent", "root.default", tags, user},
		{"acl deny", "root.acldeny", "root.default", tags, deny},
		{"create", "unknown", "root.namespace", map[string]string{"namespace": "namespace"}, user},
		{"deny create", "unknown", "root.default", map[string]string{"namespace": "namespace"}, deny},
		{"forced exist", defQ, defQ, map[string]string{siCommon.AppTagCreateForce: "true"}, user},
		{"forced and create", "unknown", "root.namespace", map[string]string{siCommon.AppTagCreateForce: "true", "namespace": "namespace"}, user},
		{"forced and deny create", "unknown", common.RecoveryQueueFull, map[string]string{siCommon.AppTagCreateForce: "true", "namespace": "namespace"}, deny},
		{"forced parent", "root.parent", common.RecoveryQueueFull, map[string]string{siCommon.AppTagCreateForce: "true"}, user},
		{"forced acl deny", "root.acldeny", common.RecoveryQueueFull, map[string]string{siCommon.AppTagCreateForce: "true"}, deny},
		{"forced not exist", "unknown", common.RecoveryQueueFull, map[string]string{siCommon.AppTagCreateForce: "true"}, user},
		{"forced not exist acl deny", "unknown", common.RecoveryQueueFull, map[string]string{siCommon.AppTagCreateForce: "true"}, deny},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newApplication("app1", "default", tt.queue, tt.user, tt.tags, nil, "")
			err = man.PlaceApplication(app)
			if tt.placed == "" {
				assert.Assert(t, errors.Is(err, RejectedError), "unexpected error or no error returned")
			} else {
				assert.NilError(t, err, "unexpected placement failure")
				assert.Equal(t, tt.placed, app.GetQueuePath(), "incorrect queue set")
			}
		})
	}
}

func TestManagerPlaceApp_Error(t *testing.T) {
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
	rules := []configs.PlacementRule{
		{
			Name:   "user",
			Create: false,
			Parent: &configs.PlacementRule{
				Name:   "user",
				Create: false,
				Parent: &configs.PlacementRule{
					Name:  "fixed",
					Value: "testparent",
				},
			},
		},
	}
	user := security.UserGroup{
		User:   "testchild",
		Groups: []string{},
	}
	tags := make(map[string]string)
	err = man.UpdateRules(rules)
	assert.NilError(t, err, "failed to update existing manager")
	app := newApplication("app1", "default", "", user, tags, nil, "")
	err = man.PlaceApplication(app)
	queueName := app.GetQueuePath()
	if err == nil || queueName != "" {
		t.Errorf("failed placed app, queue: '%s', error: %v", queueName, err)
	}
}
