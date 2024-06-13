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

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

func TestUserRulePlace(t *testing.T) {
	// Create the structure for the test
	data := `
partitions:
  - name: default
    queues:
      - name: test_dot_user
      - name: testchild
      - name: testparent
        queues:
          - name: testchild
`
	err := initQueueStructure([]byte(data))
	assert.NilError(t, err, "setting up the queue config failed")

	tags := make(map[string]string)
	user := security.UserGroup{
		User:   "testchild",
		Groups: []string{},
	}
	appInfo := newApplication("app1", "default", "ignored", user, tags, nil, "")

	// user queue that exists directly under the root
	conf := configs.PlacementRule{
		Name: "user",
	}
	var ur rule
	ur, err = newRule(conf)
	if err != nil || ur == nil {
		t.Errorf("user rule create failed, err %v", err)
	}
	var queue string
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != "root.testchild" || err != nil {
		t.Errorf("user rule failed to place queue in correct queue '%s', err %v", queue, err)
	}
	// trying to place in a parent queue should fail on queue create not in the rule
	user = security.UserGroup{
		User:   "testparent",
		Groups: []string{},
	}
	appInfo = newApplication("app1", "default", "ignored", user, tags, nil, "")
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != "root.testparent" || err != nil {
		t.Errorf("user rule failed with parent queue '%s', error %v", queue, err)
	}

	user = security.UserGroup{
		User:   "test.user",
		Groups: []string{},
	}
	appInfo = newApplication("app1", "default", "ignored", user, tags, nil, "")
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue == "" || err != nil {
		t.Errorf("user rule with dotted user should not have failed '%s', error %v", queue, err)
	}

	// user queue that exists directly in hierarchy
	conf = configs.PlacementRule{
		Name: "user",
		Parent: &configs.PlacementRule{
			Name:  "fixed",
			Value: "testparent",
		},
	}
	user = security.UserGroup{
		User:   "testchild",
		Groups: []string{},
	}
	appInfo = newApplication("app1", "default", "ignored", user, tags, nil, "")
	ur, err = newRule(conf)
	if err != nil || ur == nil {
		t.Errorf("user rule create failed with queue name, err %v", err)
	}
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != "root.testparent.testchild" || err != nil {
		t.Errorf("user rule failed to place queue in correct queue '%s', err %v", queue, err)
	}

	// user queue that does not exists
	user = security.UserGroup{
		User:   "unknown",
		Groups: []string{},
	}
	appInfo = newApplication("app1", "default", "ignored", user, tags, nil, "")

	conf = configs.PlacementRule{
		Name:   "user",
		Create: true,
	}
	ur, err = newRule(conf)
	if err != nil || ur == nil {
		t.Errorf("user rule create failed with queue name, err %v", err)
	}
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != "root.unknown" || err != nil {
		t.Errorf("user rule placed in to be created queue with create false '%s', err %v", queue, err)
	}

	user = security.UserGroup{
		User:   "test.user@gmail.com",
		Groups: []string{},
	}
	appInfo = newApplication("app1", "default", "ignored", user, tags, nil, "")
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != "root.test_dot_user@gmail_dot_com" || err != nil {
		t.Errorf("user rule with dotted user should not have failed '%s', error %v", queue, err)
	}

	user = security.UserGroup{
		User:   "http://domain.com/server1/testuser@cloudera.com",
		Groups: []string{},
	}
	appInfo = newApplication("app1", "default", "ignored", user, tags, nil, "")
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != "root.http://domain_dot_com/server1/testuser@cloudera_dot_com" || err != nil {
		t.Errorf("user rule with dotted user should not have failed '%s', error %v", queue, err)
	}

	user = security.UserGroup{
		User:   "invalid!us>er",
		Groups: []string{},
	}
	appInfo = newApplication("app1", "default", "ignored", user, tags, nil, "")
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if err == nil {
		t.Errorf("user rule with invalid username should have failed, error %v", err)
	}

	// deny filter type should got got empty queue
	conf = configs.PlacementRule{
		Name: "user",
		Filter: configs.Filter{
			Type: filterDeny,
		},
	}
	ur, err = newRule(conf)
	if err != nil || ur == nil {
		t.Errorf("user rule create failed with queue name, err %v", err)
	}
	appInfo = newApplication("app1", "default", "ignored", user, tags, nil, "")
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("user rule with deny filter type should got empty queue, err nil")
	}
}

func TestUserRuleParent(t *testing.T) {
	err := initQueueStructure([]byte(confParentChild))
	assert.NilError(t, err, "setting up the queue config failed")

	tags := make(map[string]string)
	user := security.UserGroup{
		User:   "testchild",
		Groups: []string{},
	}

	// trying to place in a child using a parent, fail to create child
	conf := configs.PlacementRule{
		Name:   "user",
		Create: false,
		Parent: &configs.PlacementRule{
			Name:  "fixed",
			Value: "testparent",
		},
	}
	var ur rule
	ur, err = newRule(conf)
	if err != nil || ur == nil {
		t.Errorf("user rule create failed, err %v", err)
	}

	appInfo := newApplication("app1", "default", "unknown", user, tags, nil, "")
	var queue string
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("user rule placed app in incorrect queue '%s', err %v", queue, err)
	}

	// trying to place in a child using a non creatable parent
	conf = configs.PlacementRule{
		Name:   "user",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:   "fixed",
			Value:  "testother",
			Create: false,
		},
	}
	ur, err = newRule(conf)
	if err != nil || ur == nil {
		t.Errorf("user rule create failed, err %v", err)
	}

	appInfo = newApplication("app1", "default", "unknown", user, tags, nil, "")
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("user rule placed app in incorrect queue '%s', err %v", queue, err)
	}

	// trying to place in a child using a creatable parent
	conf = configs.PlacementRule{
		Name:   "user",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:   "fixed",
			Value:  "testparentnew",
			Create: true,
		},
	}
	ur, err = newRule(conf)
	if err != nil || ur == nil {
		t.Errorf("user rule create failed with queue name, err %v", err)
	}
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != nameParentChild || err != nil {
		t.Errorf("user rule with non existing parent queue should create '%s', error %v", queue, err)
	}

	user1 := security.UserGroup{
		User:   "test!child",
		Groups: []string{},
	}
	appInfo1 := newApplication("app1", "default", "unknown", user1, tags, nil, "")
	queue, err = ur.placeApplication(appInfo1, queueFunc)
	if err == nil {
		t.Errorf("user rule with non existing parent queue and invalid child queue should have failed, error %v", err)
	}

	// trying to place in a child using a parent which is defined as a leaf
	conf = configs.PlacementRule{
		Name:   "user",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:  "fixed",
			Value: "testchild",
		},
	}
	ur, err = newRule(conf)
	if err != nil || ur == nil {
		t.Errorf("user rule create failed, err %v", err)
	}

	appInfo = newApplication("app1", "default", "unknown", user, tags, nil, "")
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != "" || err == nil {
		t.Errorf("user rule placed app in incorrect queue '%s', err %v", queue, err)
	}

	// failed parent rule
	conf = configs.PlacementRule{
		Name:   "user",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:  "fixed",
			Value: "testchild",
			Parent: &configs.PlacementRule{
				Name:  "fixed",
				Value: "testchild",
			},
		},
	}
	ur, err = newRule(conf)
	if err != nil || ur == nil {
		t.Errorf("user rule create failed, err %v", err)
	}
	appInfo = newApplication("app1", "default", "unknown", user, tags, nil, "")
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != "" || err == nil {
		t.Errorf("user rule placed app in incorrect queue '%s', err %v", queue, err)
	}

	// parent name not has prefix
	conf = configs.PlacementRule{
		Name:   "user",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:  "fixed",
			Value: "root",
		},
	}
	ur, err = newRule(conf)
	if err != nil || ur == nil {
		t.Errorf("user rule create failed, err %v", err)
	}
	appInfo = newApplication("app1", "default", "unknown", user, tags, nil, "")
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != "root.root.testchild" || err != nil {
		t.Errorf("user rule placed app in incorrect queue '%s', err %v", queue, err)
	}
}

func Test_userRule_ruleDAO(t *testing.T) {
	tests := []struct {
		name string
		conf configs.PlacementRule
		want *dao.RuleDAO
	}{
		{
			"base",
			configs.PlacementRule{Name: "user"},
			&dao.RuleDAO{Name: "user", Parameters: map[string]string{"create": "false"}},
		},
		{
			"parent",
			configs.PlacementRule{Name: "user", Create: true, Parent: &configs.PlacementRule{Name: "test", Create: true}},
			&dao.RuleDAO{Name: "user", Parameters: map[string]string{"create": "true"}, ParentRule: &dao.RuleDAO{Name: "test", Parameters: map[string]string{"create": "true"}}},
		},
		{
			"filter",
			configs.PlacementRule{Name: "user", Create: true, Filter: configs.Filter{Type: filterDeny, Users: []string{"user"}}},
			&dao.RuleDAO{Name: "user", Parameters: map[string]string{"create": "true"}, Filter: &dao.FilterDAO{Type: filterDeny, UserList: []string{"user"}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ur, err := newRule(tt.conf)
			assert.NilError(t, err, "setting up the rule failed")
			ruleDAO := ur.ruleDAO()
			assert.DeepEqual(t, tt.want, ruleDAO)
		})
	}
}
