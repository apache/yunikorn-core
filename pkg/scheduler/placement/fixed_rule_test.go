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
	"sort"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
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

	var tests = []struct {
		name          string
		expectedQueue string
		config        configs.PlacementRule
		nilError      bool
	}{
		{"fixed queue that exists directly under the root", "root.testqueue", configs.PlacementRule{Name: "fixed", Value: "testqueue"}, true},
		{"fixed queue that exists directly in hierarchy", "root.testparent.testchild", configs.PlacementRule{Name: "fixed", Value: "testparent.testchild"}, true},
		{"fixed queue that does not exists", "root.newqueue", configs.PlacementRule{Name: "fixed", Value: "newqueue", Create: true}, true},
		{"place in a parent queue should not fail: failure happens on create in this case", "root.testparent", configs.PlacementRule{Name: "fixed", Value: "root.testparent"}, true},
		{"place in a child using a parent", "root.testparent.testchild", configs.PlacementRule{Name: "fixed", Value: "testchild", Parent: &configs.PlacementRule{Name: "fixed", Value: "testparent"}}, true},
		{"invalid queue name", "", configs.PlacementRule{Name: "fixed", Value: "testqueue!>invalid<"}, false},
		{"invalid queue name with full queue hierarchy", "", configs.PlacementRule{Name: "fixed", Value: "root.testparent!>invalid<test.testqueue"}, false},
		{"deny filter type should got empty queue", "", configs.PlacementRule{Name: "fixed", Value: "testchild", Filter: configs.Filter{Type: filterDeny}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var fr rule
			fr, err = newRule(tt.config)
			if tt.nilError {
				if err != nil || fr == nil {
					t.Errorf("fixed rule create failed with queue name, err %v", err)
				}
				var queue string
				if tt.nilError {
					queue, err = fr.placeApplication(app, queueFunc)
					if queue != tt.expectedQueue || err != nil {
						t.Errorf("fixed rule failed to place queue in correct queue '%s', err %v", queue, err)
					}
				} else {
					_, err = fr.placeApplication(app, queueFunc)
					if err == nil {
						t.Errorf("fixed rule should have failed to place queue, err %v", err)
					}
				}
			} else {
				if err == nil {
					t.Errorf("fixed rule should have failed with queue name, err %v", err)
				}
			}
		})
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

	// trying to place in invalid child using a creatable parent
	conf = configs.PlacementRule{
		Name:   "fixed",
		Value:  "testchild",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:   "fixed",
			Value:  "test!invalid<tes>t",
			Create: true,
		},
	}
	fr, err = newRule(conf)
	if err == nil {
		t.Errorf("fixed rule create should have failed with queue name, err %v", err)
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

	// failed parent rule
	conf = configs.PlacementRule{
		Name:  "fixed",
		Value: "testchild",
		Parent: &configs.PlacementRule{
			Name:  "fixed",
			Value: "testchild",
			Parent: &configs.PlacementRule{
				Name:  "fixed",
				Value: "testchild",
			},
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

	// parent name not has prefix
	conf = configs.PlacementRule{
		Name:   "fixed",
		Value:  "testchild",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:  "fixed",
			Value: "root",
		},
	}
	fr, err = newRule(conf)
	if err != nil || fr == nil {
		t.Errorf("fixed rule create failed with queue name, err %v", err)
	}
	queue, err = fr.placeApplication(app, queueFunc)
	if queue != "root.root.testchild" || err != nil {
		t.Errorf("fixed rule with parent declared as leaf should have failed '%s', error %v", queue, err)
	}
}

func Test_fixedRule_ruleDAO(t *testing.T) {
	tests := []struct {
		name string
		conf configs.PlacementRule
		want *dao.RuleDAO
	}{
		{
			"base",
			configs.PlacementRule{Name: "fixed", Value: "default"},
			&dao.RuleDAO{Name: "fixed", Parameters: map[string]string{"queue": "default", "qualified": "false", "create": "false"}},
		},
		{
			"qualified",
			configs.PlacementRule{Name: "fixed", Value: "root.default"},
			&dao.RuleDAO{Name: "fixed", Parameters: map[string]string{"queue": "root.default", "qualified": "true", "create": "false"}},
		},
		{
			"parent",
			configs.PlacementRule{Name: "fixed", Value: "default", Create: true, Parent: &configs.PlacementRule{Name: "test", Create: true}},
			&dao.RuleDAO{Name: "fixed", Parameters: map[string]string{"queue": "default", "qualified": "false", "create": "true"}, ParentRule: &dao.RuleDAO{Name: "test", Parameters: map[string]string{"create": "true"}}},
		},
		{
			"filter",
			configs.PlacementRule{Name: "fixed", Value: "default", Create: true, Filter: configs.Filter{Type: filterAllow, Groups: []string{"group1", "group2"}}},
			&dao.RuleDAO{Name: "fixed", Parameters: map[string]string{"queue": "default", "qualified": "false", "create": "true"}, Filter: &dao.FilterDAO{Type: filterAllow, GroupList: []string{"group1", "group2"}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ur, err := newRule(tt.conf)
			assert.NilError(t, err, "setting up the rule failed")
			ruleDAO := ur.ruleDAO()
			if tt.want.Filter != nil {
				sort.Strings(tt.want.Filter.UserList)
				sort.Strings(ruleDAO.Filter.UserList)
				sort.Strings(tt.want.Filter.GroupList)
				sort.Strings(ruleDAO.Filter.GroupList)
			}
			assert.DeepEqual(t, tt.want, ruleDAO)
		})
	}
}
