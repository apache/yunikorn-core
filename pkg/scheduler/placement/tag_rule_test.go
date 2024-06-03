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

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

func TestTagRule(t *testing.T) {
	conf := configs.PlacementRule{
		Name: "tag",
	}
	tr, err := newRule(conf)
	if err == nil || tr != nil {
		t.Errorf("tag rule create did not fail without tag name, err 'nil' , rule: %v, ", tr)
	}
	conf = configs.PlacementRule{
		Name:  "tag",
		Value: "label1",
	}
	tr, err = newRule(conf)
	if err != nil || tr == nil {
		t.Errorf("tag rule create failed with tag name, err %v", err)
	}
	// trying to create using a parent with a fully qualified child
	conf = configs.PlacementRule{
		Name:  "tag",
		Value: "label1",
		Parent: &configs.PlacementRule{
			Name:  "tag",
			Value: "label2",
		},
	}
	tr, err = newRule(conf)
	if err != nil || tr == nil {
		t.Errorf("tag rule create failed with tag as parent rule, err %v", err)
	}
}

func TestTagRulePlace(t *testing.T) {
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
	conf := configs.PlacementRule{
		Name:  "tag",
		Value: "label1",
	}
	tr, err := newRule(conf)
	if err != nil || tr == nil {
		t.Errorf("tag rule create failed with queue name, err %v", err)
	}

	// tag does not have a value
	tags := make(map[string]string)
	appInfo := newApplication("app1", "default", "ignored", user, tags, nil, "")
	var queue string
	queue, err = tr.placeApplication(appInfo, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("tag rule failed with no tag value '%s', err %v", queue, err)
	}

	// tag queue that exists directly in hierarchy
	tags = map[string]string{"label1": "testqueue"}
	appInfo = newApplication("app1", "default", "ignored", user, tags, nil, "")
	queue, err = tr.placeApplication(appInfo, queueFunc)
	if queue != "root.testqueue" || err != nil {
		t.Errorf("tag rule failed to place queue in correct queue '%s', err %v", queue, err)
	}

	// tag queue that does not exists
	tags = map[string]string{"label1": "unknown"}
	appInfo = newApplication("app1", "default", "ignored", user, tags, nil, "")
	queue, err = tr.placeApplication(appInfo, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("tag rule placed in queue that does not exists '%s', err %v", queue, err)
	}

	// tag queue fully qualified
	tags = map[string]string{"label1": "root.testparent.testchild"}
	appInfo = newApplication("app1", "default", "ignored", user, tags, nil, "")
	queue, err = tr.placeApplication(appInfo, queueFunc)
	if queue != "root.testparent.testchild" || err != nil {
		t.Errorf("tag rule did fail with qualified queue '%s', error %v", queue, err)
	}

	// tag queue references recovery
	tags = map[string]string{"label1": common.RecoveryQueueFull}
	appInfo = newApplication("app1", "default", "ignored", user, tags, nil, "")
	queue, err = tr.placeApplication(appInfo, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("tag rule failed with explicit recovery queue: queue '%s', error %v", queue, err)
	}

	// trying to place in a child using a parent
	conf = configs.PlacementRule{
		Name:  "tag",
		Value: "label1",
		Parent: &configs.PlacementRule{
			Name:  "tag",
			Value: "label2",
		},
	}
	tr, err = newRule(conf)
	if err != nil || tr == nil {
		t.Errorf("tag rule create failed with parent rule and qualified value, err %v", err)
	}
	tags = map[string]string{"label1": "testchild"}
	appInfo = newApplication("app1", "default", "ignored", user, tags, nil, "")
	queue, err = tr.placeApplication(appInfo, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("tag rule with parent queue should have failed value not set '%s', error %v", queue, err)
	}
	tags = map[string]string{"label1": "testchild", "label2": "testparent"}
	appInfo = newApplication("app1", "default", "ignored", user, tags, nil, "")
	queue, err = tr.placeApplication(appInfo, queueFunc)
	if queue != "root.testparent.testchild" || err != nil {
		t.Errorf("tag rule with parent queue incorrect queue '%s', error %v", queue, err)
	}
}

func TestTagRuleParent(t *testing.T) {
	err := initQueueStructure([]byte(confParentChild))
	assert.NilError(t, err, "setting up the queue config failed")

	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{},
	}

	// trying to place in a child using a parent, fail to create child
	conf := configs.PlacementRule{
		Name:   "tag",
		Value:  "label1",
		Create: false,
		Parent: &configs.PlacementRule{
			Name:  "tag",
			Value: "label2",
		},
	}
	var ur rule
	ur, err = newRule(conf)
	if err != nil || ur == nil {
		t.Errorf("tag rule create failed, err %v", err)
	}

	tags := map[string]string{"label1": "testchild", "label2": "testparent"}
	appInfo := newApplication("app1", "default", "unknown", user, tags, nil, "")
	var queue string
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("tag rule placed app in incorrect queue '%s', err %v", queue, err)
	}

	// trying to place in a child using a non creatable parent
	conf = configs.PlacementRule{
		Name:   "tag",
		Value:  "label1",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:   "tag",
			Value:  "label2",
			Create: false,
		},
	}
	ur, err = newRule(conf)
	if err != nil || ur == nil {
		t.Errorf("tag rule create failed, err %v", err)
	}

	tags = map[string]string{"label1": "testchild", "label2": "testparentnew"}
	appInfo = newApplication("app1", "default", "unknown", user, tags, nil, "")
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("tag rule placed app in incorrect queue '%s', err %v", queue, err)
	}

	// trying to place in a child using a creatable parent
	conf = configs.PlacementRule{
		Name:   "tag",
		Value:  "label1",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:   "tag",
			Value:  "label2",
			Create: true,
		},
	}
	ur, err = newRule(conf)
	if err != nil || ur == nil {
		t.Errorf("tag rule create failed with queue name, err %v", err)
	}
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != nameParentChild || err != nil {
		t.Errorf("user rule with non existing parent queue should create '%s', error %v", queue, err)
	}

	// trying to place in a child using a parent which is defined as a leaf
	conf = configs.PlacementRule{
		Name:   "tag",
		Value:  "label2",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:  "tag",
			Value: "label1",
		},
	}
	ur, err = newRule(conf)
	if err != nil || ur == nil {
		t.Errorf("tag rule create failed, err %v", err)
	}

	appInfo = newApplication("app1", "default", "unknown", user, tags, nil, "")
	queue, err = ur.placeApplication(appInfo, queueFunc)
	if queue != "" || err == nil {
		t.Errorf("tag rule placed app in incorrect queue '%s', err %v", queue, err)
	}
}

func Test_tagRule_ruleDAO(t *testing.T) {
	tests := []struct {
		name string
		conf configs.PlacementRule
		want *dao.RuleDAO
	}{
		{
			"base",
			configs.PlacementRule{Name: "tag", Value: "namespace"},
			&dao.RuleDAO{Name: "tag", Parameters: map[string]string{"tagName": "namespace", "create": "false"}},
		},
		{
			"mixedcase",
			configs.PlacementRule{Name: "tag", Value: "MixedCaseTag"},
			&dao.RuleDAO{Name: "tag", Parameters: map[string]string{"tagName": "mixedcasetag", "create": "false"}},
		},
		{
			"parent",
			configs.PlacementRule{Name: "tag", Value: "one", Create: true, Parent: &configs.PlacementRule{Name: "test", Create: true}},
			&dao.RuleDAO{Name: "tag", Parameters: map[string]string{"tagName": "one", "create": "true"}, ParentRule: &dao.RuleDAO{Name: "test", Parameters: map[string]string{"create": "true"}}},
		},
		{
			"filter",
			configs.PlacementRule{Name: "tag", Value: "two", Create: true, Filter: configs.Filter{Type: filterDeny, Groups: []string{"group[0-9]"}}},
			&dao.RuleDAO{Name: "tag", Parameters: map[string]string{"tagName": "two", "create": "true"}, Filter: &dao.FilterDAO{Type: filterDeny, GroupExp: "group[0-9]"}},
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
