/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package placement

import (
	"github.com/cloudera/yunikorn-core/pkg/cache"
	"github.com/cloudera/yunikorn-core/pkg/common/configs"
	"github.com/cloudera/yunikorn-core/pkg/common/security"
	"testing"
)

func TestFixedRule(t *testing.T) {
	conf := configs.PlacementRule{
		Name: "fixed",
	}
	rule, err := newRule(conf)
	if err == nil || rule != nil {
		t.Errorf("fixed rule create did not fail without queue name, err 'nil' , rule: %v", rule)
	}
	conf = configs.PlacementRule{
		Name:  "fixed",
		Value: "testqueue",
	}
	rule, err = newRule(conf)
	if err != nil || rule == nil {
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
	rule, err = newRule(conf)
	if err == nil || rule != nil {
		t.Errorf("fixed rule create did not fail with parent rule and qualified child queue name, err 'nil' , rule: %v", rule)
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
	partInfo, err := CreatePartitionInfo([]byte(data))
	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{},
	}
	tags := make(map[string]string, 0)
	appInfo := cache.NewApplicationInfo("app1", "default", "ignored", user, tags)

	// fixed queue that exists directly under the root
	conf := configs.PlacementRule{
		Name:  "fixed",
		Value: "testqueue",
	}
	rule, err := newRule(conf)
	if err != nil || rule == nil {
		t.Errorf("fixed rule create failed with queue name, err %v", err)
	}
	queue, err := rule.placeApplication(appInfo, partInfo)
	if queue != "root.testqueue" || err != nil {
		t.Errorf("fixed rule failed to place queue in correct queue '%s', err %v", queue, err)
	}

	// fixed queue that exists directly in hierarchy
	conf = configs.PlacementRule{
		Name:  "fixed",
		Value: "root.testparent.testchild",
	}
	rule, err = newRule(conf)
	if err != nil || rule == nil {
		t.Errorf("fixed rule create failed with queue name, err %v", err)
	}
	queue, err = rule.placeApplication(appInfo, partInfo)
	if queue != "root.testparent.testchild" || err != nil {
		t.Errorf("fixed rule failed to place queue in correct queue '%s', err %v", queue, err)
	}

	// fixed queue that does not exists
	conf = configs.PlacementRule{
		Name:   "fixed",
		Value:  "newqueue",
		Create: true,
	}
	rule, err = newRule(conf)
	if err != nil || rule == nil {
		t.Errorf("fixed rule create failed with queue name, err %v", err)
	}
	queue, err = rule.placeApplication(appInfo, partInfo)
	if queue != "root.newqueue" || err != nil {
		t.Errorf("fixed rule failed to place queue in to be created queue '%s', err %v", queue, err)
	}

	// trying to place in a parent queue should not fail: failure happens on create in this case
	conf = configs.PlacementRule{
		Name:  "fixed",
		Value: "root.testparent",
	}
	rule, err = newRule(conf)
	if err != nil || rule == nil {
		t.Errorf("fixed rule create failed with queue name, err %v", err)
	}
	queue, err = rule.placeApplication(appInfo, partInfo)
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
	rule, err = newRule(conf)
	if err != nil || rule == nil {
		t.Errorf("fixed rule create failed with queue name, err %v", err)
	}
	queue, err = rule.placeApplication(appInfo, partInfo)
	if queue != "root.testparent.testchild" || err != nil {
		t.Errorf("fixed rule with parent queue should not have failed '%s', error %v", queue, err)
	}
}
