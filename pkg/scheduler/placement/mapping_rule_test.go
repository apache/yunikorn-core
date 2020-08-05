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

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
)

func TestMappingRuleCreation(t *testing.T) {
	// missing value case
	conf := configs.PlacementRule{
		Name: "mapping",
	}
	mr, err := newRule(conf)
	if err == nil || mr != nil {
		t.Errorf("mapping rule create did not fail without queue name")
	}

	// not 2 separators
	conf = configs.PlacementRule{
		Name:  "mapping",
		Value: "a:b",
	}
	mr, err = newRule(conf)
	if err == nil || mr != nil {
		t.Errorf("mapping rule create did not fail with 1 separator")
	}
	conf = configs.PlacementRule{
		Name:  "mapping",
		Value: "a:b:c:d",
	}
	mr, err = newRule(conf)
	if err == nil || mr != nil {
		t.Errorf("mapping rule create did not fail with 3 separators")
	}

	// empty tag value
	conf = configs.PlacementRule{
		Name:  "mapping",
		Value: ":b:c",
	}
	mr, err = newRule(conf)
	if err == nil || mr != nil {
		t.Errorf("mapping rule create did not fail with empty tag part")
	}

	// empty values part
	conf = configs.PlacementRule{
		Name:  "mapping",
		Value: "a::c",
	}
	mr, err = newRule(conf)
	if err == nil || mr != nil {
		t.Errorf("mapping rule create did not fail with empty values part")
	}

	// empty queue name
	conf = configs.PlacementRule{
		Name:  "mapping",
		Value: "a:b:",
	}
	mr, err = newRule(conf)
	if err == nil || mr != nil {
		t.Errorf("mapping rule create did not fail with empty queue part")
	}

	// non-existing parent
	conf = configs.PlacementRule{
		Name:  "mapping",
		Value: "a:b:c",
		Parent: &configs.PlacementRule{
			Name: "nonexisting",
		},
	}
	mr, err = newRule(conf)
	if err == nil || mr != nil {
		t.Errorf("mapping rule create did not fail with wrong parent")
	}
}

func TestMappingRuleName(t *testing.T) {
	conf := configs.PlacementRule{
		Name:  "mapping",
		Value: "a:b,c:d",
		Parent: &configs.PlacementRule{
			Name:  "mapping",
			Value: "e:f:g",
		},
	}
	mr, err := newRule(conf)
	if err != nil || mr == nil {
		t.Fatalf("mapping rule creation failed")
	}
	assert.Equal(t, mr.getName(), "mapping", "the rule should be of type mapping")
	assert.Assert(t, mr.getParent() != nil, "the parent rule should not bt nil")
	assert.Equal(t, mr.getParent().getName(), "mapping", "the parent rule should be of type mapping")
}

func TestMappingRulePlace(t *testing.T) {
	// Create the structure for the test
	data := `
partitions:
  - name: default
    queues:
      - name: testqueue
      - name: testparent
        queues:
          - name: testqueue
`
	partInfo, err := CreatePartitionInfo([]byte(data))
	assert.NilError(t, err, "Partition create failed with error")
	user1 := security.UserGroup{
		User:   "testuser1",
		Groups: []string{},
	}
	user2 := security.UserGroup{
		User:   "testuser2",
		Groups: []string{},
	}
	conf := configs.PlacementRule{
		Name:  "mapping",
		Value: "tag:value1,value2:root.testqueue",
		Filter: configs.Filter{
			Type:  "deny",
			Users: []string{"testuser2"},
		},
	}
	var mr rule
	mr, err = newRule(conf)
	if err != nil || mr == nil {
		t.Fatalf("mapping rule creation failed")
	}

	var queue string

	// deny user
	emptyTags := make(map[string]string)
	appInfo1 := cache.NewApplicationInfo("app1", "default", "ignored", user2, emptyTags)
	queue, err = mr.placeApplication(appInfo1, partInfo)
	if queue != "" || err != nil {
		t.Errorf("mapping rule should deny user, but instead got queue: '%s', err: %v", queue, err)
	}

	// no placement without tags
	appInfo2 := cache.NewApplicationInfo("app2", "default", "ignored", user1, emptyTags)
	queue, err = mr.placeApplication(appInfo2, partInfo)
	if queue != "" || err != nil {
		t.Errorf("mapping rule should not place app without tags, but instead got queue: '%s', err: %v", queue, err)
	}

	// has the tag but not the value
	wrongTag := map[string]string{
		"tag": "wrongvalue",
	}
	appInfo3 := cache.NewApplicationInfo("app3", "default", "ignored", user1, wrongTag)
	queue, err = mr.placeApplication(appInfo3, partInfo)
	if queue != "" || err != nil {
		t.Errorf("mapping rule should not place app without matching tags, but instead got queue: '%s', err: %v", queue, err)
	}

	// has matching tag
	matchingTag := map[string]string{
		"tag": "value1",
	}
	appInfo4 := cache.NewApplicationInfo("app4", "default", "ignored", user1, matchingTag)
	queue, err = mr.placeApplication(appInfo4, partInfo)
	if queue != "root.testqueue" || err != nil {
		t.Errorf("mapping rule failed to place app to correct queue '%s', err %v", queue, err)
	}

	// non-existing queue without create and parent
	conf = configs.PlacementRule{
		Name:   "mapping",
		Value:  "tag3:value4:testqueue2",
		Create: true,
		Parent: &configs.PlacementRule{
			Name:  "fixed",
			Value: "root.testparent2",
		},
	}
	mr, err = newRule(conf)
	if err != nil || mr == nil {
		t.Fatalf("mapping rule creation failed")
	}
	matchingTag = map[string]string{
		"tag3": "value4",
	}
	appInfo5 := cache.NewApplicationInfo("app5", "default", "ignored", user1, matchingTag)
	queue, err = mr.placeApplication(appInfo5, partInfo)
	if queue != "" {
		t.Errorf("mapping rule did not fail to place app to queue '%s', err %v", queue, err)
	}
}
