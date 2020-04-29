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

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"gotest.tools/assert"
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
	partInfo, err := CreatePartitionInfo([]byte(data))
	assert.NilError(t, err, "Partition create failed with error")
	tags := make(map[string]string)
	user := security.UserGroup{
		User:   "testchild",
		Groups: []string{},
	}
	appInfo := cache.NewApplicationInfo("app1", "default", "ignored", user, tags)

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
	queue, err = ur.placeApplication(appInfo, partInfo)
	if queue != "root.testchild" || err != nil {
		t.Errorf("user rule failed to place queue in correct queue '%s', err %v", queue, err)
	}
	// trying to place in a parent queue should fail on queue create not in the rule
	user = security.UserGroup{
		User:   "testparent",
		Groups: []string{},
	}
	appInfo = cache.NewApplicationInfo("app1", "default", "ignored", user, tags)
	queue, err = ur.placeApplication(appInfo, partInfo)
	if queue != "root.testparent" || err != nil {
		t.Errorf("user rule failed with parent queue '%s', error %v", queue, err)
	}

	user = security.UserGroup{
		User:   "test.user",
		Groups: []string{},
	}
	appInfo = cache.NewApplicationInfo("app1", "default", "ignored", user, tags)
	queue, err = ur.placeApplication(appInfo, partInfo)
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
	appInfo = cache.NewApplicationInfo("app1", "default", "ignored", user, tags)
	ur, err = newRule(conf)
	if err != nil || ur == nil {
		t.Errorf("user rule create failed with queue name, err %v", err)
	}
	queue, err = ur.placeApplication(appInfo, partInfo)
	if queue != "root.testparent.testchild" || err != nil {
		t.Errorf("user rule failed to place queue in correct queue '%s', err %v", queue, err)
	}

	// user queue that does not exists
	user = security.UserGroup{
		User:   "unknown",
		Groups: []string{},
	}
	appInfo = cache.NewApplicationInfo("app1", "default", "ignored", user, tags)

	conf = configs.PlacementRule{
		Name:   "user",
		Create: true,
	}
	ur, err = newRule(conf)
	if err != nil || ur == nil {
		t.Errorf("user rule create failed with queue name, err %v", err)
	}
	queue, err = ur.placeApplication(appInfo, partInfo)
	if queue != "root.unknown" || err != nil {
		t.Errorf("user rule placed in to be created queue with create false '%s', err %v", queue, err)
	}
}
