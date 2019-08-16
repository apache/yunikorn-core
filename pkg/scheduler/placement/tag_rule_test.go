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

func TestTagRule(t *testing.T) {
    conf := configs.PlacementRule{
        Name: "tag",
    }
    rule, err := newRule(conf)
    if err == nil || rule != nil {
        t.Errorf("tag rule create did not fail without tag name, err 'nil' , rule: %v, ", rule)
    }
    conf = configs.PlacementRule{
        Name: "tag",
        Value: "label1",
    }
    rule, err = newRule(conf)
    if err != nil || rule == nil {
        t.Errorf("tag rule create failed with tag name, err %v", err)
    }
    // trying to create using a parent with a fully qualified child
    conf = configs.PlacementRule{
        Name: "tag",
        Value: "label1",
        Parent: &configs.PlacementRule{
            Name: "tag",
            Value: "label2",
        },
    }
    rule, err = newRule(conf)
    if err != nil || rule == nil {
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
    partInfo, err := CreatePartitionInfo([]byte(data))
    user := security.UserGroup{
        User: "testuser",
        Groups: []string{},
    }
    conf := configs.PlacementRule{
        Name: "tag",
        Value: "label1",
    }
    rule, err := newRule(conf)
    if err != nil || rule == nil {
        t.Errorf("tag rule create failed with queue name, err %v", err)
    }

    // tag does not have a value
    tags := make(map[string]string, 0)
    appInfo := cache.NewApplicationInfo("app1", "default", "ignored", user, tags)
    queue, err := rule.placeApplication(appInfo, partInfo)
    if queue != "" || err != nil {
        t.Errorf("tag rule failed with no tag value '%s', err %v", queue, err)
    }

    // tag queue that exists directly in hierarchy
    tags = map[string]string{"label1":"testqueue"}
    appInfo = cache.NewApplicationInfo("app1", "default", "ignored", user, tags)
    queue, err = rule.placeApplication(appInfo, partInfo)
    if queue != "root.testqueue" || err != nil {
        t.Errorf("tag rule failed to place queue in correct queue '%s', err %v", queue, err)
    }

    // tag queue that does not exists
    tags = map[string]string{"label1":"unknown"}
    appInfo = cache.NewApplicationInfo("app1", "default", "ignored", user, tags)
    queue, err = rule.placeApplication(appInfo, partInfo)
    if queue != "" || err != nil {
        t.Errorf("tag rule placed in queue that does not exists '%s', err %v", queue, err)
    }

    // tag queue fully qualified
    tags = map[string]string{"label1":"root.testparent.testchild"}
    appInfo = cache.NewApplicationInfo("app1", "default", "ignored", user, tags)
    queue, err = rule.placeApplication(appInfo, partInfo)
    if queue != "root.testparent.testchild" || err != nil {
        t.Errorf("tag rule did fail with qualified queue '%s', error %v", queue, err)
    }

    // trying to place in a child using a parent
    conf = configs.PlacementRule{
        Name: "tag",
        Value: "label1",
        Parent: &configs.PlacementRule{
            Name: "tag",
            Value: "label2",
        },
    }
    rule, err = newRule(conf)
    if err != nil || rule == nil {
        t.Errorf("tag rule create failed with parent rule and qualified value, err %v", err)
    }
    tags = map[string]string{"label1":"testchild"}
    appInfo = cache.NewApplicationInfo("app1", "default", "ignored", user, tags)
    queue, err = rule.placeApplication(appInfo, partInfo)
    if queue != "" || err != nil {
        t.Errorf("tag rule with parent queue should have failed value not set '%s', error %v", queue, err)
    }
    tags = map[string]string{"label1":"testchild", "label2":"testparent"}
    appInfo = cache.NewApplicationInfo("app1", "default", "ignored", user, tags)
    queue, err = rule.placeApplication(appInfo, partInfo)
    if queue != "root.testparent.testchild" || err != nil {
        t.Errorf("tag rule with parent queue incorrect queue '%s', error %v", queue, err)
    }
}