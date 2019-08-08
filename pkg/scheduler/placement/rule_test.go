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
    "testing"
)

func TestNewRule(t *testing.T) {
    conf := configs.PlacementRule{
        Name: "bogus",
    }
    rule, err := newRule(conf)
    if err == nil || rule != nil {
        t.Errorf("new rule create did not fail with bogus rule name, err 'nil' , rule: %v, ", rule)
    }

    conf = configs.PlacementRule{
        Name: "test",
    }
    rule, err = newRule(conf)
    if err != nil || rule == nil {
        t.Errorf("new rule build failed which should not, rule 'nil' , err: %v, ", err)
    }

    // test normalise rule name
    conf = configs.PlacementRule{
        Name: "TeSt",
    }
    rule, err = newRule(conf)
    if err != nil || rule == nil {
        t.Errorf("new normalised rule build failed which should not, rule 'nil' , err: %v, ", err)
    }
}

// Test for a basic test rule.
func TestPlaceApp(t *testing.T) {
    conf := configs.PlacementRule{
        Name: "test",
    }
    rule, err := newRule(conf)
    if err != nil {
        t.Fatal("unexpected rule initialisation error")
    }
    // place application that should fail
    _, err = rule.placeApplication(nil, nil)
    if err == nil {
        t.Error("test rule place application did not fail as expected")
    }
    var queue string
    // place application that should not fail and return "test"
    queue, err = rule.placeApplication(&cache.ApplicationInfo{}, nil)
    if err != nil || queue != "test" {
        t.Errorf("test rule place application did not fail, err: %v, ", err)
    }
    // place application that should not fail and return the queue in the object
    queue, err = rule.placeApplication(&cache.ApplicationInfo{QueueName: "passedin"}, nil)
    if err != nil || queue != "passedin" {
        t.Errorf("test rule place application did not fail, err: %v, ", err)
    }
    // place application that should not fail and return the queue in the object
    queue, err = rule.placeApplication(&cache.ApplicationInfo{QueueName: "user.name"}, nil)
    if err != nil || queue != "user_dot_name" {
        t.Errorf("test rule place application did not fail, err: %v, ", err)
    }
}

func TestReplacDot(t *testing.T) {
    name := replaceDot("name.name")
    if name != "name_dot_name" {
        t.Errorf("replace dot failed, name: %s, ", name)
    }
    name = replaceDot("name...name")
    if name != "name_dot__dot__dot_name" {
        t.Errorf("replace consequtive dots failed, name: %s, ", name)
    }
    name = replaceDot("name.name.name")
    if name != "name_dot_name_dot_name" {
        t.Errorf("replace multi dots failed, name: %s, ", name)
    }
    name = replaceDot(".name.")
    if name != "_dot_name_dot_" {
        t.Errorf("replace start or end dots failed, name: %s, ", name)
    }
}
