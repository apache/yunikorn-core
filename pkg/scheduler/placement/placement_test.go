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

// basic test to check if no rules leave the manager unusable
func TestManagerNew(t *testing.T) {
    pi := cache.PartitionInfo{
        Name: "test",
        Root: &cache.QueueInfo{Name: "Root"},
        RMId: "test",
    }
    // basic info without rules, manager should not init
    man := NewPlacementManager(&pi)
    if man.initialised {
        t.Error("Placement manager marked initialised without rules")
    }
    if len(man.rules) != 0 {
        t.Error("Placement manager marked initialised without rules")
    }
}

func TestManagerInit(t *testing.T) {
    pi := cache.PartitionInfo{
        Name: "test",
        Root: &cache.QueueInfo{Name: "Root"},
        RMId: "test",
    }
    // basic info without rules, manager should not init no error
    man := NewPlacementManager(&pi)
    if man.initialised {
        t.Error("Placement manager marked initialised without rules")
    }
    // try to init with empty list must error
    rules := []configs.PlacementRule{}
    err := man.initialise(rules)
    if err == nil || man.initialised {
        t.Error("initialise without rules should have failed")
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
    pi := cache.PartitionInfo{
        Name: "test",
        Root: &cache.QueueInfo{Name: "Root"},
        RMId: "test",
    }
    // basic info without rules, manager should not init
    man := NewPlacementManager(&pi)
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
    pi := cache.PartitionInfo{
        Name: "test",
        Root: &cache.QueueInfo{Name: "Root"},
        RMId: "test",
    }
    // basic with 1 rule
    man := NewPlacementManager(&pi)
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
    } else {
        if ruleObjs[0].getName() != "user" || ruleObjs[1].getName() != "test" {
            t.Errorf("rule build order is not preserved: %v", ruleObjs)
        }
    }
}

func TestManagerPlaceApp(t *testing.T) {
}
