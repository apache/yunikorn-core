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

package configs

import (
    "fmt"
    "github.com/cloudera/yunikorn-core/pkg/common"
    "github.com/cloudera/yunikorn-core/pkg/common/security"
    "github.com/cloudera/yunikorn-core/pkg/log"
    "go.uber.org/zap"
    "regexp"
    "strconv"
    "strings"
)

const (
    RootQueue        = "root"
    DefaultPartition = "default"
)

// A queue can be a username with the dot replaced. Most systems allow a 32 character user name.
// The queue name must thus allow for at least that length with the replacement of dots.
var QueueNameRegExp = regexp.MustCompile("^[a-zA-Z0-9_-]{1,64}$")

// User and group name check: systems allow different things POSIX is the base but we need to be lenient and allow more.
// allow upper and lower case, add the @ and . (dot) and officially no length.
var UserRegExp = regexp.MustCompile("^[_a-zA-Z][a-zA-Z0-9_.@-]*[$]?$")

// Groups should have a slightly more restrictive regexp (no @ . or $ at the end)
var GroupRegExp = regexp.MustCompile("^[_a-zA-Z][a-zA-Z0-9_-]*$")

// all characters that make a name different from a regexp
var SpecialRegExp = regexp.MustCompile("[\\^\\$\\*\\+\\?\\(\\)\\[\\{\\}\\|]")

// The rule maps to a go identifier check that regexp only
var RuleNameRegExp = regexp.MustCompile("^[_a-zA-Z][a-zA-Z0-9_]*$")

// Check the ACL
func checkACL(acl string) error {
    // trim any white space
    acl = strings.TrimSpace(acl)
    // handle special cases: deny and wildcard
    if len(acl) == 0 || acl == security.WildCard {
        return nil
    }

    // should have no more than two groups defined
    fields := strings.Fields(acl)
    if len(fields) > 2 {
        return fmt.Errorf("multiple spaces found in ACL: '%s'", acl)
    }
    return nil
}

// Temporary convenience method: should use resource package to do this
// currently no check for the type of resource as long as the value is OK all is OK
func checkResource(res map[string]string) error {
    for _, val := range res {
        _, err := strconv.ParseInt(val, 10, 64)
        if err != nil {
            return fmt.Errorf("resource parsing failed: %v", err)
        }
    }
    return nil
}

// Check the resource configuration
func checkResources(resource Resources) error {
    // check guaranteed resources
    if resource.Guaranteed != nil && len(resource.Guaranteed) != 0 {
        err := checkResource(resource.Guaranteed)
        if err != nil {
            return err
        }
    }
    // check max resources
    if resource.Max != nil && len(resource.Max) != 0 {
        err := checkResource(resource.Max)
        if err != nil {
            return err
        }
    }
    return nil
}

// Check the placement rules for correctness
func checkPlacementRules(partition *PartitionConfig) error {
    // return if nothing defined
    if partition.PlacementRules == nil || len(partition.PlacementRules) == 0 {
        return nil
    }

    log.Logger().Debug("checking placement rule config",
        zap.String("partitionName", partition.Name))
    // top level rule checks, parents are called recursively
    for _, rule := range partition.PlacementRules {
        if err := checkPlacementRule(rule); err != nil {
            return err
        }
    }
    return nil
}

// Check the specific rule for syntax.
// The create flag is checked automatically by the config parser and is not checked.
func checkPlacementRule(rule PlacementRule) error {
    // name must be valid go as it normally maps 1:1 to an object
    if !RuleNameRegExp.MatchString(rule.Name) {
        return fmt.Errorf("invalid rule name %s, a name must be a valid identifier", rule.Name)
    }
    // check the parent rule
    if rule.Parent != nil {
        if err := checkPlacementRule(*rule.Parent); err != nil {
            log.Logger().Debug("parent placement rule failed",
                zap.String("rule", rule.Name),
                zap.String("parentRule", rule.Parent.Name))
            return err
        }
    }
    // check filter if given
    if err := checkPlacementFilter(rule.Filter); err != nil {
        log.Logger().Debug("placement rule filter failed",
            zap.String("rule", rule.Name),
            zap.Any("filter", rule.Filter))
        return err
    }
    return nil
}

// Check the filter for syntax issues
// Trickery for the regexp part to make sure we filter out just a name and do not see it as a regexp.
// If the list is 1 item check if it is a valid user, then compile as a regexp and check for regexp characters.
// Each check must pass.
// If the list is more than one item we see all as a name and ignore anything that does not comply when initialising the filter.
func checkPlacementFilter(filter Filter) error {
    // empty (equals allow) or the literal "allow" or "deny" (case insensitive)
    if filter.Type != "" && !strings.EqualFold(filter.Type, "allow") && !strings.EqualFold(filter.Type, "deny") {
        return fmt.Errorf("invalid rule filter type %s, filter type  must be either '', allow or deny", filter.Type)
    }
    // check users and groups: as long as we have 1 good entry we accept it and continue
    // anything that does not parse in a list of users is ignored (like ACL list)
    if len(filter.Users) == 1 {
        // for a length of 1 we could either have regexp or username
        isUser := UserRegExp.MatchString(filter.Users[0])
        // if it is not a user name it must be a regexp
        // two step check: first compile if that fails it is
        if !isUser {
            if _, err := regexp.Compile(filter.Users[0]); err != nil || !SpecialRegExp.MatchString(filter.Users[0]) {
                return fmt.Errorf("invalid rule filter user list is not a proper list or regexp: %v", filter.Users)
            }
        }
    }
    if len(filter.Groups) == 1 {
        // for a length of 1 we could either have regexp or groupname
        isGroup := GroupRegExp.MatchString(filter.Groups[0])
        // if it is not a group name it must be a regexp
        if !isGroup {
            if _, err := regexp.Compile(filter.Groups[0]); err != nil || !SpecialRegExp.MatchString(filter.Groups[0]) {
                return fmt.Errorf("invalid rule filter group list is not a proper list or regexp: %v", filter.Groups)
            }
        }
    }
    return nil
}

// Check the defined users
func checkUserDefinition(partition *PartitionConfig) error {
    // return if nothing defined
    if partition.Users == nil || len(partition.Users) == 0 {
        return nil
    }

    log.Logger().Debug("checking partition user config",
        zap.String("partitionName", partition.Name))
    // TODO check users
    return nil
}

// Check for global policy
func checkNodeSortingPolicy(partition *PartitionConfig) error {

    // get the policy
    policy := partition.NodeSortPolicy

    // Defined polices.
    configuredNodeSortingPolicy, error := common.FromString(policy.Type)

    log.Logger().Info("Node sorting policy:", zap.Any("policy name", policy.Type), zap.Any("value", configuredNodeSortingPolicy))
    return error
}

// Check the queue names configured for compliance and uniqueness
// - no duplicate names at each branched level in the tree
// - queue name is alphanumeric (case ignore) with - and _
// - queue name is maximum 16 char long
func checkQueues(queue *QueueConfig, level int) error {
    // check the resource (if defined)
    err := checkResources(queue.Resources)
    if err != nil {
        return err
    }

    // check the ACLs (if defined)
    err = checkACL(queue.AdminACL)
    if err != nil {
        return err
    }
    err = checkACL(queue.SubmitACL)
    if err != nil {
        return err
    }

    // check this level for name compliance and uniqueness
    queueMap := make(map[string]bool)
    for _, queue := range queue.Queues {
        if !QueueNameRegExp.MatchString(queue.Name) {
            return fmt.Errorf("invalid queue name %s, a name must only have alphanumeric characters,"+
                " - or _, and be no longer than 64 characters", queue.Name)
        }
        if queueMap[strings.ToLower(queue.Name)] {
            return fmt.Errorf("duplicate queue name found with name %s, level %d", queue.Name, level)
        } else {
            queueMap[strings.ToLower(queue.Name)] = true
        }
    }

    // recurse into the depth if this level passed
    for _, queue := range queue.Queues {
        err := checkQueues(&queue, level+1)
        if err != nil {
            return err
        }
    }
    return nil
}

// Check the structure of the queue in the config:
// - exactly 1 root queue, added if missing
// - the parent flag is set on queues that are missing it
// - no duplicates at each level
// - name must comply with regexp
func checkQueuesStructure(partition *PartitionConfig) error {
    if partition.Queues == nil {
        return fmt.Errorf("queue config is not set")
    }

    log.Logger().Debug("checking partition queue config",
        zap.String("partitionName", partition.Name))

    // handle no root queue cases
    var insertRoot bool
    if len(partition.Queues) != 1 {
        // multiple or no top level queues: insert the root queue
        insertRoot = true
    } else {
        // A single queue at the top must be the root queue, if not insert it
        if strings.ToLower(partition.Queues[0].Name) != RootQueue {
            insertRoot = true
        } else {
            // make sure root is a parent
            partition.Queues[0].Parent = true
        }
    }

    // insert the root queue if not there
    if insertRoot {
        log.Logger().Debug("inserting root queue",
            zap.Int("numOfQueues", len(partition.Queues)))
        var rootQueue QueueConfig
        rootQueue.Name = RootQueue
        rootQueue.Parent = true
        rootQueue.Queues = partition.Queues
        var newRoot []QueueConfig
        newRoot = append(newRoot, rootQueue)
        partition.Queues = newRoot
    }

    // check name uniqueness: we have a root to start with directly
    var rootQueue = partition.Queues[0]
    // special check for root resources: must not be set
    if rootQueue.Resources.Guaranteed != nil || rootQueue.Resources.Max != nil {
        return fmt.Errorf("root queue must not have resource limits set")
    }
    return checkQueues(&rootQueue, 1)
}

// Check the partition configuration. Any parsing issues will return an error which means that the
// configuration is invalid. This *must* be called before the configuration is activated. Any
// configuration that does not pass must be rejected.
// Check performed:
// - at least 1 partition must be defined
// - no more than 1 partition called "default"
// For the sub components:
// - The queue config is syntax checked
// - The placement rules are syntax checked
// - The user objects are syntax checked
func Validate(newConfig *SchedulerConfig) error {
    if newConfig == nil {
        return fmt.Errorf("scheduler config is not set")
    }

    // check for the default partition, if the partion is unnamed set it to default
    var defaultPartition bool
    for i, partition := range newConfig.Partitions {
        if partition.Name == "" || strings.ToLower(partition.Name) == DefaultPartition {
            if defaultPartition {
                return fmt.Errorf("multiple default partitions defined")
            }
            defaultPartition = true
            partition.Name = DefaultPartition
        }
        // check the queue structure
        log.Logger().Debug("checking partition",
            zap.String("partitionName", partition.Name))
        err := checkQueuesStructure(&partition)
        if err != nil {
            return err
        }
        err = checkPlacementRules(&partition)
        if err != nil {
            return err
        }
        err = checkUserDefinition(&partition)
        if err != nil {
            return err
        }
        err = checkNodeSortingPolicy(&partition)
        if err != nil {
            return err
        }
        // write back the partition to keep changes
        newConfig.Partitions[i] = partition
    }
    return nil
}
