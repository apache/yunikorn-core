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
    "github.com/golang/glog"
    "github.com/cloudera/yunikorn-core/pkg/common/security"
    "regexp"
    "strconv"
    "strings"
)

const (
    RootQueue = "root"
    DefaultPartition = "default"
)

var QueueNameRegExp = regexp.MustCompile("^[a-zA-Z0-9_-]{1,16}$")

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
    glog.V(0).Infof("Checking partition %s placement rule config", partition.Name)
    // TODO check rules
    return nil
}

// Check the defined users
func checkUserDefinition(partition *PartitionConfig) error {
    // return if nothing defined
    if partition.Users == nil || len(partition.Users) == 0 {
        return nil
    }
    glog.V(0).Infof("Checking partition %s users config", partition.Name)
    // TODO check users
    return nil
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
            return fmt.Errorf("invalid queue name %s, a name must only have alphanumeric characters," +
                " - or _, and be no longer than 16 characters", queue.Name)
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
    glog.V(0).Infof("Checking partition %s queue config", partition.Name)

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
        glog.V(0).Infof("Inserting root queue, found %d top level queues", len(partition.Queues))
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
        glog.V(0).Infof("Checking partition: %s", partition.Name)
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
        // write back the partition to keep changes
        newConfig.Partitions[i] = partition
    }
    return nil
}
