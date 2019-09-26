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
    "crypto/sha256"
    "fmt"
    "github.com/cloudera/yunikorn-core/pkg/log"
    "go.uber.org/zap"
    "gopkg.in/yaml.v2"
    "io/ioutil"
    "os"
    "path"
)

// The configuration can contain multiple partitions. Each partition contains the queue definition for a logical
// set of scheduler resources.
type SchedulerConfig struct {
    Partitions []PartitionConfig
    Checksum   []byte
}

type PartitionConfig struct {
    Name           string
    Queues         []QueueConfig
    PlacementRules []PlacementRule           `yaml:",omitempty" json:",omitempty"`
    Users          []User                    `yaml:",omitempty" json:",omitempty"`
    Preemption     PartitionPreemptionConfig `yaml:",omitempty" json:",omitempty"`
    GlobalPolicies []GlobalPolicy            `yaml:",omitempty" json:",omitempty"`
}

type PartitionPreemptionConfig struct {
    Enabled bool
}

// The queue object for each queue:
// - the name of the queue
// - a resources object to specify resource limits on the queue
// - a set of properties, exact definition of what can be set is not part of the yaml
// - a list of sub or child queues
type QueueConfig struct {
    Name            string
    Parent          bool              `yaml:",omitempty" json:",omitempty"`
    Resources       Resources         `yaml:",omitempty" json:",omitempty"`
    Properties      map[string]string `yaml:",omitempty" json:",omitempty"`
    AdminACL        string            `yaml:",omitempty" json:",omitempty"`
    SubmitACL       string            `yaml:",omitempty" json:",omitempty"`
    MaxApplications uint64            `yaml:",omitempty" json:",omitempty"`
    Queues          []QueueConfig     `yaml:",omitempty" json:",omitempty"`
}

// The resource limits to set on the queue. The definition allows for an unlimited number of types to be used.
// The mapping to "known" resources is not handled here.
// - guaranteed resources
// - max resources
type Resources struct {
    Guaranteed map[string]string `yaml:",omitempty" json:",omitempty"`
    Max        map[string]string `yaml:",omitempty" json:",omitempty"`
}

// The queue placement rule definition
// - the name of the rule
// - create flag: can the rule create a queue
// - user and group filter to be applied on the callers
// - rule link to allow setting a rule to generate the parent
// - value a generic value interpreted depending on the rule type (i.e queue name for the "fixed" rule
// or the application label name for the "tag" rule)
type PlacementRule struct {
    Name   string
    Create bool           `yaml:",omitempty" json:",omitempty"`
    Filter Filter         `yaml:",omitempty" json:",omitempty"`
    Parent *PlacementRule `yaml:",omitempty" json:",omitempty"`
    Value  string         `yaml:",omitempty" json:",omitempty"`
}

// The user and group filter for a rule.
// - type of filter (allow or deny filter, empty means allow)
// - comma separated list of users to filter
// - comma separated list of groups to filter
// if the list of users or groups is exactly 1 long it is interpreted as a regular expression
type Filter struct {
    Type   string
    Users  []string `yaml:",omitempty" json:",omitempty"`
    Groups []string `yaml:",omitempty" json:",omitempty"`
}

// The user object to specify user limits at different levels in the partition or queues
// Different limits for the same user may be defined at different levels in the hierarchy
// - name of the user
// - maximum resources as a resource object to allow for the user
// - maximum number of applications the user can have running
type User struct {
    Name            string
    MaxResources    map[string]string `yaml:",omitempty" json:",omitempty"`
    MaxApplications uint64            `yaml:",omitempty" json:",omitempty"`
}

// Global Policy section
// - the name of the policy (user defined)
// - type: different type of policies supported (binpacking, fifo, random etc)
type GlobalPolicy struct {
    Name   string
    Policy string
    AutoRefresh bool      `yaml:",omitempty" json:",omitempty"`
}

type LoadSchedulerConfigFunc func(policyGroup string) (*SchedulerConfig, error)

// Visible by tests
func LoadSchedulerConfigFromByteArray(content []byte) (*SchedulerConfig, error) {
    conf := &SchedulerConfig{}
    err := yaml.Unmarshal(content, conf)
    if err != nil {
        log.Logger().Error("failed to parse queue configuration",
            zap.Error(err))
        return nil, err
    }
    // validate the config
    err = Validate(conf)
    if err != nil {
        log.Logger().Error("queue configuration validation failed",
            zap.Error(err))
        return nil, err
    }

    h := sha256.New()
    conf.Checksum = h.Sum(content)
    return conf, err
}

func loadSchedulerConfigFromFile(policyGroup string) (*SchedulerConfig, error) {
    filePath := resolveConfigurationFileFunc(policyGroup)
    log.Logger().Debug("loading configuration",
        zap.String("configurationPath", filePath))
    buf, err := ioutil.ReadFile(filePath)
    if err != nil {
        log.Logger().Error("failed to load configuration",
            zap.Error(err))
        return nil, err
    }

    return LoadSchedulerConfigFromByteArray(buf)
}

func resolveConfigurationFileFunc(policyGroup string) string {
    var filePath string
    if configDir, ok := ConfigMap[SchedulerConfigPath]; ok {
        // if scheduler config path is explicitly set, load conf from there
        filePath = path.Join(configDir, fmt.Sprintf("%s.yaml", policyGroup))
    } else {
        // if scheduler config path is not explicitly set
        // first try to load from default dir
        filePath = path.Join(DefaultSchedulerConfigPath, fmt.Sprintf("%s.yaml", policyGroup))
        if _, err := os.Stat(filePath); err != nil {
            // then try to load from current directory
            filePath = fmt.Sprintf("%s.yaml", policyGroup)
        }
    }
    return filePath
}

// Default loader, can be updated by tests
var SchedulerConfigLoader LoadSchedulerConfigFunc = loadSchedulerConfigFromFile
