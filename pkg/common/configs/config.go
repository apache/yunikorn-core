/*
Copyright 2019 The Unity Scheduler Authors

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
    "github.com/golang/glog"
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
    PlacementRules []PlacementRule `yaml:",omitempty" json:",omitempty"`
    Users          []User          `yaml:",omitempty" json:",omitempty"`}

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
type PlacementRule struct {
    Name   string
    Create bool           `yaml:",omitempty" json:",omitempty"`
    Filter Filter         `yaml:",omitempty" json:",omitempty"`
    Parent *PlacementRule `yaml:",omitempty" json:",omitempty"`
}

// The user and group filter for a rule.
// - type of filter (allow or deny filter)
// - comma separated list of users to filter
// - comma separated list of groups to filter
type Filter struct {
    Type   string
    Users  []string `yaml:",omitempty" json:",omitempty"`
    Groups []string `yaml:",omitempty" json:",omitempty"`
}

type User struct {
    Name            string
    MaxResources    map[string]string `yaml:",omitempty" json:",omitempty"`
    MaxApplications uint64            `yaml:",omitempty" json:",omitempty"`
}

type LoadSchedulerConfigFunc func(policyGroup string) (*SchedulerConfig, error)

// Visible by tests
func LoadSchedulerConfigFromByteArray(content []byte) (*SchedulerConfig, error) {
    conf := &SchedulerConfig{}
    err := yaml.Unmarshal(content, conf)
    if err != nil {
        glog.V(2).Infof("Queue configuration parsing failed, error: %s", err)
        return nil, err
    }
    glog.V(0).Info("Queue configuration parsing finished")
    // validate the config
    err = Validate(conf)
    if err != nil {
        glog.V(0).Infof("Queue configuration validation failed: %s", err)
        return nil, err
    }

    h := sha256.New()
    conf.Checksum = h.Sum(content)
    return conf, err
}

func loadSchedulerConfigFromFile(policyGroup string) (*SchedulerConfig, error) {
    filePath := resolveConfigurationFileFunc(policyGroup)
    glog.Infof("loading configuration from path %s", filePath)
    buf, err := ioutil.ReadFile(filePath)
    if err != nil {
        glog.V(2).Infof("Queue configuration file failed to load: %s", err)
        return nil, err
    }
    glog.V(0).Info("Queue configuration loaded from file")

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
