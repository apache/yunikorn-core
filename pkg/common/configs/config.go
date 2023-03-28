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

package configs

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"github.com/apache/yunikorn-core/pkg/log"
)

// The configuration can contain multiple partitions. Each partition contains the queue definition for a logical
// set of scheduler resources.
type SchedulerConfig struct {
	Partitions []PartitionConfig
	Checksum   string `yaml:",omitempty" json:",omitempty"`
}

// The partition object for each partition:
// - the name of the partition
// - a list of sub or child queues
// - a list of placement rule definition objects
// - a list of users specifying limits on the partition
// - the preemption configuration for the partition
type PartitionConfig struct {
	Name              string
	Queues            []QueueConfig
	PlacementRules    []PlacementRule           `yaml:",omitempty" json:",omitempty"`
	Limits            []Limit                   `yaml:",omitempty" json:",omitempty"`
	Preemption        PartitionPreemptionConfig `yaml:",omitempty" json:",omitempty"` // deprecated
	NodeSortPolicy    NodeSortingPolicy         `yaml:",omitempty" json:",omitempty"`
	StateDumpFilePath string                    `yaml:",omitempty" json:",omitempty"`
}

// deprecated
type PartitionPreemptionConfig struct {
	Enabled bool
}

// The queue object for each queue:
// - the name of the queue
// - a resources object to specify resource limits on the queue
// - the maximum number of applications that can run in the queue
// - a set of properties, exact definition of what can be set is not part of the yaml
// - ACL for submit and or admin access
// - a list of sub or child queues
// - a list of users specifying limits on a queue
type QueueConfig struct {
	Name            string
	Parent          bool              `yaml:",omitempty" json:",omitempty"`
	Resources       Resources         `yaml:",omitempty" json:",omitempty"`
	MaxApplications uint64            `yaml:",omitempty" json:",omitempty"`
	Properties      map[string]string `yaml:",omitempty" json:",omitempty"`
	AdminACL        string            `yaml:",omitempty" json:",omitempty"`
	SubmitACL       string            `yaml:",omitempty" json:",omitempty"`
	ChildTemplate   ChildTemplate     `yaml:",omitempty" json:",omitempty"`
	Queues          []QueueConfig     `yaml:",omitempty" json:",omitempty"`
	Limits          []Limit           `yaml:",omitempty" json:",omitempty"`
}

type ChildTemplate struct {
	MaxApplications uint64            `yaml:",omitempty" json:",omitempty"`
	Properties      map[string]string `yaml:",omitempty" json:",omitempty"`
	Resources       Resources         `yaml:",omitempty" json:",omitempty"`
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
// - list of users to filter (maybe empty)
// - list of groups to filter (maybe empty)
// if the list of users or groups is exactly 1 long it is interpreted as a regular expression
type Filter struct {
	Type   string
	Users  []string `yaml:",omitempty" json:",omitempty"`
	Groups []string `yaml:",omitempty" json:",omitempty"`
}

// A list of limit objects to define limits for a partition or queue
type Limits struct {
	Limit []Limit
}

// The limit object to specify user and or group limits at different levels in the partition or queues
// Different limits for the same user or group may be defined at different levels in the hierarchy
// - limit description (optional)
// - list of users (maybe empty)
// - list of groups (maybe empty)
// - maximum resources as a resource object to allow for the user or group
// - maximum number of applications the user or group can have running
type Limit struct {
	Limit           string
	Users           []string          `yaml:",omitempty" json:",omitempty"`
	Groups          []string          `yaml:",omitempty" json:",omitempty"`
	MaxResources    map[string]string `yaml:",omitempty" json:",omitempty"`
	MaxApplications uint64            `yaml:",omitempty" json:",omitempty"`
}

// Global Node Sorting Policy section
// - type: different type of policies supported (binpacking, fair etc)
type NodeSortingPolicy struct {
	Type            string
	ResourceWeights map[string]float64 `yaml:",omitempty" json:",omitempty"`
}

// Visible by tests
func LoadSchedulerConfigFromByteArray(content []byte) (*SchedulerConfig, error) {
	conf, err := ParseAndValidateConfig(content)
	if err != nil {
		return nil, err
	}
	// Create a sha256 checksum for this validated config
	SetChecksum(content, conf)
	return conf, err
}

func SetChecksum(content []byte, conf *SchedulerConfig) {
	noChecksumContent := GetConfigurationString(content)
	conf.Checksum = fmt.Sprintf("%X", sha256.Sum256([]byte(noChecksumContent)))
}

func ParseAndValidateConfig(content []byte) (*SchedulerConfig, error) {
	conf := &SchedulerConfig{}
	err := yaml.UnmarshalStrict(content, conf)
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
	return conf, nil
}

func GetConfigurationString(requestBytes []byte) string {
	conf := string(requestBytes)
	checksum := "checksum: "
	checksumLength := 64 + len(checksum)
	if strings.Contains(conf, checksum) {
		checksum += strings.Split(conf, checksum)[1]
		checksum = strings.TrimRight(checksum, "\n")
		if len(checksum) > checksumLength {
			checksum = checksum[:checksumLength]
		}
	}
	return strings.ReplaceAll(conf, checksum, "")
}

// DefaultSchedulerConfig contains the default scheduler configuration; used if no other is provided
var DefaultSchedulerConfig = `
partitions:
  - name: default
    placementrules:
      - name: tag
        value: namespace
        create: true
    queues:
      - name: root
        submitacl: '*'
`
