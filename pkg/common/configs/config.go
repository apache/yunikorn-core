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
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"go.uber.org/zap"
	"go.yaml.in/yaml/v3"

	"github.com/apache/yunikorn-core/pkg/log"
)

const (
	// checksumKey is the YAML key that holds the checksum in a serialised scheduler config.
	checksumKey = "checksum:"
	// partitionsKey is the first YAML key of a serialised scheduler config. It must always be present
	// and is used to locate the start of the config when the checksum is stored before it.
	partitionsKey = "partitions:"
	// checksumScanWindow is the number of bytes scanned at the start and the end of a serialised config
	// to locate the checksum line. Any checksum line would never exceed this size ("checksum: " plus a 64 character
	// hex string with an optional trailing newline and quotes).
	// Scanning only the head and the tail keeps the detection cheap on large configs which could be megabytes in size.
	checksumScanWindow = 80
)

// SchedulerConfig can contain multiple partitions. Each partition contains the queue definition for a logical
// set of scheduler resources.
type SchedulerConfig struct {
	Partitions []PartitionConfig
	Checksum   string `yaml:",omitempty" json:",omitempty"`
}

// PartitionConfig for each partition:
// - the name of the partition
// - a list of sub or child queues
// - a list of placement rule definition objects
// - a list of users specifying limits on the partition
// - the preemption configuration for the partition
// - user group resolver type (os, ldap, "")
type PartitionConfig struct {
	Name              string
	Queues            []QueueConfig
	PlacementRules    []PlacementRule           `yaml:",omitempty" json:",omitempty"`
	Limits            []Limit                   `yaml:",omitempty" json:",omitempty"`
	Preemption        PartitionPreemptionConfig `yaml:",omitempty" json:",omitempty"`
	NodeSortPolicy    NodeSortingPolicy         `yaml:",omitempty" json:",omitempty"`
	UserGroupResolver UserGroupResolver         `yaml:",omitempty" json:",omitempty"`
}

type UserGroupResolver struct {
	Type string `yaml:"type,omitempty" json:"type,omitempty"`
}

// PartitionPreemptionConfig defines global flags for both preemption types
type PartitionPreemptionConfig struct {
	Enabled                *bool `yaml:",omitempty" json:",omitempty"`
	QuotaPreemptionEnabled *bool `yaml:",omitempty" json:",omitempty"`
}

// QueueConfig object for each queue:
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

// ChildTemplate set on a parent queue with settings to be applied to the child created via a placement rule.
type ChildTemplate struct {
	MaxApplications uint64            `yaml:",omitempty" json:",omitempty"`
	Properties      map[string]string `yaml:",omitempty" json:",omitempty"`
	Resources       Resources         `yaml:",omitempty" json:",omitempty"`
}

// The Resources limit to apply on the queue. The definition allows for an unlimited number of types to be used.
// The mapping to "known" resources is not handled here.
// - guaranteed resources
// - max resources
type Resources struct {
	Guaranteed map[string]string `yaml:",omitempty" json:",omitempty"`
	Max        map[string]string `yaml:",omitempty" json:",omitempty"`
}

// The PlacementRule definition:
// - the name of the rule
// - create flag: can the rule create a queue
// - user and group filter to be applied on the callers
// - rule link to allow setting a rule to generate the parent
// - value a generic value interpreted depending on the rule type (i.e. queue name for the "fixed" rule
// or the application label name for the "tag" rule)
type PlacementRule struct {
	Name   string
	Create bool           `yaml:",omitempty" json:",omitempty"`
	Filter Filter         `yaml:",omitempty" json:",omitempty"`
	Parent *PlacementRule `yaml:",omitempty" json:",omitempty"`
	Value  string         `yaml:",omitempty" json:",omitempty"`
}

// Filter for users and groups for a PlacementRule.
// - type of filter (allow or deny filter, empty means allow)
// - list of users to filter (maybe empty)
// - list of groups to filter (maybe empty)
// if the list of users or groups is exactly 1 long it is interpreted as a regular expression
type Filter struct {
	Type   string
	Users  []string `yaml:",omitempty" json:",omitempty"`
	Groups []string `yaml:",omitempty" json:",omitempty"`
}

// Limits is a list of Limit objects to define user and group limits for a partition or queue.
type Limits struct {
	Limit []Limit
}

// A Limit object to specify user and or group limits at different levels in the partition or queues.
// Different limits for the same user or group may be defined at different levels in the hierarchy:
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

// NodeSortingPolicy to be applied globally.
// - type: different type of policies supported (binpacking, fair etc.)
// - resource weight: factor to be applied to comparisons of different resource types when sorting nodes. Types not
// mentioned have a weight of 1.0.
type NodeSortingPolicy struct {
	Type            string
	ResourceWeights map[string]float64 `yaml:",omitempty" json:",omitempty"`
}

func LoadSchedulerConfigFromByteArray(content []byte) (*SchedulerConfig, error) {
	conf, err := ParseAndValidateConfig(content)
	if err != nil {
		return nil, err
	}
	// Create a sha256 checksum for this validated config
	SetChecksum(content, conf)
	return conf, err
}

// SetChecksum calculates the sha256 checksum for the serialised config and stores it in the config.
// The config might already contain a checksum read from the YAML, it could be missing or set to an incorrect value.
// The correct checksum will always override it, the action taken is logged so that an incorrect checksum can be traced.
func SetChecksum(content []byte, conf *SchedulerConfig) {
	// nil safety
	if conf == nil {
		return
	}
	checksum := fmt.Sprintf("%X", sha256.Sum256([]byte(GetConfigurationString(content))))
	old := conf.Checksum
	conf.Checksum = checksum
	switch {
	case old == "":
		log.Log(log.Config).Debug("checksum not set in configuration, calculated and stored",
			zap.String("checksum", checksum))
	case old != checksum:
		log.Log(log.Config).Warn("checksum in configuration incorrect, overriding with calculated value",
			zap.String("oldChecksum", old), zap.String("newChecksum", checksum))
	}
}

func ParseAndValidateConfig(content []byte) (*SchedulerConfig, error) {
	conf := &SchedulerConfig{}
	decoder := yaml.NewDecoder(bytes.NewReader(content))
	decoder.KnownFields(true) // Enable strict unmarshaling behavior
	err := decoder.Decode(conf)
	if err != nil && !errors.Is(err, io.EOF) { // empty content may have EOF error, skip it
		log.Log(log.Config).Error("failed to parse queue configuration",
			zap.Error(err))
		return nil, err
	}
	// validate the config
	err = Validate(conf)
	if err != nil {
		log.Log(log.Config).Error("queue configuration validation failed",
			zap.Error(err))
		return nil, err
	}
	return conf, nil
}

// GetConfigurationString returns the serialised config content without checksum.
// The checksum is placed at the start or the end of the config and to avoid walking a potentially very large
// config end to end, only the first and the last checksumScanWindow bytes are scanned for the checksum key.
func GetConfigurationString(requestBytes []byte) string {
	length := len(requestBytes)
	if length == 0 {
		return ""
	}
	key := []byte(checksumKey)
	// look for the checksum in the head of the config first, then in the tail
	head := min(checksumScanWindow, length)
	checksumIdx := bytes.Index(requestBytes[:head], key)
	if checksumIdx == -1 {
		tail := max(length-checksumScanWindow, 0)
		if idx := bytes.Index(requestBytes[tail:], key); idx != -1 {
			checksumIdx = tail + idx
		}
	}
	// no checksum found: the whole content is used to calculate the checksum
	if checksumIdx == -1 {
		return string(requestBytes)
	}
	// a checksum is present: use the partitions key to decide whether it sits before or after the config
	partitionsIdx := bytes.Index(requestBytes, []byte(partitionsKey))
	if partitionsIdx == -1 {
		// no partitions in the config: nothing to calculate a checksum over
		return ""
	}
	if checksumIdx < partitionsIdx {
		// checksum stored before the config: the config runs from the partitions key to the end
		return string(requestBytes[partitionsIdx:])
	}
	// checksum stored after the config: the config runs up to the checksum line
	return string(requestBytes[:checksumIdx])
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
