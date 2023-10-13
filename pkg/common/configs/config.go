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
	"strings"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"github.com/apache/yunikorn-core/pkg/log"
)

const (
	stringDefaultValue = "defaultStringValue"
	uint64DefaultValue = uint64(18446744073709551615) // max in uint64
)

// The configuration can contain multiple partitions. Each partition contains the queue definition for a logical
// set of scheduler resources.
type SchedulerConfig struct {
	Partitions []PartitionConfig
	Checksum   string `yaml:",omitempty" json:",omitempty"`
}

type DefaultValues struct {
	defaultFields []string
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
	DefaultValues     `yaml:"-"`
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
	DefaultValues   `yaml:"-"`
}

type ChildTemplate struct {
	MaxApplications uint64            `yaml:",omitempty" json:",omitempty"`
	Properties      map[string]string `yaml:",omitempty" json:",omitempty"`
	Resources       Resources         `yaml:",omitempty" json:",omitempty"`
	DefaultValues   `yaml:"-"`
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
	Name          string
	Create        bool           `yaml:",omitempty" json:",omitempty"`
	Filter        Filter         `yaml:",omitempty" json:",omitempty"`
	Parent        *PlacementRule `yaml:",omitempty" json:",omitempty"`
	Value         string         `yaml:",omitempty" json:",omitempty"`
	DefaultValues `yaml:"-"`
}

// The user and group filter for a rule.
// - type of filter (allow or deny filter, empty means allow)
// - list of users to filter (maybe empty)
// - list of groups to filter (maybe empty)
// if the list of users or groups is exactly 1 long it is interpreted as a regular expression
type Filter struct {
	Type          string
	Users         []string `yaml:",omitempty" json:",omitempty"`
	Groups        []string `yaml:",omitempty" json:",omitempty"`
	DefaultValues `yaml:"-"`
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
	DefaultValues   `yaml:"-"`
}

// Global Node Sorting Policy section
// - type: different type of policies supported (binpacking, fair etc)
type NodeSortingPolicy struct {
	Type            string
	ResourceWeights map[string]float64 `yaml:",omitempty" json:",omitempty"`
	DefaultValues   `yaml:"-"`
}

// CheckAndSetDefault checks if the value is the predefined default value.
// If yes, add the lowercase field name to the defaultFields slice and return the default value of this variable type.
// If no, return the value itself
func (i *DefaultValues) CheckAndSetDefault(v interface{}, name string) interface{} {
	insertName := strings.ToLower(name)
	switch v.(type) {
	case string:
		if v == stringDefaultValue {
			i.defaultFields = append(i.defaultFields, insertName)
			return ""
		}
		return v
	case uint64:
		if v == uint64DefaultValue {
			i.defaultFields = append(i.defaultFields, insertName)
			return uint64(0)
		}
		return v
	default:
		return v
	}
}

// IsDefault checks whether the provided fieldName exists in the list of default fields.
// It performs a case-insensitive comparison of the fieldName parameter
// against the names in the defaultFields slice.
//
// This function is designed for use with field types that are either strings or uint64.
// Other types are not supported and could lead to wrong result.
//
// Parameters:
// - fieldName (string): The name of the field to check.
//
// Returns:
// - bool: True if the fieldName exists in the defaultFields, false otherwise.
func (i *DefaultValues) IsDefault(fieldName string) bool {
	searchedName := strings.ToLower(fieldName)
	for _, f := range i.defaultFields {
		if f == searchedName {
			return true
		}
	}
	return false
}

func (pf *PartitionConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	pf.Name = stringDefaultValue
	pf.StateDumpFilePath = stringDefaultValue

	type plain PartitionConfig
	if err := unmarshal((*plain)(pf)); err != nil {
		return err
	}

	pf.Name = pf.CheckAndSetDefault(pf.Name, "Name").(string)                                        //nolint:errcheck
	pf.StateDumpFilePath = pf.CheckAndSetDefault(pf.StateDumpFilePath, "StateDumpFilePath").(string) //nolint:errcheck

	return nil
}

func (qc *QueueConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	qc.Name = stringDefaultValue
	qc.MaxApplications = uint64DefaultValue
	qc.AdminACL = stringDefaultValue
	qc.SubmitACL = stringDefaultValue
	// Initial a ChildTemplate with all fields set as default. This is to avoid the user's config
	// is empty, causing the unmarshal process of ChildTemplate to skip and not identity which fields are default.
	qc.ChildTemplate = ChildTemplate{
		DefaultValues: DefaultValues{defaultFields: []string{"maxapplications"}},
	}

	type plain QueueConfig
	if err := unmarshal((*plain)(qc)); err != nil {
		return err
	}

	qc.Name = qc.CheckAndSetDefault(qc.Name, "Name").(string)                                  //nolint:errcheck
	qc.MaxApplications = qc.CheckAndSetDefault(qc.MaxApplications, "MaxApplications").(uint64) //nolint:errcheck
	qc.AdminACL = qc.CheckAndSetDefault(qc.AdminACL, "AdminACL").(string)                      //nolint:errcheck
	qc.SubmitACL = qc.CheckAndSetDefault(qc.SubmitACL, "SubmitACL").(string)                   //nolint:errcheck

	return nil
}

func (ct *ChildTemplate) UnmarshalYAML(unmarshal func(interface{}) error) error {
	ct.MaxApplications = uint64DefaultValue
	ct.defaultFields = make([]string, 0)

	type plain ChildTemplate
	if err := unmarshal((*plain)(ct)); err != nil {
		return err
	}

	ct.MaxApplications = ct.CheckAndSetDefault(ct.MaxApplications, "MaxApplications").(uint64) //nolint:errcheck

	return nil
}

func (pr *PlacementRule) UnmarshalYAML(unmarshal func(interface{}) error) error {
	pr.Value = stringDefaultValue
	// Initial a Filter with all fields set as default. This is to avoid the user's config
	// is empty, causing the unmarshal process of Filter to skip and not identity which fields are default.
	pr.Filter = Filter{
		DefaultValues: DefaultValues{defaultFields: []string{"type"}},
	}

	type plain PlacementRule

	if err := unmarshal((*plain)(pr)); err != nil {
		return err
	}

	pr.Value = pr.CheckAndSetDefault(pr.Value, "Value").(string) //nolint:errcheck

	return nil
}

func (f *Filter) UnmarshalYAML(unmarshal func(interface{}) error) error {
	f.Type = stringDefaultValue
	f.defaultFields = make([]string, 0)

	type plain Filter
	if err := unmarshal((*plain)(f)); err != nil {
		return err
	}

	f.Type = f.CheckAndSetDefault(f.Type, "Type").(string) //nolint:errcheck

	return nil
}

func (l *Limit) UnmarshalYAML(unmarshal func(interface{}) error) error {
	l.Limit = stringDefaultValue
	l.MaxApplications = uint64DefaultValue

	type plain Limit
	if err := unmarshal((*plain)(l)); err != nil {
		return err
	}

	l.Limit = l.CheckAndSetDefault(l.Limit, "Limit").(string)                               //nolint:errcheck
	l.MaxApplications = l.CheckAndSetDefault(l.MaxApplications, "MaxApplications").(uint64) //nolint:errcheck

	return nil
}

func (nsp *NodeSortingPolicy) UnmarshalYAML(unmarshal func(interface{}) error) error {
	nsp.Type = stringDefaultValue

	type plain NodeSortingPolicy
	if err := unmarshal((*plain)(nsp)); err != nil {
		return err
	}

	nsp.Type = nsp.CheckAndSetDefault(nsp.Type, "Type").(string) //nolint:errcheck

	return nil
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

func SetChecksum(content []byte, conf *SchedulerConfig) {
	noChecksumContent := GetConfigurationString(content)
	conf.Checksum = fmt.Sprintf("%X", sha256.Sum256([]byte(noChecksumContent)))
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
