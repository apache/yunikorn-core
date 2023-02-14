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

package template

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
)

func getResourceConf() map[string]string {
	resource := make(map[string]string)
	resource["memory"] = strconv.Itoa(time.Now().Second()%1000 + 10)
	resource["vcore"] = strconv.Itoa(time.Now().Second()%1000 + 10)
	return resource
}

func getProperties() map[string]string {
	properties := make(map[string]string)
	properties[strconv.Itoa(time.Now().Second()%1000)] = strconv.Itoa(time.Now().Second() % 1000)
	return properties
}

func getResource(t *testing.T) *resources.Resource {
	r, err := resources.NewResourceFromConf(getResourceConf())
	assert.NilError(t, err, "failed to parse resource: %v", err)
	return r
}

func checkMembers(t *testing.T, template *Template, properties map[string]string, maxResource *resources.Resource, guaranteedResource *resources.Resource) {
	// test inner members
	assert.Assert(t, reflect.DeepEqual(template.properties, properties))
	assert.Assert(t, reflect.DeepEqual(template.maxResource, maxResource))
	assert.Assert(t, reflect.DeepEqual(template.guaranteedResource, guaranteedResource))

	// test all getters
	assert.Assert(t, reflect.DeepEqual(template.GetProperties(), properties))
	assert.Assert(t, reflect.DeepEqual(template.GetMaxResource(), maxResource))
	assert.Assert(t, reflect.DeepEqual(template.GetGuaranteedResource(), guaranteedResource))
}

func TestNewTemplate(t *testing.T) {
	properties := getProperties()
	guaranteedResource := getResource(t)
	maxResource := getResource(t)

	checkMembers(t, newTemplate(properties, maxResource, guaranteedResource), properties, maxResource, guaranteedResource)
}

func TestFromConf(t *testing.T) {
	properties := getProperties()
	guaranteedResourceConf := getResourceConf()
	maxResourceConf := getResourceConf()

	// case 0: normal case
	template, err := FromConf(&configs.ChildTemplate{
		Properties: properties,
		Resources: configs.Resources{
			Max:        maxResourceConf,
			Guaranteed: guaranteedResourceConf,
		},
	})
	assert.NilError(t, err, "failed to create template: %v", err)

	maxResource, err := resources.NewResourceFromConf(maxResourceConf)
	assert.NilError(t, err, "failed to parse resource: %v", err)
	guaranteedResource, err := resources.NewResourceFromConf(guaranteedResourceConf)
	assert.NilError(t, err, "failed to parse resource: %v", err)
	checkMembers(t, template, properties, maxResource, guaranteedResource)

	// case 1: empty map produces nil template
	empty0, err := FromConf(&configs.ChildTemplate{
		Properties: make(map[string]string),
		Resources: configs.Resources{
			Max:        make(map[string]string),
			Guaranteed: make(map[string]string),
		},
	})
	assert.NilError(t, err)
	assert.Assert(t, empty0 == nil)

	empty1, err := FromConf(nil)
	assert.NilError(t, err)
	assert.Assert(t, empty1 == nil)

	// case 2: empty key-value produces nil template
	emptyProps := make(map[string]string)
	emptyProps[""] = ""
	empty2, err := FromConf(&configs.ChildTemplate{
		Properties: emptyProps,
		Resources: configs.Resources{
			Max:        emptyProps,
			Guaranteed: emptyProps,
		},
	})
	assert.NilError(t, err)
	assert.Assert(t, empty2 == nil)

	// case 3: one item can produce template
	props := make(map[string]string)
	props["k"] = "v"
	validTemplate, err := FromConf(&configs.ChildTemplate{
		Properties: make(map[string]string),
		Resources: configs.Resources{
			Max:        getResourceConf(),
			Guaranteed: make(map[string]string),
		},
	})
	assert.NilError(t, err)
	assert.Assert(t, validTemplate != nil)
}
