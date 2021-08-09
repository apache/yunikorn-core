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
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
)

type Template struct {
	properties         map[string]string
	maxResource        *resources.Resource
	guaranteedResource *resources.Resource
}

func FromConf(template *configs.ChildTemplate) (*Template, error) {
	maxResource, err := resources.NewResourceFromConf(template.Resources.Max)
	if err != nil {
		return nil, err
	}

	guaranteedResource, err := resources.NewResourceFromConf(template.Resources.Guaranteed)
	if err != nil {
		return nil, err
	}
	return NewTemplate(template.Properties, maxResource, guaranteedResource), nil
}

func NewTemplate(properties map[string]string, maxResource *resources.Resource, guaranteedResource *resources.Resource) *Template {
	template := &Template{
		properties:         make(map[string]string),
		maxResource:        maxResource.Clone(),
		guaranteedResource: guaranteedResource.Clone(),
	}
	for k, v := range properties {
		template.properties[k] = v
	}
	return template
}

// GetProperties returns a copy of properties. An empty map replaces the null value
func (t *Template) GetProperties() map[string]string {
	props := make(map[string]string)
	for k, v := range t.properties {
		props[k] = v
	}
	return props
}

// GetMaxResource returns a copy of max resource. it can be null
func (t *Template) GetMaxResource() *resources.Resource {
	if t == nil {
		return nil
	}
	return t.maxResource.Clone()
}

// GetGuaranteedResource returns a copy of guaranteed resource. it can be null
func (t *Template) GetGuaranteedResource() *resources.Resource {
	if t == nil {
		return nil
	}
	return t.guaranteedResource.Clone()
}

// GetTemplateInfo converts this to a TemplateInfo
func (t *Template) GetTemplateInfo() *dao.TemplateInfo {
	if t == nil {
		return nil
	}
	return &dao.TemplateInfo{
		Properties:         t.GetProperties(),
		MaxResource:        t.maxResource.DAOString(),
		GuaranteedResource: t.guaranteedResource.DAOString(),
	}
}
