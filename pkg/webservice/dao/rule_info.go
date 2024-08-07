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

package dao

type RuleDAOInfo struct {
	Partition string     `json:"partition"` // no omitempty, partition name should not be empty
	Rules     []*RuleDAO `json:"rules,omitempty"`
}

type FilterDAO struct {
	Type      string   `json:"type"` // no omitempty, type must exist
	UserList  []string `json:"userList,omitempty"`
	GroupList []string `json:"groupList,omitempty"`
	UserExp   string   `json:"userExp,omitempty"`
	GroupExp  string   `json:"groupExp,omitempty"`
}

type RuleDAO struct {
	Name       string            `json:"name"` // no omitempty, name must exist
	Parameters map[string]string `json:"parameters,omitempty"`
	Filter     *FilterDAO        `json:"filter,omitempty"`
	ParentRule *RuleDAO          `json:"parentRule,omitempty"`
}
