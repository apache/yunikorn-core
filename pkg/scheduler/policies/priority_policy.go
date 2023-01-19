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

package policies

import (
	"fmt"
	"strings"
)

type PriorityPolicy int

const (
	DefaultPriorityPolicy PriorityPolicy = iota // priority propagates upward
	FencePriorityPolicy                         // priority is not considered outside queue subtree
)

func (p PriorityPolicy) String() string {
	return [...]string{"default", "fence"}[p]
}

func PriorityPolicyFromString(str string) (PriorityPolicy, error) {
	switch strings.ToLower(str) {
	case DefaultPriorityPolicy.String(), "":
		return DefaultPriorityPolicy, nil
	case FencePriorityPolicy.String():
		return FencePriorityPolicy, nil
	default:
		return DefaultPriorityPolicy, fmt.Errorf("undefined priority.policy: %s", str)
	}
}
