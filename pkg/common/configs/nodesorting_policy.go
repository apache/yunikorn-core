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
	"fmt"
)

// NodeSortingPolicy Global Node Sorting Policy section
// - type: different type of policies supported (binpacking, fair etc)
type NodeSortingPolicy struct {
	Type string
}

const (
	BinPackingPolicy string = "binpacking"
	FairnessPolicy   string = "fair"
	UnknownPolicy    string = "undefined"
)

// CheckPolicyType returns adjusted type string and error (if the input string is undefined)
func CheckPolicyType(policyType string) (string, error) {
	switch policyType {
	// fair is the default policy when not set
	case FairnessPolicy, "":
		return FairnessPolicy, nil
	case BinPackingPolicy:
		return BinPackingPolicy, nil
	default:
		return UnknownPolicy, fmt.Errorf("undefined policy: %s", policyType)
	}
}

func checkNodeSortingPolicy(policy *NodeSortingPolicy) error {
	_, err := CheckPolicyType(policy.Type)
	return err
}
