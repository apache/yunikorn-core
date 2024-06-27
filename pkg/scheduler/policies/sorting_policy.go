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

	"github.com/apache/yunikorn-core/pkg/log"
)

// Sort type for queues & apps.
type SortPolicy int

const (
	FifoSortPolicy             SortPolicy = iota // first in first out, submit time
	FairSortPolicy                               // fair based on usage
	deprecatedStateAwarePolicy                   // deprecated: now alias for FIFO
	Undefined                                    // not initialised or parsing failed
)

func (s SortPolicy) String() string {
	return [...]string{"fifo", "fair", "stateaware", "undefined"}[s]
}

func SortPolicyFromString(str string) (SortPolicy, error) {
	switch str {
	// fifo is the default policy when not set
	case FifoSortPolicy.String(), "":
		return FifoSortPolicy, nil
	case FairSortPolicy.String():
		return FairSortPolicy, nil
	case deprecatedStateAwarePolicy.String():
		log.Log(log.Deprecation).Warn("Sort policy 'stateaware' is deprecated; using 'fifo' instead")
		return FifoSortPolicy, nil
	default:
		return Undefined, fmt.Errorf("undefined policy: %s", str)
	}
}
