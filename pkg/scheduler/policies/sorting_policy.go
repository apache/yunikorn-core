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
)

// Sort type for queues & apps.
type SortPolicy int

const (
	FifoSortPolicy   SortPolicy = iota // fair based on usage
	FairSortPolicy                     // first in first out, submit time
	StateAwarePolicy                   // only 1 app in starting state
	UndefinedApp                       // not initialised or parsing failed
)

func (s SortPolicy) String() string {
	return [...]string{"fifo", "fair", "status", "undefined"}[s]
}

func SortPolicyFromString(str string) (SortPolicy, error) {
	switch str {
	// fifo is the default policy when not set
	case FifoSortPolicy.String(), "":
		return FifoSortPolicy, nil
	case FairSortPolicy.String():
		return FairSortPolicy, nil
	case StateAwarePolicy.String():
		return StateAwarePolicy, nil
	default:
		return UndefinedApp, fmt.Errorf("undefined policy: %s", str)
	}
}
