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

package common

import "fmt"

type AppSortPolicyType int

const (
    FairAppSortPolicyType = iota
    FifoAppSortPolicyType
    UndefinedAppSortPolicyType

    // Property name decides how to sort applications, valid options are fair / fifo
    ApplicationSortPolicy = "application.sort.policy"
)

func (aspt AppSortPolicyType) String() string {
    return [...]string{"fair", "fifo", "undefined"}[aspt]
}

func ParseAppSortPolicyType(str string) (AppSortPolicyType, error) {
    switch str {
    // fair is the default policy when not set
    case "fair", "":
        return FairAppSortPolicyType, nil
    case "fifo":
        return FifoAppSortPolicyType, nil
    default:
        return UndefinedAppSortPolicyType, fmt.Errorf("undefined app sort policy: %s", str)
    }
}