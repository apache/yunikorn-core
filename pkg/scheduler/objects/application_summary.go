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

package objects

import (
	"fmt"
	"time"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
)

type ApplicationSummary struct {
	ApplicationID       string
	SubmissionTime      time.Time
	StartTime           time.Time
	FinishTime          time.Time
	User                string
	Queue               string
	State               string
	RmID                string
	ResourceUsage       *resources.TrackedResource
	PreemptedResource   *resources.TrackedResource
	PlaceholderResource *resources.TrackedResource
}

func (as *ApplicationSummary) String() string {
	return fmt.Sprintf("ApplicationID: %s, SubmissionTime: %d, StartTime: %d, FinishTime: %d, User: %s, "+
		"Queue: %s, State: %s, RmID: %s, ResourceUsage: %s, PreemptedResource: %s, PlaceholderResource: %s",
		as.ApplicationID,
		as.SubmissionTime.UnixMilli(),
		as.StartTime.UnixMilli(),
		as.FinishTime.UnixMilli(),
		as.User,
		as.Queue,
		as.State,
		as.RmID,
		as.ResourceUsage,
		as.PreemptedResource,
		as.PlaceholderResource)
}

func (as *ApplicationSummary) DoLogging() {
	log.Log(log.SchedAppUsage).Info(fmt.Sprintf("YK_APP_SUMMARY: {%s}", as))
}
