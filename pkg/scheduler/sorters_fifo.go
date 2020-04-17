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

package scheduler

import (
	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"go.uber.org/zap"
)

type FifoPolicy interface {
	Apply(apps []*SchedulingApplication)
}

type BasicFifoPolicy struct {

}

type AppStateAwareFifoPolicy struct {

}


func (b *BasicFifoPolicy) Apply(apps []*SchedulingApplication) {
	// basic policy
	for _, app := range apps {
		if app.ApplicationInfo.GetApplicationState() == cache.Accepted.String() {
			if err := app.ApplicationInfo.HandleApplicationEvent(cache.RunApplication); err != nil {
				log.Logger().Info("app state transit failed: Accepted -> Running")
			}
		}
	}
}

func (a *AppStateAwareFifoPolicy) Apply(apps []*SchedulingApplication) {
	var preAppState cache.ApplicationState
	for idx, app := range apps {
		curAppState := app.ApplicationInfo.GetApplicationState()
		if curAppState == cache.Accepted.String() {
			if idx == 0 || int(preAppState) >= int(cache.Running) {
				if err := app.ApplicationInfo.HandleApplicationEvent(cache.ScheduleApplication); err != nil {
					log.Logger().Info("app state transit failed: Running -> Scheduable")
				}
			}
		}

		// reset pre app state
		preAppState = cache.LoadAppStateFrom(app.ApplicationInfo.GetApplicationState())
		log.Logger().Info("states",
			zap.String("appID", app.ApplicationInfo.ApplicationID),
			zap.String("state", app.ApplicationInfo.GetApplicationState()))
	}
}