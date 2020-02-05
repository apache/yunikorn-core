/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package entrypoint

import (
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/api"
	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice"
)

type ServiceContext struct {
	RMProxy   api.SchedulerAPI
	Cache     *cache.ClusterInfo
	Scheduler *scheduler.Scheduler
	WebApp    *webservice.WebService
}

func (s *ServiceContext) StopAll() {
	// TODO implement stop for services
	if s.WebApp != nil {
		if err := s.WebApp.StopWebApp(); err != nil {
			log.Logger().Error("failed to stop web-app",
				zap.Error(err))
		}
	}
}
