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

package entrypoint

import (
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/rmproxy"
	"github.com/apache/yunikorn-core/pkg/scheduler"
	"github.com/apache/yunikorn-core/pkg/webservice"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
)

type ServiceContext struct {
	RMProxy          api.SchedulerAPI
	Scheduler        *scheduler.Scheduler
	WebApp           *webservice.WebService
	MetricsCollector metrics.InternalMetricsCollector
}

func (s *ServiceContext) StopAll() {
	log.Log(log.Entrypoint).Info("ServiceContext stop all services")
	if s.WebApp != nil {
		if err := s.WebApp.StopWebApp(); err != nil {
			log.Log(log.Entrypoint).Error("failed to stop web-app",
				zap.Error(err))
		}
	}
	if s.MetricsCollector != nil {
		s.MetricsCollector.Stop()
	}
	s.Scheduler.Stop()
	if proxyImpl, ok := s.RMProxy.(*rmproxy.RMProxy); ok {
		proxyImpl.Stop()
	}
	events.GetEventSystem().Stop()
}
