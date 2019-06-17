/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

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
	"github.com/golang/glog"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/cache"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/rmproxy"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/scheduler"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/webservice"
)

type ServiceContext struct {
	RMProxy   *rmproxy.RMProxy
	Cache     *cache.ClusterInfo
	Scheduler *scheduler.Scheduler
	WebApp    *webservice.WebService
}


func (s *ServiceContext) StopAll() {
	// TODO implement stop for services
	if s.WebApp != nil {
		if err := s.WebApp.StopWebApp(); err != nil {
			glog.V(0).Infof("failed to stop web-app %v", err)
		}
	}
}
