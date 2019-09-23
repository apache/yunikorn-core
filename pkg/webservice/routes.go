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
package webservice

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc http.HandlerFunc
}

type Routes []Route

var routes = Routes{
	Route{
		"Scheduler",
		"GET",
		"/ws/v1/queues",
		GetQueueInfo,
	},
	Route{
		"Cluster",
		"GET",
		"/ws/v1/clusters",
		GetClusterInfo,
	},
	Route{
		"Scheduler",
		"GET",
		"/ws/v1/apps",
		GetApplicationsInfo,
	},
	Route{
		"Scheduler",
		"GET",
		"/ws/v1/stack",
		GetStackInfo,
	},
	Route {
		"Scheduler",
		"GET",
		"/ws/v1/metrics",
		promhttp.Handler().ServeHTTP,
	},
}
