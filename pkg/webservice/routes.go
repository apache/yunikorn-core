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

package webservice

import (
	"net/http"
	"net/http/pprof"
)

type route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc http.HandlerFunc
}

type routes []route

var webRoutes = routes{
	// endpoints to retrieve general cluster info
	route{
		"Cluster",
		"GET",
		"/ws/v1/clusters",
		getClusterInfo,
	},
	route{
		"Cluster",
		"GET",
		"/ws/v1/metrics",
		getMetrics,
	},
	route{
		"Cluster",
		"GET",
		"/ws/v1/config",
		getClusterConfig,
	},
	route{
		"Cluster",
		"POST",
		"/ws/v1/validate-conf",
		validateConf,
	},

	// endpoints to retrieve general scheduler info
	route{
		"Scheduler",
		"GET",
		"/ws/v1/history/apps",
		getApplicationHistory,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/history/containers",
		getContainerHistory,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partitions",
		getPartitions,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/:partition/placementrules",
		getPartitionRules,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/:partition/queues",
		getPartitionQueues,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/:partition/queue/:queue",
		getPartitionQueue,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/:partition/nodes",
		getPartitionNodes,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/:partition/node/:node",
		getPartitionNode,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/:partition/queue/:queue/applications",
		getQueueApplications,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/:partition/queue/:queue/application/:application",
		getApplication,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/:partition/application/:application",
		getApplication,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/:partition/applications/:state",
		getPartitionApplicationsByState,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/:partition/queue/:queue/applications/:state",
		getQueueApplicationsByState,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/:partition/usage/users",
		getUsersResourceUsage,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/:partition/usage/user/:user",
		getUserResourceUsage,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/:partition/usage/groups",
		getGroupsResourceUsage,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/:partition/usage/group/:group",
		getGroupResourceUsage,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/events/batch",
		getEvents,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/events/stream",
		getStream,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/scheduler/healthcheck",
		checkHealthStatus,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/scheduler/node-utilizations",
		getNodeUtilisations,
	},

	// endpoints to retrieve debug info
	//
	// These endpoints are not to be proxied by the web server. The content is not for general consumption.
	// The content is not considered stable and can change from release to release.
	// All pprof endpoints provide profiling data in the format expected by the pprof visualization tool.
	// We need to explicitly register all handlers as we do not use the DefaultServeMux
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/stack",
		HandlerFunc: getStackInfo,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/fullstatedump",
		HandlerFunc: getFullStateDump,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/heap",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/threadcreate",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/goroutine",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/allocs",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/block",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/mutex",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/cmdline",
		HandlerFunc: pprof.Cmdline,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/profile",
		HandlerFunc: pprof.Profile,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/symbol",
		HandlerFunc: pprof.Symbol,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/trace",
		HandlerFunc: pprof.Trace,
	},

	// Deprecated REST calls
	//
	// Replaced with /ws/v1/scheduler/node-utilizations as part of YuniKorn 1.5
	// Remove as part of YuniKorn 1.8
	route{
		Name:        "Scheduler",
		Method:      "GET",
		Pattern:     "/ws/v1/scheduler/node-utilization",
		HandlerFunc: getNodeUtilisation,
	},
	// Permanently moved to the debug endpoint as part of YuniKorn 1.7
	// Remove redirect in YuniKorn 1.10
	route{
		Name:        "Scheduler",
		Method:      "GET",
		Pattern:     "/ws/v1/stack",
		HandlerFunc: redirectDebug,
	},
	// Permanently moved to the debug endpoint as part of YuniKorn 1.7
	// Remove redirect in YuniKorn 1.10
	route{
		Name:        "Scheduler",
		Method:      "GET",
		Pattern:     "/ws/v1/fullstatedump",
		HandlerFunc: redirectDebug,
	},
}
