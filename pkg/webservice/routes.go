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

const (
	clusterRoute   = "Cluster"
	schedulerRoute = "Scheduler"
	systemRoute    = "System"
)

var webRoutes = routes{
	// endpoints to retrieve general cluster info
	route{
		clusterRoute,
		http.MethodGet,
		"/ws/v1/clusters",
		getClusterInfo,
	},
	route{
		clusterRoute,
		http.MethodGet,
		"/ws/v1/metrics",
		getMetrics,
	},
	route{
		clusterRoute,
		http.MethodGet,
		"/ws/v1/config",
		getClusterConfig,
	},
	route{
		clusterRoute,
		"POST",
		"/ws/v1/validate-conf",
		validateConf,
	},

	// endpoints to retrieve general scheduler info
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/history/apps",
		getApplicationHistory,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/history/containers",
		getContainerHistory,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/partitions",
		getPartitions,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/partition/:partition/placementrules",
		getPartitionRules,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/partition/:partition/queues",
		getPartitionQueues,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/partition/:partition/queue/:queue",
		getPartitionQueue,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/partition/:partition/nodes",
		getPartitionNodes,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/partition/:partition/node/:node",
		getPartitionNode,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/partition/:partition/queue/:queue/applications",
		getQueueApplications,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/partition/:partition/queue/:queue/application/:application",
		getApplication,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/partition/:partition/application/:application",
		getApplication,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/partition/:partition/applications/:state",
		getPartitionApplicationsByState,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/partition/:partition/queue/:queue/applications/:state",
		getQueueApplicationsByState,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/partition/:partition/usage/users",
		getUsersResourceUsage,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/partition/:partition/usage/user/:user",
		getUserResourceUsage,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/partition/:partition/usage/groups",
		getGroupsResourceUsage,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/partition/:partition/usage/group/:group",
		getGroupResourceUsage,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/events/batch",
		getEvents,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/events/stream",
		getStream,
	},
	route{
		schedulerRoute,
		http.MethodGet,
		"/ws/v1/scheduler/healthcheck",
		checkHealthStatus,
	},
	route{
		schedulerRoute,
		http.MethodGet,
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
		Name:        systemRoute,
		Method:      http.MethodGet,
		Pattern:     "/debug/stack",
		HandlerFunc: getStackInfo,
	},
	route{
		Name:        systemRoute,
		Method:      http.MethodGet,
		Pattern:     "/debug/fullstatedump",
		HandlerFunc: getFullStateDump,
	},
	route{
		Name:        systemRoute,
		Method:      http.MethodGet,
		Pattern:     "/debug/pprof/",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        systemRoute,
		Method:      http.MethodGet,
		Pattern:     "/debug/pprof/heap",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        systemRoute,
		Method:      http.MethodGet,
		Pattern:     "/debug/pprof/threadcreate",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        systemRoute,
		Method:      http.MethodGet,
		Pattern:     "/debug/pprof/goroutine",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        systemRoute,
		Method:      http.MethodGet,
		Pattern:     "/debug/pprof/allocs",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        systemRoute,
		Method:      http.MethodGet,
		Pattern:     "/debug/pprof/block",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        systemRoute,
		Method:      http.MethodGet,
		Pattern:     "/debug/pprof/mutex",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        systemRoute,
		Method:      http.MethodGet,
		Pattern:     "/debug/pprof/cmdline",
		HandlerFunc: pprof.Cmdline,
	},
	route{
		Name:        systemRoute,
		Method:      http.MethodGet,
		Pattern:     "/debug/pprof/profile",
		HandlerFunc: pprof.Profile,
	},
	route{
		Name:        systemRoute,
		Method:      http.MethodGet,
		Pattern:     "/debug/pprof/symbol",
		HandlerFunc: pprof.Symbol,
	},
	route{
		Name:        systemRoute,
		Method:      http.MethodGet,
		Pattern:     "/debug/pprof/trace",
		HandlerFunc: pprof.Trace,
	},
}
