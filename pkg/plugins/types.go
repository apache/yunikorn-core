/*
Copyright 2019 The Unity Scheduler Authors

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

package plugins

type SchedulerPlugins struct {
	predicatesPlugin PredicatesPlugin
}

type PredicatesPlugin interface {
	// RM side implements this API when it can provide plugin for predicates
	// Run a certain set of predicate functions to determine if a proposed allocation
	// can be allocated onto a node.
	Predicates(allocationId string, node string) error
}