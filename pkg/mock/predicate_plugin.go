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

package mock

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type PredicatePlugin struct {
	ResourceManagerCallback
	mustPreFilterFail bool
	mustFilterFail    bool
	nodes             map[string]int
}

func (f *PredicatePlugin) PreFilterPredicates(args *si.PreFilterPredicatesArgs) *si.PreFilterPredicatesResponse {
	feasibleNodes := make(map[string]*si.Empty)
	result := &si.PreFilterPredicatesResponse{
		Success:       false,
		FeasibleNodes: map[string]*si.Empty{},
	}
	if f.mustPreFilterFail {
		log.Log(log.Test).Info("fake predicate prefilter plugin fail: must fail set")
		return result
	}
	for k, v := range f.nodes {
		if args.Allocate {
			if v < 0 {
				feasibleNodes[k] = &si.Empty{}
			}
		} else {
			if v > 0 {
				feasibleNodes[k] = &si.Empty{}
			}
		}
	}
	log.Log(log.Test).Info("fake predicate prefilter plugin pass",
		zap.Bool("allocate", args.Allocate),
		zap.String("allocationKey", args.AllocationKey),
		zap.Any("feasibleNodes", feasibleNodes),
		zap.Int("feasibleNodes", len(feasibleNodes)))
	result.Success = true
	result.FeasibleNodes = feasibleNodes
	return result
}

func (f *PredicatePlugin) Predicates(args *si.PredicatesArgs) error {
	if f.mustFilterFail {
		log.Log(log.Test).Info("fake predicate filter plugin fail: must fail set")
		return fmt.Errorf("fake predicate plugin failed")
	}
	log.Log(log.Test).Info("fake predicate filter plugin pass",
		zap.Bool("allocate", args.Allocate),
		zap.String("allocationKey", args.AllocationKey),
		zap.String("node", args.NodeID))
	return nil
}

// NewPredicatePlugin returns a mock that can either always fail or fail based on the node that is checked.
// mustPreFilterFail will cause the predicate prefilter check to fail always
// mustFilterFail will cause the predicate filter check to fail always
// nodes allows specifying which node to make it to feasibleNodes list based on its own value:
// possible values: 1 - Feasible in case of reserve, -1 Feasible in case of allow, 0 Not feasible always
func NewPredicatePlugin(mustPreFilterFail bool, mustFilterFail bool, nodes map[string]int) *PredicatePlugin {
	return &PredicatePlugin{
		mustPreFilterFail: mustPreFilterFail,
		mustFilterFail:    mustFilterFail,
		nodes:             nodes,
	}
}
