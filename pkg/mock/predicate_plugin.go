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
	mustFail bool
	nodes    map[string]int
}

func (f *PredicatePlugin) Predicates(args *si.PredicatesArgs) error {
	if f.mustFail {
		log.Log(log.Test).Info("fake predicate plugin fail: must fail set")
		return fmt.Errorf("fake predicate plugin failed")
	}
	if fail, ok := f.nodes[args.NodeID]; ok {
		if args.Allocate && fail >= 0 {
			log.Log(log.Test).Info("fake predicate plugin node allocate fail",
				zap.String("node", args.NodeID),
				zap.Int("fail mode", fail))
			return fmt.Errorf("fake predicate plugin failed")
		}
		if !args.Allocate && fail <= 0 {
			log.Log(log.Test).Info("fake predicate plugin node reserve fail",
				zap.String("node", args.NodeID),
				zap.Int("fail mode", fail))
			return fmt.Errorf("fake predicate plugin failed")
		}
	}
	log.Log(log.Test).Info("fake predicate plugin pass",
		zap.String("node", args.NodeID))
	return nil
}

// NewPredicatePlugin returns a mock that can either always fail or fail based on the node that is checked.
// mustFail will cause the predicate check to always fail
// nodes allows specifying which node to fail for which check using the nodeID:
// possible values: -1 fail reserve, 0 fail always, 1 fail alloc (defaults to always)
func NewPredicatePlugin(mustFail bool, nodes map[string]int) *PredicatePlugin {
	return &PredicatePlugin{
		mustFail: mustFail,
		nodes:    nodes,
	}
}
