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

package cache

import (
	"github.com/cloudera/yunikorn-core/pkg/common/configs"
	"github.com/cloudera/yunikorn-core/pkg/log"
	"go.uber.org/zap"
	"strings"
)

const (
	SCHEDULING_BINPACKING = 0
	SCHEDULING_FAIRPOLICY = 1
)

type SchedulingPolicy struct {
	Name string
	PolicyType string

	AutoRefreshEnabled bool
}

func NewSchedulingPolicy(policy configs.GlobalPolicy) (*SchedulingPolicy) {
	sp := &SchedulingPolicy{Name: strings.ToLower(policy.Name),
		PolicyType: policy.Policy,
		AutoRefreshEnabled: policy.AutoRefresh,
	}

	log.Logger().Debug("new scheduling policy added",
		zap.String("policy name", sp.Name),
		zap.String("type", sp.PolicyType))
	return sp
}
