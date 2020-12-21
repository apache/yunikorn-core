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

package dao

type SchedulerHealthDAOInfo struct {
	Healthy      bool
	HealthChecks []HealthCheckInfo
}

type HealthCheckInfo struct {
	Name             string
	Succeeded        bool
	Description      string
	DiagnosisMessage string
}

func (s *SchedulerHealthDAOInfo) SetHealthStatus() {
	s.Healthy = len(s.HealthChecks) == 0
}

func (s *SchedulerHealthDAOInfo) AddHealthCheckInfo(succeeded bool, name, description, diagnosis string) {
	info := HealthCheckInfo{
		Name:             name,
		Succeeded:        succeeded,
		Description:      description,
		DiagnosisMessage: diagnosis,
	}
	s.HealthChecks = append(s.HealthChecks, info)
}
