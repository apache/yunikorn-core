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

package history

import (
	"testing"

	"gotest.tools/assert"
)

func TestHistoricalClusterInfo(t *testing.T) {
	limit := 2
	hpInfo := NewInternalMetricsHistory(limit)

	assert.Equal(t, limit, hpInfo.GetLimit(), "Limit should have been set to 2!")

	hpInfo.Store(2, 3)
	assert.Equal(t, 1, len(hpInfo.GetRecords()), "Expected to have 1 record")

	hpInfo.Store(3, 4)
	assert.Equal(t, 2, len(hpInfo.GetRecords()), "Expected to have 2 records")

	hpInfo.Store(5, 6)
	assert.Equal(t, 2, len(hpInfo.GetRecords()), "Expected to have 2 records")

	for i, record := range hpInfo.GetRecords() {
		switch i {
		case 0:
			assert.Equal(t, 3, record.TotalApplications)
			assert.Equal(t, 4, record.TotalContainers)
		case 1:
			assert.Equal(t, 5, record.TotalApplications)
			assert.Equal(t, 6, record.TotalContainers)
		}
	}
}
