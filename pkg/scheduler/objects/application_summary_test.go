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

package objects

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestNilApplicationSummaryToString(t *testing.T) {
	// make sure we're nil safe IDE will complain about the non nil check
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil ApplicationSummary in TestNilApplicationSummaryToString")
		}
	}()
	var appSummary *ApplicationSummary
	appSummaryString := appSummary.String()
	assert.Equal(t, appSummaryString, "nil ApplicationSummary", "Unexpected string returned for nil ApplicationSummary")
}
