/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

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

package metrics

import (
	"crypto/rand"
	"testing"

	"github.com/prometheus/common/model"
	"go.uber.org/zap"
	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

func TestFormatMetricName(t *testing.T) {
	testStrings := []string{"0", "ad_vs:ad", "~23", "test/a", "-dfs", "012~`s@dd#$b%23^&5^3*(45){78}|00[]\\1ssd"}
	for _, testString := range testStrings {
		replaceStr := formatMetricName(testString)
		assert.Equal(t, true, model.IsValidMetricName(model.LabelValue(replaceStr)))
	}
	numRandomTestStrings := 1000
	randomTestStrings := make([]string, numRandomTestStrings)
	for i := 0; i < numRandomTestStrings; i++ {
		randomTestStrings[i] = generateRandomString(100)
	}
	for _, testString := range randomTestStrings {
		replaceStr := formatMetricName(testString)
		assert.Equal(t, true, model.IsValidMetricName(model.LabelValue(replaceStr)))
	}
}

func generateRandomString(len int) string {
	randomBytes := make([]byte, len)
	n, err := rand.Read(randomBytes)
	if err != nil {
		log.Logger().Warn("Random running low on entropy",
			zap.Int("bytesRequested", len),
			zap.Int("bytesRead", n))
	}
	return string(randomBytes)
}
