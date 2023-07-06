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

package ugm

import (
	"strings"

	"github.com/apache/yunikorn-core/pkg/common/configs"
)

func getChildQueuePath(queuePath string) (string, string) {
	idx := strings.Index(queuePath, configs.DOT)
	if idx == -1 {
		return "", ""
	}
	childQueuePath := queuePath[idx+1:]
	idx = strings.Index(childQueuePath, configs.DOT)
	if idx == -1 {
		return childQueuePath, childQueuePath
	}
	return childQueuePath, childQueuePath[:idx]
}

func getParentQueuePath(queuePath string) (string, string) {
	idx := strings.LastIndex(queuePath, configs.DOT)
	if idx == -1 {
		return "", ""
	}
	parentQueuePath := queuePath[:idx]
	idx = strings.LastIndex(parentQueuePath, configs.DOT)
	if idx == -1 {
		return parentQueuePath, parentQueuePath
	}
	return parentQueuePath, parentQueuePath[idx+1:]
}
