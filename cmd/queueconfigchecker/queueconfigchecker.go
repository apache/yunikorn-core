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

package main

import (
	"log"
	"os"

	"github.com/apache/yunikorn-core/pkg/common/configs"
)

/*
A utility command to load queue configuration file and check its validity
*/
func main() {
	if len(os.Args) != 2 {
		log.Println("Usage: " + os.Args[0] + " <queue-config-file>")
		os.Exit(1)
	}
	queueFile := os.Args[1]
	iv, err := os.ReadFile(queueFile)
	if err != nil {
		log.Println(err)
		os.Exit(2)
	}
	_, err1 := configs.LoadSchedulerConfigFromByteArray(iv)
	if err1 != nil {
		log.Println(err1)
		os.Exit(3)
	}
}
