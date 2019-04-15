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

package configs

import (
    "crypto/sha256"
    "io"
    "os"
)

const (
    SchedulerConfigPath  = "scheduler-config-path"
    DefaultSchedulerConfigPath = "/etc/yunikorn"
)

type CheckSumFn func(path string) ([]byte, error)

var ConfigMap = make(map[string]string)
var FileCheckSummer = checkSum

// returns sha256 checksum of a given file
func checkSum(path string) ([]byte, error) {
    f, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer f.Close()
    h := sha256.New()
    if _, err := io.Copy(h, f); err != nil {
        return nil, err
    }
    return h.Sum(nil), err
}
