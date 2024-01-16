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

package entrypoint

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"

	"go.uber.org/zap"
	"gotest.tools/v3/assert"
)

const relTestDataDir = "build" // test data directory, relative to the project root
const logFileName = "test.log"
const logMessage = "log message sent via the core logger"
const logMessage2 = "log message sent via the app logger"

// TestCustomLoggingConfiguration ensures that custom logging configuration takes even in the presence of "objects"
// package initialization. "objects" package initialization used to break custom logging configuration in the past,
// by triggering rateLimitedLogger initialization in the package lexical scope, which did trigger in turn one-time
// logging system initialization, preventing subsequent custom configuration. See YUNIKORN-2320.
func TestCustomLoggingConfiguration(t *testing.T) {
	defer cleanup()
	// ensure that the "object" package initialization happens
	app := objects.Application{}
	assert.Equal(t, "", app.ApplicationID)
	testDataDir, err := getWritableTestDataDir()
	assert.NilError(t, err, "failed to get the test data directory")
	logFile := filepath.Join(testDataDir, logFileName)
	config := zap.NewDevelopmentConfig()
	config.OutputPaths = []string{logFile}
	config.ErrorOutputPaths = []string{logFile}
	logger, err := config.Build()
	assert.NilError(t, err, "zap Logger creation failed")
	log.InitializeLogger(logger, &config)
	StartAllServices()
	ykManagedLogger := log.Log(log.Core)
	ykManagedLogger.Info(logMessage)
	objects.GetRateLimitedAppLog().Info(logMessage2)
	err = ykManagedLogger.Sync()
	if err != nil {
		// if it fails to sync, it may be because the logger is still using /dev/stderr
		fmt.Printf("%v\n", err)
	}
	// make sure the test log messages are in the log file
	bs, err := os.ReadFile(logFile)
	assert.NilError(t, err, "failed to read the log file", logFile)
	for _, m := range []string{logMessage, logMessage2} {
		assert.Equal(t, strings.Contains(string(bs), m), true, "'%s' not found in the log file %s", m, logFile)
	}
}

// getWritableTestDataDir returns the absolute path of the validated (in that it ensures it exists in the file system)
// directory where we can write test data on the local file system.
func getWritableTestDataDir() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	buildDir := filepath.Join(dir, "../../", relTestDataDir)
	_, err = os.Stat(buildDir)
	if err != nil {
		return "", err
	}
	return buildDir, nil
}

// cleanup removes the test log file, if created in the writable test data directory. Noop if the file is not present.
func cleanup() {
	testDataDir, err := getWritableTestDataDir()
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	err = os.Remove(filepath.Join(testDataDir, logFileName))
	if err != nil {
		if os.IsNotExist(err) {
			// ignore
			return
		}
		fmt.Printf("%v\n", err)
	}
}
