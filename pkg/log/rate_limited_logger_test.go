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

package log

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
	"gotest.tools/v3/assert"
)

type logMessage struct {
	Level   string `json:"level"`
	Message string `json:"msg"`
}

func TestRateLimitedLog(t *testing.T) {
	zapLogger, logFile := createZapTestLogger()
	logger := getTestRateLimitedLog(zapLogger, time.Minute)

	// this won't last over one minute, assert there is only one record in log file
	for i := 0; i < 10000; i++ {
		logger.Info("YuniKorn")
	}

	content, err := os.ReadFile(logFile)
	if err != nil {
		fmt.Printf("Failed reading file: %s", err)
	}
	var logMessage logMessage
	err = json.Unmarshal(content, &logMessage)
	assert.NilError(t, err, "failed to unmarshal logMessage from log file: %s", content)
	assert.Equal(t, zapcore.InfoLevel.String(), logMessage.Level)
	assert.Equal(t, "YuniKorn", logMessage.Message)
}

func getTestRateLimitedLog(logger *zap.Logger, every time.Duration) *rateLimitedLogger {
	return &rateLimitedLogger{
		logger:  logger,
		limiter: rate.NewLimiter(rate.Every(every), 1),
	}
}

// create zap logger that log in json format and output to temp file
func createZapTestLogger() (*zap.Logger, string) {
	logDir, err := os.MkdirTemp("", "log*")
	if err != nil {
		panic(err)
	}
	logFile := fmt.Sprintf("%s/log.stdout", logDir)
	outputPaths := []string{logFile}
	zapConfig := zap.NewProductionConfig()
	zapConfig.Encoding = "json"
	zapConfig.OutputPaths = outputPaths

	zapLogger, err := zapConfig.Build()
	if err != nil {
		panic(err)
	}

	return zapLogger, logFile
}
