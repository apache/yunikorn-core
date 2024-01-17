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
	"bufio"
	"bytes"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gotest.tools/v3/assert"
)

type logMessage struct {
	Level   string `json:"L"`
	Message string `json:"M"`
}

func TestRateLimitedLog(t *testing.T) {
	defer resetTestLogger()
	once = sync.Once{}
	config := zap.NewDevelopmentConfig()
	encoderConfig := zap.NewDevelopmentEncoderConfig()
	buf := bytes.Buffer{}
	writer := bufio.NewWriter(&buf)
	zapLogger := zap.New(
		zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderConfig),
			zapcore.AddSync(writer),
			zap.NewAtomicLevelAt(zap.InfoLevel),
		),
	)
	InitializeLogger(zapLogger, &config)
	// log once within one minute
	logger := NewRateLimitedLogger(Core, 1*time.Minute)
	startTime := time.Now()
	for {
		elapsed := time.Since(startTime)
		if elapsed > 500*time.Millisecond {
			break
		}
		logger.Info("YuniKorn")
		time.Sleep(10 * time.Millisecond)
	}
	err := writer.Flush()
	assert.NilError(t, err, "failed to flush writer")
	var lm logMessage
	err = json.Unmarshal(buf.Bytes(), &lm)
	assert.NilError(t, err, "failed to unmarshal logMessage from buffer: %s", buf.Bytes())
	assert.Equal(t, "INFO", lm.Level)
	assert.Equal(t, "YuniKorn", lm.Message)
}
