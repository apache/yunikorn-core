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
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
	"gotest.tools/v3/assert"
)

type logMessage struct {
	Level   string `json:"L"`
	Message string `json:"M"`
}

func TestRateLimitedLog(t *testing.T) {
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
	// log once within one minute
	logger := &rateLimitedLogger{
		logger:  zapLogger,
		limiter: rate.NewLimiter(rate.Every(time.Minute), 1),
	}

	startTime := time.Now()
	for {
		elapsed := time.Since(startTime)
		if elapsed > 2*time.Second {
			break
		}
		logger.Info("YuniKorn")
		time.Sleep(100 * time.Millisecond)
	}
	writer.Flush()

	var logMessage logMessage
	err := json.Unmarshal(buf.Bytes(), &logMessage)
	assert.NilError(t, err, "failed to unmarshal logMessage from buffer: %s", buf.Bytes())
	assert.Equal(t, "INFO", logMessage.Level)
	assert.Equal(t, "YuniKorn", logMessage.Message)
}
