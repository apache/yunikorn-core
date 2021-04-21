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

package webservice

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics/history"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"
)

var imHistory *history.InternalMetricsHistory
var lock sync.RWMutex
var schedulerContext *scheduler.ClusterContext

type WebService struct {
	httpServer *http.Server
}

func newRouter() *mux.Router {
	router := mux.NewRouter().StrictSlash(true)
	for _, webRoute := range webRoutes {
		handler := loggingHandler(webRoute.HandlerFunc, webRoute.Name)
		router.
			Methods(webRoute.Method).
			Path(webRoute.Pattern).
			Name(webRoute.Name).
			Handler(handler)
	}
	return router
}

func loggingHandler(inner http.Handler, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &YResponseWriter{ResponseWriter: w, statusCode: -1}
		inner.ServeHTTP(rw, r)
		var body = string(rw.body)
		if rw.statusCode != http.StatusOK && rw.statusCode != -1 {
			errorInfo := dao.NewYAPIError(nil, rw.statusCode, body)
			if err := json.NewEncoder(w).Encode(errorInfo); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
		log.Logger().Debug(fmt.Sprintf("%s\t%s\t%s\t%s",
			r.Method, r.RequestURI, name, time.Since(start)))
	})
}

type YResponseWriter struct {
	http.ResponseWriter
	statusCode int
	body       []byte
}

func (rw *YResponseWriter) Write(bytes []byte) (int, error) {
	rw.body = bytes
	if rw.statusCode == -1  || rw.statusCode == http.StatusOK {
		_, err := rw.ResponseWriter.Write(bytes)
		if err != nil {
			log.Logger().Error(fmt.Sprintf("Unable to serve response. Problem in writing the response \"%s\". Reason: %s", string(bytes), err.Error()))
		}
	}
	return len(bytes), nil
}

func (rw *YResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
	writeHeaders(rw.ResponseWriter)
}

// TODO we need the port to be configurable
func (m *WebService) StartWebApp() {
	router := newRouter()
	m.httpServer = &http.Server{Addr: ":9080", Handler: router}

	log.Logger().Info("web-app started", zap.Int("port", 9080))
	go func() {
		httpError := m.httpServer.ListenAndServe()
		if httpError != nil && httpError != http.ErrServerClosed {
			log.Logger().Error("HTTP serving error",
				zap.Error(httpError))
		}
	}()
}

func NewWebApp(context *scheduler.ClusterContext, internalMetrics *history.InternalMetricsHistory) *WebService {
	m := &WebService{}
	schedulerContext = context
	imHistory = internalMetrics
	return m
}

func (m *WebService) StopWebApp() error {
	if m.httpServer != nil {
		// graceful shutdown in 5 seconds
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return m.httpServer.Shutdown(ctx)
	}

	return nil
}
