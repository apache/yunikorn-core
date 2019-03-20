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
package webservice

import (
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/cache"
	"net/http"
	"sync"
	"time"
)

var gClusterInfo *cache.ClusterInfo

type WebService struct {
	clusterInfo *cache.ClusterInfo
	lock        sync.RWMutex
}

func NewRouter(info *cache.ClusterInfo) *mux.Router {

	router := mux.NewRouter().StrictSlash(true)
	for _, route := range routes {
		var handler http.Handler

		handler = route.HandlerFunc
		handler = Logger(handler, route.Name, info)

		router.
			Methods(route.Method).
			Path(route.Pattern).
			Name(route.Name).
			Handler(handler)

	}
	return router
}

func Logger(inner http.Handler, name string, info *cache.ClusterInfo) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		inner.ServeHTTP(w, r)

		glog.Infof(
			"%s\t%s\t%s\t%s",
			r.Method,
			r.RequestURI,
			name,
			time.Since(start),
		)
	})
}

func (m *WebService) StartWebApp() {
	router := NewRouter(m.clusterInfo)

	glog.Info("Webapp started at port=9080")
	go func() {
		httpError := http.ListenAndServe(":9080", router)
		if httpError != nil {
			glog.Errorf("While serving HTTP: ", httpError)
		}
	}()
}

func NewWebApp(clusterInfo *cache.ClusterInfo) *WebService {
	m := &WebService{}
	gClusterInfo = clusterInfo
	return m
}
