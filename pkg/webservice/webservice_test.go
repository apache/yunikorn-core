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
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/metrics/history"
	"github.com/apache/yunikorn-core/pkg/scheduler"
)

const base = "http://localhost:9080"

func Test_RedirectDebugHandler(t *testing.T) {
	defer ResetIMHistory()
	s := NewWebApp(&scheduler.ClusterContext{}, history.NewInternalMetricsHistory(5))
	s.StartWebApp()
	defer func(s *WebService) {
		err := s.StopWebApp()
		if err != nil {
			t.Fatal("failed to stop webapp")
		}
	}(s)
	tests := []struct {
		name     string
		reqURL   string
		redirect string
	}{
		{"statedump", "/ws/v1/fullstatedump", "/debug/fullstatedump"},
		{"stacks", "/ws/v1/stack", "/debug/stack"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &http.Client{
				CheckRedirect: func(req *http.Request, via []*http.Request) error {
					if req.URL.Path != tt.redirect {
						return fmt.Errorf("expected redirect to '%s' got '%s'", tt.redirect, req.URL.Path)
					}
					return nil
				},
			}
			resp, err := client.Get(base + tt.reqURL)
			assert.NilError(t, err, "unexpected error returned")
			_ = resp.Body.Close() // not interested in the error
			assert.Equal(t, resp.StatusCode, http.StatusOK, "expected OK after redirect")
		})
	}
}

func Test_RouterHandling(t *testing.T) {
	s := NewWebApp(&scheduler.ClusterContext{}, nil)
	s.StartWebApp()
	defer func(s *WebService) {
		err := s.StopWebApp()
		if err != nil {
			t.Fatal("failed to stop webapp")
		}
	}(s)
	client := &http.Client{}
	// unsupported POST
	resp, err := client.Post(base+"/ws/v1/clusters", "application/json; charset=UTF-8", nil)
	assert.NilError(t, err, "unexpected error returned")
	assert.Equal(t, resp.StatusCode, http.StatusMethodNotAllowed, "expected method not allowed")
	var body []byte
	body, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close() // not interested in the error
	assert.NilError(t, err, "unexpected error reading body")
	assert.Assert(t, body != nil, "expected body with status text")
	resp, err = client.Head(base + "/ws/v1/clusters")
	assert.NilError(t, err, "unexpected error returned")
	body, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.NilError(t, err, "unexpected error reading body")
	assert.Assert(t, body != nil, "expected body with status text")
	assert.Equal(t, resp.StatusCode, http.StatusMethodNotAllowed, "expected method not allowed")
	// get with trailing slash
	resp, err = client.Get(base + "/ws/v1/clusters/")
	assert.NilError(t, err, "unexpected error returned")
	_ = resp.Body.Close()
	assert.Equal(t, resp.StatusCode, http.StatusOK, "expected OK")
	// get with case difference
	resp, err = client.Get(base + "/ws/v1/CLUSTERS")
	assert.NilError(t, err, "unexpected error returned")
	_ = resp.Body.Close()
	assert.Equal(t, resp.StatusCode, http.StatusOK, "expected OK")
}

func Test_HeaderChecks(t *testing.T) {
	s := NewWebApp(&scheduler.ClusterContext{}, nil)
	s.StartWebApp()
	defer func(s *WebService) {
		err := s.StopWebApp()
		if err != nil {
			t.Fatal("failed to stop webapp")
		}
	}(s)
	client := http.DefaultClient
	tests := []struct {
		name     string
		reqURL   string
		method   string
		expected string
	}{
		{"get options", "/ws/v1/clusters", http.MethodOptions, "GET, OPTIONS"},
		{"get", "/ws/v1/clusters", http.MethodGet, "GET, OPTIONS"},
		{"post options", "/ws/v1/validate-conf", http.MethodOptions, "OPTIONS, POST"},
		{"post", "/ws/v1/validate-conf", http.MethodPost, "OPTIONS, POST"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, base+tt.reqURL, nil)
			assert.NilError(t, err, "unexpected error creating request")
			var resp *http.Response
			//nolint:gosec // safe to ignore, this is a test client and the URL is hardcoded to localhost
			resp, err = client.Do(req)
			assert.NilError(t, err, "unexpected error executing request")
			assert.Equal(t, resp.StatusCode, http.StatusOK, "expected OK")
			switch tt.method {
			case http.MethodGet, http.MethodPost:
				assert.Equal(t, resp.Header.Get("Access-Control-Allow-Methods"), tt.expected, "wrong methods returned")
			case http.MethodOptions:
				// OPTIONS requests are handled by default via httpdrouter, not defined in the routes
				assert.Equal(t, resp.Header.Get("Allow"), tt.expected, "expected only get and options to be returned")
			}
			var body []byte
			body, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			assert.NilError(t, err, "unexpected error reading body")
			assert.Assert(t, body != nil, "expected body with status text")
		})
	}
}

// newTestClient returns an HTTP client with automatic gzip decompression disabled,
// so that tests can inspect raw response headers and body bytes.
func newTestClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{DisableCompression: true},
	}
}

func Test_GzipCompression(t *testing.T) {
	// Lower the threshold to 0 so that even the small responses returned by the
	// empty-context endpoints are compressed. This test focuses on the Accept-Encoding
	// negotiation logic, not the threshold behaviour (see Test_GzipMinCompressionSize).
	orig := minCompressionSize
	minCompressionSize = 0
	t.Cleanup(func() { minCompressionSize = orig })

	s := NewWebApp(&scheduler.ClusterContext{}, nil)
	s.StartWebApp()
	defer func() {
		if err := s.StopWebApp(); err != nil {
			t.Fatal("failed to stop webapp")
		}
	}()

	client := newTestClient()

	tests := []struct {
		name           string
		acceptEncoding string
		wantGzip       bool
	}{
		{
			name:           "gzip requested — response must be gzip-encoded",
			acceptEncoding: "gzip",
			wantGzip:       true,
		},
		{
			name:           "gzip with quality factor — response must be gzip-encoded",
			acceptEncoding: "gzip;q=0.9",
			wantGzip:       true,
		},
		{
			name:           "gzip explicitly excluded — response must be uncompressed",
			acceptEncoding: "gzip;q=0",
			wantGzip:       false,
		},
		{
			name:           "no Accept-Encoding — response must be uncompressed",
			acceptEncoding: "",
			wantGzip:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, base+"/ws/v1/clusters", nil)
			assert.NilError(t, err, "unexpected error creating request")
			if tt.acceptEncoding != "" {
				req.Header.Set("Accept-Encoding", tt.acceptEncoding)
			}

			//nolint:gosec
			resp, err := client.Do(req)
			assert.NilError(t, err, "unexpected error executing request")
			assert.Equal(t, resp.StatusCode, http.StatusOK, "expected 200 OK")

			body, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			assert.NilError(t, err, "unexpected error reading response body")

			if tt.wantGzip {
				assert.Equal(t, resp.Header.Get("Content-Encoding"), "gzip",
					"expected Content-Encoding: gzip in response headers")
				assert.Equal(t, resp.Header.Get("Vary"), "Accept-Encoding",
					"expected Vary: Accept-Encoding in response headers")

				gr, gzErr := gzip.NewReader(bytes.NewReader(body))
				assert.NilError(t, gzErr, "expected body to be valid gzip data")
				decompressed, readErr := io.ReadAll(gr)
				assert.NilError(t, readErr, "unexpected error decompressing body")
				_ = gr.Close()
				assert.Assert(t, len(decompressed) > 0, "decompressed body must not be empty")
			} else {
				assert.Equal(t, resp.Header.Get("Content-Encoding"), "",
					"expected no Content-Encoding header for uncompressed response")
				assert.Assert(t, len(body) > 0, "response body must not be empty")
			}
		})
	}
}

func Test_GzipMinCompressionSize(t *testing.T) {
	s := NewWebApp(&scheduler.ClusterContext{}, nil)
	s.StartWebApp()
	defer func() {
		if err := s.StopWebApp(); err != nil {
			t.Fatal("failed to stop webapp")
		}
	}()

	client := newTestClient()

	// With the default threshold (1500 bytes), the empty-context /ws/v1/clusters
	// endpoint returns a small JSON payload well below the threshold. Even when the
	// client requests gzip, the response must be sent uncompressed because compression
	// would add more bytes than it saves.
	req, err := http.NewRequest(http.MethodGet, base+"/ws/v1/clusters", nil)
	assert.NilError(t, err, "unexpected error creating request")
	req.Header.Set("Accept-Encoding", "gzip")

	//nolint:gosec
	resp, err := client.Do(req)
	assert.NilError(t, err, "unexpected error executing request")
	assert.Equal(t, resp.StatusCode, http.StatusOK, "expected 200 OK")

	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.NilError(t, err, "unexpected error reading response body")

	assert.Assert(t, len(body) < minCompressionSize,
		"test pre-condition: response must be smaller than minCompressionSize")
	assert.Equal(t, resp.Header.Get("Content-Encoding"), "",
		"small response must not be gzip-encoded even when gzip is requested")
	assert.Equal(t, resp.Header.Get("Vary"), "",
		"Vary header must not be set when response is not compressed")
}

func Test_GzipClientAcceptsGzip(t *testing.T) {
	tests := []struct {
		name           string
		acceptEncoding string
		want           bool
	}{
		{"empty header", "", false},
		{"gzip only", "gzip", true},
		{"gzip with whitespace", " gzip ", true},
		{"gzip, deflate", "gzip, deflate", true},
		{"deflate only", "deflate", false},
		{"gzip;q=0.5", "gzip;q=0.5", true},
		{"gzip;q=0 (excluded)", "gzip;q=0", false},
		{"uppercase GZIP", "GZIP", true},
		{"identity only", "identity", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, "/", nil)
			assert.NilError(t, err)
			if tt.acceptEncoding != "" {
				req.Header.Set("Accept-Encoding", tt.acceptEncoding)
			}
			got := clientAcceptsGzip(req)
			assert.Equal(t, got, tt.want, "clientAcceptsGzip(%q)", tt.acceptEncoding)
		})
	}
}

func Test_GzipExcludesEventStream(t *testing.T) {
	s := NewWebApp(&scheduler.ClusterContext{}, nil)
	s.StartWebApp()
	defer func() {
		if err := s.StopWebApp(); err != nil {
			t.Fatal("failed to stop webapp")
		}
	}()

	client := newTestClient()

	// The event-stream endpoint must never receive a gzip Content-Encoding header,
	// even when the client sends Accept-Encoding: gzip, because it uses SSE / Flusher.
	req, err := http.NewRequest(http.MethodGet, base+"/ws/v1/events/stream", nil)
	assert.NilError(t, err, "unexpected error creating request")
	req.Header.Set("Accept-Encoding", "gzip")

	//nolint:gosec
	resp, err := client.Do(req)
	assert.NilError(t, err, "unexpected error executing request")
	_ = resp.Body.Close()

	assert.Equal(t, resp.Header.Get("Content-Encoding"), "",
		"event-stream endpoint must not be gzip-encoded")
}

func Test_GzipVaryHeaderNotDuplicated(t *testing.T) {
	orig := minCompressionSize
	minCompressionSize = 0
	t.Cleanup(func() { minCompressionSize = orig })

	s := NewWebApp(&scheduler.ClusterContext{}, nil)
	s.StartWebApp()
	defer func() {
		if err := s.StopWebApp(); err != nil {
			t.Fatal("failed to stop webapp")
		}
	}()

	client := newTestClient()

	req, err := http.NewRequest(http.MethodGet, base+"/ws/v1/clusters", nil)
	assert.NilError(t, err)
	req.Header.Set("Accept-Encoding", "gzip")

	//nolint:gosec
	resp, err := client.Do(req)
	assert.NilError(t, err, "unexpected error executing request")
	_ = resp.Body.Close()

	vary := resp.Header["Vary"]
	// Vary header should be present exactly once with value "Accept-Encoding".
	assert.Equal(t, len(vary), 1, "expected exactly one Vary header value")
	assert.Equal(t, strings.TrimSpace(vary[0]), "Accept-Encoding", "unexpected Vary header value")
}
