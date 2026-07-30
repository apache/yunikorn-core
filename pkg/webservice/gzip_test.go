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
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

// largeBody returns a compressible body comfortably above minCompressionSize so the
// middleware commits to compression rather than passing the buffered bytes through.
func largeBody() []byte {
	return bytes.Repeat([]byte("yunikorn metrics payload "), 200)
}

// incompressibleBody returns a deterministic high-entropy body whose *gzipped* form is
// still well above minCompressionSize. Self-encoded handler tests need this: a highly
// compressible body would gzip down below the threshold, the middleware would never
// commit to a second compression pass, and the test would pass even against the
// double-encoding bug it is meant to catch.
func incompressibleBody(t *testing.T) []byte {
	t.Helper()
	// Deterministic xorshift keeps the test reproducible without depending on
	// math/rand defaults across Go versions. The length is a multiple of 4 so each
	// round fills a whole word.
	out := make([]byte, 16*1024)
	state := uint32(0x9E3779B9)
	for i := 0; i < len(out); i += 4 {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		binary.LittleEndian.PutUint32(out[i:], state)
	}

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(out)
	assert.NilError(t, err)
	assert.NilError(t, zw.Close())
	assert.Assert(t, buf.Len() > minCompressionSize,
		"gzipped fixture must exceed minCompressionSize to exercise the double-encoding path, got %d", buf.Len())

	return out
}

func gunzip(t *testing.T, b []byte) []byte {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(b))
	assert.NilError(t, err, "body is not valid gzip")
	defer zr.Close()
	out, err := io.ReadAll(zr)
	assert.NilError(t, err)
	return out
}

func serve(handler http.Handler, acceptEncoding string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/ws/v1/metrics", nil)
	if acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}
	rec := httptest.NewRecorder()
	compressResponse(handler).ServeHTTP(rec, req)
	return rec
}

// TestCompressResponseSelfEncodedHandler covers the case where the wrapped handler
// compresses the body itself and sets Content-Encoding, as promhttp does when the
// client advertises gzip support. The middleware must not compress a second time:
// a single gzip decode has to yield the original payload, otherwise clients such as
// the Prometheus scraper fail to parse the response.
func TestCompressResponseSelfEncodedHandler(t *testing.T) {
	payload := incompressibleBody(t)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		zw := gzip.NewWriter(w)
		_, err := zw.Write(payload)
		assert.NilError(t, err)
		assert.NilError(t, zw.Close())
	})

	rec := serve(handler, "gzip")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
	// promhttp sets Content-Encoding but never Vary, so the middleware must add it.
	assert.Equal(t, "Accept-Encoding", rec.Header().Get("Vary"))
	// Exactly one layer of gzip: decoding once must produce the original bytes.
	assert.DeepEqual(t, payload, gunzip(t, rec.Body.Bytes()))
}

// TestCompressResponseSelfEncodedSmallBody covers a self-encoded body that stays below
// minCompressionSize. The buffered bytes must still be flushed verbatim rather than
// being dropped or re-encoded when the handler returns.
func TestCompressResponseSelfEncodedSmallBody(t *testing.T) {
	payload := []byte("small self-encoded payload")

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		zw := gzip.NewWriter(w)
		_, err := zw.Write(payload)
		assert.NilError(t, err)
		assert.NilError(t, zw.Close())
	})

	rec := serve(handler, "gzip")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
	assert.Equal(t, "Accept-Encoding", rec.Header().Get("Vary"))
	assert.DeepEqual(t, payload, gunzip(t, rec.Body.Bytes()))
}

// TestCompressResponseSelfEncodedPreservesStatus verifies that a status code set
// before the first write survives the pass-through path.
func TestCompressResponseSelfEncodedPreservesStatus(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusAccepted)
		zw := gzip.NewWriter(w)
		_, err := zw.Write(incompressibleBody(t))
		assert.NilError(t, err)
		assert.NilError(t, zw.Close())
	})

	rec := serve(handler, "gzip")

	assert.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
	assert.Equal(t, "Accept-Encoding", rec.Header().Get("Vary"))
}

// TestCompressResponseCompressesLargeBody is the happy path: a plain handler with a
// body over the threshold gets compressed by the middleware.
func TestCompressResponseCompressesLargeBody(t *testing.T) {
	payload := largeBody()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write(payload)
		assert.NilError(t, err)
	})

	rec := serve(handler, "gzip")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
	assert.Equal(t, "Accept-Encoding", rec.Header().Get("Vary"))
	assert.Assert(t, rec.Body.Len() < len(payload), "body was not compressed")
	assert.DeepEqual(t, payload, gunzip(t, rec.Body.Bytes()))
}

// TestCompressResponseSmallBodyUncompressed verifies the sub-threshold path still
// sends the body raw.
func TestCompressResponseSmallBodyUncompressed(t *testing.T) {
	payload := []byte("tiny")

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write(payload)
		assert.NilError(t, err)
	})

	rec := serve(handler, "gzip")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "", rec.Header().Get("Content-Encoding"))
	// Nothing was encoded, so neither header is set - see Test_GzipMinCompressionSize.
	assert.Equal(t, "", rec.Header().Get("Vary"))
	assert.DeepEqual(t, payload, rec.Body.Bytes())
}

// TestCompressResponseNoGzipRequested verifies a client that does not advertise gzip
// always receives an uncompressed body.
func TestCompressResponseNoGzipRequested(t *testing.T) {
	payload := largeBody()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write(payload)
		assert.NilError(t, err)
	})

	rec := serve(handler, "")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "", rec.Header().Get("Content-Encoding"))
	// The middleware hands off before wrapping, so it adds no headers of its own.
	assert.Equal(t, "", rec.Header().Get("Vary"))
	assert.DeepEqual(t, payload, rec.Body.Bytes())
}

func TestClientAcceptsGzip(t *testing.T) {
	tests := []struct {
		acceptEncoding string
		expected       bool
	}{
		{"", false},
		{"gzip", true},
		{"GZIP", true},
		{" gzip ", true},
		{"gzip;q=1.0", true},
		{"gzip;q=0.5", true},
		{"gzip;q=0", false},
		{"deflate, gzip", true},
		{"deflate", false},
		{"identity", false},
		{"deflate, gzip;q=0", false},
	}

	for _, tt := range tests {
		t.Run(strings.ReplaceAll(tt.acceptEncoding, " ", "_"), func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/ws/v1/metrics", nil)
			if tt.acceptEncoding != "" {
				req.Header.Set("Accept-Encoding", tt.acceptEncoding)
			}
			assert.Equal(t, tt.expected, clientAcceptsGzip(req))
		})
	}
}
