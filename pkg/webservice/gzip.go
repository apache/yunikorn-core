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
	"net/http"
	"strings"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
)

// minCompressionSize is the minimum response body size in bytes required before gzip
// compression is applied. Responses smaller than this threshold are sent uncompressed
// because the gzip framing overhead would exceed any size savings. The value is set
// below the typical Ethernet MTU (1500 bytes) to account for TCP/IP and HTTP header
// overhead, ensuring a small response body fits within a single network packet.
var minCompressionSize = 1400

// deferredGzipResponseWriter buffers the first minCompressionSize bytes of the
// response body and defers the compression decision until either the buffer is full
// (switch to gzip streaming) or the handler returns (send raw if still under threshold).
// This avoids the memory cost of buffering the entire response while still skipping
// gzip for small payloads where the overhead outweighs the savings.
type deferredGzipResponseWriter struct {
	http.ResponseWriter
	buf        bytes.Buffer
	gz         *gzip.Writer
	statusCode int
	decided    bool
	useGzip    bool
}

// WriteHeader captures the status code until the compression decision is made, then
// forwards it to the underlying ResponseWriter.
func (d *deferredGzipResponseWriter) WriteHeader(code int) {
	if d.decided {
		d.ResponseWriter.WriteHeader(code)
		return
	}
	d.statusCode = code
}

// Write buffers incoming bytes until the threshold is reached, at which point it
// commits to gzip streaming. After the decision is made, writes go directly to the
// chosen writer.
func (d *deferredGzipResponseWriter) Write(b []byte) (int, error) {
	if d.decided {
		if d.useGzip {
			return d.gz.Write(b)
		}
		return d.ResponseWriter.Write(b)
	}

	n, err := d.buf.Write(b)
	if d.buf.Len() >= minCompressionSize {
		d.switchToGzip()
	}
	return n, err
}

// switchToGzip commits to gzip encoding: sets response headers, flushes the buffered
// bytes through the gzip writer, and marks the decision as final.
func (d *deferredGzipResponseWriter) switchToGzip() {
	d.decided = true
	d.useGzip = true
	d.ResponseWriter.Header().Set("Content-Encoding", "gzip")
	d.ResponseWriter.Header().Add("Vary", "Accept-Encoding")
	if d.statusCode != 0 {
		d.ResponseWriter.WriteHeader(d.statusCode)
	}
	if _, err := d.gz.Write(d.buf.Bytes()); err != nil {
		log.Log(log.REST).Error("failed to write buffered bytes to gzip writer",
			zap.Error(err))
	}
	d.buf.Reset()
}

// finalize is called via defer after the handler returns. If no compression decision
// was made yet (response stayed below threshold), the buffered bytes are written raw.
// The gzip writer is closed if compression was used.
func (d *deferredGzipResponseWriter) finalize() {
	if !d.decided {
		d.decided = true
		if d.statusCode != 0 {
			d.ResponseWriter.WriteHeader(d.statusCode)
		}
		if _, err := d.ResponseWriter.Write(d.buf.Bytes()); err != nil {
			log.Log(log.REST).Error("failed to write buffered response bytes",
				zap.Error(err))
		}
	}
	if d.useGzip {
		if closeErr := d.gz.Close(); closeErr != nil {
			log.Log(log.REST).Warn("failed to flush and close gzip writer",
				zap.Error(closeErr))
		}
	}
}

// compressResponse is an HTTP middleware that compresses the response body using
// gzip when the client declares gzip support in the Accept-Encoding request header
// (per RFC 9110 §12.5.3). Compression is only applied when the response body exceeds
// minCompressionSize bytes; smaller responses are sent uncompressed to avoid the
// gzip framing overhead exceeding the size savings.
//
// Responses are always served uncompressed when the client has not requested gzip,
// ensuring full backwards compatibility.
//
// The event-stream endpoint (/ws/v1/events/stream) is excluded because it uses
// server-sent events with http.Flusher, which requires writing directly to the
// underlying connection.
func compressResponse(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ws/v1/events/stream" || !clientAcceptsGzip(r) {
			next.ServeHTTP(w, r)
			return
		}

		gz, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
		if err != nil {
			// gzip.BestSpeed is always a valid level, so this branch is unreachable in
			// practice. Fall back to an uncompressed response rather than failing the call.
			log.Log(log.REST).Error("failed to create gzip writer, sending uncompressed response",
				zap.Error(err))
			next.ServeHTTP(w, r)
			return
		}

		dw := &deferredGzipResponseWriter{
			ResponseWriter: w,
			gz:             gz,
		}
		defer dw.finalize()
		next.ServeHTTP(dw, r)
	})
}

// clientAcceptsGzip reports whether the request's Accept-Encoding header lists gzip
// as an acceptable content encoding. The q=0 case (gzip explicitly excluded) is
// handled so that "gzip;q=0" correctly returns false.
func clientAcceptsGzip(r *http.Request) bool {
	for _, token := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		token = strings.TrimSpace(token)
		parts := strings.SplitN(token, ";", 2)
		if strings.ToLower(strings.TrimSpace(parts[0])) != "gzip" {
			continue
		}
		// "gzip" with no q-value means q=1.0 — acceptable.
		if len(parts) == 1 {
			return true
		}
		// "gzip;q=0" means explicitly not acceptable; any other q-value is acceptable.
		return strings.TrimSpace(parts[1]) != "q=0"
	}
	return false
}
