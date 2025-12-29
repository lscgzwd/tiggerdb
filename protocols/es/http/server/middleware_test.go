// Copyright (c) 2024 TigerDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLoggingMiddleware(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	logging := LoggingMiddleware(h)
	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	logging.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", w.Code)
	}
}

func TestCORSMiddleware(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	cors := CORSMiddleware([]string{"http://localhost:3000"})(h)
	req := httptest.NewRequest("OPTIONS", "/test", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	w := httptest.NewRecorder()
	cors.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", w.Code)
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Fatalf("cors header missing")
	}
}

func TestRateLimitMiddleware(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	rl := RateLimitMiddleware(60)(h) // 1 token per second
	time.Sleep(1100 * time.Millisecond)
	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	rl.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", w.Code)
	}
}

func TestTimeoutMiddleware(t *testing.T) {
	slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})
	to := TimeoutMiddleware(50 * time.Millisecond)(slow)
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	to.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 got %d", w.Code)
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	panicH := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { panic("boom") })
	rec := RecoveryMiddleware(panicH)
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	rec.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 got %d", w.Code)
	}
}

func TestSecurityHeadersMiddleware(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	sec := SecurityHeadersMiddleware(h)
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	sec.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200")
	}
	if w.Header().Get("X-Frame-Options") != "DENY" {
		t.Fatalf("security header missing")
	}
}

func TestChainMiddleware(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	ch := ChainMiddleware(LoggingMiddleware, SecurityHeadersMiddleware)(h)
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	ch.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200")
	}
	if w.Header().Get("X-XSS-Protection") == "" {
		t.Fatalf("expected security headers")
	}
}

func TestRequestSizeLimitMiddleware(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	sizeLimit := RequestSizeLimitMiddleware(100)(h)
	// small
	small := strings.Repeat("x", 50)
	req := httptest.NewRequest("POST", "/", strings.NewReader(small))
	req.ContentLength = int64(len(small))
	w := httptest.NewRecorder()
	sizeLimit.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", w.Code)
	}
	// large
	large := strings.Repeat("x", 200)
	req2 := httptest.NewRequest("POST", "/", strings.NewReader(large))
	req2.ContentLength = int64(len(large))
	w2 := httptest.NewRecorder()
	sizeLimit.ServeHTTP(w2, req2)
	if w2.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 got %d", w2.Code)
	}
}

func TestMetricsMiddleware(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	metrics := MetricsMiddleware(h)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	metrics.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", w.Code)
	}
}

func TestDefaultMiddlewareStack(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.EnableCORS = true
	cfg.EnableRateLimit = false
	cfg.EnableMetrics = true

	middleware := DefaultMiddlewareStack(cfg)

	if middleware == nil {
		t.Fatal("DefaultMiddlewareStack() returned nil")
	}

	// Test that middleware chain works
	handler := func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }
	wrapped := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	wrapped(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", w.Code)
	}
}

func TestDefaultMiddlewareStackWithRateLimit(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.EnableRateLimit = true
	cfg.RateLimitRPM = 1000

	middleware := DefaultMiddlewareStack(cfg)

	if middleware == nil {
		t.Fatal("DefaultMiddlewareStack() returned nil")
	}
}

func TestRateLimitMiddlewareDisabled(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	rl := RateLimitMiddleware(0)(h) // Disabled

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	rl.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", w.Code)
	}
}

func TestTimeoutMiddlewareDisabled(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	to := TimeoutMiddleware(0)(h) // Disabled

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	to.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", w.Code)
	}
}
