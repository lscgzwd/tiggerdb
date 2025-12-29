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
	"testing"
)

// helper to serve using router's mux
func serve(r *Router, req *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	mux := r.Build()
	mux.ServeHTTP(w, req)
	return w
}

func TestNewRouter(t *testing.T) {
	router := NewRouter()
	if router == nil || router.muxRouter == nil || router.routes == nil {
		t.Fatalf("router not initialized")
	}
}

func TestRouterAddRoute(t *testing.T) {
	r := NewRouter()
	h := func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }
	r.AddRoute("GET", "/test", h)
	if len(r.routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(r.routes))
	}
}

func TestRouterServeHTTP(t *testing.T) {
	r := NewRouter()
	r.AddRoute("GET", "/hello", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	req := httptest.NewRequest("GET", "/hello", nil)
	w := serve(r, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", w.Code)
	}
}

func TestRouterAddRouteWithMiddleware(t *testing.T) {
	r := NewRouter()
	h := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Test", "ok")
		w.WriteHeader(http.StatusOK)
	}
	mw := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) { w.Header().Set("X-M", "1"); next(w, r) }
	}
	r.AddRoute("GET", "/mw", h, mw)
	req := httptest.NewRequest("GET", "/mw", nil)
	w := serve(r, req)
	if w.Header().Get("X-M") != "1" {
		t.Fatalf("middleware not applied")
	}
}

func TestRouterNotFound(t *testing.T) {
	r := NewRouter()
	req := httptest.NewRequest("GET", "/nonexist", nil)
	w := serve(r, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 got %d", w.Code)
	}
}

func TestRouterMethodNotAllowed(t *testing.T) {
	r := NewRouter()
	r.AddRoute("GET", "/test", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	req := httptest.NewRequest("POST", "/test", nil)
	w := serve(r, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 got %d", w.Code)
	}
}

// ES相关测试已迁移到protocols/es/handler
