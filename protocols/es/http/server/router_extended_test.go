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

func TestRouterAddRoutes(t *testing.T) {
	r := NewRouter()
	routes := []Route{
		{Method: "GET", Path: "/route1", Handler: func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }},
		{Method: "POST", Path: "/route2", Handler: func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusCreated) }},
	}

	r.AddRoutes(routes)

	if len(r.routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(r.routes))
	}
}

func TestRouterBuild(t *testing.T) {
	r := NewRouter()
	r.AddRoute("GET", "/test", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })

	mux := r.Build()
	if mux == nil {
		t.Fatal("Build() returned nil")
	}

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 got %d", w.Code)
	}
}

func TestRouterBuildWithMiddleware(t *testing.T) {
	r := NewRouter()
	mw := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Middleware", "applied")
			next(w, r)
		}
	}

	r.AddRoute("GET", "/test", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }, mw)

	mux := r.Build()
	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Header().Get("X-Middleware") != "applied" {
		t.Error("middleware not applied in Build()")
	}
}

// ES相关测试已迁移到protocols/es/handler
