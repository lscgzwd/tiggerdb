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
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestServerGetRouter(t *testing.T) {
	s, _ := NewServer(DefaultServerConfig())
	router := s.GetRouter()

	if router == nil {
		t.Fatal("GetRouter() returned nil")
	}
}

func TestServerAddRoutes(t *testing.T) {
	s, _ := NewServer(DefaultServerConfig())
	routes := []Route{
		{Method: "GET", Path: "/test1", Handler: func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }},
		{Method: "POST", Path: "/test2", Handler: func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusCreated) }},
	}

	s.AddRoutes(routes)

	mux := s.GetRouter().Build()
	req := httptest.NewRequest("GET", "/test1", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 got %d", w.Code)
	}
}

func TestServerAddHealthCheck(t *testing.T) {
	s, _ := NewServer(DefaultServerConfig())
	s.AddHealthCheck()

	mux := s.GetRouter().Build()
	req := httptest.NewRequest("GET", "/_health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 got %d", w.Code)
	}
}

func TestServerAddMetrics(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.EnableMetrics = true
	s, _ := NewServer(cfg)
	s.AddMetrics()

	mux := s.GetRouter().Build()
	req := httptest.NewRequest("GET", "/_metrics", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 got %d", w.Code)
	}
}

func TestServerIsRunning(t *testing.T) {
	s, _ := NewServer(DefaultServerConfig())

	if s.IsRunning() {
		t.Error("expected IsRunning() to return false before start")
	}
}

func TestServerStopNotStarted(t *testing.T) {
	s, _ := NewServer(DefaultServerConfig())
	ctx := context.Background()

	err := s.Stop(ctx)
	if err != nil {
		t.Errorf("Stop() on not started server should not error, got %v", err)
	}
}

func TestServerStartAlreadyStarted(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.Port = 9203
	s, _ := NewServer(cfg)

	// Start server in background
	go func() {
		_ = s.Start()
	}()
	time.Sleep(50 * time.Millisecond)

	// Try to start again
	err := s.Start()
	if err == nil {
		t.Error("expected error when starting already started server")
	}

	// Cleanup
	_ = s.Stop(context.Background())
	time.Sleep(50 * time.Millisecond)
}

func TestServerHealthCheckHandler(t *testing.T) {
	s, _ := NewServer(DefaultServerConfig())
	s.AddRoute("GET", "/_health", s.healthCheckHandler)

	mux := s.GetRouter().Build()
	req := httptest.NewRequest("GET", "/_health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 got %d", w.Code)
	}
}

func TestServerMetricsHandler(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.EnableMetrics = true
	s, _ := NewServer(cfg)
	s.AddRoute("GET", "/_metrics", s.metricsHandler)

	mux := s.GetRouter().Build()
	req := httptest.NewRequest("GET", "/_metrics", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 got %d", w.Code)
	}
}
