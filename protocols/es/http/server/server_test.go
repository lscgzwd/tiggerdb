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

func TestNewServer(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = 9201
	s, err := NewServer(cfg)
	if err != nil || s == nil {
		t.Fatalf("server create failed: %v", err)
	}
}

func TestServerConfig(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.Port = 9200
	if cfg.Address() == "" || cfg.BaseURL() == "" {
		t.Fatalf("address/baseURL empty")
	}
}

func TestServerDefaultRoutes(t *testing.T) {
	scfg := DefaultServerConfig()
	scfg.Host = "127.0.0.1"
	scfg.Port = 9202
	s, _ := NewServer(scfg)
	s.AddDefaultRoutes()
	mux := s.GetRouter().Build()
	req := httptest.NewRequest("GET", "/_health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", w.Code)
	}
}
