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
	"testing"
	"time"
)

func TestServerConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *ServerConfig
		wantErr bool
	}{
		{
			name:    "valid config",
			config:  DefaultServerConfig(),
			wantErr: false,
		},
		{
			name: "invalid port too low",
			config: func() *ServerConfig {
				cfg := DefaultServerConfig()
				cfg.Port = 0
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "invalid port too high",
			config: func() *ServerConfig {
				cfg := DefaultServerConfig()
				cfg.Port = 65536
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "invalid max_connections",
			config: func() *ServerConfig {
				cfg := DefaultServerConfig()
				cfg.MaxConnections = 0
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "invalid rate_limit_rpm negative",
			config: func() *ServerConfig {
				cfg := DefaultServerConfig()
				cfg.RateLimitRPM = -1
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "invalid max_request_size",
			config: func() *ServerConfig {
				cfg := DefaultServerConfig()
				cfg.MaxRequestSize = 0
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "invalid shutdown_timeout",
			config: func() *ServerConfig {
				cfg := DefaultServerConfig()
				cfg.ShutdownTimeout = 0
				return cfg
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServerConfigAddress(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = 9200

	addr := cfg.Address()
	if addr != "127.0.0.1:9200" {
		t.Errorf("Address() = %v, want 127.0.0.1:9200", addr)
	}
}

func TestServerConfigBaseURL(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = 9200

	baseURL := cfg.BaseURL()
	if baseURL != "http://127.0.0.1:9200" {
		t.Errorf("BaseURL() = %v, want http://127.0.0.1:9200", baseURL)
	}

	// Test HTTPS
	cfg.TLSEnable = true
	baseURL = cfg.BaseURL()
	if baseURL != "https://127.0.0.1:9200" {
		t.Errorf("BaseURL() with TLS = %v, want https://127.0.0.1:9200", baseURL)
	}
}

func TestServerConfigClone(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.CORSOrigins = []string{"http://localhost:3000", "http://localhost:3001"}

	clone := cfg.Clone()

	if clone == cfg {
		t.Error("Clone() returned same reference")
	}

	if len(clone.CORSOrigins) != len(cfg.CORSOrigins) {
		t.Errorf("Clone() CORSOrigins length = %v, want %v", len(clone.CORSOrigins), len(cfg.CORSOrigins))
	}

	// Modify clone and verify original is unchanged
	clone.CORSOrigins[0] = "modified"
	if cfg.CORSOrigins[0] == "modified" {
		t.Error("Clone() did not create deep copy of CORSOrigins")
	}
}

func TestDefaultServerConfig(t *testing.T) {
	cfg := DefaultServerConfig()

	if cfg.Host != "0.0.0.0" {
		t.Errorf("Default Host = %v, want 0.0.0.0", cfg.Host)
	}

	if cfg.Port != 9200 {
		t.Errorf("Default Port = %v, want 9200", cfg.Port)
	}

	if cfg.ReadTimeout != 30*time.Second {
		t.Errorf("Default ReadTimeout = %v, want 30s", cfg.ReadTimeout)
	}

	if cfg.MaxConnections != 1000 {
		t.Errorf("Default MaxConnections = %v, want 1000", cfg.MaxConnections)
	}

	if cfg.HealthPath != "/_health" {
		t.Errorf("Default HealthPath = %v, want /_health", cfg.HealthPath)
	}

	if cfg.MetricsPath != "/_metrics" {
		t.Errorf("Default MetricsPath = %v, want /_metrics", cfg.MetricsPath)
	}
}
