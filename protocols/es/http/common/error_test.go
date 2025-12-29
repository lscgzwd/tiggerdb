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

package common

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestNewIndexNotFoundError(t *testing.T) {
	err := NewIndexNotFoundError("test_index")

	if err.Type() != "index_not_found_exception" {
		t.Errorf("expected type index_not_found_exception, got %s", err.Type())
	}
	if err.StatusCode() != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, err.StatusCode())
	}
	resp := err.Response()
	if resp.Error == nil || resp.Error.Type != "index_not_found_exception" {
		t.Fatalf("expected error response set with correct type")
	}
}

func TestNewBadRequestError(t *testing.T) {
	err := NewBadRequestError("invalid param")
	if err.Type() != "illegal_argument_exception" {
		t.Errorf("unexpected type: %s", err.Type())
	}
	if err.StatusCode() != http.StatusBadRequest {
		t.Errorf("unexpected status: %d", err.StatusCode())
	}
}

func TestHandleErrorWritesJSON(t *testing.T) {
	w := httptestResponseWriter{}
	err := NewInternalServerError("boom")
	HandleError(&w, err)
	if w.status != http.StatusInternalServerError {
		t.Fatalf("expected %d got %d", http.StatusInternalServerError, w.status)
	}
	var r Response
	if json.Unmarshal(w.body, &r) != nil || r.Error == nil || r.Error.Type != "internal_server_error" {
		t.Fatalf("unexpected json body: %s", string(w.body))
	}
}

func TestNewDocumentNotFoundError(t *testing.T) {
	err := NewDocumentNotFoundError("test_index", "doc123")

	if err.Type() != "not_found" {
		t.Errorf("expected type 'not_found', got '%s'", err.Type())
	}
	if err.StatusCode() != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, err.StatusCode())
	}
}

func TestNewConflictError(t *testing.T) {
	err := NewConflictError("version conflict")

	if err.Type() != "version_conflict_engine_exception" {
		t.Errorf("expected type 'version_conflict_engine_exception', got '%s'", err.Type())
	}
	if err.StatusCode() != http.StatusConflict {
		t.Errorf("expected status %d, got %d", http.StatusConflict, err.StatusCode())
	}
}

func TestHandleSuccess(t *testing.T) {
	w := httptestResponseWriter{}
	resp := SuccessResponse().WithIndex("test")
	HandleSuccess(&w, resp, http.StatusCreated)

	if w.status != http.StatusCreated {
		t.Fatalf("expected %d got %d", http.StatusCreated, w.status)
	}
}

func TestBaseErrorResponse(t *testing.T) {
	err := &BaseError{
		ErrType:    "test_error",
		Message:    "test message",
		HTTPStatus: http.StatusBadRequest,
		Index:      "test_index",
		Shard:      "0",
	}

	resp := err.Response()
	if resp.Error == nil {
		t.Fatal("Response() did not set Error")
	}
	if resp.Error.Type != "test_error" {
		t.Errorf("expected error type 'test_error', got '%s'", resp.Error.Type)
	}
	if resp.Error.Index != "test_index" {
		t.Errorf("expected error index 'test_index', got '%s'", resp.Error.Index)
	}
	if resp.Error.Shard != "0" {
		t.Errorf("expected error shard '0', got '%s'", resp.Error.Shard)
	}
}

// minimal test writer

type httptestResponseWriter struct {
	header http.Header
	body   []byte
	status int
}

func (w *httptestResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = http.Header{}
	}
	return w.header
}
func (w *httptestResponseWriter) Write(b []byte) (int, error) {
	w.body = append(w.body, b...)
	return len(b), nil
}
func (w *httptestResponseWriter) WriteHeader(statusCode int) { w.status = statusCode }
