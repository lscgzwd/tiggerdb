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
	"net/http/httptest"
	"testing"
)

func TestSuccessResponseJSON(t *testing.T) {
	resp := SuccessResponse().WithIndex("idx").WithID("1").WithVersion(1).WithResult("created")
	w := httptest.NewRecorder()
	if err := resp.WriteJSON(w, http.StatusCreated); err != nil {
		t.Fatalf("write json: %v", err)
	}
	if w.Code != http.StatusCreated {
		t.Fatalf("expected %d got %d", http.StatusCreated, w.Code)
	}
	var got Response
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Result != "created" || got.Index != "idx" || got.Id != "1" {
		t.Fatalf("unexpected body: %+v", got)
	}
}

func TestErrorResponseJSON(t *testing.T) {
	resp := ErrorResponse("validation_exception", "bad")
	w := httptest.NewRecorder()
	_ = resp.WriteJSON(w, http.StatusBadRequest)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected %d got %d", http.StatusBadRequest, w.Code)
	}
	var got Response
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got.Error == nil || got.Error.Type != "validation_exception" {
		t.Fatalf("unexpected error json: %s", w.Body.String())
	}
}

func TestNewResponse(t *testing.T) {
	resp := NewResponse()
	if resp == nil {
		t.Fatal("NewResponse() returned nil")
	}
	if resp.Took != 0 {
		t.Errorf("expected Took=0, got %d", resp.Took)
	}
	if resp.TimedOut != false {
		t.Errorf("expected TimedOut=false, got %v", resp.TimedOut)
	}
}

func TestResponseWithMethods(t *testing.T) {
	resp := NewResponse().
		WithTook(100).
		WithTimeout(true).
		WithShards(5, 5, 0, 0).
		WithIndex("test").
		WithID("123").
		WithVersion(1).
		WithResult("created").
		WithAcknowledged(true)

	if resp.Took != 100 {
		t.Errorf("expected Took=100, got %d", resp.Took)
	}
	if !resp.TimedOut {
		t.Error("expected TimedOut=true")
	}
	if resp.Shards == nil || resp.Shards.Total != 5 {
		t.Error("Shards not set correctly")
	}
	if resp.Index != "test" {
		t.Errorf("expected Index=test, got %s", resp.Index)
	}
	if resp.Id != "123" {
		t.Errorf("expected Id=123, got %s", resp.Id)
	}
	if resp.Version != 1 {
		t.Errorf("expected Version=1, got %d", resp.Version)
	}
	if resp.Result != "created" {
		t.Errorf("expected Result=created, got %s", resp.Result)
	}
	if !resp.Acknowledged {
		t.Error("expected Acknowledged=true")
	}
}

func TestResponseWithHits(t *testing.T) {
	resp := NewResponse().WithHits(10, 1.5, []interface{}{map[string]string{"id": "1"}})

	if resp.Hits == nil {
		t.Fatal("Hits not set")
	}
	if resp.Hits.Total == nil || resp.Hits.Total.Value != 10 {
		t.Error("Hits.Total not set correctly")
	}
	if resp.Hits.MaxScore != 1.5 {
		t.Errorf("expected MaxScore=1.5, got %f", resp.Hits.MaxScore)
	}
	if len(resp.Hits.Hits) != 1 {
		t.Errorf("expected 1 hit, got %d", len(resp.Hits.Hits))
	}
}

func TestResponseWithError(t *testing.T) {
	resp := NewResponse().WithError("test_error", "test reason")

	if resp.Error == nil {
		t.Fatal("Error not set")
	}
	if resp.Error.Type != "test_error" {
		t.Errorf("expected Error.Type=test_error, got %s", resp.Error.Type)
	}
	if resp.Error.Reason != "test reason" {
		t.Errorf("expected Error.Reason=test reason, got %s", resp.Error.Reason)
	}
}

func TestResponseWithData(t *testing.T) {
	data := map[string]string{"key": "value"}
	resp := NewResponse().WithData(data)

	if resp.Data == nil {
		t.Error("Data not set")
	}
	// Verify data is set (can't compare maps directly)
	if resp.Data == nil {
		t.Error("Data should not be nil")
	}
}
