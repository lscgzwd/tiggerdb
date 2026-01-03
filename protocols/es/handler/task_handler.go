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

package handler

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/lscgzwd/tiggerdb/logger"
	"github.com/lscgzwd/tiggerdb/protocols/es/http/common"
)

// GetTask 获取任务状态
// GET /_tasks/{task_id}
func (h *DocumentHandler) GetTask(w http.ResponseWriter, r *http.Request) {
	taskID := mux.Vars(r)["task_id"]

	// 获取任务
	task := h.taskMgr.GetTask(taskID)
	if task == nil {
		common.HandleError(w, common.NewBadRequestError("task ["+taskID+"] not found"))
		return
	}

	// 计算运行时间
	var runningTimeNanos int64
	if task.StartedAt != nil {
		if task.CompletedAt != nil {
			runningTimeNanos = task.CompletedAt.Sub(*task.StartedAt).Nanoseconds()
		} else {
			runningTimeNanos = time.Since(*task.StartedAt).Nanoseconds()
		}
	}

	// 构建ES格式的响应
	response := map[string]interface{}{
		"completed": task.Status == TaskStatusCompleted || task.Status == TaskStatusFailed || task.Status == TaskStatusCancelled,
		"task": map[string]interface{}{
			"node":                  task.NodeID,
			"id":                    task.TaskID,
			"type":                  "transport",
			"action":                "indices:data/write/bulk[delete_by_query]",
			"description":           "delete_by_query [" + task.IndexName + "]",
			"start_time_in_millis":  task.CreatedAt.UnixMilli(),
			"running_time_in_nanos": runningTimeNanos,
			"cancellable":           true,
			"status": map[string]interface{}{
				"total":             task.Total,
				"updated":           0,
				"created":           0,
				"deleted":           task.Deleted,
				"batches":           task.Batches,
				"version_conflicts": task.VersionConflicts,
				"noops":             0,
				"retries": map[string]interface{}{
					"bulk":   0,
					"search": 0,
				},
				"throttled_millis":       0,
				"requests_per_second":    -1.0,
				"throttled_until_millis": 0,
			},
		},
	}

	// 如果任务失败，添加错误信息
	if task.Status == TaskStatusFailed {
		response["error"] = map[string]interface{}{
			"type":   "internal_server_error",
			"reason": task.Error,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(response); err != nil {
		logger.Error("Failed to encode task response: %v", err)
	}
}

// CancelTask 取消任务
// POST /_tasks/{task_id}/_cancel
func (h *DocumentHandler) CancelTask(w http.ResponseWriter, r *http.Request) {
	taskID := mux.Vars(r)["task_id"]

	// 取消任务
	cancelled := h.taskMgr.CancelTask(taskID)
	if !cancelled {
		common.HandleError(w, common.NewBadRequestError("task ["+taskID+"] not found or cannot be cancelled"))
		return
	}

	// 返回成功响应
	response := map[string]interface{}{
		"acknowledged": true,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(response); err != nil {
		logger.Error("Failed to encode cancel task response: %v", err)
	}
}
