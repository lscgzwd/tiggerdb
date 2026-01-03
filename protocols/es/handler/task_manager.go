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
	"sync"
	"time"

	"github.com/google/uuid"
	bleve "github.com/lscgzwd/tiggerdb"
	"github.com/lscgzwd/tiggerdb/protocols/es/search/dsl"
	"github.com/lscgzwd/tiggerdb/search/query"
)

// TaskStatus 任务状态
type TaskStatus string

const (
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// DeleteTask 删除任务
type DeleteTask struct {
	TaskID           string                 `json:"task_id"`
	NodeID           string                 `json:"node_id"`
	IndexName        string                 `json:"index_name"`
	Query            map[string]interface{} `json:"query"` // 原始查询
	BleveQuery       query.Query            `json:"-"`     // 解析后的Bleve查询
	Status           TaskStatus             `json:"status"`
	Total            int64                  `json:"total"`
	Deleted          int64                  `json:"deleted"`
	Batches          int64                  `json:"batches"`
	VersionConflicts int64                  `json:"version_conflicts"`
	CreatedAt        time.Time              `json:"created_at"`
	StartedAt        *time.Time             `json:"started_at,omitempty"`
	CompletedAt      *time.Time             `json:"completed_at,omitempty"`
	Error            string                 `json:"error,omitempty"`
	mutex            sync.RWMutex           `json:"-"` // 保护并发访问
}

// TaskManager 任务管理器
// 负责管理异步删除任务，支持任务查询、取消等功能
type TaskManager struct {
	tasks  map[string]*DeleteTask
	mutex  sync.RWMutex
	nodeID string // 节点ID，用于生成task_id
}

// NewTaskManager 创建任务管理器
func NewTaskManager() *TaskManager {
	return &TaskManager{
		tasks:  make(map[string]*DeleteTask),
		nodeID: "node-1", // 单节点模式下固定节点ID
	}
}

// CreateDeleteTask 创建删除任务
func (tm *TaskManager) CreateDeleteTask(
	indexName string,
	query map[string]interface{},
	bleveQuery query.Query,
) *DeleteTask {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	taskID := uuid.New().String()
	fullTaskID := tm.nodeID + ":" + taskID

	task := &DeleteTask{
		TaskID:           fullTaskID,
		NodeID:           tm.nodeID,
		IndexName:        indexName,
		Query:            query,
		BleveQuery:       bleveQuery,
		Status:           TaskStatusRunning,
		Total:            0,
		Deleted:          0,
		Batches:          0,
		VersionConflicts: 0,
		CreatedAt:        time.Now(),
		StartedAt:        nil,
		CompletedAt:      nil,
	}

	tm.tasks[fullTaskID] = task
	return task
}

// GetTask 获取任务
func (tm *TaskManager) GetTask(taskID string) *DeleteTask {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	task, exists := tm.tasks[taskID]
	if !exists {
		return nil
	}

	// 返回任务副本，避免外部修改
	return &DeleteTask{
		TaskID:           task.TaskID,
		NodeID:           task.NodeID,
		IndexName:        task.IndexName,
		Query:            task.Query,
		BleveQuery:       task.BleveQuery,
		Status:           task.Status,
		Total:            task.Total,
		Deleted:          task.Deleted,
		Batches:          task.Batches,
		VersionConflicts: task.VersionConflicts,
		CreatedAt:        task.CreatedAt,
		StartedAt:        task.StartedAt,
		CompletedAt:      task.CompletedAt,
		Error:            task.Error,
	}
}

// UpdateTask 更新任务状态
func (tm *TaskManager) UpdateTask(taskID string, updater func(*DeleteTask)) {
	tm.mutex.RLock()
	task, exists := tm.tasks[taskID]
	tm.mutex.RUnlock()

	if !exists {
		return
	}

	task.mutex.Lock()
	defer task.mutex.Unlock()

	updater(task)
}

// CompleteTask 完成任务
func (tm *TaskManager) CompleteTask(taskID string, deleted, batches, versionConflicts int64) {
	tm.UpdateTask(taskID, func(task *DeleteTask) {
		task.Status = TaskStatusCompleted
		task.Deleted = deleted
		task.Batches = batches
		task.VersionConflicts = versionConflicts
		now := time.Now()
		task.CompletedAt = &now
	})
}

// FailTask 标记任务失败
func (tm *TaskManager) FailTask(taskID string, err error) {
	tm.UpdateTask(taskID, func(task *DeleteTask) {
		task.Status = TaskStatusFailed
		task.Error = err.Error()
		now := time.Now()
		task.CompletedAt = &now
	})
}

// CancelTask 取消任务
func (tm *TaskManager) CancelTask(taskID string) bool {
	tm.mutex.RLock()
	task, exists := tm.tasks[taskID]
	tm.mutex.RUnlock()

	if !exists {
		return false
	}

	task.mutex.Lock()
	defer task.mutex.Unlock()

	if task.Status == TaskStatusRunning {
		task.Status = TaskStatusCancelled
		now := time.Now()
		task.CompletedAt = &now
		return true
	}

	return false
}

// CleanupOldTasks 清理旧任务（超过指定时间的已完成/失败/取消任务）
func (tm *TaskManager) CleanupOldTasks(maxAge time.Duration) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	now := time.Now()
	for taskID, task := range tm.tasks {
		if task.CompletedAt != nil {
			if now.Sub(*task.CompletedAt) > maxAge {
				delete(tm.tasks, taskID)
			}
		}
	}
}

// executeDeleteTask 执行删除任务（后台goroutine）
func (h *DocumentHandler) executeDeleteTask(task *DeleteTask) {
	// 标记任务开始
	task.mutex.Lock()
	now := time.Now()
	task.StartedAt = &now
	task.mutex.Unlock()

	// 获取索引实例
	idx, err := h.indexMgr.GetIndex(task.IndexName)
	if err != nil {
		h.taskMgr.FailTask(task.TaskID, err)
		return
	}

	// 解析查询（如果还没有解析）
	if task.BleveQuery == nil {
		parser := dsl.NewQueryParser()
		bleveQuery, parseErr := parser.ParseQuery(task.Query)
		if parseErr != nil {
			h.taskMgr.FailTask(task.TaskID, parseErr)
			return
		}
		task.BleveQuery = bleveQuery
	}

	// 搜索所有匹配的文档
	searchReq := bleve.NewSearchRequest(task.BleveQuery)
	searchReq.Fields = []string{"_id"} // 只需要ID字段
	searchReq.Size = 10000000          // 最多处理1000万文档
	searchReq.From = 0

	searchResults, err := idx.Search(searchReq)
	if err != nil {
		h.taskMgr.FailTask(task.TaskID, err)
		return
	}

	// 更新总数
	h.taskMgr.UpdateTask(task.TaskID, func(t *DeleteTask) {
		t.Total = int64(len(searchResults.Hits))
	})

	// 批量删除所有匹配文档
	deleted := int64(0)
	batches := int64(0)
	versionConflicts := int64(0)

	if len(searchResults.Hits) > 0 {
		batch := idx.NewBatch()
		batchSize := 0

		for _, hit := range searchResults.Hits {
			// 检查任务是否被取消
			task.mutex.RLock()
			cancelled := task.Status == TaskStatusCancelled
			task.mutex.RUnlock()

			if cancelled {
				break
			}

			batch.Delete(hit.ID)
			batchSize++

			// 每1000个文档执行一次batch，避免内存占用过大
			if batchSize >= 1000 {
				if err := idx.Batch(batch); err != nil {
					versionConflicts += int64(batchSize)
				} else {
					batches++
					deleted += int64(batchSize)
				}
				batch = idx.NewBatch()
				batchSize = 0

				// 更新进度
				h.taskMgr.UpdateTask(task.TaskID, func(t *DeleteTask) {
					t.Deleted = deleted
					t.Batches = batches
					t.VersionConflicts = versionConflicts
				})
			}
		}

		// 执行剩余的batch
		if batchSize > 0 {
			if err := idx.Batch(batch); err != nil {
				versionConflicts += int64(batchSize)
			} else {
				batches++
				deleted += int64(batchSize)
			}
		}
	}

	// 完成任务
	h.taskMgr.CompleteTask(task.TaskID, deleted, batches, versionConflicts)
}
