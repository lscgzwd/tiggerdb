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

package version

import (
	"fmt"
	"time"
)

// Migration 迁移定义
type Migration struct {
	Version     int64     `json:"version"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	UpScript    string    `json:"up_script"`
	DownScript  string    `json:"down_script"`
	CreatedAt   time.Time `json:"created_at"`
}

// MigrationManager 迁移管理器
type MigrationManager struct {
	migrations []Migration
}

// NewMigrationManager 创建迁移管理器
func NewMigrationManager() *MigrationManager {
	return &MigrationManager{
		migrations: []Migration{},
	}
}

// RegisterMigration 注册迁移
func (mm *MigrationManager) RegisterMigration(migration Migration) error {
	// 验证迁移
	if err := mm.validateMigration(migration); err != nil {
		return fmt.Errorf("invalid migration: %w", err)
	}

	// 检查版本冲突
	for _, existing := range mm.migrations {
		if existing.Version == migration.Version {
			return fmt.Errorf("migration version %d already exists", migration.Version)
		}
	}

	mm.migrations = append(mm.migrations, migration)
	return nil
}

// GetMigration 获取迁移
func (mm *MigrationManager) GetMigration(version int64) (*Migration, error) {
	for _, migration := range mm.migrations {
		if migration.Version == version {
			return &migration, nil
		}
	}
	return nil, fmt.Errorf("migration version %d not found", version)
}

// ListMigrations 列出所有迁移
func (mm *MigrationManager) ListMigrations() []Migration {
	return mm.migrations
}

// GetPendingMigrations 获取待执行的迁移
func (mm *MigrationManager) GetPendingMigrations(currentVersion int64) []Migration {
	var pending []Migration
	for _, migration := range mm.migrations {
		if migration.Version > currentVersion {
			pending = append(pending, migration)
		}
	}
	return pending
}

// validateMigration 验证迁移
func (mm *MigrationManager) validateMigration(migration Migration) error {
	if migration.Version <= 0 {
		return fmt.Errorf("version must be positive")
	}

	if migration.Name == "" {
		return fmt.Errorf("name cannot be empty")
	}

	if migration.UpScript == "" {
		return fmt.Errorf("up script cannot be empty")
	}

	return nil
}

// MigrationPlan 迁移计划
type MigrationPlan struct {
	CurrentVersion int64       `json:"current_version"`
	TargetVersion  int64       `json:"target_version"`
	Migrations     []Migration `json:"migrations"`
	Direction      string      `json:"direction"` // "up" or "down"
}

// CreateMigrationPlan 创建迁移计划
func (mm *MigrationManager) CreateMigrationPlan(currentVersion, targetVersion int64) (*MigrationPlan, error) {
	plan := &MigrationPlan{
		CurrentVersion: currentVersion,
		TargetVersion:  targetVersion,
	}

	if targetVersion > currentVersion {
		// 升级迁移
		plan.Direction = "up"
		plan.Migrations = mm.GetPendingMigrations(currentVersion)
	} else if targetVersion < currentVersion {
		// 降级迁移
		plan.Direction = "down"
		// 这里需要实现降级迁移的逻辑
		plan.Migrations = []Migration{} // 暂时为空
	} else {
		// 版本相同，无需迁移
		plan.Direction = "none"
		plan.Migrations = []Migration{}
	}

	return plan, nil
}

// ValidateMigrationPlan 验证迁移计划
func (mm *MigrationPlan) ValidateMigrationPlan() error {
	if mm.Direction == "none" {
		return nil
	}

	if len(mm.Migrations) == 0 {
		return fmt.Errorf("no migrations found for %s migration", mm.Direction)
	}

	// 验证迁移顺序
	if mm.Direction == "up" {
		for i := 1; i < len(mm.Migrations); i++ {
			if mm.Migrations[i].Version <= mm.Migrations[i-1].Version {
				return fmt.Errorf("migration versions not in ascending order")
			}
		}
	}

	return nil
}

// ExecuteMigrationPlan 执行迁移计划
func (mm *MigrationPlan) ExecuteMigrationPlan() error {
	if err := mm.ValidateMigrationPlan(); err != nil {
		return err
	}

	for _, migration := range mm.Migrations {
		if mm.Direction == "up" {
			err := mm.executeUpMigration(migration)
			if err != nil {
				return fmt.Errorf("failed to execute up migration %d: %w", migration.Version, err)
			}
		} else if mm.Direction == "down" {
			err := mm.executeDownMigration(migration)
			if err != nil {
				return fmt.Errorf("failed to execute down migration %d: %w", migration.Version, err)
			}
		}
	}

	return nil
}

// executeUpMigration 执行升级迁移
func (mm *MigrationPlan) executeUpMigration(migration Migration) error {
	// 这里需要实现实际的迁移执行逻辑
	// 例如：执行SQL脚本、更新元数据等

	fmt.Printf("Executing up migration: %s (version %d)\n", migration.Name, migration.Version)
	fmt.Printf("Description: %s\n", migration.Description)
	fmt.Printf("Script: %s\n", migration.UpScript)

	// 模拟执行
	time.Sleep(100 * time.Millisecond)

	return nil
}

// executeDownMigration 执行降级迁移
func (mm *MigrationPlan) executeDownMigration(migration Migration) error {
	// 这里需要实现实际的降级迁移执行逻辑

	fmt.Printf("Executing down migration: %s (version %d)\n", migration.Name, migration.Version)
	fmt.Printf("Script: %s\n", migration.DownScript)

	// 模拟执行
	time.Sleep(100 * time.Millisecond)

	return nil
}

// CreateInitialMigration 创建初始迁移
func CreateInitialMigration() Migration {
	return Migration{
		Version:     1,
		Name:        "initial_schema",
		Description: "Create initial database schema",
		UpScript: `CREATE TABLE IF NOT EXISTS metadata (
			key VARCHAR(255) PRIMARY KEY,
			value JSON,
			version BIGINT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		);`,
		DownScript: `DROP TABLE IF EXISTS metadata;`,
		CreatedAt:  time.Now(),
	}
}

// CreateIndexMetadataMigration 创建索引元数据迁移
func CreateIndexMetadataMigration(version int64) Migration {
	return Migration{
		Version:     version,
		Name:        "index_metadata_support",
		Description: "Add support for index metadata management",
		UpScript: `ALTER TABLE metadata ADD COLUMN IF NOT EXISTS index_name VARCHAR(255);
		CREATE INDEX IF NOT EXISTS idx_metadata_index ON metadata(index_name);`,
		DownScript: `DROP INDEX IF EXISTS idx_metadata_index;
		ALTER TABLE metadata DROP COLUMN IF EXISTS index_name;`,
		CreatedAt: time.Now(),
	}
}

// CreateTableMetadataMigration 创建表元数据迁移
func CreateTableMetadataMigration(version int64) Migration {
	return Migration{
		Version:     version,
		Name:        "table_metadata_support",
		Description: "Add support for table metadata management",
		UpScript: `ALTER TABLE metadata ADD COLUMN IF NOT EXISTS table_name VARCHAR(255);
		CREATE INDEX IF NOT EXISTS idx_metadata_table ON metadata(table_name);`,
		DownScript: `DROP INDEX IF EXISTS idx_metadata_table;
		ALTER TABLE metadata DROP COLUMN IF NOT EXISTS table_name;`,
		CreatedAt: time.Now(),
	}
}
