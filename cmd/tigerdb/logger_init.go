package main

import (
	"fmt"

	"github.com/lscgzwd/tiggerdb/config"
	"github.com/lscgzwd/tiggerdb/logger"
)

// initLogger initializes the logger based on global configuration
func initLogger(cfg *config.GlobalConfig) error {
	if cfg.Log == nil {
		// Use default logger config
		return logger.Init(logger.DefaultConfig())
	}

	logCfg := &logger.Config{
		Level:           logger.ParseLevel(cfg.Log.Level),
		Output:          cfg.Log.Output,
		Format:          cfg.Log.Format,
		EnableCaller:    cfg.Log.EnableCaller,
		EnableTimestamp: cfg.Log.EnableTimestamp,
		MaxSize:         cfg.Log.MaxSize,
		MaxBackups:      cfg.Log.MaxBackups,
		MaxAge:          cfg.Log.MaxAge,
		Compress:        cfg.Log.Compress,
	}

	if err := logger.Init(logCfg); err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	return nil
}
