// Package logger provides a unified logging system for TigerDB
package logger

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

// Level represents the log level
type Level int

const (
	// LevelDebug for detailed debugging information
	LevelDebug Level = iota
	// LevelInfo for general informational messages
	LevelInfo
	// LevelWarn for warning messages
	LevelWarn
	// LevelError for error messages
	LevelError
	// LevelSilent disables all logging
	LevelSilent
)

// String returns the string representation of the log level
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	case LevelSilent:
		return "SILENT"
	default:
		return "UNKNOWN"
	}
}

// ParseLevel converts a string to a Level
func ParseLevel(s string) Level {
	switch s {
	case "debug", "DEBUG":
		return LevelDebug
	case "info", "INFO":
		return LevelInfo
	case "warn", "WARN", "warning", "WARNING":
		return LevelWarn
	case "error", "ERROR":
		return LevelError
	case "silent", "SILENT":
		return LevelSilent
	default:
		return LevelInfo // default to INFO
	}
}

// Config represents logger configuration
type Config struct {
	// Level is the minimum log level to output
	Level Level
	// Output specifies where to write logs: "stdout", "stderr", or a file path
	Output string
	// Format specifies log format: "text" or "json"
	Format string
	// EnableCaller adds file:line information to logs
	EnableCaller bool
	// EnableTimestamp adds timestamp to logs
	EnableTimestamp bool
	// File rotation settings (only used when Output is a file path)
	MaxSize    int  // megabytes
	MaxBackups int  // number of backups to keep
	MaxAge     int  // days
	Compress   bool // compress rotated files
}

// DefaultConfig returns the default logger configuration
func DefaultConfig() *Config {
	return &Config{
		Level:           LevelInfo,
		Output:          "stdout",
		Format:          "text",
		EnableCaller:    false,
		EnableTimestamp: true,
		MaxSize:         100,  // 100 MB
		MaxBackups:      3,    // keep 3 backups
		MaxAge:          7,    // 7 days
		Compress:        true, // compress old logs
	}
}

// Logger is the main logger instance
type Logger struct {
	mu              sync.RWMutex
	level           Level
	output          io.Writer
	format          string
	enableCaller    bool
	enableTimestamp bool
	debugLogger     *log.Logger
	infoLogger      *log.Logger
	warnLogger      *log.Logger
	errorLogger     *log.Logger
}

var (
	// globalLogger is the default logger instance
	globalLogger *Logger
	once         sync.Once
)

// Init initializes the global logger with the given configuration
func Init(cfg *Config) error {
	var initErr error
	once.Do(func() {
		globalLogger, initErr = NewLogger(cfg)
	})
	if initErr != nil {
		return fmt.Errorf("failed to initialize logger: %w", initErr)
	}
	return nil
}

// NewLogger creates a new logger with the given configuration
func NewLogger(cfg *Config) (*Logger, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	var output io.Writer

	// Determine output destination
	switch cfg.Output {
	case "stdout", "":
		output = os.Stdout
	case "stderr":
		output = os.Stderr
	default:
		// File output with rotation
		dir := filepath.Dir(cfg.Output)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %w", err)
		}
		output = &lumberjack.Logger{
			Filename:   cfg.Output,
			MaxSize:    cfg.MaxSize,
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAge,
			Compress:   cfg.Compress,
			LocalTime:  true,
		}
	}

	// Create logger flags
	flags := 0
	if cfg.EnableTimestamp {
		flags |= log.Ldate | log.Ltime | log.Lmicroseconds
	}
	if cfg.EnableCaller {
		flags |= log.Lshortfile
	}

	l := &Logger{
		level:           cfg.Level,
		output:          output,
		format:          cfg.Format,
		enableCaller:    cfg.EnableCaller,
		enableTimestamp: cfg.EnableTimestamp,
	}

	// Create sub-loggers for each level
	l.debugLogger = log.New(output, "[DEBUG] ", flags)
	l.infoLogger = log.New(output, "[INFO] ", flags)
	l.warnLogger = log.New(output, "[WARN] ", flags)
	l.errorLogger = log.New(output, "[ERROR] ", flags)

	return l, nil
}

// SetLevel changes the log level
func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// GetLevel returns the current log level
func (l *Logger) GetLevel() Level {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.level
}

// IsLevelEnabled checks if a log level is enabled
func (l *Logger) IsLevelEnabled(level Level) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return level >= l.level
}

// logJSON outputs a log entry in JSON format
func (l *Logger) logJSON(level string, msg string) {
	entry := map[string]interface{}{
		"level":   level,
		"message": msg,
	}
	if l.enableTimestamp {
		entry["timestamp"] = time.Now().Format("2006-01-02T15:04:05.000000Z07:00")
	}
	jsonBytes, err := json.Marshal(entry)
	if err != nil {
		// Fallback to text format if JSON marshaling fails
		fmt.Fprintf(l.output, "[%s] %s\n", level, msg)
		return
	}
	fmt.Fprintln(l.output, string(jsonBytes))
}

// Debug logs a debug message
func (l *Logger) Debug(format string, v ...interface{}) {
	if l.IsLevelEnabled(LevelDebug) {
		msg := fmt.Sprintf(format, v...)
		if l.format == "json" {
			l.logJSON("DEBUG", msg)
		} else if l.enableCaller {
			l.debugLogger.Output(2, msg)
		} else {
			l.debugLogger.Print(msg)
		}
	}
}

// Info logs an info message
func (l *Logger) Info(format string, v ...interface{}) {
	if l.IsLevelEnabled(LevelInfo) {
		msg := fmt.Sprintf(format, v...)
		if l.format == "json" {
			l.logJSON("INFO", msg)
		} else if l.enableCaller {
			l.infoLogger.Output(2, msg)
		} else {
			l.infoLogger.Print(msg)
		}
	}
}

// Warn logs a warning message
func (l *Logger) Warn(format string, v ...interface{}) {
	if l.IsLevelEnabled(LevelWarn) {
		msg := fmt.Sprintf(format, v...)
		if l.format == "json" {
			l.logJSON("WARN", msg)
		} else if l.enableCaller {
			l.warnLogger.Output(2, msg)
		} else {
			l.warnLogger.Print(msg)
		}
	}
}

// Error logs an error message
func (l *Logger) Error(format string, v ...interface{}) {
	if l.IsLevelEnabled(LevelError) {
		msg := fmt.Sprintf(format, v...)
		if l.format == "json" {
			l.logJSON("ERROR", msg)
		} else if l.enableCaller {
			l.errorLogger.Output(2, msg)
		} else {
			l.errorLogger.Print(msg)
		}
	}
}

// Global logger functions

// GetGlobalLogger returns the global logger instance
func GetGlobalLogger() *Logger {
	if globalLogger == nil {
		// Initialize with default config if not already initialized
		_ = Init(DefaultConfig())
	}
	return globalLogger
}

// SetLevel changes the global logger level
func SetLevel(level Level) {
	GetGlobalLogger().SetLevel(level)
}

// Debug logs a debug message using the global logger
func Debug(format string, v ...interface{}) {
	GetGlobalLogger().Debug(format, v...)
}

// Info logs an info message using the global logger
func Info(format string, v ...interface{}) {
	GetGlobalLogger().Info(format, v...)
}

// Warn logs a warning message using the global logger
func Warn(format string, v ...interface{}) {
	GetGlobalLogger().Warn(format, v...)
}

// Error logs an error message using the global logger
func Error(format string, v ...interface{}) {
	GetGlobalLogger().Error(format, v...)
}

// IsDebugEnabled checks if debug logging is enabled
func IsDebugEnabled() bool {
	return GetGlobalLogger().IsLevelEnabled(LevelDebug)
}

// IsInfoEnabled checks if info logging is enabled
func IsInfoEnabled() bool {
	return GetGlobalLogger().IsLevelEnabled(LevelInfo)
}

// WithField returns a FieldLogger with a single field
func WithField(key string, value interface{}) *FieldLogger {
	return &FieldLogger{
		logger: GetGlobalLogger(),
		fields: map[string]interface{}{key: value},
	}
}

// WithFields returns a FieldLogger with multiple fields
func WithFields(fields map[string]interface{}) *FieldLogger {
	return &FieldLogger{
		logger: GetGlobalLogger(),
		fields: fields,
	}
}

// FieldLogger provides structured logging with fields
type FieldLogger struct {
	logger *Logger
	fields map[string]interface{}
}

// formatWithFields formats a message with fields
func (fl *FieldLogger) formatWithFields(format string, v ...interface{}) string {
	msg := fmt.Sprintf(format, v...)
	if len(fl.fields) == 0 {
		return msg
	}

	// Append fields
	fieldStr := ""
	for k, v := range fl.fields {
		fieldStr += fmt.Sprintf(" %s=%v", k, v)
	}
	return msg + fieldStr
}

// Debug logs a debug message with fields
func (fl *FieldLogger) Debug(format string, v ...interface{}) {
	fl.logger.Debug(fl.formatWithFields(format, v...))
}

// Info logs an info message with fields
func (fl *FieldLogger) Info(format string, v ...interface{}) {
	fl.logger.Info(fl.formatWithFields(format, v...))
}

// Warn logs a warning message with fields
func (fl *FieldLogger) Warn(format string, v ...interface{}) {
	fl.logger.Warn(fl.formatWithFields(format, v...))
}

// Error logs an error message with fields
func (fl *FieldLogger) Error(format string, v ...interface{}) {
	fl.logger.Error(fl.formatWithFields(format, v...))
}
