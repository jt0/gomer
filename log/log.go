package log

import (
	"context"
	"log/slog"
	"strings"
)

var logger = slog.Default()

func SetLogger(l *slog.Logger) {
	logger = l
}

func SetDefaultLoggerLevel(logLevel string) {
	if logLevel == "" {
		return
	}
	slogLevel, ok := nameToLevel[strings.ToLower(logLevel)]
	if !ok {
		Error("unrecognized log level; ignoring", "logLevel", logLevel)
		return
	}
	slog.SetLogLoggerLevel(slogLevel)
}

//const levelTrace = slog.Level(-8)

var nameToLevel = map[string]slog.Level{
	"error": slog.LevelError,
	"warn":  slog.LevelWarn,
	"info":  slog.LevelInfo,
	"debug": slog.LevelDebug,
	//"trace": levelTrace,
}

func Logger() *slog.Logger {
	return logger
}

// Debug calls [Logger.Debug] on the current logger.
func Debug(msg string, args ...any) {
	Logger().Log(context.Background(), slog.LevelDebug, msg, args...)
}

// DebugContext calls [Logger.DebugContext] on the current logger.
func DebugContext(ctx context.Context, msg string, args ...any) {
	Logger().Log(ctx, slog.LevelDebug, msg, args...)
}

func DebugEnabled() bool {
	return Logger().Enabled(nil, slog.LevelDebug)
}

// Info calls [Logger.Info] on the current logger.
func Info(msg string, args ...any) {
	Logger().Log(context.Background(), slog.LevelInfo, msg, args...)
}

// InfoContext calls [Logger.InfoContext] on the current logger.
func InfoContext(ctx context.Context, msg string, args ...any) {
	Logger().Log(ctx, slog.LevelInfo, msg, args...)
}

// Warn calls [Logger.Warn] on the current logger.
func Warn(msg string, args ...any) {
	Logger().Log(context.Background(), slog.LevelWarn, msg, args...)
}

// WarnContext calls [Logger.WarnContext] on the current logger.
func WarnContext(ctx context.Context, msg string, args ...any) {
	Logger().Log(ctx, slog.LevelWarn, msg, args...)
}

// Error calls [Logger.Error] on the current logger.
func Error(msg string, args ...any) {
	Logger().Log(context.Background(), slog.LevelError, msg, args...)
}

// ErrorContext calls [Logger.ErrorContext] on the current logger.
func ErrorContext(ctx context.Context, msg string, args ...any) {
	Logger().Log(ctx, slog.LevelError, msg, args...)
}

// Log calls [Logger.Log] on the current logger.
func Log(ctx context.Context, level slog.Level, msg string, args ...any) {
	Logger().Log(ctx, level, msg, args...)
}

// LogAttrs calls [Logger.LogAttrs] on the current logger.
func LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	Logger().LogAttrs(ctx, level, msg, attrs...)
}

//func Trace(ctx context.Context, msg string, args ...any) {
//	Logger().Log(ctx, levelTrace, msg, args)
//}
//
//func IsTraceEnabled(ctx context.Context) bool {
//	return Logger().Enabled(ctx, levelTrace)
//}
