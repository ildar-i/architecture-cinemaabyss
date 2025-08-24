package logger

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/labstack/gommon/log"
)

// Logger представляет собой простой логгер
type Logger struct {
	logger *log.Logger
}

// NewLogger создает новый экземпляр логгера
func NewLogger() *Logger {
	logger := log.New("events")
	logger.SetLevel(log.INFO)
	logger.SetHeader("${time_rfc3339} ${level} ${short_file}:${line}")

	// Настройка вывода логов в консоль и файл
	file, err := os.OpenFile("events.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		logger.SetOutput(io.MultiWriter(os.Stdout, file))
	} else {
		logger.SetOutput(os.Stdout)
		logger.Warn("Failed to open log file, using stdout only")
	}

	return &Logger{
		logger: logger,
	}
}

// Info логирует информационное сообщение
func (l *Logger) Info(message string) {
	l.logger.Info(message)
}

// Error логирует сообщение об ошибке
func (l *Logger) Error(message string) {
	l.logger.Error(message)
}

// Debug логирует отладочное сообщение
func (l *Logger) Debug(message string) {
	l.logger.Debug(message)
}

// Warn логирует предупреждение
func (l *Logger) Warn(message string) {
	l.logger.Warn(message)
}

// Fatal логирует критическую ошибку и завершает программу
func (l *Logger) Fatal(message string) {
	l.logger.Fatal(message)
}

// LogEvent логирует информацию о событии
func (l *Logger) LogEvent(eventType, action, details string) {
	msg := fmt.Sprintf("[%s] %s - %s - %s", eventType, action, time.Now().Format(time.RFC3339), details)
	l.logger.Info(msg)
}
