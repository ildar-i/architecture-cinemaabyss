package handlers

import (
	"events/internal/logger"
	"github.com/labstack/echo/v4"
	"net/http"
)

// HealthHandler обрабатывает запросы на проверку работоспособности
type HealthHandler struct {
	logger *logger.Logger
}

// NewHealthHandler создает новый экземпляр HealthHandler
func NewHealthHandler(logger *logger.Logger) *HealthHandler {
	return &HealthHandler{
		logger: logger,
	}
}

// HandleHealthCheck обрабатывает запросы на проверку работоспособности
func (h *HealthHandler) HandleHealthCheck(c echo.Context) error {
	h.logger.Info("Health check requested")

	response := map[string]string{
		"status":  "ok",
		"service": "events",
		"version": "1.0.0",
	}

	return c.JSON(http.StatusOK, response)
}
