package api

import (
	"events/internal/api/handlers"
	"events/internal/api/middleware"
	"events/internal/config"
	"events/internal/kafka"
	"events/internal/logger"
	"github.com/labstack/echo/v4"

	echomiddleware "github.com/labstack/echo/v4/middleware"
)

// SetupRouter настраивает маршруты API
func SetupRouter(
	e *echo.Echo,
	producer *kafka.Producer,
	userConsumer *kafka.Consumer,
	paymentConsumer *kafka.Consumer,
	movieConsumer *kafka.Consumer,
	logger *logger.Logger,
) {
	// Загружаем конфигурацию для получения брокеров
	cfg, _ := config.LoadConfig()

	// Настраиваем middleware
	e.Use(echomiddleware.Recover())
	e.Use(echomiddleware.CORS())
	e.Use(middleware.RequestLogger(logger))

	// Создаем обработчики
	eventHandlers := handlers.NewEventHandlers(
		producer,
		userConsumer,
		paymentConsumer,
		movieConsumer,
		logger,
		cfg.Kafka.Brokers,
	)

	healthHandler := handlers.NewHealthHandler(logger)

	// API группа
	api := e.Group("/api")

	// События
	events := api.Group("/events")
	events.POST("/user", eventHandlers.HandleUserEvent)
	events.POST("/payment", eventHandlers.HandlePaymentEvent)
	events.POST("/movie", eventHandlers.HandleMovieEvent)

	// Проверка работоспособности
	e.GET("/health", healthHandler.HandleHealthCheck)

	logger.Info("Router configured successfully")
}
