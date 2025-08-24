package handlers

import (
	"context"
	"encoding/json"
	"events/internal/kafka"
	"events/internal/logger"
	"events/internal/models"
	"github.com/labstack/echo/v4"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// EventHandlers содержит обработчики API для событий
type EventHandlers struct {
	producer        *kafka.Producer
	userConsumer    *kafka.Consumer
	paymentConsumer *kafka.Consumer
	movieConsumer   *kafka.Consumer
	logger          *logger.Logger
	brokers         []string
}

// NewEventHandlers создает новый экземпляр EventHandlers
func NewEventHandlers(
	producer *kafka.Producer,
	userConsumer *kafka.Consumer,
	paymentConsumer *kafka.Consumer,
	movieConsumer *kafka.Consumer,
	logger *logger.Logger,
	brokers []string,
) *EventHandlers {
	return &EventHandlers{
		producer:        producer,
		userConsumer:    userConsumer,
		paymentConsumer: paymentConsumer,
		movieConsumer:   movieConsumer,
		logger:          logger,
		brokers:         brokers,
	}
}

// UserEventRequest представляет запрос на создание события пользователя
type UserEventRequest struct {
	UserID    string `json:"user_id,omitempty"`
	Action    string `json:"action" validate:"required"`
	UserName  string `json:"user_name,omitempty"`
	UserEmail string `json:"user_email,omitempty"`
}

// HandleUserEvent обрабатывает запросы на создание события пользователя
func (h *EventHandlers) HandleUserEvent(c echo.Context) error {
	var req UserEventRequest
	if err := c.Bind(&req); err != nil {
		h.logger.Error("Failed to bind request: " + err.Error())
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Action == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Action is required"})
	}

	// Если UserID не указан, генерируем его
	if req.UserID == "" {
		req.UserID = uuid.New().String()
	}

	// Создаем событие
	event := models.UserEvent{
		BaseEvent: models.BaseEvent{
			ID:        uuid.New().String(),
			Timestamp: time.Now(),
		},
		UserID:    req.UserID,
		Action:    req.Action,
		UserName:  req.UserName,
		UserEmail: req.UserEmail,
	}

	// Сериализуем событие в JSON
	eventJSON, err := event.ToJSON()
	if err != nil {
		h.logger.Error("Failed to serialize event: " + err.Error())
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Internal server error"})
	}

	// Логируем событие
	h.logger.LogEvent(event.GetType(), "PRODUCING", string(eventJSON))

	// Отправляем событие в Kafka
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "user-events"
	err = h.producer.Produce(ctx, topic, []byte(event.ID), eventJSON, h.brokers)
	if err != nil {
		h.logger.Error("Failed to produce event: " + err.Error())
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to produce event"})
	}

	// Ожидаем подтверждения получения события консьюмером
	receivedEvent, err := h.userConsumer.WaitForEvent(event.ID, 5*time.Second)
	if err != nil {
		h.logger.Error("Error waiting for event confirmation: " + err.Error())
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Error waiting for event confirmation"})
	}

	// Формируем ответ
	response := map[string]interface{}{
		"status":  "success",
		"message": "User event created and consumed",
		"event": map[string]interface{}{
			"id":            event.ID,
			"user_id":       req.UserID,
			"action":        req.Action,
			"timestamp":     event.Timestamp,
			"received":      receivedEvent != nil,
			"event_data":    string(eventJSON),
			"recent_events": h.userConsumer.GetRecentEvents(5),
		},
	}

	return c.JSON(http.StatusCreated, response)
}

// PaymentEventRequest представляет запрос на создание события платежа
type PaymentEventRequest struct {
	PaymentID     string  `json:"payment_id,omitempty"`
	UserID        string  `json:"user_id" validate:"required"`
	Amount        float64 `json:"amount" validate:"required,gt=0"`
	Currency      string  `json:"currency" validate:"required"`
	PaymentMethod string  `json:"payment_method,omitempty"`
	Status        string  `json:"status" validate:"required"`
}

// HandlePaymentEvent обрабатывает запросы на создание события платежа
func (h *EventHandlers) HandlePaymentEvent(c echo.Context) error {
	var req PaymentEventRequest
	if err := c.Bind(&req); err != nil {
		h.logger.Error("Failed to bind request: " + err.Error())
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.UserID == "" || req.Amount <= 0 || req.Currency == "" || req.Status == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "UserID, Amount, Currency, and Status are required"})
	}

	// Если PaymentID не указан, генерируем его
	if req.PaymentID == "" {
		req.PaymentID = uuid.New().String()
	}

	// Создаем событие
	event := models.PaymentEvent{
		BaseEvent: models.BaseEvent{
			ID:        uuid.New().String(),
			Timestamp: time.Now(),
		},
		PaymentID:     req.PaymentID,
		UserID:        req.UserID,
		Amount:        req.Amount,
		Currency:      req.Currency,
		PaymentMethod: req.PaymentMethod,
		Status:        req.Status,
	}

	// Сериализуем событие в JSON
	eventJSON, err := event.ToJSON()
	if err != nil {
		h.logger.Error("Failed to serialize event: " + err.Error())
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Internal server error"})
	}

	// Логируем событие
	h.logger.LogEvent(event.GetType(), "PRODUCING", string(eventJSON))

	// Отправляем событие в Kafka
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "payment-events"
	err = h.producer.Produce(ctx, topic, []byte(event.ID), eventJSON, h.brokers)
	if err != nil {
		h.logger.Error("Failed to produce event: " + err.Error())
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to produce event"})
	}

	// Ожидаем подтверждения получения события консьюмером
	receivedEvent, err := h.paymentConsumer.WaitForEvent(event.ID, 5*time.Second)
	if err != nil {
		h.logger.Error("Error waiting for event confirmation: " + err.Error())
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Error waiting for event confirmation"})
	}

	// Формируем ответ
	response := map[string]interface{}{
		"status":  "success",
		"message": "Payment event created and consumed",
		"event": map[string]interface{}{
			"id":            event.ID,
			"payment_id":    req.PaymentID,
			"user_id":       req.UserID,
			"amount":        req.Amount,
			"currency":      req.Currency,
			"status":        req.Status,
			"timestamp":     event.Timestamp,
			"received":      receivedEvent != nil,
			"event_data":    string(eventJSON),
			"recent_events": h.paymentConsumer.GetRecentEvents(5),
		},
	}

	return c.JSON(http.StatusCreated, response)
}

// MovieEventRequest представляет запрос на создание события фильма
type MovieEventRequest struct {
	MovieID     string `json:"movie_id,omitempty"`
	Title       string `json:"title" validate:"required"`
	Director    string `json:"director,omitempty"`
	ReleaseYear int    `json:"release_year,omitempty"`
	Action      string `json:"action" validate:"required"`
}

// HandleMovieEvent обрабатывает запросы на создание события фильма
func (h *EventHandlers) HandleMovieEvent(c echo.Context) error {
	var req MovieEventRequest
	if err := c.Bind(&req); err != nil {
		h.logger.Error("Failed to bind request: " + err.Error())
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	if req.Title == "" || req.Action == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Title and Action are required"})
	}

	// Если MovieID не указан, генерируем его
	if req.MovieID == "" {
		req.MovieID = uuid.New().String()
	}

	// Создаем событие
	event := models.MovieEvent{
		BaseEvent: models.BaseEvent{
			ID:        uuid.New().String(),
			Timestamp: time.Now(),
		},
		MovieID:     req.MovieID,
		Title:       req.Title,
		Director:    req.Director,
		ReleaseYear: req.ReleaseYear,
		Action:      req.Action,
	}

	// Сериализуем событие в JSON
	eventJSON, err := event.ToJSON()
	if err != nil {
		h.logger.Error("Failed to serialize event: " + err.Error())
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Internal server error"})
	}

	// Логируем событие
	h.logger.LogEvent(event.GetType(), "PRODUCING", string(eventJSON))

	// Отправляем событие в Kafka
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "movie-events"
	err = h.producer.Produce(ctx, topic, []byte(event.ID), eventJSON, h.brokers)
	if err != nil {
		h.logger.Error("Failed to produce event: " + err.Error())
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to produce event"})
	}

	// Ожидаем подтверждения получения события консьюмером
	receivedEvent, err := h.movieConsumer.WaitForEvent(event.ID, 5*time.Second)
	if err != nil {
		h.logger.Error("Error waiting for event confirmation: " + err.Error())
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Error waiting for event confirmation"})
	}

	// Преобразуем recent_events в более читаемый формат
	recentEvents := h.movieConsumer.GetRecentEvents(5)
	prettyRecentEvents := make(map[string]interface{})

	for id, eventData := range recentEvents {
		var eventObj map[string]interface{}
		json.Unmarshal(eventData, &eventObj)
		prettyRecentEvents[id] = eventObj
	}

	// Формируем ответ
	response := map[string]interface{}{
		"status":  "success",
		"message": "Movie event created and consumed",
		"event": map[string]interface{}{
			"id":            event.ID,
			"movie_id":      req.MovieID,
			"title":         req.Title,
			"director":      req.Director,
			"release_year":  req.ReleaseYear,
			"action":        req.Action,
			"timestamp":     event.Timestamp,
			"received":      receivedEvent != nil,
			"event_data":    string(eventJSON),
			"recent_events": prettyRecentEvents,
		},
	}

	return c.JSON(http.StatusCreated, response)
}
