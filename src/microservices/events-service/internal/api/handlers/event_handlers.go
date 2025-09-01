package handlers

import (
	"context"
	"encoding/json"
	"events/internal/kafka"
	"events/internal/logger"
	"events/internal/models"
	"github.com/labstack/echo/v4"
	"net/http"
	"strconv"
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
	UserId    int    `json:"user_id"`
	Username  string `json:"username"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
}

// HandleUserEvent обрабатывает запросы на создание события пользователя
func (h *EventHandlers) HandleUserEvent(c echo.Context) error {
	var req UserEventRequest
	if err := c.Bind(&req); err != nil {
		h.logger.Error("Failed to bind request: " + err.Error())
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	// Создаем событие
	event := models.UserEvent{
		BaseEvent: models.BaseEvent{
			ID:        uuid.New().String(),
			Timestamp: time.Now(),
		},
		UserID:   strconv.Itoa(req.UserId),
		Action:   req.Action,
		UserName: req.Username,
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
	_, err = h.userConsumer.WaitForEvent(event.ID, 5*time.Second)
	if err != nil {
		h.logger.Error("Error waiting for event confirmation: " + err.Error())
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Error waiting for event confirmation"})
	}

	type Out struct {
		Status string `json:"status"`
	}

	return c.JSON(http.StatusCreated, Out{
		Status: "success",
	})
}

// PaymentEventRequest представляет запрос на создание события платежа
type PaymentEventRequest struct {
	PaymentId  int       `json:"payment_id"`
	UserId     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType string    `json:"method_type"`
}

// HandlePaymentEvent обрабатывает запросы на создание события платежа
func (h *EventHandlers) HandlePaymentEvent(c echo.Context) error {
	var req PaymentEventRequest
	if err := c.Bind(&req); err != nil {
		h.logger.Error("Failed to bind request: " + err.Error())
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	// Создаем событие
	event := models.PaymentEvent{
		BaseEvent: models.BaseEvent{
			ID:        uuid.New().String(),
			Timestamp: time.Now(),
		},
		PaymentID:  strconv.Itoa(req.PaymentId),
		UserID:     strconv.Itoa(req.UserId),
		Amount:     req.Amount,
		Timestamp:  req.Timestamp,
		MethodType: req.MethodType,
		Status:     req.Status,
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
	_, err = h.paymentConsumer.WaitForEvent(event.ID, 5*time.Second)
	if err != nil {
		h.logger.Error("Error waiting for event confirmation: " + err.Error())
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Error waiting for event confirmation"})
	}

	type Out struct {
		Status string `json:"status"`
	}

	return c.JSON(http.StatusCreated, Out{
		Status: "success",
	})
}

// MovieEventRequest представляет запрос на создание события фильма
type MovieEventRequest struct {
	MovieId int    `json:"movie_id"`
	Title   string `json:"title"`
	Action  string `json:"action"`
	UserId  int    `json:"user_id"`
}

// HandleMovieEvent обрабатывает запросы на создание события фильма
func (h *EventHandlers) HandleMovieEvent(c echo.Context) error {
	var req MovieEventRequest
	if err := c.Bind(&req); err != nil {
		h.logger.Error("Failed to bind request: " + err.Error())
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	// Создаем событие
	event := models.MovieEvent{
		BaseEvent: models.BaseEvent{
			ID:        uuid.New().String(),
			Timestamp: time.Now(),
		},
		MovieID: strconv.Itoa(req.MovieId),
		Title:   req.Title,
		UserId:  req.UserId,
		Action:  req.Action,
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
	_, err = h.movieConsumer.WaitForEvent(event.ID, 5*time.Second)
	if err != nil {
		h.logger.Error("Error waiting for event confirmation: " + err.Error())
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Error waiting for event confirmation"})
	}

	// Преобразуем recent_events в более читаемый формат
	recentEvents := h.movieConsumer.GetRecentEvents(5)
	prettyRecentEvents := make(map[string]interface{})

	for id, eventData := range recentEvents {
		var eventObj map[string]interface{}
		err := json.Unmarshal(eventData, &eventObj)
		if err != nil {
			h.logger.Error("HandleMovieEvent json.Unmarshal: " + err.Error())

			return err
		}
		prettyRecentEvents[id] = eventObj
	}

	type Out struct {
		Status string `json:"status"`
	}

	return c.JSON(http.StatusCreated, Out{
		Status: "success",
	})
}
