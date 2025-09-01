package models

import (
	"encoding/json"
	"time"
)

// Event интерфейс для всех событий
type Event interface {
	GetType() string
	GetID() string
	GetTimestamp() time.Time
	ToJSON() ([]byte, error)
}

// BaseEvent содержит общие поля для всех событий
type BaseEvent struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
}

// GetID возвращает идентификатор события
func (e BaseEvent) GetID() string {
	return e.ID
}

// GetTimestamp возвращает время события
func (e BaseEvent) GetTimestamp() time.Time {
	return e.Timestamp
}

// UserEvent представляет событие пользователя
type UserEvent struct {
	BaseEvent
	UserID   string `json:"user_id"`
	Action   string `json:"action"`
	UserName string `json:"user_name,omitempty"`
}

// GetType возвращает тип события
func (e UserEvent) GetType() string {
	return "USER_EVENT"
}

// ToJSON сериализует событие в JSON
func (e UserEvent) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// PaymentEvent представляет событие платежа
type PaymentEvent struct {
	BaseEvent
	PaymentID     string    `json:"payment_id"`
	UserID        string    `json:"user_id"`
	Amount        float64   `json:"amount"`
	Currency      string    `json:"currency"`
	PaymentMethod string    `json:"payment_method"`
	Status        string    `json:"status"`
	Timestamp     time.Time `json:"timestamp"`
	MethodType    string    `json:"MethodType"`
}

// GetType возвращает тип события
func (e PaymentEvent) GetType() string {
	return "PAYMENT_EVENT"
}

// ToJSON сериализует событие в JSON
func (e PaymentEvent) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// MovieEvent представляет событие фильма
type MovieEvent struct {
	BaseEvent
	MovieID string `json:"movie_id"`
	Title   string `json:"title,omitempty"`
	UserId  int    `json:"user_id,omitempty"`
	Action  string `json:"action,omitempty"`
}

// GetType возвращает тип события
func (e MovieEvent) GetType() string {
	return "MOVIE_EVENT"
}

// ToJSON сериализует событие в JSON
func (e MovieEvent) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}
