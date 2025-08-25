package kafka

import (
	"context"
	"events/internal/logger"
	"github.com/segmentio/kafka-go"
	"sync"
	"time"
)

// Consumer представляет Kafka консьюмер
type Consumer struct {
	reader        *kafka.Reader
	logger        *logger.Logger
	topic         string
	recentEvents  map[string][]byte
	mutex         sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	eventReceived chan struct{}
}

// NewConsumer создает новый экземпляр Kafka консьюмера
func NewConsumer(brokers []string, topic, groupID string, logger *logger.Logger) (*Consumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		StartOffset: kafka.FirstOffset,
		MaxWait:     500 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())

	return &Consumer{
		reader:        reader,
		logger:        logger,
		topic:         topic,
		recentEvents:  make(map[string][]byte),
		ctx:           ctx,
		cancel:        cancel,
		eventReceived: make(chan struct{}, 1),
	}, nil
}

// Consume начинает потребление сообщений из Kafka
func (c *Consumer) Consume() {
	c.logger.Info("Starting consumer for topic: " + c.topic)

	for {
		select {
		case <-c.ctx.Done():
			c.logger.Info("Consumer for topic " + c.topic + " is shutting down")
			return
		default:
			m, err := c.reader.ReadMessage(c.ctx)
			if err != nil {
				// Если контекст был отменен, выходим из цикла
				if c.ctx.Err() != nil {
					return
				}
				c.logger.Error("Error reading message from Kafka: " + err.Error())
				time.Sleep(time.Second) // Небольшая задержка перед повторной попыткой
				continue
			}

			// Обработка полученного сообщения
			c.logger.LogEvent(c.topic, "CONSUMED", string(m.Value))

			// Сохраняем сообщение в кэше по ключу
			messageKey := string(m.Key)
			c.mutex.Lock()
			c.recentEvents[messageKey] = m.Value
			c.mutex.Unlock()

			// Сигнализируем о получении нового сообщения
			select {
			case c.eventReceived <- struct{}{}:
			default:
			}
		}
	}
}

// WaitForEvent ожидает события с указанным ID или таймаут
func (c *Consumer) WaitForEvent(id string, timeout time.Duration) ([]byte, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		// Проверяем, есть ли уже сообщение с таким ID
		c.mutex.RLock()
		event, exists := c.recentEvents[id]
		c.mutex.RUnlock()

		if exists {
			return event, nil
		}

		// Ждем уведомления о новом сообщении или таймаут
		select {
		case <-c.eventReceived:
			// Продолжаем цикл и проверим снова
		case <-time.After(100 * time.Millisecond):
			// Продолжаем цикл
		}
	}

	return nil, nil // Таймаут
}

// GetRecentEvents возвращает недавние события
func (c *Consumer) GetRecentEvents(limit int) map[string][]byte {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	result := make(map[string][]byte, limit)
	count := 0

	for id, event := range c.recentEvents {
		result[id] = event
		count++
		if count >= limit {
			break
		}
	}

	return result
}

// Close закрывает reader
func (c *Consumer) Close() error {
	c.cancel()
	return c.reader.Close()
}
