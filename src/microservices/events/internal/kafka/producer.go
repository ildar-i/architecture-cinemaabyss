package kafka

import (
	"context"
	"events/internal/logger"
	"github.com/segmentio/kafka-go"
	"time"
)

// Producer представляет Kafka продюсера
type Producer struct {
	writers map[string]*kafka.Writer
	logger  *logger.Logger
}

// NewProducer создает новый экземпляр Kafka продюсера
func NewProducer(brokers []string, logger *logger.Logger) (*Producer, error) {
	writers := make(map[string]*kafka.Writer)

	return &Producer{
		writers: writers,
		logger:  logger,
	}, nil
}

// getOrCreateWriter получает или создает Writer для топика
func (p *Producer) getOrCreateWriter(topic string, brokers []string) *kafka.Writer {
	if writer, exists := p.writers[topic]; exists {
		return writer
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    1, // Для тестирования отправляем сообщения сразу
		BatchTimeout: 10 * time.Millisecond,
		Async:        false, // Синхронная отправка для легкости тестирования
	}

	p.writers[topic] = writer
	return writer
}

// Produce отправляет сообщение в Kafka топик
func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte, brokers []string) error {
	writer := p.getOrCreateWriter(topic, brokers)

	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	})

	if err != nil {
		p.logger.Error("Failed to write message to Kafka: " + err.Error())
		return err
	}

	p.logger.Debug("Message sent to topic " + topic)
	return nil
}

// Close закрывает все writers
func (p *Producer) Close() error {
	for topic, writer := range p.writers {
		if err := writer.Close(); err != nil {
			p.logger.Error("Error closing writer for topic " + topic + ": " + err.Error())
		}
	}
	return nil
}
