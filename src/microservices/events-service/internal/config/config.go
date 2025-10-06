package config

import (
	"os"
	"strings"
)

// Config содержит конфигурацию сервиса
type Config struct {
	Server struct {
		Port string
	}
	Kafka struct {
		Brokers []string
		GroupID string
		Topics  struct {
			User    string
			Payment string
			Movie   string
		}
	}
}

// LoadConfig загружает конфигурацию из переменных окружения
func LoadConfig() (*Config, error) {
	config := &Config{}

	// Настройки сервера
	config.Server.Port = getEnv("PORT", "8082")

	// Настройки Kafka
	kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")
	config.Kafka.Brokers = strings.Split(kafkaBrokers, ",")
	config.Kafka.GroupID = getEnv("KAFKA_CONSUMER_GROUP_ID", "events-service-group")

	// Топики Kafka
	config.Kafka.Topics.User = getEnv("KAFKA_TOPIC_USER", "user-events")
	config.Kafka.Topics.Payment = getEnv("KAFKA_TOPIC_PAYMENT", "payment-events")
	config.Kafka.Topics.Movie = getEnv("KAFKA_TOPIC_MOVIE", "movie-events")

	return config, nil
}

// getEnv получает значение из переменной окружения или возвращает значение по умолчанию
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		return value
	}
	return defaultValue
}
