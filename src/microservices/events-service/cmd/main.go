package main

import (
	"context"
	"fmt"
	"github.com/labstack/echo/v4"
	"os"
	"os/signal"
	"syscall"
	"time"

	"events/internal/api"
	"events/internal/config"
	"events/internal/kafka"
	"events/internal/logger"
)

func main() {
	// Инициализация логгера
	log := logger.NewLogger()
	log.Info("Starting events service...")

	// Загрузка конфигурации
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatal("Failed to load configuration: " + err.Error())

		return
	}

	log.Info(fmt.Sprintf("kafka configs: %+v", cfg.Kafka))

	log.Info("Configuration loaded successfully")

	time.Sleep(3 * time.Second)

	// Инициализация Kafka продюсера
	producer, err := kafka.NewProducer(cfg.Kafka.Brokers, log)
	if err != nil {
		log.Fatal("Failed to create Kafka producer: " + err.Error())

		return
	}
	defer func(producer *kafka.Producer) {
		err := producer.Close()
		if err != nil {
			log.Fatal("producer.Close err: " + err.Error())
		}
	}(producer)
	log.Info("Kafka producer initialized")

	// Инициализация Kafka консьюмеров
	userConsumer, err := kafka.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.Topics.User, cfg.Kafka.GroupID, log)
	if err != nil {
		log.Fatal("Failed to create User consumer: " + err.Error())

		return
	}
	defer func(userConsumer *kafka.Consumer) {
		err := userConsumer.Close()
		if err != nil {
			log.Fatal("userConsumer.Close err: " + err.Error())
		}
	}(userConsumer)

	paymentConsumer, err := kafka.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.Topics.Payment, cfg.Kafka.GroupID, log)
	if err != nil {
		log.Fatal("Failed to create Payment consumer: " + err.Error())

		return
	}
	defer func(paymentConsumer *kafka.Consumer) {
		err := paymentConsumer.Close()
		if err != nil {
			log.Fatal("paymentConsumer.Close err: " + err.Error())

			return
		}
	}(paymentConsumer)

	movieConsumer, err := kafka.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.Topics.Movie, cfg.Kafka.GroupID, log)
	if err != nil {
		log.Fatal("Failed to create Movie consumer: " + err.Error())

		return
	}
	defer func(movieConsumer *kafka.Consumer) {
		err := movieConsumer.Close()
		if err != nil {
			log.Fatal("movieConsumer.Close err: " + err.Error())
		}
	}(movieConsumer)

	// Запуск консьюмеров
	go userConsumer.Consume()
	go paymentConsumer.Consume()
	go movieConsumer.Consume()

	log.Info("Kafka consumers started")

	// Инициализация Echo сервера и маршрутов
	e := echo.New()
	api.SetupRouter(e, producer, userConsumer, paymentConsumer, movieConsumer, log)

	// Запуск сервера в отдельной горутине
	go func() {
		if err := e.Start(":" + cfg.Server.Port); err != nil {
			log.Info("Shutting down the server: " + err.Error())
		}
	}()

	log.Info("HTTP server started on port " + cfg.Server.Port)

	// Обработка сигналов завершения
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// Грациозное завершение
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := e.Shutdown(ctx); err != nil {
		log.Fatal("Server shutdown failed: " + err.Error())
	}

	log.Info("Server gracefully stopped")
}
