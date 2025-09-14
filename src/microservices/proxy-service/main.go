package main

import (
	"errors"
	//"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"api-gateway/config"
	"api-gateway/middleware"
	"api-gateway/routing"
)

func main() {
	cfg := &config.Config{}
	cfg.Features.MoviesFeatureFlag = 0.5

	cfg.CinemaMetadata.Timeout = 30
	cfg.CinemaMetadata.BaseURL = "http://movies-service:8081"

	cfg.Server.Address = ":8000"
	cfg.Server.GradualMigration = true

	cfg.Monolith.Timeout = 30
	cfg.Monolith.BaseURL = "http://monolith:8080"

	// Создаем и настраиваем маршруты
	router := routing.SetupRoutes(cfg)

	// Добавляем глобальные мидлвары
	handler := middleware.Logger(router)
	handler = middleware.Metrics(handler)

	// Запускаем сервер
	server := &http.Server{
		Addr:    cfg.Server.Address,
		Handler: handler,
	}

	// Запускаем сервер в горутине
	go func() {
		log.Printf("Starting server on %s", cfg.Server.Address)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Ожидаем сигнал для грациозного завершения
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
}
