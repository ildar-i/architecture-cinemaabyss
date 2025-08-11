package middleware

import (
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests by path and status",
		},
		[]string{"path", "status", "service"},
	)

	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"path", "service"},
	)
)

// Metrics middleware для сбора метрик
func Metrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		recorder := &ResponseRecorder{
			ResponseWriter: w,
			Status:         200,
		}

		// Определяем, какой сервис используется
		service := "monolith"
		if strings.Contains(r.URL.Path, "/api/cinema-metadata") {
			service = "cinema-metadata"
		} else if strings.Contains(r.URL.Path, "/api/monolith/movies") {
			// Для этого маршрута мы не знаем, какой сервис будет использоваться,
			// это определит фича-флаг внутри обработчика
			service = "movies"
		}

		next.ServeHTTP(recorder, r)

		duration := time.Since(start).Seconds()

		// Регистрируем метрики
		status := http.StatusText(recorder.Status)
		path := getPathPattern(r.URL.Path)

		httpRequestsTotal.WithLabelValues(path, status, service).Inc()
		httpRequestDuration.WithLabelValues(path, service).Observe(duration)
	})
}

// getPathPattern извлекает шаблон пути для метрик,
// чтобы избежать кардинальности для путей с ID
func getPathPattern(path string) string {
	// Упрощенная версия - заменяем числовые ID на {id}
	parts := strings.Split(path, "/")
	result := []string{}

	for _, part := range parts {
		if part == "" {
			continue
		}

		// Если часть похожа на ID, заменяем на {id}
		if isNumeric(part) {
			result = append(result, "{id}")
		} else {
			result = append(result, part)
		}
	}

	return "/" + strings.Join(result, "/")
}

// isNumeric проверяет, является ли строка числом
func isNumeric(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}
