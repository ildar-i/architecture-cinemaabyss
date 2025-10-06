package middleware

import (
	"log"
	"net/http"
	"time"
)

// ResponseRecorder для логирования статуса ответа
type ResponseRecorder struct {
	http.ResponseWriter
	Status int
	Size   int
}

// WriteHeader переопределяет стандартный метод для записи статуса
func (r *ResponseRecorder) WriteHeader(status int) {
	r.Status = status
	r.ResponseWriter.WriteHeader(status)
}

// Write переопределяет стандартный метод для записи размера ответа
func (r *ResponseRecorder) Write(b []byte) (int, error) {
	size, err := r.ResponseWriter.Write(b)
	r.Size += size
	return size, err
}

// Logger - middleware для логирования запросов
func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		recorder := &ResponseRecorder{
			ResponseWriter: w,
			Status:         200,
		}

		next.ServeHTTP(recorder, r)

		duration := time.Since(start)

		log.Printf(
			"[%s] %s %s %d %dB %s",
			r.Method,
			r.URL.Path,
			r.RemoteAddr,
			recorder.Status,
			recorder.Size,
			duration,
		)
	})
}
