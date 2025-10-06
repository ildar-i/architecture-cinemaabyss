package middleware

import (
	"events/internal/logger"
	"github.com/labstack/echo/v4"
	"time"
)

// RequestLogger создает middleware для логирования HTTP запросов
func RequestLogger(log *logger.Logger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			start := time.Now()

			req := c.Request()
			res := c.Response()

			log.Info("Request: " + req.Method + " " + req.URL.String() + " from " + req.RemoteAddr)

			if err := next(c); err != nil {
				c.Error(err)
			}

			duration := time.Since(start)

			log.Info("Response: " + req.URL.Path + " " + req.Method + " " +
				"status=" + string(rune(res.Status)) + " " +
				"latency=" + duration.String())

			return nil
		}
	}
}
