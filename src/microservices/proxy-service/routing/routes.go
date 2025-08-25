package routing

import (
	"api-gateway/config"
	"api-gateway/feature"
	"api-gateway/proxy"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
)

// SetupRoutes настраивает маршрутизацию для API Gateway
func SetupRoutes(cfg *config.Config) http.Handler {
	mux := http.NewServeMux()
	proxyHandler := proxy.NewProxyHandler(cfg)

	// Инициализируем модуль фича-флагов
	feature.Init()

	// health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "OK", http.StatusOK)
	})

	if cfg.Server.GradualMigration != false {
		// Маршрут для запросов, связанных с фильмами
		mux.HandleFunc("/api/movies", func(w http.ResponseWriter, r *http.Request) {
			// Решаем, использовать ли новый сервис на основе фича-флага
			useNewService := feature.ShouldUseNewService(cfg.Features.MoviesFeatureFlag)
			proxyHandler.ProxyMoviesRequest(useNewService, w, r)
		})
	} else {
		// Проксирование напрямую к новому сервису
		mux.HandleFunc("/api/movies", func(w http.ResponseWriter, r *http.Request) {
			r.URL.Path = strings.TrimPrefix(r.URL.Path, "/api")
			targetURL := cfg.CinemaMetadata.BaseURL + "/api" + r.URL.Path
			outReq, _ := http.NewRequest(r.Method, targetURL, r.Body)
			outReq.URL.RawQuery = r.URL.RawQuery

			proxy.CopyHeaders(outReq.Header, r.Header)

			resp, err := http.DefaultClient.Do(outReq)
			if err != nil {
				http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
				return
			}
			defer func(Body io.ReadCloser) {
				err := Body.Close()
				if err != nil {
					log.Printf("HandleFunc Body.Close error: %v", err)
				}
			}(resp.Body)

			proxy.CopyHeaders(w.Header(), resp.Header)
			w.WriteHeader(resp.StatusCode)
			http.MaxBytesReader(w, resp.Body, 1<<20) // 1MB limit

			bts, err := json.Marshal(resp.Body)
			if err != nil {
				log.Printf("HandleFunc Body.Marshal error: %v", err)

				return
			}

			_, err = w.Write(bts)
			if err != nil {
				log.Printf("HandleFunc write error: %v", err)

				return
			}
		})
	}

	// Все остальные запросы монолита проксируем на монолит
	mux.HandleFunc("/api/users", func(w http.ResponseWriter, r *http.Request) {
		proxyHandler.ProxyRequest(w, r)
	})

	// Все остальные запросы монолита проксируем на монолит
	mux.HandleFunc("/api/payments", func(w http.ResponseWriter, r *http.Request) {
		proxyHandler.ProxyRequest(w, r)
	})

	// Все остальные запросы монолита проксируем на монолит
	mux.HandleFunc("/api/subscriptions", func(w http.ResponseWriter, r *http.Request) {
		proxyHandler.ProxyRequest(w, r)
	})

	return mux
}
