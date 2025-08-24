package proxy

import (
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"api-gateway/config"
)

// ProxyHandler обрабатывает проксирование запросов
type ProxyHandler struct {
	config     *config.Config
	monolith   *http.Client
	newService *http.Client
}

// NewProxyHandler создает новый обработчик проксирования
func NewProxyHandler(cfg *config.Config) *ProxyHandler {
	return &ProxyHandler{
		config: cfg,
		monolith: &http.Client{
			Timeout: time.Duration(cfg.Monolith.Timeout) * time.Second,
		},
		newService: &http.Client{
			Timeout: time.Duration(cfg.CinemaMetadata.Timeout) * time.Second,
		},
	}
}

// ProxyMoviesRequest проксирует запросы, связанные с метаданными фильмов
func (p *ProxyHandler) ProxyMoviesRequest(useNewService bool, w http.ResponseWriter, r *http.Request) {
	var targetURL string
	var client *http.Client

	// Определяем, куда направить запрос
	if useNewService {
		// Новый сервис метаданных фильмов
		//todo fix it
		targetPath := strings.TrimPrefix(r.URL.Path, "/api/movies")
		targetURL = p.config.CinemaMetadata.BaseURL + "/api/movies" + targetPath
		client = p.newService

		log.Printf("Routing to new service: %s", targetURL)
	} else {
		// Старый монолит
		targetURL = p.config.Monolith.BaseURL + r.URL.Path
		client = p.monolith

		log.Printf("Routing to monolith: %s", targetURL)
	}

	// Создаем новый запрос
	targetURLParsed, err := url.Parse(targetURL)
	if err != nil {
		http.Error(w, "Invalid URL", http.StatusInternalServerError)

		return
	}

	// Копируем query параметры
	targetURLParsed.RawQuery = r.URL.RawQuery

	// Создаем новый запрос
	outReq, err := http.NewRequestWithContext(
		r.Context(),
		r.Method,
		targetURLParsed.String(),
		r.Body,
	)
	if err != nil {
		http.Error(w, "Error creating request", http.StatusInternalServerError)

		return
	}

	// Копируем заголовки
	CopyHeaders(outReq.Header, r.Header)

	// Выполняем запрос
	resp, err := client.Do(outReq)
	if err != nil {
		log.Printf("Proxy error: %v", err)
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)

		return
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("Error closing response body: %v", err)
		}
	}(resp.Body)

	// Копируем заголовки ответа
	CopyHeaders(w.Header(), resp.Header)

	// Устанавливаем статус ответа
	w.WriteHeader(resp.StatusCode)

	// Копируем тело ответа
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		log.Printf("Error copying response body: %v", err)
	}
}

// ProxyRequest проксирует запросы на монолит
func (p *ProxyHandler) ProxyRequest(w http.ResponseWriter, r *http.Request) {
	targetURL := p.config.Monolith.BaseURL + r.URL.Path

	// Создаем новый запрос
	outReq, err := http.NewRequestWithContext(
		r.Context(),
		r.Method,
		targetURL,
		r.Body,
	)
	if err != nil {
		http.Error(w, "Error creating request", http.StatusInternalServerError)
		return
	}

	// Копируем query параметры
	outReq.URL.RawQuery = r.URL.RawQuery

	// Копируем заголовки
	CopyHeaders(outReq.Header, r.Header)

	// Выполняем запрос
	resp, err := p.monolith.Do(outReq)
	if err != nil {
		log.Printf("Proxy error: %v", err)
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)

		return
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("Error closing response body: %v", err)
		}
	}(resp.Body)

	// Копируем заголовки ответа
	CopyHeaders(w.Header(), resp.Header)

	// Устанавливаем статус ответа
	w.WriteHeader(resp.StatusCode)

	// Копируем тело ответа
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		log.Printf("Error copying response body: %v", err)

		return
	}
}

// CopyHeaders Вспомогательная функция для копирования заголовков
func CopyHeaders(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}
