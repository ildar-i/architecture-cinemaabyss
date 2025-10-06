FROM golang:1.24-alpine AS builder

WORKDIR /app

# Копируем файлы с зависимостями
COPY go.mod go.sum ./

# Загружаем зависимости
RUN go mod download

# Копируем исходный код
COPY . .

# Собираем приложение
RUN go build -o events ./cmd/main.go

# Финальный образ
FROM alpine:3.14

WORKDIR /app

# Устанавливаем зависимости для работы с сетью
RUN apk add --no-cache ca-certificates tzdata iputils

# Копируем бинарный файл из билдера
COPY --from=builder /app/events .

# Открываем порт
EXPOSE 8082

# Запускаем приложение
CMD ["./events"]
