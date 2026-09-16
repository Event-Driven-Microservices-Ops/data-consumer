# --- Stage 1: Build the application ---
FROM golang:alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o data-consumer ./cmd/consumer

# --- Stage 2: Lightweight runtime image ---
FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/data-consumer /app/data-consumer
COPY internal/config/config.yaml /app/internal/config/config.yaml

RUN chmod +x /app/data-consumer

EXPOSE 8081

ENTRYPOINT ["/app/data-consumer"]