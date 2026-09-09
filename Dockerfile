# --- Stage 1: Build the application ---
FROM golang:1.27-alpine AS builder

WORKDIR /app

# Download dependencies (cached layer)
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build a static binary
RUN CGO_ENABLED=0 GOOS=linux go build -o data-consumer ./cmd/consumer

# --- Stage 2: Lightweight runtime image ---
FROM alpine:latest

WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/data-consumer /app/data-consumer

# Copy the configuration file required by the app
COPY internal/config/config.yaml /app/internal/config/config.yaml

EXPOSE 8081

ENTRYPOINT ["/app/data-consumer"]