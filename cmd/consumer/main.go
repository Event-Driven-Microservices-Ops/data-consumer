package main

import (
	"context"
	"log"
	"net/url"
	"os"
	"strconv"

	"github.com/segmentio/kafka-go"
	"github.com/urbaniakmichal/data-consumer/internal/api/consumer"
	"github.com/urbaniakmichal/data-consumer/internal/broker/producer"
	"github.com/urbaniakmichal/data-consumer/internal/config"
)

func main() {
	cfg := loadConfig("internal/config/config.yaml")

	writer, ctx := setUpProducer()
	defer writer.Close()

	kS := producer.NewKafkaService(ctx, writer)

	dts := consumer.NewDataConsumerService(kS)

	setMode(cfg, dts)
}

func setUpProducer() (*kafka.Writer, context.Context) {
	writer := &kafka.Writer{
		Addr:         kafka.TCP("kafka1:9092"),
		Topic:        "data",
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	return writer, context.Background()
}

func setMode(cfg *config.Config, dts *consumer.DataConsumerService) {
	var mode string
	if cfg.Mode == nil {
		mode = "both"
	} else {
		mode = *cfg.Mode
	}

	switch mode {
	case "batch":
		log.Println("Starting in BATCH mode...")
		dts.RunBatch(cfg, parsedURL(cfg, cfg.GeneratorBatchURL))

	case "stream":
		log.Println("Starting in STREAM mode...")
		dts.RunStream(cfg, parsedURL(cfg, cfg.GeneratorStreamURL))
		select {}

	case "both":
		log.Println("Starting in BOTH modes...")
		dts.RunBatch(cfg, parsedURL(cfg, cfg.GeneratorBatchURL))
		go dts.RunStream(cfg, parsedURL(cfg, cfg.GeneratorStreamURL))
		select {}

	default:
		log.Fatalf("Unknown mode: %s. Use 'batch', 'stream', or 'both'.", mode)
	}
}

func parsedURL(cfg *config.Config, rawURL string) string {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		log.Fatalf("Failed to parse batch URL: %v", err)
	}
	parsedURL.RawQuery = dynamicMappingAllFlagsToQueryParameters(cfg, parsedURL).Encode()

	log.Printf("Connecting to generator %s", parsedURL.String())
	return parsedURL.String()
}

func loadConfig(path string) *config.Config {
	cfg, err := config.Load(path)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if envStreamURL := os.Getenv("GENERATOR_STREAM_URL"); envStreamURL != "" {
		cfg.GeneratorStreamURL = envStreamURL
	}
	if envBatchURL := os.Getenv("GENERATOR_BATCH_URL"); envBatchURL != "" {
		cfg.GeneratorBatchURL = envBatchURL
	}
	if envPort := os.Getenv("PORT"); envPort != "" {
		cfg.Port = ":" + envPort
	}
	if envDBURL := os.Getenv("DB_URL"); envDBURL != "" {
		cfg.DatabaseURL = envDBURL
	}

	return cfg
}

func dynamicMappingAllFlagsToQueryParameters(cfg *config.Config, url *url.URL) url.Values {
	query := url.Query()
	if cfg.BatchSize != nil {
		query.Set("batch_size", strconv.Itoa(*cfg.BatchSize))
	}
	if cfg.Interval != nil {
		query.Set("interval", strconv.Itoa(*cfg.Interval))
	}
	if cfg.Account != nil {
		query.Set("account", strconv.FormatBool(*cfg.Account))
	}
	if cfg.EventType != nil {
		query.Set("event_type", *cfg.EventType)
	}
	if cfg.Fraud != nil {
		query.Set("fraud", strconv.FormatBool(*cfg.Fraud))
	}
	if cfg.FraudScore != nil {
		query.Set("fraud_score", strconv.Itoa(*cfg.FraudScore))
	}
	if cfg.CountryCode != nil {
		query.Set("country_code", *cfg.CountryCode)
	}
	if cfg.DeviceType != nil {
		query.Set("device_type", *cfg.DeviceType)
	}
	if cfg.Transaction != nil {
		query.Set("transaction", strconv.FormatBool(*cfg.Transaction))
	}
	if cfg.Amount != nil {
		query.Set("amount", strconv.FormatFloat(*cfg.Amount, 'f', -1, 64))
	}
	if cfg.Currency != nil {
		query.Set("currency", *cfg.Currency)
	}
	if cfg.Status != nil {
		query.Set("status", *cfg.Status)
	}
	if cfg.PaymentType != nil {
		query.Set("payment_type", *cfg.PaymentType)
	}

	return query
}
