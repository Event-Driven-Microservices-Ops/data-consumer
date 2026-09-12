package main

import (
	"context"
	"log"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/urbaniakmichal/data-consumer/internal/api/consumer"
	"github.com/urbaniakmichal/data-consumer/internal/config"
	"github.com/urbaniakmichal/data-consumer/internal/database"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	cfg := loadConfig("internal/config/config.yaml")
	mongoClient := connectToMongo(cfg)
	dbService := database.NewDataBaseService(cfg, mongoClient)
	dts := consumer.NewDataConsumerService(dbService)

	setMode(cfg, dts)
}

func connectToMongo(cfg *config.Config) *mongo.Client {
	opts := options.Client().ApplyURI(cfg.DatabaseURL)

	client, err := mongo.Connect(opts)
	if err != nil {
		log.Fatalf("Failed to connect to mongodb: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatalf("Pinged database. You failure connected to MongoDB!: %v", err)
	}

	return client
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
		runBatch(dts, cfg)

	case "stream":
		log.Println("Starting in STREAM mode...")
		runStream(dts, cfg)
		select {}

	case "both":
		log.Println("Starting in BOTH modes...")
		runBatch(dts, cfg)
		go runStream(dts, cfg)
		select {}

	default:
		log.Fatalf("Unknown mode: %s. Use 'batch', 'stream', or 'both'.", mode)
	}
}

func runBatch(dts *consumer.DataConsumerService, cfg *config.Config) {
	parsedBatchURL, err := url.Parse(cfg.GeneratorBatchURL)
	if err != nil {
		log.Fatalf("Failed to parse batch URL: %v", err)
	}
	parsedBatchURL.RawQuery = dynamicMappingAllFlagsToQueryParameters(cfg, parsedBatchURL).Encode()

	log.Printf("Connecting to generator (Batch): %s", parsedBatchURL.String())
	if err := dts.CollectDataAsBatch(parsedBatchURL.String(), cfg); err != nil {
		log.Printf("Batch consumer error: %v", err)
	}
}

func runStream(dts *consumer.DataConsumerService, cfg *config.Config) {
	parsedStreamURL, err := url.Parse(cfg.GeneratorStreamURL)
	if err != nil {
		log.Fatalf("Failed to parse stream URL: %v", err)
	}
	parsedStreamURL.RawQuery = dynamicMappingAllFlagsToQueryParameters(cfg, parsedStreamURL).Encode()

	for {
		log.Printf("Connecting to generator (Stream): %s", parsedStreamURL.String())
		if err := dts.CollectDataAsStream(parsedStreamURL.String(), cfg); err != nil {
			log.Printf("Stream consumer error: %v. Reconnecting in %v...", err, cfg.DelayRetries)
			time.Sleep(cfg.DelayRetries)
		}
	}
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
