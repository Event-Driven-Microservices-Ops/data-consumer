package main

import (
	"log"
	"net/url"
	"strconv"

	"github.com/urbaniakmichal/data-consumer/internal/api/consumer"
	"github.com/urbaniakmichal/data-consumer/internal/config"
)

func main() {
	cfg := loadConfig("internal/config/config.yaml")

	parsedURL, err := url.Parse(cfg.GeneratorURL)
	if err != nil {
		log.Fatalf("Failed to parse generator URL: %v", err)
	}

	parsedURL.RawQuery = dynamicMappingAllFlagsToQueryParameters(cfg, parsedURL).Encode()
	finalURL := parsedURL.String()

	log.Printf("Connecting to generator: %s", finalURL)

	dts := consumer.NewDataConsumerService()
	if err := dts.CollectDataAsStream(finalURL); err != nil {
		log.Fatalf("Stream consumer error: %v", err)
	}
}

func loadConfig(path string) *config.ServerConfig {
	cfg, err := config.Load(path)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	return cfg
}

func dynamicMappingAllFlagsToQueryParameters(cfg *config.ServerConfig, url *url.URL) url.Values {
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
