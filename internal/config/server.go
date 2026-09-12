package config

import (
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Port              string        `yaml:"port"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	ReadTimeout       time.Duration `yaml:"read_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	MaxRetries        *int          `yaml:"max_retries"`
	DelayRetries      time.Duration `yaml:"delay_retries"`

	GeneratorStreamURL string `yaml:"generator_stream_url"`
	GeneratorBatchURL  string `yaml:"generator_batch_url"`
	DatabaseURL        string `yaml:"database_url"`
	DatabaseName       string `yaml:"database_name"`
	StreamCollection   string `yaml:"stream_collection"`
	BatchCollection    string `yaml:"batch_collection"`

	Mode        *string  `yaml:"mode"`
	BatchSize   *int     `yaml:"batch_size"`
	Interval    *int     `yaml:"interval"`
	Account     *bool    `yaml:"account"`
	EventType   *string  `yaml:"event_type"`
	Fraud       *bool    `yaml:"fraud"`
	FraudScore  *int     `yaml:"fraud_score"`
	CountryCode *string  `yaml:"country_code"`
	DeviceType  *string  `yaml:"device_type"`
	Transaction *bool    `yaml:"transaction"`
	Amount      *float64 `yaml:"amount"`
	Currency    *string  `yaml:"currency"`
	Status      *string  `yaml:"status"`
	PaymentType *string  `yaml:"payment_type"`
}

func Load(path string) (*Config, error) {
	cleanPath := filepath.Clean(path)

	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return nil, err
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
