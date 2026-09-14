package producer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaService struct {
	ctx context.Context
	kW  *kafka.Writer
}

func NewKafkaService(ctx context.Context, kW *kafka.Writer) *KafkaService {
	return &KafkaService{
		ctx: ctx,
		kW:  kW,
	}
}

func (kF *KafkaService) Publish(payload []any) {
	for _, event := range payload {
		jsonValue, err := json.Marshal(event)
		if err != nil {
			log.Printf("Failed to marshal event to JSON: %v", err)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		message := kafka.Message{
			Key:   []byte("data"),
			Value: jsonValue,
		}

		err = kF.kW.WriteMessages(ctx, message)
		cancel()

		if err != nil {
			log.Printf("Failed to write message to Kafka: %v", err)
		} else {
			log.Printf("Successfully published event to Kafka topic 'data'")
		}
	}
}
