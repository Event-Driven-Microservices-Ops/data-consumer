package database

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

type EventDocument struct {
	ID        bson.ObjectID `bson:"_id,omitempty"`
	Metadata  Metadata      `bson:"metadata"`
	Data      []DataItem    `bson:"data"`
	CreatedAt time.Time     `bson:"created_at"`
}

type Metadata struct {
	UUID      string `bson:"uuid"`
	Timestamp string `bson:"timestamp"`
}

type DataItem struct {
	Account     Account     `bson:"account"`
	Fraud       Fraud       `bson:"fraud"`
	Transaction Transaction `bson:"transaction"`
}

type Account struct {
	AccountID string `bson:"account_id"`
	UserID    string `bson:"user_id"`
	EventType string `bson:"event_type"`
	SessionID string `bson:"session_id"`
	Timestamp string `bson:"timestamp"`
}

type Fraud struct {
	EventID      string `bson:"event_id"`
	UserID       string `bson:"user_id"`
	FraudScore   int    `bson:"fraud_score"`
	IPAddress    string `bson:"ip_address"`
	CountryCode  string `bson:"country_code"`
	DeviceType   string `bson:"device_type"`
	IsSuspicious bool   `bson:"is_suspicious"`
	Timestamp    string `bson:"timestamp"`
}

type Transaction struct {
	TransactionID string  `bson:"transaction_id"`
	UserID        string  `bson:"user_id"`
	Amount        float64 `bson:"amount"`
	Currency      string  `bson:"currency"`
	Status        string  `bson:"status"`
	PaymentType   string  `bson:"payment_type"`
	Timestamp     string  `bson:"timestamp"`
}
