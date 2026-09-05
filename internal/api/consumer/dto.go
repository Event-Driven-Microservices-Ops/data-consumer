package consumer

type EventPayload struct {
	Metadata Metadata   `json:"metadata"`
	Data     []DataItem `json:"data"`
}

type Metadata struct {
	UUID      string `json:"uuid"`
	Timestamp string `json:"timestamp"`
}

type DataItem struct {
	Account     Account     `json:"account"`
	Fraud       Fraud       `json:"fraud"`
	Transaction Transaction `json:"transaction"`
}

type Account struct {
	AccountID string `json:"account_id"`
	UserID    string `json:"user_id"`
	EventType string `json:"event_type"`
	SessionID string `json:"session_id"`
	Timestamp string `json:"timestamp"`
}

type Fraud struct {
	EventID      string `json:"event_id"`
	UserID       string `json:"user_id"`
	FraudScore   int    `json:"fraud_score"`
	IPAddress    string `json:"ip_address"`
	CountryCode  string `json:"country_code"`
	DeviceType   string `json:"device_type"`
	IsSuspicious bool   `json:"is_suspicious"`
	Timestamp    string `json:"timestamp"`
}

type Transaction struct {
	TransactionID string  `json:"transaction_id"`
	UserID        string  `json:"user_id"`
	Amount        float64 `json:"amount"`
	Currency      string  `json:"currency"`
	Status        string  `json:"status"`
	PaymentType   string  `json:"payment_type"`
	Timestamp     string  `json:"timestamp"`
}
