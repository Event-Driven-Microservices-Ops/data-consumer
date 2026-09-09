package consumer

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/urbaniakmichal/data-consumer/internal/config"
)

type DataConsumerService struct {
}

func NewDataConsumerService() *DataConsumerService {
	return &DataConsumerService{}
}

func (dts *DataConsumerService) CollectDataAsBatch(generatorURL string, cfg *config.ServerConfig) error {
	retryRes, retryErr := fetchWithRetry(generatorURL, cfg)
	if retryErr != nil {
		log.Printf("Error during fetching request: %v", retryErr)
		return retryErr
	}

	req, err := http.NewRequest("GET", generatorURL, nil)
	if err != nil {
		log.Printf("Error during create request: %v", err)
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cache-Control", "no-cache")

	defer retryRes.Body.Close()

	byteSlice, err := io.ReadAll(retryRes.Body)
	if err != nil {
		log.Printf("Error in reading request body")
	}

	var payload []EventPayload
	err = json.Unmarshal(byteSlice, &payload)

	if err != nil {
		log.Println("Error in json unmarshal")
		return err
	}

	log.Printf("Data from data-generator as batch: %+v", payload)

	return nil
}

func (dts *DataConsumerService) CollectDataAsStream(generatorURL string, cfg *config.ServerConfig) error {
	retryRes, retryErr := fetchWithRetry(generatorURL, cfg)
	if retryErr != nil {
		log.Printf("Error during fetching request: %v", retryErr)
		return retryErr
	}

	req, err := http.NewRequest("GET", generatorURL, nil)
	if err != nil {
		log.Printf("Error during create request: %v", err)
		return err
	}

	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Connection", "keep-alive")

	defer retryRes.Body.Close()

	var currentData string
	scanner := bufio.NewScanner(retryRes.Body)

	for scanner.Scan() {
		line := scanner.Text()

		if line == "" {
			if currentData != "" {
				var payload []EventPayload

				err := json.Unmarshal([]byte(currentData), &payload)
				if err != nil {
					log.Printf("Error during parse JSON: %v", err)
					continue
				}

				log.Printf("Data from data-generator as stream: %s", currentData)

				currentData = ""
			}
			continue
		}

		if strings.HasPrefix(line, "data: ") {
			currentData = strings.TrimPrefix(line, "data: ")
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error during read stream: %v", err)
		return err
	}

	return nil
}

func fetchWithRetry(url string, cfg *config.ServerConfig) (*http.Response, error) {
	var resp *http.Response
	var err error
	currentDelay := cfg.DelayRetries

	for attempt := 1; attempt <= *cfg.MaxRetries; attempt++ {
		log.Printf("Attempt %d/%d: Connecting to generator...", attempt, *cfg.MaxRetries)
		resp, err = http.Get(url)
		if err == nil {
			return resp, nil
		}

		log.Printf("Connection failed: %v. Retrying in %v...", err, currentDelay)
		time.Sleep(currentDelay)
	}

	return nil, err
}
