package consumer

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/urbaniakmichal/data-consumer/internal/broker/producer"
	"github.com/urbaniakmichal/data-consumer/internal/config"
)

type DataConsumerService struct {
	kS *producer.KafkaService
}

func NewDataConsumerService(kS *producer.KafkaService) *DataConsumerService {
	return &DataConsumerService{
		kS: kS,
	}
}

func (dts *DataConsumerService) RunBatch(cfg *config.Config, url string) {
	err, payload := collectDataAsBatch(cfg, url)
	if err != nil {
		log.Printf("Batch consumer error: %v", err)
		return
	}

	anyPayload := preparePayload(payload)
	dts.kS.Publish(anyPayload)
}

func (dts *DataConsumerService) RunStream(cfg *config.Config, url string) {
	for {
		err := collectDataAsStream(cfg, url, func(payload []EventPayload) {
			anyPayload := preparePayload(payload)
			dts.kS.Publish(anyPayload)
		})

		if err != nil {
			log.Printf("Stream consumer error: %v. Reconnecting in %v...", err, cfg.DelayRetries)
			time.Sleep(cfg.DelayRetries)
		}
	}
}

func collectDataAsBatch(cfg *config.Config, url string) (error, []EventPayload) {
	headers := map[string]string{
		"Content-Type":  "application/json",
		"Cache-Control": "no-cache",
	}

	retryRes, retryErr := fetchWithRetry(url, headers, cfg)
	if retryErr != nil {
		log.Printf("Error during fetching request: %v", retryErr)
		return retryErr, nil
	}
	defer retryRes.Body.Close()

	byteSlice, err := io.ReadAll(retryRes.Body)
	if err != nil {
		log.Printf("Error in reading request body: %v", err)
		return err, nil
	}

	var payload []EventPayload
	err = json.Unmarshal(byteSlice, &payload)
	if err != nil {
		log.Println("Error in json unmarshal")
		return err, nil
	}

	log.Printf("Data from data-generator as batch: %+v", payload)

	return nil, payload
}

func collectDataAsStream(cfg *config.Config, url string, onData func([]EventPayload)) error {
	headers := map[string]string{
		"Accept":        "text/event-stream",
		"Cache-Control": "no-cache",
		"Connection":    "keep-alive",
	}

	retryRes, retryErr := fetchWithRetry(url, headers, cfg)
	if retryErr != nil {
		log.Printf("Error during fetching request: %v", retryErr)
		return retryErr
	}
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
					var singleEvent EventPayload
					errSingle := json.Unmarshal([]byte(currentData), &singleEvent)
					if errSingle != nil {
						log.Printf("Error during parse JSON: %v", err)
						currentData = ""
						continue
					}
					payload = []EventPayload{singleEvent}
				}

				log.Printf("Data from data-generator as stream: %s", currentData)

				onData(payload)

				currentData = ""
			}
			continue
		}

		if strings.HasPrefix(line, "data: ") {
			currentData = strings.TrimPrefix(line, "data: ")
		}
	}

	return scanner.Err()
}

func fetchWithRetry(url string, headers map[string]string, cfg *config.Config) (*http.Response, error) {
	var resp *http.Response
	var lastErr error
	currentDelay := cfg.DelayRetries
	maxRetries := 5
	if cfg.MaxRetries != nil {
		maxRetries = *cfg.MaxRetries
	}

	client := &http.Client{}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("Attempt %d/%d: Connecting to generator...", attempt, maxRetries)

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Printf("Error during create request: %v", err)
			return nil, err
		}

		for k, v := range headers {
			req.Header.Set(k, v)
		}

		resp, err = client.Do(req)
		if err == nil {
			if resp.StatusCode == http.StatusOK {
				return resp, nil
			}
			lastErr = fmt.Errorf("server returned status: %d", resp.StatusCode)
			log.Printf("Connection failed with status: %d. Retrying in %v...", resp.StatusCode, currentDelay)
			resp.Body.Close()
		} else {
			lastErr = err
			log.Printf("Connection failed: %v. Retrying in %v...", err, currentDelay)
		}

		time.Sleep(currentDelay)
	}

	return nil, lastErr
}

func preparePayload(payload []EventPayload) []any {
	anyPayload := make([]any, len(payload))
	for i, v := range payload {
		anyPayload[i] = v
	}

	return anyPayload
}
