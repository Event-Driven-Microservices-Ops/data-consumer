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

	"github.com/urbaniakmichal/data-consumer/internal/config"
	"github.com/urbaniakmichal/data-consumer/internal/database"
)

type DataConsumerService struct {
	dbService *database.DataBaseService
}

func NewDataConsumerService(dS *database.DataBaseService) *DataConsumerService {
	return &DataConsumerService{
		dbService: dS,
	}
}

func (dts *DataConsumerService) CollectDataAsBatch(generatorURL string, cfg *config.Config) error {
	headers := map[string]string{
		"Content-Type":  "application/json",
		"Cache-Control": "no-cache",
	}

	retryRes, retryErr := fetchWithRetry(generatorURL, headers, cfg)
	if retryErr != nil {
		log.Printf("Error during fetching request: %v", retryErr)
		return retryErr
	}
	defer retryRes.Body.Close()

	byteSlice, err := io.ReadAll(retryRes.Body)
	if err != nil {
		log.Printf("Error in reading request body: %v", err)
		return err
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

func (dts *DataConsumerService) CollectDataAsStream(generatorURL string, cfg *config.Config) error {
	headers := map[string]string{
		"Accept":        "text/event-stream",
		"Cache-Control": "no-cache",
		"Connection":    "keep-alive",
	}

	retryRes, retryErr := fetchWithRetry(generatorURL, headers, cfg)
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
