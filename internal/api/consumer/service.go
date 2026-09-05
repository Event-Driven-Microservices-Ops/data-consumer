package consumer

import (
	"bufio"
	"encoding/json"
	"log"
	"net/http"
	"strings"
)

type DataConsumerService struct {
}

func NewDataConsumerService() *DataConsumerService {
	return &DataConsumerService{}
}

func (dts *DataConsumerService) CollectDataAsStream(generatorURL string) error {
	req, err := http.NewRequest("GET", generatorURL, nil)
	if err != nil {
		log.Printf("Error during create request: %v", err)
		return err
	}

	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Connection", "keep-alive")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error during create connection: %v", err)
		return err
	}
	defer resp.Body.Close()

	var currentData string
	scanner := bufio.NewScanner(resp.Body)

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

				log.Printf("Data from data-generator: %s", currentData)

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
