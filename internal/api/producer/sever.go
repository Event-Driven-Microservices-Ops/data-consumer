package api

import (
	"net/http"

	"github.com/urbaniakmichal/data-consumer/internal/config"
)

func NewServer(cs *config.Config) *http.Server {
	mux := http.NewServeMux()

	return &http.Server{
		Addr:              cs.Port,
		Handler:           mux,
		ReadHeaderTimeout: cs.ReadHeaderTimeout,
		ReadTimeout:       cs.ReadTimeout,
		WriteTimeout:      cs.WriteTimeout,
		IdleTimeout:       cs.IdleTimeout,
	}
}
