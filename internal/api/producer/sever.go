package api

import (
	"net/http"

	"github.com/urbaniakmichal/data-consumer/internal/config"
)

func NewServer(cs *config.ServerConfig) *http.Server {
	// s := NewService()
	// rh := NewRestHandler(s)

	mux := http.NewServeMux()
	// mux.HandleFunc("GET "+ApiPathDataAsStream, rh.GetDataAsBatch)

	return &http.Server{
		Addr:              cs.Port,
		Handler:           mux,
		ReadHeaderTimeout: cs.ReadHeaderTimeout,
		ReadTimeout:       cs.ReadTimeout,
		WriteTimeout:      cs.WriteTimeout,
		IdleTimeout:       cs.IdleTimeout,
	}
}
