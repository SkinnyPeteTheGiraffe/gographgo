package main

import (
	"log"
	"net/http"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/internal/runtimecfg"
	"github.com/SkinnyPeteTheGiraffe/gographgo/internal/server"
)

func main() {
	bootstrap, err := runtimecfg.LoadServerBootstrapFromEnv()
	if err != nil {
		log.Fatalf("invalid server runtime config: %v", err)
	}

	s := server.New(server.Options{Checkpointer: bootstrap.Checkpointer, APIKey: bootstrap.APIKey})
	httpServer := &http.Server{
		Addr:              bootstrap.Addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       0,
		WriteTimeout:      0,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("gographgo server listening on %s", bootstrap.Addr)
	if err := httpServer.ListenAndServe(); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
