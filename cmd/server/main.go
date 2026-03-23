package main

import (
	"log"
	"net/http"

	"github.com/SkinnyPeteTheGiraffe/gographgo/internal/runtimecfg"
	"github.com/SkinnyPeteTheGiraffe/gographgo/internal/server"
)

func main() {
	bootstrap, err := runtimecfg.LoadServerBootstrapFromEnv()
	if err != nil {
		log.Fatalf("invalid server runtime config: %v", err)
	}

	s := server.New(server.Options{Checkpointer: bootstrap.Checkpointer, APIKey: bootstrap.APIKey})
	log.Printf("gographgo server listening on %s", bootstrap.Addr)
	if err := http.ListenAndServe(bootstrap.Addr, s.Handler()); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
