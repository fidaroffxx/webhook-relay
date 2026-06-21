// Mock webhook subscribers for local delivery testing.
//
// Run:
//
//	go run ./cmd/mock-subscribers
//
// Servers:
//   - http://127.0.0.1:9001/post  → always 200
//   - http://127.0.0.1:9002/post  → always 200
//   - http://127.0.0.1:9003/post  → always 500 (retry testing)
package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type serverConfig struct {
	addr    string
	name    string
	handler http.HandlerFunc
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	servers := []serverConfig{
		{addr: ":9001", name: "subscriber-ok-1", handler: handlerOK("subscriber-ok-1")},
		{addr: ":9002", name: "subscriber-ok-2", handler: handlerOK("subscriber-ok-2")},
		{addr: ":9003", name: "subscriber-fail", handler: handlerFail("subscriber-fail")},
	}

	for _, cfg := range servers {
		go startServer(cfg)
	}

	log.Println("mock subscribers running:")
	log.Println("  http://127.0.0.1:9001/post")
	log.Println("  http://127.0.0.1:9002/post")
	log.Println("  http://127.0.0.1:9003/post  (returns 500)")
	log.Println("Ctrl+C to stop")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down")
}

func startServer(cfg serverConfig) {
	mux := http.NewServeMux()
	mux.HandleFunc("/post", cfg.handler)
	mux.HandleFunc("/", cfg.handler)

	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("[%s] listening on %s", cfg.name, cfg.addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[%s] server error: %v", cfg.name, err)
	}
}

func handlerOK(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		_ = r.Body.Close()

		log.Printf("[%s] %s %s from=%s body=%s",
			name,
			r.Method,
			r.URL.Path,
			r.RemoteAddr,
			string(body),
		)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}
}

func handlerFail(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		_ = r.Body.Close()

		log.Printf("[%s] %s %s from=%s body=%s (responding 500)",
			name,
			r.Method,
			r.URL.Path,
			r.RemoteAddr,
			string(body),
		)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"ok":false,"error":"simulated failure"}`))
	}
}
