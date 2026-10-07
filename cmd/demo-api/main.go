// This localhost-only fixture demonstrates HTTP orchestration. It is not a
// connector for a real customer system and it stores nothing permanently.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

func main() {
	var customers, notifications atomic.Int64
	mux := http.NewServeMux()
	async := registerAsyncDemo(mux)
	mux.HandleFunc("POST /customers", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Name string `json:"name"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
		defer r.Body.Close()
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.Name == "" {
			http.Error(w, "name required", 400)
			return
		}
		id := fmt.Sprintf("customer_%d", customers.Add(1))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "name": input.Name})
	})
	mux.HandleFunc("POST /notifications/{id}", func(w http.ResponseWriter, r *http.Request) {
		notifications.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "customer_id": r.PathValue("id")})
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		stats := async.stats()
		stats["customers"], stats["notifications"] = customers.Load(), notifications.Load()
		_ = json.NewEncoder(w).Encode(stats)
	})
	mux.HandleFunc("POST /files/echo", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		defer r.Body.Close()
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "file exceeds demo limit", 413)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	})
	mux.HandleFunc("POST /files/inspect", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		defer r.Body.Close()
		reader, err := r.MultipartReader()
		if err != nil {
			http.Error(w, "multipart required", 400)
			return
		}
		var received []map[string]any
		for {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				http.Error(w, "invalid multipart data", 400)
				return
			}
			sum := sha256.New()
			size, err := io.Copy(sum, part)
			if err != nil {
				http.Error(w, "incomplete file", 400)
				return
			}
			if part.FileName() != "" {
				received = append(received, map[string]any{"field": part.FormName(), "name": part.FileName(), "size": size, "sha256": fmt.Sprintf("%x", sum.Sum(nil))})
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"files": received})
	})
	server := &http.Server{Addr: "127.0.0.1:9081", Handler: mux, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	log.Print("local demonstration API listening on 127.0.0.1:9081")
	log.Fatal(server.ListenAndServe())
}
