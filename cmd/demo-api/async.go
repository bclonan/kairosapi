package main

import (
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"sync"

	"github.com/bclonan/kairosapi/internal/workflow"
)

type demoOperation struct {
	ID      string
	Value   string
	Fail    bool
	Polls   int
	Receipt bool
	Hash    [32]byte
}

type asyncDemo struct {
	mu         sync.Mutex
	operations map[string]*demoOperation
	keys       map[string]string
	polls      int64
	receipts   int64
}

// This fixture intentionally completes on its second status check. That makes
// the pending -> terminal transition repeatable without wall-clock guesses.
func registerAsyncDemo(mux *http.ServeMux) *asyncDemo {
	demo := &asyncDemo{operations: map[string]*demoOperation{}, keys: map[string]string{}}
	mux.HandleFunc("POST /operations", demo.start)
	mux.HandleFunc("GET /operations/{id}", demo.poll)
	mux.HandleFunc("POST /operations/{id}/receipt", demo.receipt)
	return demo
}

func (d *asyncDemo) start(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 256 {
		http.Error(w, "Idempotency-Key required, at most 256 bytes", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<16))
	var input struct {
		Value string `json:"value"`
		Fail  bool   `json:"fail"`
	}
	if err != nil || workflow.Decode(data, &input) != nil || input.Value == "" {
		http.Error(w, "value required", http.StatusBadRequest)
		return
	}
	sum := sha256.Sum256(data)
	d.mu.Lock()
	defer d.mu.Unlock()
	id := d.keys[key]
	if id != "" && d.operations[id].Hash != sum {
		http.Error(w, "idempotency key payload changed", http.StatusConflict)
		return
	}
	if id == "" {
		if len(d.operations) >= 1000 {
			http.Error(w, "demo operation limit reached, restart fixture", http.StatusTooManyRequests)
			return
		}
		id = workflow.NewID()
		d.operations[id] = &demoOperation{ID: id, Value: input.Value, Fail: input.Fail, Hash: sum}
		d.keys[key] = id
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location", "/operations/"+id)
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "status": "pending", "operation_url": "/operations/" + id})
}

func (d *asyncDemo) poll(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	operation := d.operations[r.PathValue("id")]
	if operation == nil {
		http.Error(w, "operation missing", http.StatusNotFound)
		return
	}
	operation.Polls++
	d.polls++
	status := "pending"
	if operation.Polls >= 2 {
		status = "completed"
		if operation.Fail {
			status = "failed"
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"id": operation.ID, "status": status, "value": operation.Value, "polls": operation.Polls})
}

func (d *asyncDemo) receipt(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	operation := d.operations[r.PathValue("id")]
	if operation == nil {
		http.Error(w, "operation missing", http.StatusNotFound)
		return
	}
	if operation.Polls < 2 || operation.Fail {
		http.Error(w, "operation must complete first", http.StatusConflict)
		return
	}
	if !operation.Receipt {
		operation.Receipt = true
		d.receipts++
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"operation_id": operation.ID, "accepted": true, "value": operation.Value})
}

func (d *asyncDemo) stats() map[string]int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return map[string]int64{"operations": int64(len(d.operations)), "operation_polls": d.polls, "operation_receipts": d.receipts}
}
