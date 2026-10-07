package orchestrator

import (
	"encoding/json"
	"errors"
	"net/url"
	"sort"
	"time"

	"github.com/bclonan/kairosapi/internal/workflow"
	bolt "go.etcd.io/bbolt"
)

type Delivery struct {
	EventID   string `json:"event_id"`
	Runs      []Run  `json:"runs"`
	Duplicate bool   `json:"duplicate"`
	ReceiptID string `json:"receipt_id"`
}

// Emit accepts structured CloudEvents 1.0 with JSON data. Each latest matching
// specification receives {"event": the complete event, "data": its data value}.
func (s *Service) Emit(event map[string]any) (Delivery, error) {
	data, err := json.Marshal(event)
	if err != nil || len(data) > workflow.MaxDataBytes {
		return Delivery{}, errors.New("event exceeds data limit")
	}
	var normalized map[string]any
	if err := workflow.Decode(data, &normalized); err != nil {
		return Delivery{}, errors.New("invalid event JSON")
	}
	event = normalized
	if err := validateEvent(event); err != nil {
		return Delivery{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	identity := []any{event["source"], event["id"]}
	runs, duplicate, err := s.admit("event/"+hash(identity), hash(event), func() ([]storedRun, error) {
		ids := make([]string, 0, len(s.latest))
		for id := range s.latest {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		records := []storedRun{}
		for _, id := range ids {
			version := s.latest[id]
			def := s.definitions[definitionKey(id, version)]
			matched := false
			for _, trigger := range def.Triggers {
				if workflow.Matches(trigger, event) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			record, _, err := s.prepare(Request{WorkflowID: id, Version: version, Input: map[string]any{"event": event, "data": event["data"]}})
			if err != nil {
				return nil, err
			}
			records = append(records, record)
		}
		return records, nil
	})
	if runs == nil {
		runs = []Run{}
	}
	var saved receipt
	if err == nil {
		err = s.db.View(func(tx *bolt.Tx) error {
			return workflow.Decode(tx.Bucket(receiptsBucket).Get([]byte("event/"+hash(identity))), &saved)
		})
	}
	return Delivery{event["id"].(string), runs, duplicate, saved.ID}, err
}

func validateEvent(event map[string]any) error {
	if event["specversion"] != "1.0" {
		return errors.New("event specversion must be 1.0")
	}
	for _, key := range []string{"id", "source", "type"} {
		value, ok := event[key].(string)
		if !ok || value == "" || len(value) > 1024 {
			return errors.New("event needs nonempty string id, source, and type of at most 1024 bytes")
		}
	}
	if _, err := url.Parse(event["source"].(string)); err != nil {
		return errors.New("event source must be a URI reference")
	}
	for _, key := range []string{"subject", "time", "datacontenttype", "dataschema"} {
		if value, exists := event[key]; exists {
			text, ok := value.(string)
			if !ok || len(text) > 1024 {
				return errors.New("invalid event attribute")
			}
		}
	}
	if timestamp, ok := event["time"].(string); ok {
		if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
			return errors.New("event time must use RFC3339")
		}
	}
	if _, exists := event["data_base64"]; exists {
		return errors.New("binary event data is unsupported; send JSON data")
	}
	encoded, err := json.Marshal(event)
	if err != nil || len(encoded) > workflow.MaxDataBytes {
		return errors.New("event exceeds data limit")
	}
	var limits workflow.Plan
	if err := limits.ValidateInput(event); err != nil {
		return err
	}
	return nil
}
