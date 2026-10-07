package artifact

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bclonan/kairosapi/internal/identity"
	bolt "go.etcd.io/bbolt"
)

// ReferenceID reads the reserved opaque file reference representation.
func ReferenceID(value any) (string, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return "", ErrInvalid
	}
	id, ok := object["$artifact"].(string)
	if !ok || !identity.Valid(id) {
		return "", ErrInvalid
	}
	return id, nil
}

func collect(value any) (map[string]bool, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var normalized any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&normalized); err != nil {
		return nil, err
	}
	ids := map[string]bool{}
	var walk func(any) error
	walk = func(value any) error {
		switch node := value.(type) {
		case map[string]any:
			if _, exists := node["$artifact"]; exists {
				id, err := ReferenceID(node)
				if err != nil {
					return err
				}
				ids[id] = true
				if len(ids) > 256 {
					return ErrInvalid
				}
			}
			for _, child := range node {
				if err := walk(child); err != nil {
					return err
				}
			}
		case []any:
			for _, child := range node {
				if err := walk(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return ids, walk(normalized)
}

// Pin validates references and prevents deletion while their owner is retained.
// The caller commits these entries with the run or workflow definition.
func ValidateReferences(tx *bolt.Tx, stagingOwner string, value any) error {
	ids, err := collect(value)
	if err != nil {
		return err
	}
	for id := range ids {
		saved, err := get(tx, id)
		if err != nil {
			return fmt.Errorf("file reference %s: %w", id, err)
		}
		if saved.Owner != "" && saved.Owner != stagingOwner {
			return ErrNotFound
		}
	}
	return nil
}

func Pin(tx *bolt.Tx, owner, stagingOwner string, value any) error {
	ids, err := collect(value)
	if err != nil {
		return err
	}
	for id := range ids {
		saved, err := get(tx, id)
		if err != nil {
			return fmt.Errorf("file reference %s: %w", id, err)
		}
		if saved.Owner != "" && saved.Owner != stagingOwner {
			return ErrNotFound
		}
		if err := tx.Bucket(references).Put([]byte(id+"\x00"+owner), []byte{1}); err != nil {
			return err
		}
		if err := tx.Bucket(owners).Put([]byte(owner+"\x00"+id), []byte(id)); err != nil {
			return err
		}
	}
	return nil
}

func Unpin(tx *bolt.Tx, owner string) error {
	prefix := owner + "\x00"
	cursor := tx.Bucket(owners).Cursor()
	var keys [][]byte
	for key, id := cursor.Seek([]byte(prefix)); strings.HasPrefix(string(key), prefix); key, id = cursor.Next() {
		if err := tx.Bucket(references).Delete([]byte(string(id) + "\x00" + owner)); err != nil {
			return err
		}
		keys = append(keys, append([]byte(nil), key...))
	}
	for _, key := range keys {
		if err := tx.Bucket(owners).Delete(key); err != nil {
			return err
		}
	}
	return nil
}

// Finalize publishes files referenced by the final run record and discards
// unreferenced staged bytes. The run state must commit in this same transaction.
func Finalize(tx *bolt.Tx, runID string, result any) error {
	ids, err := collect(result)
	if err != nil {
		return err
	}
	if err := Pin(tx, "run/"+runID, runID, result); err != nil {
		return err
	}
	prefix := runID + "/"
	cursor := tx.Bucket(staged).Cursor()
	var records []record
	for key, id := cursor.Seek([]byte(prefix)); strings.HasPrefix(string(key), prefix); key, id = cursor.Next() {
		saved, err := get(tx, string(id))
		if err != nil {
			return err
		}
		records = append(records, saved)
	}
	for _, saved := range records {
		if ids[saved.Metadata.ID] {
			saved.Owner = ""
			if err := write(tx, saved); err != nil {
				return err
			}
			if err := tx.Bucket(staged).Delete([]byte(prefix + saved.Metadata.ID)); err != nil {
				return err
			}
		} else if err := remove(tx, saved); err != nil {
			return err
		}
	}
	return nil
}
