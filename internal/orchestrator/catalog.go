package orchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/bclonan/kairosapi/internal/artifact"
	"github.com/bclonan/kairosapi/internal/workflow"
	bolt "go.etcd.io/bbolt"
)

const MaxDefinitions = 128

func definitionKey(id string, version int) string { return id + "/" + strconv.Itoa(version) }

func copyDefinition(def workflow.Definition) (workflow.Definition, []byte, error) {
	if def.SpecVersion == "" {
		def.SpecVersion = "1.0"
	}
	data, err := json.Marshal(def)
	if err != nil || len(data) > 1<<20 {
		return def, nil, errors.New("invalid or oversized specification")
	}
	var copied workflow.Definition
	err = workflow.Decode(data, &copied)
	return copied, data, err
}

func (s *Service) loadCatalog(seeds []workflow.Definition) error {
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(definitionsBucket).ForEach(func(k, v []byte) error {
			var def workflow.Definition
			if err := workflow.Decode(v, &def); err != nil {
				return err
			}
			s.definitions[string(k)] = def
			return nil
		})
	})
	if err != nil {
		return err
	}
	for _, seed := range seeds {
		def, data, err := copyDefinition(seed)
		if err != nil {
			return err
		}
		key := definitionKey(def.ID, def.Version)
		if existing, ok := s.definitions[key]; ok {
			original, _ := json.Marshal(existing)
			if !bytes.Equal(original, data) {
				return fmt.Errorf("seed %s: %w", key, ErrConflict)
			}
		}
		s.definitions[key] = def
	}
	if len(s.definitions) > MaxDefinitions {
		return errors.New("catalog exceeds 128 retained versions")
	}
	visiting := map[string]bool{}
	var compile workflow.Resolver
	compile = func(ref workflow.Reference) (*workflow.Plan, error) {
		key := definitionKey(ref.WorkflowID, ref.Version)
		if plan := s.plans[key]; plan != nil {
			return plan, nil
		}
		def, ok := s.definitions[key]
		if !ok {
			return nil, ErrNotFound
		}
		if visiting[key] {
			return nil, errors.New("subflow cycle")
		}
		visiting[key] = true
		plan, err := workflow.Compile(def, s.registry, s.options.Timeout, compile)
		if err != nil {
			return nil, fmt.Errorf("compile %s: %w", key, err)
		}
		delete(visiting, key)
		s.plans[key] = plan
		s.latest[def.ID] = max(s.latest[def.ID], def.Version)
		return plan, nil
	}
	for _, def := range s.definitions {
		if _, err := compile(workflow.Reference{WorkflowID: def.ID, Version: def.Version}); err != nil {
			return err
		}
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		for key, def := range s.definitions {
			if err := artifact.Pin(tx, "workflow/"+key, "", def); err != nil {
				return err
			}
			id, err := publication(tx, key)
			if err != nil {
				return err
			}
			s.publications[key] = id
			data, _ := json.Marshal(def)
			if err := tx.Bucket(definitionsBucket).Put([]byte(key), data); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Service) resolve(ref workflow.Reference) (*workflow.Plan, error) {
	if ref.Version == 0 {
		ref.Version = s.latest[ref.WorkflowID]
	}
	plan := s.plans[definitionKey(ref.WorkflowID, ref.Version)]
	if plan == nil {
		return nil, ErrNotFound
	}
	return plan, nil
}

// Publish writes a new immutable version. Repeating identical content is harmless.
func (s *Service) Publish(def workflow.Definition) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.storageFailed {
		return false, ErrUnavailable
	}
	def, data, err := copyDefinition(def)
	if err != nil {
		return false, err
	}
	key := definitionKey(def.ID, def.Version)
	if existing, ok := s.definitions[key]; ok {
		original, _ := json.Marshal(existing)
		if !bytes.Equal(original, data) {
			return false, ErrConflict
		}
		return false, nil
	}
	if len(s.definitions) >= MaxDefinitions {
		return false, errors.New("catalog exceeds 128 retained versions")
	}
	plan, err := workflow.Compile(def, s.registry, s.options.Timeout, s.resolve)
	if err != nil {
		return false, err
	}
	var publicationID string
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if err := artifact.Pin(tx, "workflow/"+key, "", def); err != nil {
			return err
		}
		var err error
		publicationID, err = publication(tx, key)
		if err != nil {
			return err
		}
		return tx.Bucket(definitionsBucket).Put([]byte(key), data)
	}); err != nil {
		if errors.Is(err, artifact.ErrNotFound) || errors.Is(err, artifact.ErrInvalid) {
			return false, err
		}
		s.failStorage()
		return false, ErrUnavailable
	}
	s.definitions[key], s.plans[key] = def, plan
	s.publications[key] = publicationID
	s.latest[def.ID] = max(s.latest[def.ID], def.Version)
	return true, nil
}

type Entry struct {
	ID            string `json:"id"`
	Version       int    `json:"version"`
	Description   string `json:"description,omitempty"`
	Latest        bool   `json:"latest"`
	PublicationID string `json:"publication_id"`
}

func (s *Service) List() []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := make([]Entry, 0, len(s.definitions))
	for _, def := range s.definitions {
		entries = append(entries, Entry{def.ID, def.Version, def.Description, s.latest[def.ID] == def.Version, s.publications[definitionKey(def.ID, def.Version)]})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ID == entries[j].ID {
			return entries[i].Version > entries[j].Version
		}
		return entries[i].ID < entries[j].ID
	})
	return entries
}

func (s *Service) Definition(id string, version int) (workflow.Definition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if version == 0 {
		version = s.latest[id]
	}
	def, ok := s.definitions[definitionKey(id, version)]
	if !ok {
		return def, ErrNotFound
	}
	copy, _, err := copyDefinition(def)
	return copy, err
}
