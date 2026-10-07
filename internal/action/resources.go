package action

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"

	"github.com/bclonan/kairosapi/internal/artifact"
	"github.com/bclonan/kairosapi/internal/resource"
	"github.com/bclonan/kairosapi/internal/workflow"
)

type resourceState struct {
	files  *artifact.Store
	loader *resource.Loader
}

// Resources owns the lazy resource cache shared by dictionary and protobuf.
type Resources struct{ state atomic.Pointer[resourceState] }

func NewResources() *Resources { return &Resources{} }

func (r *Resources) SetFiles(files *artifact.Store) error {
	loader, err := resource.New(files)
	if err != nil {
		return err
	}
	if !r.state.CompareAndSwap(nil, &resourceState{files: files, loader: loader}) {
		return errors.New("resource file store is already configured")
	}
	return nil
}

type DictionaryConfig struct {
	Resource map[string]any `json:"resource"`
	Pointer  string         `json:"pointer,omitempty"`
}

func (r *Resources) Dictionary(raw json.RawMessage) (workflow.Handler, error) {
	var config DictionaryConfig
	if err := workflow.Decode(raw, &config); err != nil {
		return nil, errors.New("invalid dictionary action configuration")
	}
	if _, err := artifact.ReferenceID(config.Resource); err != nil {
		return nil, errors.New("dictionary needs an immutable resource file reference")
	}
	if err := workflow.ValidatePointer(config.Pointer); err != nil {
		return nil, err
	}
	state := r.state.Load()
	if state == nil {
		return nil, errors.New("dictionary requires a configured resource store")
	}
	return func(ctx context.Context, input map[string]any) (any, error) {
		pointer := config.Pointer
		if key, exists := input["key"]; exists {
			text, ok := key.(string)
			if !ok || len(text) > 1024 {
				return nil, errors.New("dictionary key must be a string of at most 1024 bytes")
			}
			pointer += "/" + strings.ReplaceAll(strings.ReplaceAll(text, "~", "~0"), "/", "~1")
		}
		value, err := state.loader.Lookup(ctx, config.Resource, pointer)
		if err != nil {
			return nil, err
		}
		output := map[string]any{"value": value}
		encoded, err := json.Marshal(output)
		if err != nil || len(encoded) > workflow.MaxDataBytes {
			return nil, errors.New("dictionary selection exceeds the action output limit")
		}
		return output, nil
	}, nil
}
