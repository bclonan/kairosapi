package workflow

import (
	"encoding/json"
	"errors"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

var ErrInput = errors.New("input does not satisfy the workflow schema or data limits")

type localSchemas struct{}

func (localSchemas) Load(string) (any, error) {
	return nil, errors.New("external schema references are disabled; use local $defs")
}

func compileSchema(raw json.RawMessage) (*jsonschema.Schema, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw) > 32<<10 {
		return nil, errors.New("input_schema exceeds 32 KiB")
	}
	var doc any
	if err := Decode(raw, &doc); err != nil {
		return nil, errors.New("invalid input_schema JSON")
	}
	budget := 64 << 10
	if _, err := resolve(doc, nil, &budget, 0); err != nil {
		return nil, errors.New("input_schema exceeds data limits")
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.UseLoader(localSchemas{})
	const location = "https://kairos.invalid/input.json"
	if err := c.AddResource(location, doc); err != nil {
		return nil, errors.New("invalid input_schema")
	}
	schema, err := c.Compile(location)
	if err != nil {
		return nil, errors.New("invalid input_schema or external schema reference")
	}
	return schema, nil
}

// ValidateInput checks the data before a run enters the queue or calls an action.
func (p *Plan) ValidateInput(input map[string]any) error {
	_, err := p.copyInput(input)
	return err
}

func (p *Plan) copyInput(input map[string]any) (map[string]any, error) {
	if input == nil {
		input = map[string]any{}
	}
	raw, err := json.Marshal(input)
	if err != nil || len(raw) > MaxDataBytes {
		return nil, ErrInput
	}
	var normalized map[string]any
	if err := Decode(raw, &normalized); err != nil {
		return nil, ErrInput
	}
	budget := MaxDataBytes
	if _, err := resolve(normalized, nil, &budget, 0); err != nil {
		return nil, ErrInput
	}
	if p.schema != nil && p.schema.Validate(normalized) != nil {
		return nil, ErrInput
	}
	return normalized, nil
}
