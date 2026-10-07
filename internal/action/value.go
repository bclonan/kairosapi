package action

import (
	"context"
	"encoding/json"

	"github.com/bclonan/kairosapi/internal/workflow"
)

// Value returns its resolved input. It can assemble data for later steps.
func Value(config json.RawMessage) (workflow.Handler, error) {
	if len(config) > 0 {
		if err := workflow.Decode(config, &struct{}{}); err != nil {
			return nil, err
		}
	}
	return func(ctx context.Context, input map[string]any) (any, error) {
		return input, ctx.Err()
	}, nil
}
