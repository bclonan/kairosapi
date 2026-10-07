package action

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/bclonan/kairosapi/internal/workflow"
)

// Delay is a bounded, cancelable wait inside a run. It does not survive a restart.
func Delay(raw json.RawMessage) (workflow.Handler, error) {
	var config struct {
		Milliseconds int `json:"milliseconds"`
	}
	if err := workflow.Decode(raw, &config); err != nil || config.Milliseconds < 1 || config.Milliseconds > 60000 {
		return nil, errors.New("delay needs milliseconds between 1 and 60000")
	}
	return func(ctx context.Context, input map[string]any) (any, error) {
		timer := time.NewTimer(time.Duration(config.Milliseconds) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return input, nil
		}
	}, nil
}
