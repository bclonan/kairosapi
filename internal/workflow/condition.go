package workflow

import (
	"encoding/json"
	"errors"
	"math/big"
	"reflect"
	"strconv"
	"strings"
)

func validateCondition(c *Condition, deps map[string]bool, depth int) error {
	if c == nil {
		return nil
	}
	if depth > 8 || len(c.All)+len(c.Any) > 16 {
		return errors.New("condition exceeds size or depth limit")
	}
	groups := 0
	if len(c.All) > 0 {
		groups++
	}
	if len(c.Any) > 0 {
		groups++
	}
	if c.Not != nil {
		groups++
	}
	if c.Path != "" || c.Op != "" {
		groups++
	}
	if groups != 1 {
		return errors.New("condition must have one of path/op, all, any, or not")
	}
	for _, list := range [][]Condition{c.All, c.Any} {
		for i := range list {
			if err := validateCondition(&list[i], deps, depth+1); err != nil {
				return err
			}
		}
	}
	if c.Not != nil {
		return validateCondition(c.Not, deps, depth+1)
	}
	if c.Path == "" && c.Op == "" {
		return nil
	}
	switch c.Op {
	case "eq", "ne", "exists", "not_exists", "gt", "gte", "lt", "lte", "in":
	default:
		return errors.New("unsupported condition operator")
	}
	if err := validateReferences(map[string]any{"$ref": c.Path}, deps, 0); err != nil {
		return err
	}
	if c.Op == "in" {
		if _, ok := c.Value.([]any); !ok {
			return errors.New("in condition requires an array value")
		}
	}
	if c.Op == "gt" || c.Op == "gte" || c.Op == "lt" || c.Op == "lte" {
		if _, ok := number(c.Value); !ok {
			return errors.New("comparison condition requires a numeric value")
		}
	}
	return nil
}

func evaluate(c *Condition, root map[string]any) (bool, error) {
	if c == nil {
		return true, nil
	}
	if len(c.All) > 0 {
		for i := range c.All {
			ok, err := evaluate(&c.All[i], root)
			if err != nil || !ok {
				return ok, err
			}
		}
		return true, nil
	}
	if len(c.Any) > 0 {
		for i := range c.Any {
			ok, err := evaluate(&c.Any[i], root)
			if err != nil || ok {
				return ok, err
			}
		}
		return false, nil
	}
	if c.Not != nil {
		ok, err := evaluate(c.Not, root)
		return !ok, err
	}
	actual, err := Lookup(root, c.Path)
	if c.Op == "exists" {
		return err == nil, nil
	}
	if c.Op == "not_exists" {
		return err != nil, nil
	}
	if err != nil {
		return false, err
	}
	switch c.Op {
	case "eq":
		return Equal(actual, c.Value), nil
	case "ne":
		return !Equal(actual, c.Value), nil
	case "in":
		for _, v := range c.Value.([]any) {
			if Equal(actual, v) {
				return true, nil
			}
		}
		return false, nil
	default:
		a, ok := number(actual)
		b, bOK := number(c.Value)
		if !ok || !bOK {
			return false, errors.New("comparison input is not a number")
		}
		d := a.Cmp(b)
		switch c.Op {
		case "gt":
			return d > 0, nil
		case "gte":
			return d >= 0, nil
		case "lt":
			return d < 0, nil
		case "lte":
			return d <= 0, nil
		}
	}
	return false, errors.New("unsupported condition")
}

func number(v any) (*big.Rat, bool) {
	n, ok := v.(json.Number)
	if !ok || len(n) > 1024 {
		return nil, false
	}
	// Bound exponents before big.Rat allocates memory for a caller's number.
	if i := strings.IndexAny(string(n), "eE"); i >= 0 {
		exponent, err := strconv.Atoi(string(n)[i+1:])
		if err != nil || exponent < -1024 || exponent > 1024 {
			return nil, false
		}
	}
	r, ok := new(big.Rat).SetString(string(n))
	return r, ok
}

func Equal(a, b any) bool {
	if an, ok := number(a); ok {
		if bn, ok := number(b); ok {
			return an.Cmp(bn) == 0
		}
	}
	if am, ok := a.(map[string]any); ok {
		bm, ok := b.(map[string]any)
		if !ok || len(am) != len(bm) {
			return false
		}
		for key, value := range am {
			other, exists := bm[key]
			if !exists || !Equal(value, other) {
				return false
			}
		}
		return true
	}
	if aa, ok := a.([]any); ok {
		ba, ok := b.([]any)
		if !ok || len(aa) != len(ba) {
			return false
		}
		for i := range aa {
			if !Equal(aa[i], ba[i]) {
				return false
			}
		}
		return true
	}
	return reflect.DeepEqual(a, b)
}

func validateTriggers(triggers []Trigger) error {
	if len(triggers) > 16 {
		return errors.New("at most 16 event triggers are allowed")
	}
	for _, trigger := range triggers {
		if trigger.Type == "" || len(trigger.Type) > 256 || len(trigger.Source) > 1024 || len(trigger.Subject) > 1024 || strings.Count(trigger.Type, "*") > 1 || (strings.Contains(trigger.Type, "*") && !strings.HasSuffix(trigger.Type, "*")) {
			return errors.New("invalid event trigger")
		}
		for pointer := range trigger.Match {
			if err := ValidatePointer(pointer); err != nil {
				return errors.New("trigger match uses invalid JSON pointer")
			}
		}
	}
	return nil
}

func Matches(trigger Trigger, event map[string]any) bool {
	kind, _ := event["type"].(string)
	if strings.HasSuffix(trigger.Type, "*") {
		if !strings.HasPrefix(kind, strings.TrimSuffix(trigger.Type, "*")) {
			return false
		}
	} else if kind != trigger.Type {
		return false
	}
	if trigger.Source != "" && trigger.Source != event["source"] {
		return false
	}
	if trigger.Subject != "" && trigger.Subject != event["subject"] {
		return false
	}
	for path, expected := range trigger.Match {
		value, err := Lookup(event, path)
		if err != nil || !Equal(value, expected) {
			return false
		}
	}
	return true
}
