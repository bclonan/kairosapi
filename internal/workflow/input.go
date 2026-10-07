package workflow

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

func ValidatePointer(pointer string) error {
	_, err := pointerParts(pointer)
	return err
}

func Lookup(value any, pointer string) (any, error) {
	parts, err := pointerParts(pointer)
	if err != nil {
		return nil, err
	}
	for _, part := range parts {
		switch current := value.(type) {
		case map[string]any:
			var ok bool
			value, ok = current[part]
			if !ok {
				return nil, errors.New("reference does not exist")
			}
		case []any:
			i, err := strconv.Atoi(part)
			if err != nil || i < 0 || i >= len(current) || strconv.Itoa(i) != part {
				return nil, errors.New("invalid array reference")
			}
			value = current[i]
		default:
			return nil, errors.New("reference does not identify a value")
		}
	}
	return value, nil
}

func resolve(value any, root map[string]any, budget *int, depth int) (any, error) {
	*budget -= 8
	if *budget < 0 || depth > 32 {
		return nil, errors.New("step input exceeds size or depth limit")
	}
	switch value := value.(type) {
	case map[string]any:
		if root != nil {
			if literal, exists := value["$literal"]; exists {
				return resolve(literal, nil, budget, depth+1)
			}
			if joined, exists := value["$concat"]; exists {
				items, ok := joined.([]any)
				if !ok {
					return nil, errors.New("$concat requires an array")
				}
				var out strings.Builder
				for _, item := range items {
					child, err := resolve(item, root, budget, depth+1)
					if err != nil {
						return nil, err
					}
					switch child := child.(type) {
					case string:
						out.WriteString(child)
					case json.Number:
						out.WriteString(child.String())
					case bool:
						out.WriteString(strconv.FormatBool(child))
					default:
						return nil, errors.New("$concat values must be strings, numbers, or booleans")
					}
				}
				return out.String(), nil
			}
		}
		if ref, ok := value["$ref"].(string); ok && root != nil {
			selected, err := Lookup(root, ref)
			if err != nil {
				var hasDefault bool
				selected, hasDefault = value["default"]
				if !hasDefault {
					return nil, err
				}
			}
			// Referenced data is copied, never interpreted as another template.
			return resolve(selected, nil, budget, depth+1)
		}
		out := make(map[string]any, len(value))
		for key, child := range value {
			*budget -= len(key)
			resolved, err := resolve(child, root, budget, depth+1)
			if err != nil {
				return nil, err
			}
			out[key] = resolved
		}
		return out, nil
	case []any:
		out := make([]any, len(value))
		for i, child := range value {
			resolved, err := resolve(child, root, budget, depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = resolved
		}
		return out, nil
	default:
		if n, ok := value.(json.Number); ok {
			if _, valid := number(n); !valid {
				return nil, errors.New("number exceeds precision or exponent limit")
			}
		}
		data, err := json.Marshal(value)
		if err != nil || len(data) > *budget {
			return nil, errors.New("step input exceeds size limit or contains invalid JSON")
		}
		*budget -= len(data)
		return value, nil
	}
}
