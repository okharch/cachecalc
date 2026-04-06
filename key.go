package cachecalc

import (
	"encoding/json"
	"fmt"
	"reflect"
)

type keyPart struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
}

// MakeKey builds a stable string cache key from typed parts.
// Different Go types intentionally produce different keys, so 1 and "1" do not collide.
func MakeKey(parts ...any) (string, error) {
	encoded := make([]keyPart, 0, len(parts))
	for _, part := range parts {
		encoded = append(encoded, keyPart{
			Type:  keyPartType(part),
			Value: part,
		})
	}
	buf, err := json.Marshal(encoded)
	if err != nil {
		return "", fmt.Errorf("make cache key: %w", err)
	}
	return string(buf), nil
}

func keyPartType(v any) string {
	if v == nil {
		return "nil"
	}
	return reflect.TypeOf(v).String()
}
