package config

import (
	"strings"
	"testing"
)

func TestConfigurationValidation(t *testing.T) {
	for _, values := range []map[string]string{
		{}, {"KAIROS_API_TOKEN": "short"}, {"KAIROS_WORKERS": "0"}, {"KAIROS_MAX_RUNS": "999"},
		{"KAIROS_RUN_TIMEOUT": "0s"}, {"KAIROS_RUN_TIMEOUT": "10m"},
		{"KAIROS_ADDR": "missing-port"}, {"KAIROS_ALLOW_PRIVATE_NETWORKS": "maybe"}, {"KAIROS_HTTP_ALLOWED_ORIGINS": "https://example.com,"},
		{"KAIROS_ADMIN_TOKEN": "short"}, {"KAIROS_ADMIN_TOKEN": strings.Repeat("t", 32)},
		{"KAIROS_QUEUE_SIZE": "-1"}, {"KAIROS_QUEUE_SIZE": "4097"}, {"KAIROS_MAX_RECORDS": "10"},
		{"KAIROS_RETENTION": "30s"}, {"KAIROS_RETENTION": "8761h"},
		{"KAIROS_FILE_MAX_BYTES": "0"}, {"KAIROS_FILE_MAX_BYTES": "268435457"}, {"KAIROS_FILE_MAX_BYTES": "invalid"},
		{"KAIROS_FILE_TOTAL_BYTES": "1"}, {"KAIROS_FILE_TOTAL_BYTES": "1099511627777"},
		{"KAIROS_FILE_MAX_COUNT": "100001"}, {"KAIROS_FILE_TRANSFERS": "0"}, {"KAIROS_FILE_TRANSFERS": "17"},
	} {
		get := func(key string) string {
			if len(values) > 0 && key == "KAIROS_API_TOKEN" {
				if v, ok := values[key]; ok {
					return v
				}
				return strings.Repeat("t", 32)
			}
			return values[key]
		}
		if _, err := FromEnv(get); err == nil {
			t.Errorf("accepted invalid configuration for %v", values)
		}
	}
	c, err := FromEnv(func(key string) string {
		if key == "KAIROS_API_TOKEN" {
			return strings.Repeat("t", 32)
		}
		return ""
	})
	if err != nil || c.Address != "127.0.0.1:8075" || c.Workers != 8 || c.AllowPrivate || c.Files.MaxBytes != 32<<20 || c.Files.TotalBytes != 256<<20 || c.Files.Transfers != 2 {
		t.Fatalf("invalid defaults: %v", err)
	}
}
