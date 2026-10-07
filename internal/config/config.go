package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bclonan/kairosapi/internal/artifact"
)

type Config struct {
	Address      string
	Token        string
	AdminToken   string
	DBPath       string
	QueueSize    int
	MaxRecords   int
	Retention    time.Duration
	WorkflowFile string
	Workers      int
	MaxRuns      int
	RunTimeout   time.Duration
	Origins      []string
	AllowPrivate bool
	Files        artifact.Limits
}

func Load() (Config, error) { return FromEnv(os.Getenv) }

func FromEnv(get func(string) string) (Config, error) {
	c := Config{
		Address: "127.0.0.1:8075", Token: get("KAIROS_API_TOKEN"),
		WorkflowFile: "workflows/example.json", Workers: 8, MaxRuns: 16, RunTimeout: time.Minute,
		AdminToken: get("KAIROS_ADMIN_TOKEN"), DBPath: "data/kairos.db", QueueSize: 128, MaxRecords: 1000, Retention: 7 * 24 * time.Hour,
		Files: (artifact.Limits{}).Defaults(),
	}
	if len(c.Token) < 32 || strings.TrimSpace(c.Token) != c.Token {
		return c, errors.New("KAIROS_API_TOKEN must contain at least 32 characters without surrounding whitespace")
	}
	if c.AdminToken != "" && (len(c.AdminToken) < 32 || strings.TrimSpace(c.AdminToken) != c.AdminToken || c.AdminToken == c.Token) {
		return c, errors.New("KAIROS_ADMIN_TOKEN must be distinct and contain at least 32 characters")
	}
	if value := get("KAIROS_DB_PATH"); value != "" {
		c.DBPath = value
	}
	if value := get("KAIROS_ADDR"); value != "" {
		c.Address = value
	}
	if _, _, err := net.SplitHostPort(c.Address); err != nil {
		return c, errors.New("KAIROS_ADDR must be host:port")
	}
	if value := get("KAIROS_WORKFLOWS_FILE"); value != "" {
		c.WorkflowFile = value
	}
	for key, target := range map[string]*int{"KAIROS_WORKERS": &c.Workers, "KAIROS_MAX_RUNS": &c.MaxRuns} {
		if value := get(key); value != "" {
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 || n > 128 {
				return c, fmt.Errorf("%s must be between 1 and 128", key)
			}
			*target = n
		}
	}
	for key, target := range map[string]*int{"KAIROS_QUEUE_SIZE": &c.QueueSize, "KAIROS_MAX_RECORDS": &c.MaxRecords} {
		if value := get(key); value != "" {
			n, err := strconv.Atoi(value)
			maximum := 4096
			if key == "KAIROS_MAX_RECORDS" {
				maximum = 100000
			}
			if err != nil || n < 0 || n > maximum {
				return c, fmt.Errorf("%s exceeds supported limits", key)
			}
			*target = n
		}
	}
	if c.MaxRecords < c.MaxRuns+c.QueueSize {
		return c, errors.New("KAIROS_MAX_RECORDS must cover active and queued capacity")
	}
	if value := get("KAIROS_RETENTION"); value != "" {
		d, err := time.ParseDuration(value)
		if err != nil || d < time.Minute || d > 365*24*time.Hour {
			return c, errors.New("KAIROS_RETENTION must be between 1m and 8760h")
		}
		c.Retention = d
	}
	if value := get("KAIROS_RUN_TIMEOUT"); value != "" {
		d, err := time.ParseDuration(value)
		if err != nil || d < time.Second || d > 5*time.Minute {
			return c, errors.New("KAIROS_RUN_TIMEOUT must be between 1s and 5m")
		}
		c.RunTimeout = d
	}
	if value := get("KAIROS_ALLOW_PRIVATE_NETWORKS"); value != "" {
		if value != "true" && value != "false" {
			return c, errors.New("KAIROS_ALLOW_PRIVATE_NETWORKS must be true or false")
		}
		c.AllowPrivate = value == "true"
	}
	if value := get("KAIROS_HTTP_ALLOWED_ORIGINS"); value != "" {
		for _, origin := range strings.Split(value, ",") {
			origin = strings.TrimSpace(origin)
			if origin == "" {
				return c, errors.New("allowed origins cannot contain an empty entry")
			}
			c.Origins = append(c.Origins, origin)
		}
	}
	for key, target := range map[string]*int64{"KAIROS_FILE_MAX_BYTES": &c.Files.MaxBytes, "KAIROS_FILE_TOTAL_BYTES": &c.Files.TotalBytes} {
		if value := get(key); value != "" {
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 1 {
				return c, fmt.Errorf("%s must be a positive integer", key)
			}
			*target = n
		}
	}
	for key, target := range map[string]*int{"KAIROS_FILE_MAX_COUNT": &c.Files.MaxFiles, "KAIROS_FILE_TRANSFERS": &c.Files.Transfers} {
		if value := get(key); value != "" {
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 {
				return c, fmt.Errorf("%s must be a positive integer", key)
			}
			*target = n
		}
	}
	if c.Files.MaxBytes > 256<<20 || c.Files.TotalBytes < c.Files.MaxBytes || c.Files.TotalBytes > 1<<40 || c.Files.MaxFiles > 100000 || c.Files.Transfers > 16 {
		return c, errors.New("file limits exceed supported bounds")
	}
	return c, nil
}
