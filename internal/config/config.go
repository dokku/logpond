// Package config loads Logpond's configuration from YAML files and
// environment variables. The reference table for fields lives in PRD §7.11.1.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// EnvPrefix is prepended to every environment variable Logpond reads
// (except the Dokku-compatible bare PORT).
const EnvPrefix = "LOGPOND_"

// Config is the in-memory representation of a Logpond configuration.
//
// Each field's `reloadable` tag marks whether the field can change via
// SIGHUP / POST /api/admin/reload. Fields tagged `reloadable:"true"` may
// differ between an old and a new Config in (*Config).Reloadable; others
// require a process restart.
type Config struct {
	ListenAddress   string `yaml:"listen_address" reloadable:"false"`
	Port            int    `yaml:"port" reloadable:"false"`
	DataDir         string `yaml:"data_dir" reloadable:"false"`
	SegmentWindow   string `yaml:"segment_window" reloadable:"false"`
	SealingInterval string `yaml:"sealing_interval" reloadable:"false"`
	LogLevel        string `yaml:"log_level" reloadable:"true"`

	MemoryLimits MemoryLimits `yaml:"memory_limits"`
	Query        Query        `yaml:"query"`
	Facets       Facets       `yaml:"facets"`
	LiveTail     LiveTail     `yaml:"live_tail"`
	Retention    Retention    `yaml:"retention"`
	Rehydration  Rehydration  `yaml:"rehydration"`
	Archive      Archive      `yaml:"archive"`

	Sources []Source `yaml:"sources" reloadable:"true"`
	Theme   Theme    `yaml:"theme"`
}

type MemoryLimits struct {
	DuckDB     string `yaml:"duckdb" reloadable:"false"`
	RingBuffer string `yaml:"ring_buffer" reloadable:"false"`
}

type Query struct {
	MaxTimeRange string `yaml:"max_time_range" reloadable:"true"`
}

type Facets struct {
	SegmentSampleSize     int           `yaml:"segment_sample_size" reloadable:"true"`
	DefaultCardinalityCap int           `yaml:"default_cardinality_cap" reloadable:"true"`
	Builtin               BuiltinFacets `yaml:"builtin"`
	Custom                []CustomFacet `yaml:"custom" reloadable:"true"`
}

type BuiltinFacets struct {
	Service BuiltinFacet `yaml:"service"`
	Level   BuiltinFacet `yaml:"level"`
	Host    BuiltinFacet `yaml:"host"`
	Source  BuiltinFacet `yaml:"source"`
}

type BuiltinFacet struct {
	Cap int `yaml:"cap" reloadable:"true"`
}

type CustomFacet struct {
	Name           string `yaml:"name" json:"name"`
	Field          string `yaml:"field" json:"field"`
	DisplayLabel   string `yaml:"display_label" json:"display_label,omitempty"`
	CardinalityCap int    `yaml:"cardinality_cap" json:"cardinality_cap,omitempty"`
	ValueType      string `yaml:"value_type" json:"value_type,omitempty"`
}

type LiveTail struct {
	RateCap    int `yaml:"rate_cap" reloadable:"true"`
	MaxClients int `yaml:"max_clients" reloadable:"true"`
}

type Retention struct {
	MaxAge              string `yaml:"max_age" reloadable:"true"`
	MaxSize             string `yaml:"max_size" reloadable:"true"`
	ArchiveBeforeDelete bool   `yaml:"archive_before_delete" reloadable:"true"`
	EvaluationInterval  string `yaml:"evaluation_interval" reloadable:"true"`
}

type Rehydration struct {
	TTLDays int `yaml:"ttl_days" reloadable:"true"`
}

type Archive struct {
	Backend string `yaml:"backend" reloadable:"false"`
	S3      S3     `yaml:"s3"`
	Script  Script `yaml:"script"`
}

type S3 struct {
	Endpoint        string `yaml:"endpoint" reloadable:"true"`
	Bucket          string `yaml:"bucket" reloadable:"true"`
	Prefix          string `yaml:"prefix" reloadable:"true"`
	Region          string `yaml:"region" reloadable:"true"`
	AccessKeyID     string `yaml:"access_key_id" reloadable:"true"`
	SecretAccessKey string `yaml:"secret_access_key" reloadable:"true"`
}

type Script struct {
	Path    string            `yaml:"path" reloadable:"true"`
	Timeout string            `yaml:"timeout" reloadable:"true"`
	Env     map[string]string `yaml:"env" reloadable:"true"`
}

type Source struct {
	Name    string              `yaml:"name" json:"name"`
	Extract map[string][]string `yaml:"extract" json:"extract"`
}

type Theme struct {
	Default string `yaml:"default" reloadable:"true"`
}

// Defaults returns the baseline configuration with every default value
// from PRD §7.11.1 applied.
func Defaults() *Config {
	return &Config{
		ListenAddress:   "0.0.0.0",
		Port:            8080,
		DataDir:         "/data",
		SegmentWindow:   "1h",
		SealingInterval: "60s",
		LogLevel:        "info",
		MemoryLimits: MemoryLimits{
			DuckDB:     "128MB",
			RingBuffer: "50MB",
		},
		Query: Query{MaxTimeRange: "7d"},
		Facets: Facets{
			SegmentSampleSize:     24,
			DefaultCardinalityCap: 50,
			Builtin: BuiltinFacets{
				Service: BuiltinFacet{Cap: 100},
				Level:   BuiltinFacet{Cap: 10},
				Host:    BuiltinFacet{Cap: 50},
				Source:  BuiltinFacet{Cap: 50},
			},
		},
		LiveTail: LiveTail{RateCap: 100, MaxClients: 20},
		Retention: Retention{
			ArchiveBeforeDelete: true,
			EvaluationInterval:  "5m",
		},
		Rehydration: Rehydration{TTLDays: 7},
		Archive: Archive{
			Backend: "none",
			S3:      S3{Prefix: "logpond/", Region: "auto"},
			Script:  Script{Timeout: "600s", Env: map[string]string{}},
		},
		Sources: []Source{{
			Name: "default",
			Extract: map[string][]string{
				"timestamp": {"timestamp", "ts", "time", "@timestamp"},
				"level":     {"level", "severity"},
				"message":   {"message", "msg"},
				"service":   {"service", "app"},
				"host":      {"host", "hostname"},
			},
		}},
		Theme: Theme{Default: "auto"},
	}
}

// Load reads the YAML file at path (if it exists), overlays environment
// overrides, and validates the result. An empty path skips file loading
// and uses defaults plus env overrides.
func Load(path string) (*Config, error) {
	c := Defaults()
	if path != "" {
		data, err := os.ReadFile(path)
		switch {
		case err == nil:
			if err := yaml.Unmarshal(data, c); err != nil {
				return nil, fmt.Errorf("parsing %s: %w", path, err)
			}
		case errors.Is(err, os.ErrNotExist):
			// Treat a missing file the same as no file: defaults + env.
		default:
			return nil, fmt.Errorf("reading %s: %w", path, err)
		}
	}
	if err := applyEnv(c); err != nil {
		return nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func applyEnv(c *Config) error {
	// Dokku passes the bound port via plain PORT.
	if _, set := os.LookupEnv(EnvPrefix + "PORT"); !set {
		if v := os.Getenv("PORT"); v != "" {
			p, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("PORT: %w", err)
			}
			c.Port = p
		}
	}

	if err := walkEnv(reflect.ValueOf(c).Elem(), ""); err != nil {
		return err
	}

	if v, ok := os.LookupEnv(EnvPrefix + "SOURCES_JSON"); ok {
		var srcs []Source
		if err := json.Unmarshal([]byte(v), &srcs); err != nil {
			return fmt.Errorf("LOGPOND_SOURCES_JSON: %w", err)
		}
		c.Sources = srcs
	}
	if v, ok := os.LookupEnv(EnvPrefix + "FACETS_JSON"); ok {
		var fs []CustomFacet
		if err := json.Unmarshal([]byte(v), &fs); err != nil {
			return fmt.Errorf("LOGPOND_FACETS_JSON: %w", err)
		}
		c.Facets.Custom = fs
	}

	const scriptEnvPrefix = EnvPrefix + "ARCHIVE_SCRIPT_ENV__"
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, scriptEnvPrefix) {
			continue
		}
		rest := kv[len(scriptEnvPrefix):]
		eq := strings.IndexByte(rest, '=')
		if eq <= 0 {
			continue
		}
		key, val := rest[:eq], rest[eq+1:]
		if c.Archive.Script.Env == nil {
			c.Archive.Script.Env = map[string]string{}
		}
		c.Archive.Script.Env[key] = val
	}
	return nil
}

func walkEnv(v reflect.Value, prefix string) error {
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		name := strings.Split(field.Tag.Get("yaml"), ",")[0]
		if name == "" || name == "-" {
			continue
		}
		path := name
		if prefix != "" {
			path = prefix + "_" + name
		}
		fv := v.Field(i)
		switch fv.Kind() {
		case reflect.Struct:
			if err := walkEnv(fv, path); err != nil {
				return err
			}
		case reflect.Slice, reflect.Map:
			// Slices and maps come from JSON-encoded specials.
			continue
		default:
			envName := EnvPrefix + strings.ToUpper(path)
			s, ok := os.LookupEnv(envName)
			if !ok {
				continue
			}
			if err := setScalar(fv, s); err != nil {
				return fmt.Errorf("%s: %w", envName, err)
			}
		}
	}
	return nil
}

func setScalar(fv reflect.Value, s string) error {
	switch fv.Kind() {
	case reflect.String:
		fv.SetString(s)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		fv.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return err
		}
		fv.SetUint(n)
	case reflect.Bool:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		fv.SetBool(b)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		fv.SetFloat(f)
	default:
		return fmt.Errorf("unsupported field kind %s", fv.Kind())
	}
	return nil
}

var sourceNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

var validSegmentWindows = []time.Duration{
	5 * time.Minute, 10 * time.Minute, 15 * time.Minute, 30 * time.Minute,
	1 * time.Hour, 2 * time.Hour, 3 * time.Hour, 4 * time.Hour,
	6 * time.Hour, 8 * time.Hour, 12 * time.Hour, 24 * time.Hour,
}

// ParseDuration extends time.ParseDuration with a `d` (24h) suffix so
// configuration values like "7d" parse without needing a third-party
// helper.
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	if strings.HasSuffix(s, "d") {
		n, err := strconv.ParseFloat(strings.TrimSuffix(s, "d"), 64)
		if err != nil {
			return 0, err
		}
		return time.Duration(n * float64(24*time.Hour)), nil
	}
	return time.ParseDuration(s)
}

// ParseSize parses byte-size values with optional KB/MB/GB/TB or
// KiB/MiB/GiB/TiB suffixes (case-insensitive, decimal multipliers).
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	upper := strings.ToUpper(s)
	multipliers := []struct {
		suffix string
		factor int64
	}{
		{"TIB", 1 << 40}, {"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10},
		{"TB", 1 << 40}, {"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10},
		{"T", 1 << 40}, {"G", 1 << 30}, {"M", 1 << 20}, {"K", 1 << 10},
		{"B", 1},
	}
	mult := int64(1)
	for _, m := range multipliers {
		if strings.HasSuffix(upper, m.suffix) {
			mult = m.factor
			s = s[:len(s)-len(m.suffix)]
			break
		}
	}
	n, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, err
	}
	return int64(n * float64(mult)), nil
}

// Validate ensures all fields are within accepted ranges. Errors from
// validation are concatenated so the operator sees every problem at once.
func (c *Config) Validate() error {
	var errs []string

	if w, err := ParseDuration(c.SegmentWindow); err != nil {
		errs = append(errs, fmt.Sprintf("segment_window: invalid duration %q", c.SegmentWindow))
	} else {
		match := false
		for _, v := range validSegmentWindows {
			if w == v {
				match = true
				break
			}
		}
		if !match {
			errs = append(errs, fmt.Sprintf("segment_window: must be one of %v; got %s", validSegmentWindows, w))
		}
	}

	if _, err := ParseDuration(c.SealingInterval); err != nil {
		errs = append(errs, fmt.Sprintf("sealing_interval: invalid duration %q", c.SealingInterval))
	}
	if _, err := ParseDuration(c.Query.MaxTimeRange); err != nil {
		errs = append(errs, fmt.Sprintf("query.max_time_range: invalid duration %q", c.Query.MaxTimeRange))
	}
	if _, err := ParseDuration(c.Retention.EvaluationInterval); err != nil {
		errs = append(errs, fmt.Sprintf("retention.evaluation_interval: invalid duration %q", c.Retention.EvaluationInterval))
	}
	if c.Retention.MaxAge != "" {
		if _, err := ParseDuration(c.Retention.MaxAge); err != nil {
			errs = append(errs, fmt.Sprintf("retention.max_age: invalid duration %q", c.Retention.MaxAge))
		}
	}
	if c.Retention.MaxSize != "" {
		if _, err := ParseSize(c.Retention.MaxSize); err != nil {
			errs = append(errs, fmt.Sprintf("retention.max_size: invalid size %q", c.Retention.MaxSize))
		}
	}
	if _, err := ParseSize(c.MemoryLimits.DuckDB); err != nil {
		errs = append(errs, fmt.Sprintf("memory_limits.duckdb: invalid size %q", c.MemoryLimits.DuckDB))
	}
	if _, err := ParseSize(c.MemoryLimits.RingBuffer); err != nil {
		errs = append(errs, fmt.Sprintf("memory_limits.ring_buffer: invalid size %q", c.MemoryLimits.RingBuffer))
	}

	if c.Port <= 0 || c.Port > 65535 {
		errs = append(errs, fmt.Sprintf("port: %d outside 1-65535", c.Port))
	}

	seen := map[string]int{}
	for i, s := range c.Sources {
		if !sourceNameRE.MatchString(s.Name) {
			errs = append(errs, fmt.Sprintf("sources[%d].name: %q does not match %s", i, s.Name, sourceNameRE.String()))
		}
		if prev, ok := seen[s.Name]; ok {
			errs = append(errs, fmt.Sprintf("sources[%d].name: duplicate of sources[%d]", i, prev))
		}
		seen[s.Name] = i
	}

	switch c.Archive.Backend {
	case "none", "s3", "script":
	default:
		errs = append(errs, fmt.Sprintf("archive.backend: must be one of none|s3|script; got %q", c.Archive.Backend))
	}

	if (c.Archive.Backend == "s3" || c.Archive.Backend == "script") && c.Retention.ArchiveBeforeDelete {
		if c.Retention.MaxAge == "" && c.Retention.MaxSize == "" {
			errs = append(errs, "retention: max_age or max_size must be set when archive_before_delete=true with an active archive backend")
		}
	}

	switch c.Theme.Default {
	case "", "auto", "light", "dark":
	default:
		errs = append(errs, fmt.Sprintf("theme.default: must be auto|light|dark; got %q", c.Theme.Default))
	}

	if len(errs) > 0 {
		return fmt.Errorf("config invalid: %s", strings.Join(errs, "; "))
	}
	return nil
}

// Reloadable compares c to nextC and returns the dotted-path names of
// fields that differ but are not flagged reloadable in the struct tags.
// An empty slice means a SIGHUP-style reload is safe.
func (c *Config) Reloadable(nextC *Config) []string {
	var viol []string
	diffStruct(reflect.ValueOf(c).Elem(), reflect.ValueOf(nextC).Elem(), "", &viol)
	return viol
}

func diffStruct(a, b reflect.Value, path string, viol *[]string) {
	t := a.Type()
	for i := 0; i < a.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		name := strings.Split(field.Tag.Get("yaml"), ",")[0]
		if name == "" || name == "-" {
			continue
		}
		p := name
		if path != "" {
			p = path + "." + name
		}
		av := a.Field(i)
		bv := b.Field(i)
		if av.Kind() == reflect.Struct && field.Tag.Get("reloadable") == "" {
			diffStruct(av, bv, p, viol)
			continue
		}
		if !reflect.DeepEqual(av.Interface(), bv.Interface()) {
			if !strings.EqualFold(field.Tag.Get("reloadable"), "true") {
				*viol = append(*viol, p)
			}
		}
	}
}

// Redacted returns a copy of c with secret values blanked out. Use it
// when writing the config to startup logs.
func (c *Config) Redacted() *Config {
	cp := *c
	if cp.Archive.S3.SecretAccessKey != "" {
		cp.Archive.S3.SecretAccessKey = "***"
	}
	if cp.Archive.S3.AccessKeyID != "" {
		cp.Archive.S3.AccessKeyID = "***"
	}
	if len(cp.Archive.Script.Env) > 0 {
		env := make(map[string]string, len(cp.Archive.Script.Env))
		for k := range cp.Archive.Script.Env {
			env[k] = "***"
		}
		cp.Archive.Script.Env = env
	}
	return &cp
}
