package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoad_Defaults_PopulatedWhenNoFile(t *testing.T) {
	t.Setenv("LOGPOND_CONFIG", "")
	c, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Port != 8080 {
		t.Errorf("Port: want 8080, got %d", c.Port)
	}
	if c.DataDir != "/data" {
		t.Errorf("DataDir: want /data, got %q", c.DataDir)
	}
	if c.SegmentWindow != "1h" {
		t.Errorf("SegmentWindow: want 1h, got %q", c.SegmentWindow)
	}
	if c.Facets.Builtin.Service.Cap != 100 {
		t.Errorf("builtin service cap: want 100, got %d", c.Facets.Builtin.Service.Cap)
	}
	if c.Theme.Default != "auto" {
		t.Errorf("theme default: want auto, got %q", c.Theme.Default)
	}
	if len(c.Sources) != 1 || c.Sources[0].Name != "default" {
		t.Errorf("expected one default source, got %+v", c.Sources)
	}
}

func TestLoad_YAML_OverridesDefaults(t *testing.T) {
	path := writeConfig(t, `
port: 9000
data_dir: /var/lib/logpond
segment_window: 30m
facets:
  builtin:
    level:
      cap: 5
  custom:
    - name: user_id
      field: attributes.user.id
      display_label: User ID
      cardinality_cap: 25
      value_type: string
sources:
  - name: nginx
    extract:
      timestamp: [ts]
      level: [severity]
`)
	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Port != 9000 {
		t.Errorf("Port: %d", c.Port)
	}
	if c.DataDir != "/var/lib/logpond" {
		t.Errorf("DataDir: %q", c.DataDir)
	}
	if c.SegmentWindow != "30m" {
		t.Errorf("SegmentWindow: %q", c.SegmentWindow)
	}
	if c.Facets.Builtin.Level.Cap != 5 {
		t.Errorf("level.cap: %d", c.Facets.Builtin.Level.Cap)
	}
	// Untouched builtin keeps its default.
	if c.Facets.Builtin.Service.Cap != 100 {
		t.Errorf("service.cap: %d", c.Facets.Builtin.Service.Cap)
	}
	if len(c.Facets.Custom) != 1 || c.Facets.Custom[0].Name != "user_id" {
		t.Errorf("custom facets: %+v", c.Facets.Custom)
	}
	if len(c.Sources) != 1 || c.Sources[0].Name != "nginx" {
		t.Errorf("sources: %+v", c.Sources)
	}
}

func TestLoad_EnvOverrides_Scalars(t *testing.T) {
	t.Setenv("LOGPOND_PORT", "7001")
	t.Setenv("LOGPOND_DATA_DIR", "/tmp/logpond-data")
	t.Setenv("LOGPOND_LOG_LEVEL", "debug")
	t.Setenv("LOGPOND_FACETS_BUILTIN_HOST_CAP", "200")
	t.Setenv("LOGPOND_RETENTION_ARCHIVE_BEFORE_DELETE", "false")
	t.Setenv("LOGPOND_MEMORY_LIMITS_RING_BUFFER", "10MB")

	c, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Port != 7001 {
		t.Errorf("Port: %d", c.Port)
	}
	if c.DataDir != "/tmp/logpond-data" {
		t.Errorf("DataDir: %q", c.DataDir)
	}
	if c.LogLevel != "debug" {
		t.Errorf("LogLevel: %q", c.LogLevel)
	}
	if c.Facets.Builtin.Host.Cap != 200 {
		t.Errorf("host cap: %d", c.Facets.Builtin.Host.Cap)
	}
	if c.Retention.ArchiveBeforeDelete {
		t.Errorf("ArchiveBeforeDelete: want false, got true")
	}
	if c.MemoryLimits.RingBuffer != "10MB" {
		t.Errorf("RingBuffer: %q", c.MemoryLimits.RingBuffer)
	}
}

func TestLoad_DokkuPort_UsedWhenLogpondPortUnset(t *testing.T) {
	t.Setenv("PORT", "5555")
	c, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Port != 5555 {
		t.Errorf("Port: %d", c.Port)
	}
}

func TestLoad_LogpondPort_WinsOverDokkuPort(t *testing.T) {
	t.Setenv("PORT", "5555")
	t.Setenv("LOGPOND_PORT", "6000")
	c, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Port != 6000 {
		t.Errorf("Port: %d", c.Port)
	}
}

func TestLoad_SourcesJSON_ReplacesSources(t *testing.T) {
	t.Setenv("LOGPOND_SOURCES_JSON", `[{"name":"vector","extract":{"timestamp":["@timestamp"]}}]`)
	c, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(c.Sources) != 1 || c.Sources[0].Name != "vector" {
		t.Fatalf("Sources: %+v", c.Sources)
	}
	if !reflect.DeepEqual(c.Sources[0].Extract["timestamp"], []string{"@timestamp"}) {
		t.Errorf("extract: %+v", c.Sources[0].Extract)
	}
}

func TestLoad_FacetsJSON_ReplacesCustom(t *testing.T) {
	t.Setenv("LOGPOND_FACETS_JSON", `[{"name":"req_id","field":"attributes.req_id","cardinality_cap":30}]`)
	c, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(c.Facets.Custom) != 1 || c.Facets.Custom[0].Name != "req_id" {
		t.Fatalf("Custom: %+v", c.Facets.Custom)
	}
	if c.Facets.Custom[0].CardinalityCap != 30 {
		t.Errorf("cap: %d", c.Facets.Custom[0].CardinalityCap)
	}
}

func TestLoad_ArchiveScriptEnv_CollectedFromPrefixedVars(t *testing.T) {
	t.Setenv("LOGPOND_ARCHIVE_SCRIPT_ENV__RESTIC_REPOSITORY", "/backup")
	t.Setenv("LOGPOND_ARCHIVE_SCRIPT_ENV__RESTIC_PASSWORD", "hunter2")
	c, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Archive.Script.Env["RESTIC_REPOSITORY"] != "/backup" {
		t.Errorf("RESTIC_REPOSITORY: %q", c.Archive.Script.Env["RESTIC_REPOSITORY"])
	}
	if c.Archive.Script.Env["RESTIC_PASSWORD"] != "hunter2" {
		t.Errorf("RESTIC_PASSWORD: %q", c.Archive.Script.Env["RESTIC_PASSWORD"])
	}
}

func TestValidate_RejectsBadSegmentWindow(t *testing.T) {
	c := Defaults()
	c.SegmentWindow = "7m"
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "segment_window") {
		t.Fatalf("want segment_window error; got %v", err)
	}
}

func TestValidate_RejectsBadSourceName(t *testing.T) {
	c := Defaults()
	c.Sources = []Source{{Name: "BadName!"}}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "sources[0].name") {
		t.Fatalf("want source name error; got %v", err)
	}
}

func TestValidate_RequiresRetentionWhenArchiveBackendActive(t *testing.T) {
	c := Defaults()
	c.Archive.Backend = "s3"
	// ArchiveBeforeDelete defaults to true and no retention is configured.
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "retention") {
		t.Fatalf("want retention error; got %v", err)
	}
}

func TestValidate_AcceptsBackendWithoutRetentionWhenAbdFalse(t *testing.T) {
	c := Defaults()
	c.Archive.Backend = "s3"
	c.Retention.ArchiveBeforeDelete = false
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestReloadable_OnlyTrueWhenAllChangesReloadable(t *testing.T) {
	a := Defaults()
	b := Defaults()
	b.LogLevel = "debug"
	b.Facets.Builtin.Service.Cap = 200
	b.Sources = append([]Source{}, b.Sources...)
	b.Sources[0].Extract = map[string][]string{"timestamp": {"ts"}}
	if v := a.Reloadable(b); len(v) != 0 {
		t.Errorf("expected no violations, got %v", v)
	}
}

func TestReloadable_DetectsNonReloadableChanges(t *testing.T) {
	a := Defaults()
	b := Defaults()
	b.Port = 9000
	b.DataDir = "/elsewhere"
	v := a.Reloadable(b)
	if len(v) != 2 {
		t.Fatalf("expected 2 violations, got %v", v)
	}
	got := map[string]bool{}
	for _, p := range v {
		got[p] = true
	}
	if !got["port"] || !got["data_dir"] {
		t.Errorf("missing expected violations, got %v", v)
	}
}

func TestParseDuration_Days(t *testing.T) {
	d, err := ParseDuration("7d")
	if err != nil {
		t.Fatal(err)
	}
	if d.Hours() != 168 {
		t.Errorf("7d -> %s", d)
	}
}

func TestParseSize_Suffixes(t *testing.T) {
	cases := map[string]int64{
		"128MB": 128 << 20,
		"1GB":   1 << 30,
		"50":    50,
		"2KiB":  2048,
	}
	for in, want := range cases {
		got, err := ParseSize(in)
		if err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if got != want {
			t.Errorf("%s: want %d, got %d", in, want, got)
		}
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	path := writeConfig(t, "this is: not: valid: yaml: [")
	if _, err := Load(path); err == nil {
		t.Fatal("expected parse error, got nil")
	}
}

func TestLoad_BadEnvIntFails(t *testing.T) {
	t.Setenv("LOGPOND_PORT", "not-a-number")
	if _, err := Load(""); err == nil {
		t.Fatal("expected parse error, got nil")
	}
}

func TestLoad_BadDokkuPortFails(t *testing.T) {
	t.Setenv("PORT", "not-a-number")
	if _, err := Load(""); err == nil {
		t.Fatal("expected parse error, got nil")
	}
}

func TestLoad_BadSourcesJSONFails(t *testing.T) {
	t.Setenv("LOGPOND_SOURCES_JSON", "not-json")
	if _, err := Load(""); err == nil {
		t.Fatal("expected parse error, got nil")
	}
}

func TestLoad_BadFacetsJSONFails(t *testing.T) {
	t.Setenv("LOGPOND_FACETS_JSON", "not-json")
	if _, err := Load(""); err == nil {
		t.Fatal("expected parse error, got nil")
	}
}

func TestValidate_MultipleFieldErrorsReported(t *testing.T) {
	c := Defaults()
	c.SegmentWindow = "7m"
	c.Port = -1
	c.Archive.Backend = "weird"
	c.Theme.Default = "ultraviolet"
	c.MemoryLimits.DuckDB = "lots"
	c.MemoryLimits.RingBuffer = "lots"
	c.Retention.MaxAge = "always"
	c.Retention.MaxSize = "all"
	c.Retention.EvaluationInterval = "soon"
	c.SealingInterval = "later"
	c.Query.MaxTimeRange = "forever"
	c.Sources = []Source{{Name: "ok"}, {Name: "ok"}}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	for _, want := range []string{
		"segment_window", "port", "archive.backend", "theme.default",
		"memory_limits.duckdb", "memory_limits.ring_buffer",
		"retention.max_age", "retention.max_size", "retention.evaluation_interval",
		"sealing_interval", "query.max_time_range", "duplicate",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}

func TestParseDuration_BadInput(t *testing.T) {
	if _, err := ParseDuration("not a duration"); err == nil {
		t.Error("expected error from non-duration input")
	}
	if _, err := ParseDuration("xd"); err == nil {
		t.Error("expected error from non-numeric days input")
	}
	d, err := ParseDuration("")
	if err != nil || d != 0 {
		t.Errorf("empty: want (0,nil), got (%s,%v)", d, err)
	}
}

func TestParseSize_BadInput(t *testing.T) {
	if _, err := ParseSize("xMB"); err == nil {
		t.Error("expected error from non-numeric size input")
	}
	n, err := ParseSize("")
	if err != nil || n != 0 {
		t.Errorf("empty: want (0,nil), got (%d,%v)", n, err)
	}
}

func TestEnvOverride_AllScalarKinds(t *testing.T) {
	t.Setenv("LOGPOND_REHYDRATION_TTL_DAYS", "14")
	t.Setenv("LOGPOND_FACETS_DEFAULT_CARDINALITY_CAP", "75")
	t.Setenv("LOGPOND_RETENTION_ARCHIVE_BEFORE_DELETE", "true")
	t.Setenv("LOGPOND_THEME_DEFAULT", "dark")
	c, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Rehydration.TTLDays != 14 {
		t.Errorf("TTLDays: %d", c.Rehydration.TTLDays)
	}
	if c.Facets.DefaultCardinalityCap != 75 {
		t.Errorf("DefaultCardinalityCap: %d", c.Facets.DefaultCardinalityCap)
	}
	if !c.Retention.ArchiveBeforeDelete {
		t.Errorf("ArchiveBeforeDelete: false")
	}
	if c.Theme.Default != "dark" {
		t.Errorf("Theme.Default: %q", c.Theme.Default)
	}
}

func TestEnvOverride_BadBoolFails(t *testing.T) {
	t.Setenv("LOGPOND_RETENTION_ARCHIVE_BEFORE_DELETE", "notabool")
	if _, err := Load(""); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestRedacted_HidesSecrets(t *testing.T) {
	c := Defaults()
	c.Archive.S3.SecretAccessKey = "supersecret"
	c.Archive.Script.Env = map[string]string{"PASSWORD": "x"}
	c.Sources = []Source{{Name: "default", IngestTokens: []string{"lpk_live_abc"}}}
	r := c.Redacted()
	if r.Archive.S3.SecretAccessKey != "***" {
		t.Errorf("secret not redacted: %q", r.Archive.S3.SecretAccessKey)
	}
	if r.Archive.Script.Env["PASSWORD"] != "***" {
		t.Errorf("env not redacted: %q", r.Archive.Script.Env["PASSWORD"])
	}
	if got := r.Sources[0].IngestTokens; len(got) != 1 || got[0] != "***" {
		t.Errorf("ingest tokens not redacted: %#v", got)
	}
	if c.Archive.S3.SecretAccessKey != "supersecret" {
		t.Errorf("original mutated: %q", c.Archive.S3.SecretAccessKey)
	}
	if c.Sources[0].IngestTokens[0] != "lpk_live_abc" {
		t.Errorf("original source mutated: %q", c.Sources[0].IngestTokens[0])
	}
}

func TestLoad_IngestTokenEnv_AppendsToYAMLSource(t *testing.T) {
	t.Setenv("LOGPOND_INGEST_TOKEN__default", "lpk_live_one,lpk_live_two")
	c, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var idx = -1
	for i := range c.Sources {
		if c.Sources[i].Name == "default" {
			idx = i
		}
	}
	if idx < 0 {
		t.Fatalf("default source missing from %+v", c.Sources)
	}
	got := c.Sources[idx].IngestTokens
	if len(got) != 2 || got[0] != "lpk_live_one" || got[1] != "lpk_live_two" {
		t.Errorf("tokens: %#v", got)
	}
}

func TestLoad_IngestTokenEnv_DedupesAgainstYAML(t *testing.T) {
	const yaml = `sources:
  - name: default
    ingest_tokens: [lpk_live_one]
    extract: {timestamp: [timestamp]}
`
	dir := t.TempDir()
	path := dir + "/c.yaml"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LOGPOND_INGEST_TOKEN__default", "lpk_live_one,lpk_live_two")
	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := c.Sources[0].IngestTokens
	if len(got) != 2 || got[0] != "lpk_live_one" || got[1] != "lpk_live_two" {
		t.Errorf("tokens: %#v", got)
	}
}

func TestValidate_RejectsTokenWithWhitespace(t *testing.T) {
	c := Defaults()
	c.Sources = []Source{{Name: "default", IngestTokens: []string{"lpk live"}}}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "whitespace") {
		t.Fatalf("want whitespace error; got %v", err)
	}
}

func TestValidate_RejectsEmptyToken(t *testing.T) {
	c := Defaults()
	c.Sources = []Source{{Name: "default", IngestTokens: []string{""}}}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "empty token") {
		t.Fatalf("want empty-token error; got %v", err)
	}
}
