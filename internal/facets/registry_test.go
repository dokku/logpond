package facets

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/config"
)

func newCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	ctx := context.Background()
	cat, err := catalog.Open(ctx, filepath.Join(t.TempDir(), "catalog.db"), nil)
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}

func TestRegistry_LoadBuiltinsAlwaysPresent(t *testing.T) {
	cat := newCatalog(t)
	reg := New(Options{Catalog: cat})
	cfg := config.Defaults()
	if err := reg.Load(context.Background(), cfg); err != nil {
		t.Fatalf("load: %v", err)
	}
	want := []string{"service", "level", "host", "source"}
	got := names(reg.List())
	if len(got) != len(want) {
		t.Fatalf("want %d builtins, got %d (%v)", len(want), len(got), got)
	}
	for i, n := range want {
		if got[i] != n {
			t.Errorf("position %d: want %q, got %q", i, n, got[i])
		}
	}
}

func TestRegistry_BuiltinCapOverrideFromConfig(t *testing.T) {
	cat := newCatalog(t)
	reg := New(Options{Catalog: cat})
	cfg := config.Defaults()
	cfg.Facets.Builtin.Service.Cap = 12
	if err := reg.Load(context.Background(), cfg); err != nil {
		t.Fatalf("load: %v", err)
	}
	d, ok := reg.Get("service")
	if !ok {
		t.Fatal("service facet missing")
	}
	if d.CardinalityCap != 12 {
		t.Errorf("cap = %d, want 12", d.CardinalityCap)
	}
}

func TestRegistry_ConfigConflictWithBuiltin_IsDropped(t *testing.T) {
	cat := newCatalog(t)
	reg := New(Options{Catalog: cat})
	cfg := config.Defaults()
	cfg.Facets.Custom = []config.CustomFacet{
		{Name: "service", Field: "service", DisplayLabel: "X"},
	}
	if err := reg.Load(context.Background(), cfg); err != nil {
		t.Fatalf("load: %v", err)
	}
	d, _ := reg.Get("service")
	if d.Kind != KindBuiltin {
		t.Errorf("conflict resolution wrong: kind = %s, want builtin", d.Kind)
	}
	warns := reg.Warnings()
	if len(warns) == 0 {
		t.Error("expected a warning about config/builtin conflict")
	}
}

func TestRegistry_ConfigCustomsLoaded(t *testing.T) {
	cat := newCatalog(t)
	reg := New(Options{Catalog: cat})
	cfg := config.Defaults()
	cfg.Facets.Custom = []config.CustomFacet{
		{Name: "user_id", Field: "attributes.user.id", DisplayLabel: "User", CardinalityCap: 30},
	}
	if err := reg.Load(context.Background(), cfg); err != nil {
		t.Fatalf("load: %v", err)
	}
	d, ok := reg.Get("user_id")
	if !ok {
		t.Fatal("user_id facet missing")
	}
	if d.Kind != KindCustom || d.Source != SourceConfig {
		t.Errorf("kind/source = %s/%s, want custom/config", d.Kind, d.Source)
	}
	if d.CardinalityCap != 30 {
		t.Errorf("cap = %d, want 30", d.CardinalityCap)
	}
}

func TestRegistry_CreateUIFacet_PersistsAndLists(t *testing.T) {
	cat := newCatalog(t)
	reg := New(Options{Catalog: cat})
	if err := reg.Load(context.Background(), config.Defaults()); err != nil {
		t.Fatalf("load: %v", err)
	}
	def, err := reg.CreateUI(context.Background(), CreateInput{
		Name: "request_id", Field: "attributes.request_id", DisplayLabel: "Request ID", CardinalityCap: 25,
	})
	if err != nil {
		t.Fatalf("CreateUI: %v", err)
	}
	if def.Source != SourceUI || def.Kind != KindCustom {
		t.Errorf("source/kind = %s/%s, want ui/custom", def.Source, def.Kind)
	}
	if def.CardinalityCap != 25 {
		t.Errorf("cap = %d", def.CardinalityCap)
	}
	// Re-load and confirm persistence.
	reg2 := New(Options{Catalog: cat})
	if err := reg2.Load(context.Background(), config.Defaults()); err != nil {
		t.Fatalf("re-load: %v", err)
	}
	if _, ok := reg2.Get("request_id"); !ok {
		t.Error("UI facet didn't survive reload")
	}
}

func TestRegistry_CreateUIFacet_ConflictWithBuiltin(t *testing.T) {
	cat := newCatalog(t)
	reg := New(Options{Catalog: cat})
	if err := reg.Load(context.Background(), config.Defaults()); err != nil {
		t.Fatalf("load: %v", err)
	}
	_, err := reg.CreateUI(context.Background(), CreateInput{
		Name: "service", Field: "service", DisplayLabel: "Svc",
	})
	if !errors.Is(err, ErrConflict) {
		t.Errorf("err = %v, want ErrConflict", err)
	}
}

func TestRegistry_UpdateUIFacet_RejectsBuiltin(t *testing.T) {
	cat := newCatalog(t)
	reg := New(Options{Catalog: cat})
	if err := reg.Load(context.Background(), config.Defaults()); err != nil {
		t.Fatalf("load: %v", err)
	}
	cap := 5
	_, err := reg.UpdateUI(context.Background(), "service", PatchInput{CardinalityCap: &cap})
	if !errors.Is(err, ErrConflict) {
		t.Errorf("err = %v, want ErrConflict", err)
	}
}

func TestRegistry_DeleteUIFacet(t *testing.T) {
	cat := newCatalog(t)
	reg := New(Options{Catalog: cat})
	if err := reg.Load(context.Background(), config.Defaults()); err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, err := reg.CreateUI(context.Background(), CreateInput{
		Name: "request_id", Field: "attributes.request_id",
	}); err != nil {
		t.Fatalf("CreateUI: %v", err)
	}
	if err := reg.DeleteUI(context.Background(), "request_id"); err != nil {
		t.Fatalf("DeleteUI: %v", err)
	}
	if _, ok := reg.Get("request_id"); ok {
		t.Error("UI facet still present after delete")
	}
	if err := reg.DeleteUI(context.Background(), "service"); !errors.Is(err, ErrConflict) {
		t.Errorf("deleting builtin should be conflict, got %v", err)
	}
}

func TestRegistry_UpdateUI_AdjustsCap(t *testing.T) {
	cat := newCatalog(t)
	reg := New(Options{Catalog: cat})
	if err := reg.Load(context.Background(), config.Defaults()); err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, err := reg.CreateUI(context.Background(), CreateInput{
		Name: "request_id", Field: "attributes.request_id", CardinalityCap: 10,
	}); err != nil {
		t.Fatalf("CreateUI: %v", err)
	}
	cap := 99
	def, err := reg.UpdateUI(context.Background(), "request_id", PatchInput{CardinalityCap: &cap})
	if err != nil {
		t.Fatalf("UpdateUI: %v", err)
	}
	if def.CardinalityCap != 99 {
		t.Errorf("cap = %d, want 99", def.CardinalityCap)
	}
}

func TestRegistry_UIFacet_HiddenByConfigSameName(t *testing.T) {
	cat := newCatalog(t)
	reg := New(Options{Catalog: cat})
	if err := reg.Load(context.Background(), config.Defaults()); err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, err := reg.CreateUI(context.Background(), CreateInput{
		Name: "request_id", Field: "attributes.request_id",
	}); err != nil {
		t.Fatalf("CreateUI: %v", err)
	}
	// Re-load with config facet of same name.
	cfg := config.Defaults()
	cfg.Facets.Custom = []config.CustomFacet{{
		Name: "request_id", Field: "attributes.request_id", DisplayLabel: "Configured",
	}}
	reg2 := New(Options{Catalog: cat})
	if err := reg2.Load(context.Background(), cfg); err != nil {
		t.Fatalf("reload: %v", err)
	}
	d, _ := reg2.Get("request_id")
	if d.Source != SourceConfig {
		t.Errorf("source = %s, want config", d.Source)
	}
	if len(reg2.Warnings()) == 0 {
		t.Error("expected a warning that UI facet is hidden by config")
	}
}

func names(defs []Definition) []string {
	out := make([]string, 0, len(defs))
	for _, d := range defs {
		out = append(out, d.Name)
	}
	return out
}
