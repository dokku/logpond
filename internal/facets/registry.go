// Package facets implements the in-memory facet registry that backs
// the sidebar and the search-suggest endpoint. Reference sections:
// PRD §7.4 (Facets), §13.16-§13.19 (Facet CRUD).
package facets

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/config"
)

// Kind classifies a facet by where its definition came from.
type Kind string

const (
	// KindBuiltin is the four-facet set guaranteed by §7.4.1.
	KindBuiltin Kind = "builtin"
	// KindCustom covers config-defined and UI-defined facets.
	KindCustom Kind = "custom"
)

// Source records the lifecycle origin of a custom facet. Built-in
// facets carry an empty Source.
type Source string

const (
	SourceConfig Source = "config"
	SourceUI     Source = "ui"
)

// Definition is the resolved, post-conflict view of a single facet.
type Definition struct {
	Name           string
	Field          string
	DisplayLabel   string
	CardinalityCap int
	ValueType      string
	Kind           Kind
	Source         Source
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Registry holds the active facet set in memory. It merges built-in
// facets, the static config block, and the catalog-persisted UI rows;
// callers refresh after edits via the CreateUI/UpdateUI/DeleteUI
// helpers, which keep the in-memory state aligned with the database.
type Registry struct {
	mu       sync.RWMutex
	defs     map[string]Definition
	order    []string // preserves builtin → config → UI ordering for List()
	warnings []string

	catalog *catalog.Catalog
	logger  *slog.Logger
}

// Options configures a Registry.
type Options struct {
	Catalog *catalog.Catalog
	Logger  *slog.Logger
}

// builtinSpec captures the §7.4.1 four-facet contract: name, source
// column, default cap, default label.
type builtinSpec struct {
	name, field, label string
	cap                int
}

var builtinSpecs = []builtinSpec{
	{name: "service", field: "service", label: "Service", cap: 100},
	{name: "level", field: "level", label: "Level", cap: 10},
	{name: "host", field: "host", label: "Host", cap: 50},
	{name: "source", field: "source", label: "Source", cap: 50},
}

// New constructs an empty Registry. Call Load to populate it from
// config + catalog before serving traffic.
func New(opts Options) *Registry {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Registry{
		defs:    map[string]Definition{},
		catalog: opts.Catalog,
		logger:  opts.Logger,
	}
}

// Load resets the registry from the given config and catalog state.
// Safe to call repeatedly (after SIGHUP or admin reload).
func (r *Registry) Load(ctx context.Context, cfg *config.Config) error {
	uiRows, err := r.loadUIRows(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rebuildLocked(cfg, uiRows)
	return nil
}

func (r *Registry) loadUIRows(ctx context.Context) ([]catalog.CustomFacet, error) {
	if r.catalog == nil {
		return nil, nil
	}
	all, err := r.catalog.ListCustomFacets(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading custom facets: %w", err)
	}
	out := all[:0]
	for _, f := range all {
		if f.Source == catalog.CustomFacetSourceUI {
			out = append(out, f)
		}
	}
	return out, nil
}

func (r *Registry) rebuildLocked(cfg *config.Config, uiRows []catalog.CustomFacet) {
	defs := make(map[string]Definition, len(builtinSpecs)+8)
	order := make([]string, 0, len(builtinSpecs)+8)
	warnings := []string{}

	// 1. Builtins always present. Config may customize cap/label per §7.4.1.
	for _, b := range builtinSpecs {
		d := Definition{
			Name:           b.name,
			Field:          b.field,
			DisplayLabel:   b.label,
			CardinalityCap: b.cap,
			ValueType:      "string",
			Kind:           KindBuiltin,
		}
		if cfg != nil {
			if c := overrideForBuiltin(b.name, cfg); c != nil {
				if c.Cap > 0 {
					d.CardinalityCap = c.Cap
				}
			}
		}
		defs[d.Name] = d
		order = append(order, d.Name)
	}

	defaultCap := 50
	if cfg != nil && cfg.Facets.DefaultCardinalityCap > 0 {
		defaultCap = cfg.Facets.DefaultCardinalityCap
	}

	// 2. Config-declared customs (§7.4.2). Conflicts with builtins
	// are dropped with a warning; config replaces any same-named UI row.
	if cfg != nil {
		for _, cf := range cfg.Facets.Custom {
			if _, ok := defs[cf.Name]; ok {
				warnings = append(warnings, fmt.Sprintf("config facet %q conflicts with built-in; ignored", cf.Name))
				continue
			}
			d := Definition{
				Name:           cf.Name,
				Field:          cf.Field,
				DisplayLabel:   resolveLabel(cf.DisplayLabel, cf.Name),
				CardinalityCap: cf.CardinalityCap,
				ValueType:      resolveValueType(cf.ValueType),
				Kind:           KindCustom,
				Source:         SourceConfig,
			}
			if d.CardinalityCap <= 0 {
				d.CardinalityCap = defaultCap
			}
			defs[d.Name] = d
			order = append(order, d.Name)
		}
	}

	// 3. UI-managed customs. Skip names already claimed by builtin/config.
	for _, row := range uiRows {
		if existing, ok := defs[row.Name]; ok {
			warnings = append(warnings, fmt.Sprintf("ui facet %q hidden by %s definition", row.Name, existing.Kind))
			continue
		}
		d := Definition{
			Name:           row.Name,
			Field:          row.Field,
			DisplayLabel:   resolveLabel(row.DisplayLabel.String, row.Name),
			CardinalityCap: defaultCap,
			ValueType:      resolveValueType(row.ValueType),
			Kind:           KindCustom,
			Source:         SourceUI,
			CreatedAt:      row.CreatedAt,
			UpdatedAt:      row.UpdatedAt,
		}
		if row.CardinalityCap.Valid && row.CardinalityCap.Int64 > 0 {
			d.CardinalityCap = int(row.CardinalityCap.Int64)
		}
		defs[d.Name] = d
		order = append(order, d.Name)
	}

	r.defs = defs
	r.order = order
	r.warnings = warnings
}

func overrideForBuiltin(name string, cfg *config.Config) *config.BuiltinFacet {
	switch name {
	case "service":
		return &cfg.Facets.Builtin.Service
	case "level":
		return &cfg.Facets.Builtin.Level
	case "host":
		return &cfg.Facets.Builtin.Host
	case "source":
		return &cfg.Facets.Builtin.Source
	}
	return nil
}

func resolveLabel(label, name string) string {
	if strings.TrimSpace(label) != "" {
		return label
	}
	return titleCase(name)
}

func resolveValueType(v string) string {
	switch v {
	case "string", "number", "bool":
		return v
	}
	return "string"
}

// titleCase converts a snake_case name into a Title Case label.
func titleCase(s string) string {
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == '_' || r == '-' || r == '.' })
	for i, p := range parts {
		if len(p) == 0 {
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.Join(parts, " ")
}

// List returns the active facet definitions in their registration order:
// builtin → config → UI (each group preserving insertion order).
func (r *Registry) List() []Definition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Definition, 0, len(r.order))
	for _, n := range r.order {
		out = append(out, r.defs[n])
	}
	return out
}

// Get returns the definition for name, if present.
func (r *Registry) Get(name string) (Definition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.defs[name]
	return d, ok
}

// Warnings returns the list of conflict messages produced by the most
// recent load. Useful for the admin UI to surface ignored definitions.
func (r *Registry) Warnings() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, len(r.warnings))
	copy(out, r.warnings)
	return out
}

// CreateInput captures the operator-supplied fields of a new UI facet.
type CreateInput struct {
	Name           string
	Field          string
	DisplayLabel   string
	CardinalityCap int
	ValueType      string
}

// ErrConflict signals that a write operation would clash with an
// existing definition (§13.17 / §13.18 / §13.19's 409 cases).
var ErrConflict = errors.New("facet_conflict")

// ErrNotFound signals that a UI-targeted operation found no such facet.
var ErrNotFound = errors.New("not_found")

// ErrInvalid signals an input that fails validation.
var ErrInvalid = errors.New("invalid_facet")

// CreateUI persists a new UI-sourced facet through the catalog and
// adds it to the in-memory registry. Returns ErrConflict when the name
// is already used by any facet (builtin/config/ui).
func (r *Registry) CreateUI(ctx context.Context, in CreateInput) (Definition, error) {
	if r.catalog == nil {
		return Definition{}, fmt.Errorf("registry has no catalog")
	}
	if err := validateCreate(in); err != nil {
		return Definition{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.defs[in.Name]; ok {
		return Definition{}, ErrConflict
	}
	cap := sql.NullInt64{}
	if in.CardinalityCap > 0 {
		cap = sql.NullInt64{Int64: int64(in.CardinalityCap), Valid: true}
	}
	label := sql.NullString{}
	if in.DisplayLabel != "" {
		label = sql.NullString{String: in.DisplayLabel, Valid: true}
	}
	row := catalog.CustomFacet{
		Name:           in.Name,
		Field:          in.Field,
		DisplayLabel:   label,
		CardinalityCap: cap,
		ValueType:      resolveValueType(in.ValueType),
		Source:         catalog.CustomFacetSourceUI,
	}
	if err := r.catalog.InsertCustomFacet(ctx, row); err != nil {
		return Definition{}, err
	}
	stored, err := r.catalog.GetCustomFacet(ctx, in.Name)
	if err != nil {
		return Definition{}, err
	}
	def := Definition{
		Name:           stored.Name,
		Field:          stored.Field,
		DisplayLabel:   resolveLabel(stored.DisplayLabel.String, stored.Name),
		CardinalityCap: defaultCapOr(int(stored.CardinalityCap.Int64), in.CardinalityCap),
		ValueType:      resolveValueType(stored.ValueType),
		Kind:           KindCustom,
		Source:         SourceUI,
		CreatedAt:      stored.CreatedAt,
		UpdatedAt:      stored.UpdatedAt,
	}
	r.defs[def.Name] = def
	r.order = append(r.order, def.Name)
	return def, nil
}

// PatchInput describes the partial fields a PATCH on §13.18 accepts.
type PatchInput struct {
	DisplayLabel   *string
	CardinalityCap *int
}

// UpdateUI mutates a UI-sourced facet. Returns ErrConflict if the
// target is builtin or config-defined, and ErrNotFound if missing.
func (r *Registry) UpdateUI(ctx context.Context, name string, in PatchInput) (Definition, error) {
	if r.catalog == nil {
		return Definition{}, fmt.Errorf("registry has no catalog")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, ok := r.defs[name]
	if !ok {
		return Definition{}, ErrNotFound
	}
	if existing.Kind != KindCustom || existing.Source != SourceUI {
		return Definition{}, ErrConflict
	}
	if in.CardinalityCap != nil && *in.CardinalityCap < 0 {
		return Definition{}, fmt.Errorf("%w: cardinality_cap must be >= 0", ErrInvalid)
	}
	u := catalog.CustomFacetUpdate{}
	if in.DisplayLabel != nil {
		v := *in.DisplayLabel
		u.DisplayLabel = &v
	}
	if in.CardinalityCap != nil {
		v := int64(*in.CardinalityCap)
		u.CardinalityCap = &v
	}
	row, err := r.catalog.UpdateCustomFacet(ctx, name, u)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Definition{}, ErrNotFound
		}
		return Definition{}, err
	}
	existing.DisplayLabel = resolveLabel(row.DisplayLabel.String, row.Name)
	if row.CardinalityCap.Valid && row.CardinalityCap.Int64 > 0 {
		existing.CardinalityCap = int(row.CardinalityCap.Int64)
	}
	existing.UpdatedAt = row.UpdatedAt
	r.defs[name] = existing
	return existing, nil
}

// DeleteUI removes a UI-sourced facet. Returns ErrConflict if the
// facet is builtin or config-defined, ErrNotFound if missing.
func (r *Registry) DeleteUI(ctx context.Context, name string) error {
	if r.catalog == nil {
		return fmt.Errorf("registry has no catalog")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, ok := r.defs[name]
	if !ok {
		return ErrNotFound
	}
	if existing.Kind != KindCustom || existing.Source != SourceUI {
		return ErrConflict
	}
	deleted, err := r.catalog.DeleteCustomFacet(ctx, name)
	if err != nil {
		return err
	}
	if !deleted {
		return ErrNotFound
	}
	delete(r.defs, name)
	for i, n := range r.order {
		if n == name {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}
	return nil
}

func defaultCapOr(stored, requested int) int {
	if stored > 0 {
		return stored
	}
	if requested > 0 {
		return requested
	}
	return 50
}

// nameRE bounds facet names to the same character class we accept for
// search-bar tokens, so the value can round-trip through `@name:value`
// without quoting.
var nameValidChars = func() map[rune]bool {
	m := map[rune]bool{}
	for r := 'a'; r <= 'z'; r++ {
		m[r] = true
	}
	for r := 'A'; r <= 'Z'; r++ {
		m[r] = true
	}
	for r := '0'; r <= '9'; r++ {
		m[r] = true
	}
	m['_'] = true
	m['-'] = true
	m['.'] = true
	return m
}()

func validateCreate(in CreateInput) error {
	if in.Name == "" {
		return fmt.Errorf("%w: name is required", ErrInvalid)
	}
	if in.Field == "" {
		return fmt.Errorf("%w: field is required", ErrInvalid)
	}
	for _, r := range in.Name {
		if !nameValidChars[r] {
			return fmt.Errorf("%w: name contains invalid character %q", ErrInvalid, r)
		}
	}
	if in.CardinalityCap < 0 {
		return fmt.Errorf("%w: cardinality_cap must be >= 0", ErrInvalid)
	}
	return nil
}
