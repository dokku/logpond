package ui

import (
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"strings"
)

// templates wraps the parsed *template.Template tree. The layout +
// page templates each parse alongside every fragment under
// fragments/ so {{template "..."}} works freely within a page.
type templates struct {
	root *template.Template
}

func loadTemplates(efs embed.FS) (*templates, error) {
	funcs := template.FuncMap{
		"icon":         icon,
		"levelClass":   levelClass,
		"levelLabel":   levelLabel,
		"formatCount":  formatCount,
		"shorten":      shorten,
		"add":          func(a, b int) int { return a + b },
		"jsonString":   jsonString,
		"safeAttr":     func(s string) template.HTMLAttr { return template.HTMLAttr(s) },
		"safeJS":       func(s string) template.JS { return template.JS(s) },
		"timestampISO": timestampISO,
	}

	root := template.New("").Funcs(funcs)

	// Walk templates/ collecting everything. The fragments are siblings
	// to the page templates so a single Parse pass gives every page
	// access to every named block.
	err := fs.WalkDir(efs, "templates", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if !strings.HasSuffix(p, ".html") {
			return nil
		}
		data, readErr := efs.ReadFile(p)
		if readErr != nil {
			return readErr
		}
		// The template's logical name is its path under templates/, e.g.
		// "search.html" or "fragments/result_row.html". Defining a name
		// per file lets callers reference fragments by basename.
		name := strings.TrimPrefix(p, "templates/")
		t := root.New(name)
		if _, err := t.Parse(string(data)); err != nil {
			return fmt.Errorf("parsing %s: %w", p, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &templates{root: root}, nil
}

// render executes the named template against the writer. Errors at this
// point are programming errors (template references a missing block); we
// surface them as 500s.
func (t *templates) render(name string, data any) ([]byte, error) {
	var b strings.Builder
	if err := t.root.ExecuteTemplate(&b, name, data); err != nil {
		return nil, fmt.Errorf("rendering %s: %w", name, err)
	}
	return []byte(b.String()), nil
}
