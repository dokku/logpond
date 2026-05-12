package ui

import "html/template"

// lucideIcons is a small, manually-curated subset of Lucide
// (https://lucide.dev) used across the views. Inlining the SVG avoids a
// separate font/svg-sprite request and keeps the icons themable via
// currentColor. The set tracks the table in PRD §12.6 plus a few extras
// the Phase-13 admin sketch needs.
var lucideIcons = map[string]template.HTML{
	"search":         lucide(`<circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/>`),
	"x":              lucide(`<path d="M18 6 6 18"/><path d="m6 6 12 12"/>`),
	"chevron-right":  lucide(`<path d="m9 18 6-6-6-6"/>`),
	"chevron-down":   lucide(`<path d="m6 9 6 6 6-6"/>`),
	"chevron-up":     lucide(`<path d="m18 15-6-6-6 6"/>`),
	"sun":            lucide(`<circle cx="12" cy="12" r="4"/><path d="M12 2v2"/><path d="M12 20v2"/><path d="m4.93 4.93 1.41 1.41"/><path d="m17.66 17.66 1.41 1.41"/><path d="M2 12h2"/><path d="M20 12h2"/><path d="m6.34 17.66-1.41 1.41"/><path d="m19.07 4.93-1.41 1.41"/>`),
	"moon":           lucide(`<path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z"/>`),
	"monitor":        lucide(`<rect width="20" height="14" x="2" y="3" rx="2"/><line x1="8" x2="16" y1="21" y2="21"/><line x1="12" x2="12" y1="17" y2="21"/>`),
	"play":           lucide(`<polygon points="6 3 20 12 6 21 6 3" fill="currentColor"/>`),
	"pause":          lucide(`<rect width="4" height="16" x="6" y="4" rx="1"/><rect width="4" height="16" x="14" y="4" rx="1"/>`),
	"square":         lucide(`<rect width="18" height="18" x="3" y="3" rx="2"/>`),
	"upload-cloud":   lucide(`<path d="M4 14.899A7 7 0 1 1 15.71 8h1.79a4.5 4.5 0 0 1 2.5 8.242"/><path d="M12 12v9"/><path d="m16 16-4-4-4 4"/>`),
	"download-cloud": lucide(`<path d="M4 14.899A7 7 0 1 1 15.71 8h1.79a4.5 4.5 0 0 1 2.5 8.242"/><path d="M12 12v9"/><path d="m8 17 4 4 4-4"/>`),
	"trash":          lucide(`<path d="M3 6h18"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/><path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>`),
	"pencil":         lucide(`<path d="M21.174 6.812a1 1 0 0 0-3.986-3.987L3.842 16.174a2 2 0 0 0-.5.83l-1.321 4.352a.5.5 0 0 0 .623.622l4.353-1.32a2 2 0 0 0 .83-.497z"/><path d="m15 5 4 4"/>`),
	"plus":           lucide(`<path d="M5 12h14"/><path d="M12 5v14"/>`),
	"refresh":        lucide(`<path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8"/><path d="M21 3v5h-5"/><path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16"/><path d="M8 16H3v5"/>`),
}

// lucide wraps the path data in the standard Lucide outer svg shell.
// stroke-width=1.75 lines up with the 14px-text density of the UI; the
// default of 2 reads too heavy at 16x16.
func lucide(body string) template.HTML {
	return template.HTML(`<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">` + body + `</svg>`)
}

func icon(name string) template.HTML {
	if h, ok := lucideIcons[name]; ok {
		return h
	}
	return template.HTML(``)
}
