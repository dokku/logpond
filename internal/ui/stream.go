package ui

import (
	"strings"
	"time"

	"github.com/dokku/logpond/internal/ingest"
)

// htmlStreamRenderer implements ws.Renderer for the /ui/query/stream
// endpoint. Each fan-out event becomes an oob-swap fragment that HTMX
// inserts at the top of #tail-events; status frames are dropped (HTML
// view has no place to render them, and the connection's open/close
// already conveys liveness to the user).
type htmlStreamRenderer struct {
	tmpl *templates
}

func (r htmlStreamRenderer) Event(ev ingest.Event) []byte {
	view := tailEventFromIngest(ev)
	body, err := r.tmpl.render("fragments/tail_event.html", view)
	if err != nil {
		return nil
	}
	return body
}

func (r htmlStreamRenderer) Status(filterSummary string, subscribed, rateCapped bool, dropped uint64) []byte {
	// PRD §11.2 keeps live tail status out of the HTML stream — the
	// browser side detects liveness from WebSocket open/close.
	return nil
}

func tailEventFromIngest(ev ingest.Event) eventView {
	v := eventView{
		Timestamp:        ev.Timestamp,
		TimestampDisplay: ev.Timestamp.UTC().Format("15:04:05.000"),
		TimestampISO:     ev.Timestamp.UTC().Format(time.RFC3339Nano),
		Source:           ev.Source,
		Raw:              ev.Raw,
		Attributes:       ev.Attributes,
	}
	if ev.Service != "" {
		v.Service = ev.Service
		v.HasService = true
	}
	if ev.Level != "" {
		v.Level = ev.Level
		v.HasLevel = true
		v.LevelClass = levelClass(&ev.Level)
		v.LevelLabel = strings.ToUpper(ev.Level)
	}
	if ev.Message != "" {
		v.Message = ev.Message
		v.MessagePreview = shorten(ev.Message, 240)
		v.HasMessage = true
	}
	if ev.Host != "" {
		v.Host = ev.Host
		v.HasHost = true
	}
	if v.Attributes == nil {
		v.Attributes = map[string]any{}
	}
	return v
}

// NewHTMLStreamRenderer returns a Renderer implementation suitable for
// the /ui/query/stream WebSocket route. The cmd/logpond entry point
// instantiates a second ws.Server with this renderer so HTML fragments
// flow alongside the JSON-API stream.
func (s *Server) NewHTMLStreamRenderer() htmlStreamRenderer {
	return htmlStreamRenderer{tmpl: s.tmpl}
}
