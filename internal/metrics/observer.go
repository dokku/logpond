package metrics

import (
	"strconv"
	"time"
)

// ScriptInvocationObserver is the metrics-side glue for the archive
// script backend's InvocationObserver interface. Kept here so the
// archive package doesn't import prometheus directly.
type ScriptInvocationObserver struct {
	M *Metrics
}

// ObserveScriptInvocation records a single script invocation in the
// counter (labeled by mode and exit code) and duration histogram.
func (o ScriptInvocationObserver) ObserveScriptInvocation(mode string, exitCode int, duration time.Duration) {
	if o.M == nil {
		return
	}
	o.M.ArchiveScriptInvocationsTotal.WithLabelValues(mode, strconv.Itoa(exitCode)).Inc()
	o.M.ArchiveScriptDuration.WithLabelValues(mode).Observe(duration.Seconds())
}
