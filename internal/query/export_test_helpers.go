package query

// AcquireForTest exposes the executor's per-segment refcount for tests
// that need to simulate an in-flight query (e.g., the rehydrated-delete
// busy check). The returned function releases the refcount; callers
// must invoke it exactly once.
func (e *Executor) AcquireForTest(id string) func() {
	return e.acquireSegment(id)
}
