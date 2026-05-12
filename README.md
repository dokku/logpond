# Logpond

A small, single-container log search service for Dokku hosts. Ingests JSON logs over HTTP, keeps recent logs hot for fast search and faceted browsing, archives aged segments to S3 or via a user-provided script, and lets operators rehydrate archives back into the same UI.

- Product spec: [`docs/PRD.md`](docs/PRD.md)
- Implementation plan: `docs/IMPLEMENTATION-PLAN.md` (working doc, not committed)
- Progress tracker: [`docs/IMPLEMENTATION-STATUS.md`](docs/IMPLEMENTATION-STATUS.md)
