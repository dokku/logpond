# Logpond

A small, single-container log search service for Dokku hosts. Ingests JSON logs over HTTP, keeps recent logs hot for fast search and faceted browsing, archives aged segments to S3 or via a user-provided script, and lets operators rehydrate archives back into the same UI.

- Product spec: [`docs/PRD.md`](docs/PRD.md)
- Deployment guide: [`docs/DEPLOY-DOKKU.md`](docs/DEPLOY-DOKKU.md)
- Implementation plan: `docs/IMPLEMENTATION-PLAN.md` (working doc, not committed)
- Progress tracker: [`docs/IMPLEMENTATION-STATUS.md`](docs/IMPLEMENTATION-STATUS.md)

## Integration tests

`make setup test` brings up Dokku, a local Docker registry, and MinIO via `docker compose`, builds the Logpond image, deploys it, and runs the bats suite under `tests/`. See `tests/docker-compose.yml` and the `Makefile` for the moving parts. Use `make setup-native test-native` if Dokku is installed on the host instead.
