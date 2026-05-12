Run `scripts/vendor.sh` from the repo root to populate this directory with
the pinned versions of htmx, htmx-ext-ws, Alpine.js, and Open Props CSS.
The Dockerfile runs the same script during image build; locally you only
need to run it once per dependency-version bump.

Versions are tracked in `VERSIONS` after a successful fetch.
