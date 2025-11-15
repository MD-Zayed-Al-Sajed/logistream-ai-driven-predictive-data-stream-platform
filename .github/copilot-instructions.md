<!--
Repo-specific Copilot instructions for the LogiStream AIOps Platform monorepo.
Keep this short (20–50 lines). When updating, preserve any manually-added guidance.
-->

# Copilot instructions — LogiStream AIOps

Purpose: help an AI coding agent be immediately productive in this monorepo by
pointing to the architecture, important files, and repository-specific conventions.

- Big picture (what to know first)
  - This is a mono-repo for the LogiStream AIOps Platform. See `README.md` for a short summary.
  - Major components (folders):
    - `services/` — microservices: `gateway/`, `ml/`, `producers/`, `streamproc/`.
    - `schemas/avro/` — Avro schemas used by the event bus.
    - `obs/` — observability: `prometheus/`, `grafana/` configs.
    - `storage/migrations/` — database migration artifacts (SQL/migrations).
    - `ui/dashboard/` — React dashboard front-end.
    - `docker/` and `infra/` — environment & infra orchestration assets.

- Architectural patterns to assume (verify before changing):
  - Event-driven streaming backbone (README mentions a Kafka-based event bus).
  - Small microservices organized under `services/` that communicate via events.
  - Schemas are authoritative: changes to event schemas must be coordinated in `schemas/avro/`.

- Where to look / safe edit zones
  - To update API/behavior look in the appropriate `services/<name>/` folder.
  - To change data formats or backwards-compatibility, update `schemas/avro/` and add migration notes.
  - To modify monitoring dashboards or alerts, edit `obs/grafana/` and `obs/prometheus/`.

- Project conventions discovered in the repo
  - Uses Python and Node (see `.venv/` and `node_modules/` in `.gitignore`).
  - Monorepo layout groups by feature/component (services/ + ui/ + infra/ + obs/).

- Quick rules for the AI agent (actionable)
  - ALWAYS reference `README.md` and the target service directory before proposing cross-repo changes.
  - When touching schemas: include a brief compatibility note and update any affected service code and tests.
  - Prefer small, focused changes (one service or one schema per PR). List files changed in a short summary.
  - Do not add new top-level tooling without noting rationale and where run instructions belong (`docker/` or `infra/`).

- Useful file examples to cite when editing
  - `README.md` — overall summary (components & event bus).
  - `schemas/avro/` — canonical schemas for events.
  - `obs/prometheus/`, `obs/grafana/` — monitoring assets to keep in sync with services.

If anything here is unclear or you want more explicit run/build commands (local dev, docker-compose, or CI), tell me which service or workflow to inspect and I'll extract exact commands and add them here.
