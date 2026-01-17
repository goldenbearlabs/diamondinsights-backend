# Monorepo Layout

This repository is organized by deployable apps, shared packages, and infra tooling.

## Layout

```
apps/
  backend/          # Python backend (API, jobs, db, alembic, cron)
  web/              # Website (Vercel)
  mobile/           # Mobile app (App Store / Play Store)
packages/
  shared/           # Shared code/types (optional)
infra/
  docker/           # Docker Compose files
docs/               # Project documentation
```

## Deployment Targets

- DigitalOcean server: `infra/docker/docker-compose.server.yml`
  - Runs DB + backend + portainer.
- Local dev (DB + backend): `infra/docker/docker-compose.yml`
- Runner machines (cron jobs): `infra/docker/docker-compose.runner.yml`

## Environment Files

- Root `.env` for local dev (copy from `.env.example`).
- `.env-runner` for cron runner machines.
- `.env.server` for DigitalOcean server (if you want a separate file).

## Notes

- Python backend expects imports like `from src...` inside `apps/backend`.
- Compose files mount `apps/backend` into the container.
