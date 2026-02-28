# Diamond Insights Backend

The backend is run on a digital ocean server with Docker.

## Overview

### Server Specs
2GB RAM, 1AMD CPU, 50GB Storage
IP Addres: 142.93.158.215

- Folders are under home/opt/di
- Users are on di_team

## Setup

Clone Repo and setup SSH into server.

Github is source of truth, will auto deploy main branch to server. There is branch protection setup so that you can only merge to main through a PR that auto checks a few basic tests first.

## Portainer
http://142.93.158.215:9000

## Local Development (Docker + Makefile)

### Setup
1) Copy `.env.example` to `.env`
2) Run `make up` to build and start local services

### Common commands
- `make up` builds and starts local services (db + backend + portainer)
- `make down` stops local services
- `make ps` shows running containers
- `make logs` tails local logs

## RevenueCat Webhook

Use this webhook URL in RevenueCat:

- `POST /billing/revenuecat/webhook` (for example: `https://<your-api-domain>/billing/revenuecat/webhook`)

Required env vars:

- `REVENUECAT_WEBHOOK_AUTH`: shared secret expected in webhook `Authorization` header.
- `REVENUECAT_PRO_ENTITLEMENT_ID`: entitlement id treated as Pro (default: `pro`).

Behavior:

- Webhook events are stored in `revenuecat_webhook_events`.
- Entitlements are upserted into `user_entitlements` keyed by `(user_id, entitlement_id)`.
- `/entitlements/me` returns `has_pro` and entitlement records for the authenticated user.

## Updating DB

1) update models.py
2) `make revision REV_MSG="desc"` generates a migration
3) `make migrate` applies migrations

## Run a job locally

Example:
`make run-job JOB_MODULE=src.jobs.card_sync JOB_CLASS=CardSync`

Or use the shortcuts:
- `make card-sync`
- `make market_sync`
- `make market_candle_sync`
- `make roster_update_sync`
- `make player_sync`
- `make game_boxscore_sync`

## Cron jobs with external proxy runners using tailscale

Problem - Digital Ocean IP address is blocked from making api calls to external apis for data collection. Solution (cheapest homemade version) is to use local machines that run these cron jobs themselves and with tailscale write the data to the db on the server.
Obvious issue - Creates single point of failure if someone unplugs my desktop or power goes out. Basic solution - add a secondary raspberry pi at alternative location that also tries running jobs (they race to acquire locks from the db)
Heartbeats are sent into the db to monitor. Tailscale IP: 100.84.249.5

Runner setup (cron machine)
1) copy `.env.example` to `.env-runner` and set `POSTGRES_HOST` to the server/Tailscale IP
2) `make runner-up` to build and start the cron runner

Runner maintenance
- `make runner-ps` shows runner status
- `make runner-logs` tails cron logs
- `make runner-restart` restarts the runner container
- `make runner-update` pulls latest code and rebuilds


Steps to add new machine as a runner
1) download tailscale and signin to admin gbl account (same as github sign in)
2) install docker & docker-compose.
3) clone repo
4) create .env-runner file
5) run step 2 of steps to update

** Note that if you update runner you have to apply update to each device.

## Architecture
<img width="515" height="534" alt="Screenshot 2025-12-16 at 3 43 55 PM" src="https://github.com/user-attachments/assets/43cc83ac-561e-4685-b9bc-525e580a2de4" />

[MIT](https://choosealicense.com/licenses/mit/)

## Get new set of training data
`make up` then `make training-data`

## Design Dependencies

** We rely on the firebase_service_key.json file being located in a secrets folder at runtime to init our firebase instance **
