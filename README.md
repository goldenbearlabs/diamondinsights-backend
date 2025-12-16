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

## Updating DB

1) update models.py
2) docker compose exec backend alembic revision --autogenerate -m "desc"
3) docker compose exec backend alembic upgrade head

## Run a job locally

1) docker compose exec backend python -c "from src.jobs.card_sync import CardSync; CardSync().run()

## Cron jobs with external proxy runners using tailscale

Problem - Digital Ocean IP address is blocked from making api calls to external apis for data collection. Solution (cheapest homemade version) is to use local machines that run these cron jobs themselves and with tailscale write the data to the db on the server.
Obvious issue - Creates single point of failure if someone unplugs my desktop or power goes out. Basic solution - add a secondary raspberry pi at alternative location that also tries running jobs (they race to acquire locks from the db)
Heartbeats are sent into the db to monitor. Tailscale IP: 100.84.249.5

Steps to update
1) cd to repo on runner device
2) git pull
3) docker compose -f docker-compose.runner.yml --env-file .env-runner up -d --build or docker compose -f docker-compose.yml --env-file .env-runner restart

Steps to restart
1) maker sure tailscale and docker are running
2) docker compose -f docker-compose.runner.yml --env-file .env-runner .ps (this checks if anything is running)
3) docker compose -f docker-compose.runner.yml --env-file .env-runner up -d

Steps to add new machine as a runner
1) download tailscale and signin to admin gbl account (github sign in)
2) install docker & docker-compose.
3) clone repo
4) create .env-runner file
5) run step 3 of steps to update

** Note that if you update runner you have to apply update to each device.

## Architecture
<img width="515" height="534" alt="Screenshot 2025-12-16 at 3 43 55 PM" src="https://github.com/user-attachments/assets/43cc83ac-561e-4685-b9bc-525e580a2de4" />

[MIT](https://choosealicense.com/licenses/mit/)
