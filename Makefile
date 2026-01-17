.PHONY: help up down ps logs build \
	migrate revision run-job training-data \
	runner-up runner-down runner-ps runner-logs runner-restart runner-build runner-pull runner-update runner-install \
	server-up server-down server-ps server-logs \
	card-sync game_boxscore_sync market_candle_sync market_sync player_sync roster_update_sync

ENV_FILE ?= .env
RUNNER_ENV ?= .env
SERVER_ENV ?= .env

COMPOSE_BASE = docker compose -f infra/docker/docker-compose.yml --env-file $(ENV_FILE)
COMPOSE_RUNNER = docker compose -f infra/docker/docker-compose.runner.yml --env-file $(RUNNER_ENV)
COMPOSE_SERVER = docker compose -f infra/docker/docker-compose.server.yml --env-file $(SERVER_ENV)

help:
	@printf "Usage:\n"
	@printf "  make up|down|ps|logs|build\n"
	@printf "  make migrate | revision REV_MSG='desc'\n"
	@printf "  make run-job JOB_MODULE=src.jobs.card_sync JOB_CLASS=CardSync\n"
	@printf "  make runner-up|runner-down|runner-ps|runner-logs\n"
	@printf "  make server-up|server-down|server-ps|server-logs\n"
	@printf "\nEnv files:\n"
	@printf "  ENV_FILE=.env (default)\n"
	@printf "  RUNNER_ENV=.env-runner\n"
	@printf "  SERVER_ENV=.env.server\n"

up:
	$(COMPOSE_BASE) up -d --build

down:
	$(COMPOSE_BASE) down

ps:
	$(COMPOSE_BASE) ps

logs:
	$(COMPOSE_BASE) logs -f

build:
	$(COMPOSE_BASE) build

migrate:
	$(COMPOSE_BASE) exec backend alembic upgrade head

revision:
	$(COMPOSE_BASE) exec backend alembic revision --autogenerate -m "$(REV_MSG)"

run-job:
	$(COMPOSE_BASE) exec backend python -c "from $(JOB_MODULE) import $(JOB_CLASS); $(JOB_CLASS)().run()"

training-data:
	$(COMPOSE_BASE) exec backend python -m src.scripts.training_data

card_sync: up
	$(COMPOSE_BASE) exec backend python -c "from src.jobs.card_sync import CardSync; CardSync().run()"
game_boxscore_sync: up
	$(COMPOSE_BASE) exec backend python -c "from src.jobs.game_boxscore_sync import GameBoxscoreSync; GameBoxscoreSync().run()"
market_candle_sync: up
	$(COMPOSE_BASE) exec backend python -c "from src.jobs.market_candle_sync import MarketCandleSync; MarketCandleSync().run()"
market_sync: up
	$(COMPOSE_BASE) exec backend python -c "from src.jobs.market_sync import MarketSync; MarketSync().run()"
player_sync: up
	$(COMPOSE_BASE) exec backend python -c "from src.jobs.player_sync import PlayerSync; PlayerSync().run()"
roster_update_sync: up
	$(COMPOSE_BASE) exec backend python -c "from src.jobs.roster_update_sync import RosterUpdateSync; RosterUpdateSync().run()"

runner-up:
	$(COMPOSE_RUNNER) up -d --build

runner-down:
	$(COMPOSE_RUNNER) down

runner-ps:
	$(COMPOSE_RUNNER) ps

runner-logs:
	$(COMPOSE_RUNNER) logs -f runner

runner-restart:
	$(COMPOSE_RUNNER) restart runner

runner-build:
	$(COMPOSE_RUNNER) build --pull

runner-pull:
	$(COMPOSE_RUNNER) pull

runner-update:
	git pull
	$(COMPOSE_RUNNER) up -d --build
	docker image prune -f

runner-install:
	$(COMPOSE_RUNNER) up -d --build

server-up:
	$(COMPOSE_SERVER) up -d --build

server-down:
	$(COMPOSE_SERVER) down

server-ps:
	$(COMPOSE_SERVER) ps

server-logs:
	$(COMPOSE_SERVER) logs -f
