.PHONY: help up down ps logs logs-backend build test \
	migrate revision run-job training-data \
	runner-up runner-down runner-ps runner-logs runner-restart runner-build runner-pull runner-update runner-install \
	server-up server-down server-ps server-logs \
	monitoring-up monitoring-down monitoring-ps monitoring-logs \
	card_sync card_sync_above card_sync_below card_synce_below \
	game_boxscore_sync market_candle_sync market_price_sync market_sync player_sync roster_update_sync prediction_sync card_position_overall_sync \
	show_profile_stats_updater show_game_refresh show_game_agg your_ovr_sync

ENV_FILE ?= .env
RUNNER_ENV ?= .env
SERVER_ENV ?= .env
MONITORING_ENV ?= .env
JOB_ARGS ?=

COMPOSE_BASE = docker compose -f infra/docker/docker-compose.yml --env-file $(ENV_FILE)
COMPOSE_RUNNER = docker compose -f infra/docker/docker-compose.runner.yml --env-file $(RUNNER_ENV)
COMPOSE_SERVER = docker compose -f infra/docker/docker-compose.server.yml --env-file $(SERVER_ENV)
COMPOSE_MONITORING = docker compose -f infra/monitoring/docker-compose.yml --env-file $(MONITORING_ENV)

help:
	@printf "Usage:\n"
	@printf "  make up|down|ps|logs|logs-backend|build\n"
	@printf "  make migrate | revision REV_MSG='desc'\n"
	@printf "  make run-job JOB_MODULE=apps.jobs.card_sync JOB_CLASS=CardSync [JOB_ARGS='reload_all_years=True']\n"
	@printf "  make card_sync|card_sync_above|card_sync_below|game_boxscore_sync|market_candle_sync|market_price_sync|market_sync|player_sync|roster_update_sync|prediction_sync|card_position_overall_sync|show_profile_stats_updater|show_game_refresh|show_game_agg|your_ovr_sync\n"
	@printf "  make runner-up|runner-down|runner-ps|runner-logs\n"
	@printf "  make server-up|server-down|server-ps|server-logs\n"
	@printf "  make monitoring-up|monitoring-down|monitoring-ps|monitoring-logs\n"
	@printf "\nEnv files:\n"
	@printf "  ENV_FILE=.env (default)\n"
	@printf "  RUNNER_ENV=.env-runner\n"
	@printf "  SERVER_ENV=.env.server\n"
	@printf "  MONITORING_ENV=.env\n"

up:
	$(COMPOSE_BASE) up -d --build

down:
	$(COMPOSE_BASE) down

ps:
	$(COMPOSE_BASE) ps

logs:
	$(COMPOSE_BASE) logs -f

logs-backend:
	$(COMPOSE_BASE) logs -f backend

test:
	python3 -m pytest
	python3 scripts/update_test_coverage.py

build:
	$(COMPOSE_BASE) build

migrate:
	$(COMPOSE_BASE) exec backend alembic upgrade head

revision:
	$(COMPOSE_BASE) exec backend alembic revision --autogenerate -m "$(REV_MSG)"

fix-alembic:
	$(COMPOSE_BASE) exec backend alembic upgrade head

run-job: up
	$(COMPOSE_BASE) exec jobs python -c "from shared.core.logging_config import configure_logging; configure_logging(service_name='run-job'); from shared.db.database import SessionLocal; from $(JOB_MODULE) import $(JOB_CLASS); s=SessionLocal(); $(JOB_CLASS)($(JOB_ARGS)).run(s); s.close()"

training-data:
	$(COMPOSE_BASE) exec backend python -m src.scripts.training_data

card_sync: JOB_MODULE=apps.jobs.card_sync
card_sync: JOB_CLASS=CardSync
card_sync: run-job

market_sync_above: JOB_MODULE=apps.jobs.market_sync
market_sync_above: JOB_CLASS=MarketSync
market_sync_above: JOB_ARGS=ovr_min=70
market_sync_above: run-job

market_sync_below: JOB_MODULE=apps.jobs.market_sync
market_sync_below: JOB_CLASS=MarketSync
market_sync_below: JOB_ARGS=ovr_max=69
market_sync_below: run-job

card_synce_below: card_sync_below

game_boxscore_sync: JOB_MODULE=apps.jobs.game_boxscore_sync
game_boxscore_sync: JOB_CLASS=GameBoxscoreSync
game_boxscore_sync: run-job

market_candle_sync: JOB_MODULE=apps.jobs.market_candle_sync
market_candle_sync: JOB_CLASS=MarketCandleSync
market_candle_sync: run-job

market_price_sync: JOB_MODULE=apps.jobs.market_price_sync
market_price_sync: JOB_CLASS=MarketPriceSync
market_price_sync: run-job
	
market_sync: JOB_MODULE=apps.jobs.market_sync
market_sync: JOB_CLASS=MarketSync
market_sync: run-job

player_sync: JOB_MODULE=apps.jobs.player_sync
player_sync: JOB_CLASS=PlayerSync
player_sync: run-job

roster_update_sync: JOB_MODULE=apps.jobs.roster_update_sync
roster_update_sync: JOB_CLASS=RosterUpdateSync
roster_update_sync: run-job

prediction_sync: JOB_MODULE=apps.jobs.prediction_sync
prediction_sync: JOB_CLASS=PredictionSync
prediction_sync: run-job

card_position_overall_sync: JOB_MODULE=apps.jobs.card_position_overall_sync
card_position_overall_sync: JOB_CLASS=CardPositionOverallSync
card_position_overall_sync: run-job

show_profile_stats_updater: JOB_MODULE=apps.jobs.show_profile_refresh
show_profile_stats_updater: JOB_CLASS=ShowProfileStatsUpdater
show_profile_stats_updater: run-job

show_game_refresh: JOB_MODULE=apps.jobs.show_game_refresh
show_game_refresh: JOB_CLASS=ShowGameRefresh
show_game_refresh: run-job

show_game_agg: JOB_MODULE=apps.jobs.show_game_agg
show_game_agg: JOB_CLASS=ShowGameAgg
show_game_agg: run-job

your_ovr_sync: JOB_MODULE=apps.jobs.your_ovr_sync
your_ovr_sync: JOB_CLASS=YourOvrSync
your_ovr_sync: run-job

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

monitoring-up:
	$(COMPOSE_MONITORING) up -d --build

monitoring-down:
	$(COMPOSE_MONITORING) down

monitoring-ps:
	$(COMPOSE_MONITORING) ps

monitoring-logs:
	$(COMPOSE_MONITORING) logs -f
