# Get the Poetry virtual environment path dynamically
VENV := $(shell poetry env info --path)
BIN := $(VENV)/bin

# Export PATH so all commands use the Poetry environment
export PATH := $(BIN):$(PATH)
# Load environment variables
export $(shell set -o allexport && source .env && set +o allexport)

.PHONY: all
all: run-indexer

run-indexer:
	poetry run python bitcoin_indexer.py

# Targets
install:
	poetry install

pre-commit-install:
	poetry run pre-commit install
	poetry run pre-commit autoupdate  # Always use latest hooks

pre-commit-run:
	poetry run pre-commit run --all-files

check-imports:
	poetry run isort --check .
	poetry run flake8 --config setup.cfg

fix-imports:
	poetry run isort .

mongo-start:
	chmod +x start_mongo_repset.sh
	./start_mongo_repset.sh

mongo-stop:
	chmod +x stop_mongo_repset.sh
	./stop_mongo_repset.sh
	
poetry_shell:
	poetry shell