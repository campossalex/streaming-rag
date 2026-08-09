.PHONY: help all init build up seed submit verify smoke audit alarms incidents psql console inspect consume ps logs down clean

# Every target below delegates to ./run.sh. Keep it that way.
#
# These recipes used to call docker compose directly, which meant the Makefile silently fell
# behind run.sh: `make up` skipped the Kafka topic pre-creation that stops the pipeline
# restart-looping on UnknownTopicOrPartition, and skipped the corpus re-seed. Two entry points
# with two implementations drift by default. This one is an alias layer, nothing more.
#
# `init` is the exception: it is a guard with no run.sh equivalent, so it keeps its own recipe.

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "};{printf "  \033[36m%-14s\033[0m %s\n",$$1,$$2}'

all: init  ## One shot: preflight, build, start, seed, verify, submit
	./run.sh all

init:  ## Create .env from the template if absent
	@if [ ! -f .env ]; then \
	  cp .env.example .env; \
	  echo ".env created from .env.example."; \
	  echo "Edit it to add OPENAI_API_KEY, then re-run. Compose needs this file to exist."; \
	  exit 1; \
	fi
	@grep -q '^OPENAI_API_KEY=sk-replace-me' .env && { \
	  echo "OPENAI_API_KEY in .env is still the placeholder. Set a real key, or pick a"; \
	  echo "provider that needs none:  ./run.sh use-ollama  |  ./run.sh use-launchpad <url>"; \
	  exit 1; \
	} || true
	@echo ".env looks configured."

build: init  ## Build the connector, the Flink image and the seeder
	./run.sh build

# Seeds as its last step, so there is no separate seed in the `all` chain.
# SKIP_SEED=1 make up keeps whatever collection is already there.
up: init  ## Start Milvus, Kafka and Flink, then re-seed the corpus
	./run.sh up

seed: init  ## Re-embed the corpus and load the Milvus collection
	./run.sh seed

submit:  ## Submit the streaming RAG pipeline
	./run.sh submit

verify:  ## Check the connector jar and the required libs on every Flink container
	./run.sh verify

smoke:  ## Run VECTOR_SEARCH against Milvus with a constant probe, no model calls
	./run.sh smoke

audit:  ## Score retrieval against the generator's ground truth
	./run.sh audit

alarms:  ## Tail the raw machine_alarms topic (generator output)
	./run.sh alarms

incidents:  ## Summarise the incident store: statuses, human rate, latency
	./run.sh incidents

psql:  ## Open psql against the incident store
	./run.sh psql

console:  ## Kafka UI (Redpanda Console) at localhost:8090
	./run.sh console

inspect:  ## Read the work orders back through Flink SQL
	./run.sh inspect

consume:  ## Tail the output topic straight from Kafka
	./run.sh consume

ps:  ## Show service status
	./run.sh ps

logs:  ## Follow task manager logs
	./run.sh logs

down:  ## Stop everything, keep the images
	./run.sh down

clean:  ## Stop everything and delete Milvus data and build output
	./run.sh clean
