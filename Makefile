DATE ?= 2026-10-01
DAYS ?= 7

up:
	docker compose up -d

down:
	docker compose down

reset:
	docker compose down -v

generate:
	python -m data_generator.generate --date $(DATE) --days $(DAYS)

load:
	python -m ingestion.load_raw --reference
	@for i in $$(seq 0 $$(($(DAYS)-1))); do \
		d=$$(date -d "$(DATE) + $$i days" +%F); \
		python -m ingestion.load_raw --date $$d; \
	done
