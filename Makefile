# Convenience wrapper. Windows without make: copy the command after the target name.
DC = docker compose

.PHONY: up lite bigdata billion down stop logs ps status smoke backfill gold turbo calm psql size clean urls

up:            ## build images and start the whole platform
	$(DC) up -d --build
	@$(MAKE) --no-print-directory urls

lite:          ## 8 GB laptops: same pipeline + website, Spark in one container, smaller data
	$(DC) -f docker-compose.lite.yml up -d --build
	@echo ""; echo "  Website  http://localhost:8000    Airflow  http://localhost:8080 (admin / admin)"; echo ""

bigdata:       ## billion-scale profile (needs ~20 GB RAM for Docker, 10+ cores, ~150 GB disk)
	$(DC) -f docker-compose.yml -f docker-compose.bigdata.yml up -d --build
	@$(MAKE) --no-print-directory urls

billion:       ## generate 1,000,000,000 historical orders (resumable)
	$(DC) exec airflow-scheduler airflow dags trigger zomato_historical_backfill \
	  --conf '{"orders": 1000000000, "days": 365}'

size:          ## how big is the lake right now?
	$(DC) run --rm -T --entrypoint aws s3-init --endpoint-url http://s3:9000 s3 ls s3://lakehouse --recursive --summarize --human-readable | tail -2
	$(DC) exec -T postgres psql -U zomato -d zomato -c \
	  "select to_char(sum(orders),'FM999,999,999,999') as orders_served from gold.daily_city_metrics"

down:          ## stop and remove containers (data volumes are kept)
	$(DC) down

stop:
	$(DC) stop

logs:          ## follow logs of the streaming pieces
	$(DC) logs -f --tail=50 generator spark-streaming

ps status:
	$(DC) ps

smoke:         ## end-to-end health check of the running stack
	bash scripts/smoke_test.sh

backfill:      ## regenerate history, e.g. make backfill ORDERS=50000000 DAYS=730
	$(DC) exec airflow-scheduler airflow dags trigger zomato_historical_backfill \
	  --conf '{"orders": $(or $(ORDERS),5000000), "days": $(or $(DAYS),365), "resume": false}'

gold:          ## run the gold + dbt batch pipeline now
	$(DC) exec airflow-scheduler airflow dags trigger zomato_lakehouse_batch

turbo:         ## 10x the live event rate
	ORDERS_PER_SEC=500 $(DC) up -d --no-deps generator

calm:          ## back to the default rate from .env
	$(DC) up -d --no-deps --force-recreate generator

psql:          ## open a SQL shell on the warehouse
	$(DC) exec postgres psql -U zomato -d zomato

clean:         ## DANGER: remove containers AND all data volumes
	$(DC) down -v --remove-orphans

urls:
	@echo ""
	@echo "  Zomato Analytics     http://localhost:8000"
	@echo "  Grafana dashboards   http://localhost:3000   (admin / admin)"
	@echo "  Airflow              http://localhost:8080   (admin / admin)"
	@echo "  Spark master UI      http://localhost:9090"
	@echo "  Spark streaming app  http://localhost:4040"
	@echo "  Kafka UI             http://localhost:8085"
	@echo "  Lake file browser    http://localhost:8888/buckets/lakehouse/"
	@echo "  Postgres             localhost:5433          (zomato / zomato)"
	@echo ""
