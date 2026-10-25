.PHONY: up down logs test produce lag reset demo

up:
	docker compose up -d kafka mysql redis kafka-init consumer

down:
	docker compose down

logs:
	docker compose logs -f consumer

test:
	pytest -q

produce:
	docker compose --profile demo run --rm producer

lag:
	docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group event-processors

reset:
	docker compose down -v

demo: up
	docker compose --profile demo run --rm producer
	docker compose exec mysql mysql -u$${MYSQL_USER:-events} -p$${MYSQL_PASSWORD:-events_password} events -e "SELECT COUNT(*) AS raw_events FROM raw_events; SELECT error_reason, COUNT(*) AS dlq_count FROM dlq_events GROUP BY error_reason;"
