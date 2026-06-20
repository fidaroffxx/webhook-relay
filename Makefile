.PHONY: up down logs ps topics postgres-only

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

topics:
	docker exec relay-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

postgres-only:
	docker compose up -d postgres
