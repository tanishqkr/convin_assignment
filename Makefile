.PHONY: up down reset test logs

up:
	$(MAKE) -C webhook-ingest/webhook-ingest up

down:
	$(MAKE) -C webhook-ingest/webhook-ingest down

reset:
	$(MAKE) -C webhook-ingest/webhook-ingest reset

test:
	cd webhook-ingest/webhook-ingest && go test ./...

logs:
	$(MAKE) -C webhook-ingest/webhook-ingest logs
