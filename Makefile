COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: build test race lint up down logs smoke clean

build:
	go build ./...

test:
	go test ./...

race:
	go test -race -count=1 ./...

lint: build
	go vet ./...
	go run ./cmd/bento-mysql lint deploy/bento/router.yaml

up:
	$(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f bento

smoke:
	./scripts/smoke.sh

clean: down
	go clean -testcache
