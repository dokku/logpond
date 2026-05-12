DOKKU_VERSION ?= latest
LOGPOND_TEST_HOST_DIR ?= $(CURDIR)/tmp/test-host
LOGPOND_IMAGE ?= 127.0.0.1:5000/logpond:test
VERSION ?= 0.0.0-test

# Optional path or filename relative to /logpond-src/tests passed to bats, e.g.
# `make unit-tests UNIT_TESTS=logpond_deploy.bats`. Defaults to the whole tests
# directory.
UNIT_TESTS ?= .
UNIT_TESTS_FILTER ?=
BATS_FLAGS := --timing --print-output-on-failure
ifneq ($(UNIT_TESTS_FILTER),)
BATS_FLAGS += --filter '$(UNIT_TESTS_FILTER)'
endif

COMPOSE := DOKKU_VERSION=$(DOKKU_VERSION) LOGPOND_TEST_HOST_DIR=$(LOGPOND_TEST_HOST_DIR) docker compose -f tests/docker-compose.yml
COMPOSE_COMPOSE_MODE := $(COMPOSE) --profile compose-mode
COMPOSE_EXEC_DOKKU := $(COMPOSE_COMPOSE_MODE) exec -T dokku

.PHONY: setup build-image push-image build-stack wait-stack install-logpond \
	unit-tests test clean logs \
	setup-native bring-up-services install-logpond-native unit-tests-native test-native clean-native \
	go-build go-test vendor

# --- Local Go workflow ---

vendor:
	bash scripts/vendor.sh

go-build: vendor
	go build -o tmp/claude/logpond ./cmd/logpond

go-test:
	go test ./...

# --- Compose mode: dokku runs in a docker compose container ---

setup: build-stack wait-stack build-image push-image install-logpond

build-stack:
	mkdir -p $(LOGPOND_TEST_HOST_DIR)
	$(COMPOSE_COMPOSE_MODE) build
	$(COMPOSE_COMPOSE_MODE) up -d

wait-stack:
	$(COMPOSE_COMPOSE_MODE) up -d --wait

build-image:
	docker build --build-arg VERSION=$(VERSION) -t $(LOGPOND_IMAGE) -f Dockerfile .

push-image:
	docker push $(LOGPOND_IMAGE)

install-logpond:
	$(COMPOSE_EXEC_DOKKU) bash /logpond-src/tests/setup.sh

unit-tests:
	$(COMPOSE_EXEC_DOKKU) bats $(BATS_FLAGS) /logpond-src/tests/$(UNIT_TESTS)

test: unit-tests

logs:
	$(COMPOSE_COMPOSE_MODE) logs --no-color --tail=200

clean:
	$(COMPOSE_COMPOSE_MODE) down -v --remove-orphans
	# The host-side state dir contains files owned by root inside the
	# dokku container, which the host user cannot rm without elevation.
	rm -rf $(LOGPOND_TEST_HOST_DIR) 2>/dev/null || sudo rm -rf $(LOGPOND_TEST_HOST_DIR)

# --- Native mode: dokku installed on the host, supporting services in compose ---

setup-native: bring-up-services build-image push-image install-logpond-native

bring-up-services:
	$(COMPOSE) up -d --wait

install-logpond-native:
	bash tests/setup-native.sh

unit-tests-native:
	SUDO=sudo DOKKU="sudo -u dokku dokku" bats $(BATS_FLAGS) tests/$(UNIT_TESTS)

test-native: unit-tests-native

clean-native:
	$(COMPOSE) down -v --remove-orphans
