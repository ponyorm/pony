PYTHON_VERSIONS := 3.8 3.9 3.10 3.11 3.12
DB_BACKENDS     := sqlite mysql postgres cockroach

# Pin the compose project name so the network is always pony_default.
export COMPOSE_PROJECT_NAME := pony
COMPOSE_NETWORK := pony_default

PYTHON      ?= python
PYTEST_ARGS ?=
TEST_CMD     = $(PYTHON) -m pytest pony/orm/tests/ -v $(PYTEST_ARGS)

.PHONY: usage test test-all docker-up docker-down clean \
        $(addprefix test-,$(DB_BACKENDS)) \
        $(addprefix test-,$(PYTHON_VERSIONS)) \
        $(foreach v,$(PYTHON_VERSIONS),$(addprefix test-$(v)-,$(DB_BACKENDS)))

usage:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Local targets (host Python, docker-compose DBs on localhost):"
	@echo "  test-sqlite       SQLite in-memory (no Docker needed)"
	@echo "  test-mysql        MySQL"
	@echo "  test-postgres     PostgreSQL"
	@echo "  test-cockroach    CockroachDB"
	@echo "  test-all          All local DB backends"
	@echo ""
	@echo "Docker targets (containerised Python x docker-compose DBs):"
	@echo "  test-<version>          All DB backends in Python <version>  (e.g. make test-3.12)"
	@echo "  test-<version>-<db>     One DB in Python <version>           (e.g. make test-3.12-mysql)"
	@echo "  test                    Full matrix: all versions x all DBs"
	@echo ""
	@echo "Python versions : $(PYTHON_VERSIONS)"
	@echo "DB backends     : $(DB_BACKENDS)"
	@echo ""
	@echo "Infrastructure:"
	@echo "  docker-up    Start database containers (MySQL, PostgreSQL, CockroachDB)"
	@echo "  docker-down  Stop and remove database containers"
	@echo "  clean        Remove all pony-test Docker images"

# Full matrix — -k keeps going past failures so all combinations run
test:
	$(MAKE) -k $(addprefix test-,$(PYTHON_VERSIONS))

# Local DB targets
test-sqlite:
	$(TEST_CMD)

test-mysql:
	env pony_test_db="pony/orm/tests/config/mysql.py" $(TEST_CMD)

test-postgres:
	env pony_test_db="pony/orm/tests/config/postgres.py" $(TEST_CMD)

test-cockroach:
	env pony_test_db="pony/orm/tests/config/cockroach.py" $(TEST_CMD)

test-all: test-sqlite test-mysql test-postgres test-cockroach

# Docker: test-<version> expands to all DB backends for that version
$(addprefix test-,$(PYTHON_VERSIONS)): test-%: $(addprefix test-%-,$(DB_BACKENDS))

# Docker: test-<version>-sqlite
define DOCKER_SQLITE_RULE
test-$(1)-sqlite:
	docker build --build-arg PYTHON_VERSION=$(1) -t pony-test-$(1) .
	docker run --rm pony-test-$(1)
endef
$(foreach v,$(PYTHON_VERSIONS),$(eval $(call DOCKER_SQLITE_RULE,$(v))))

# Docker: test-<version>-<db>  (non-sqlite: attach to compose network, pass service hostname)
define DOCKER_DB_RULE
test-$(1)-$(2):
	docker build --build-arg PYTHON_VERSION=$(1) -t pony-test-$(1) .
	docker run --rm \
		--network "$(COMPOSE_NETWORK)" \
		--env pony_test_db="pony/orm/tests/config/$(2).py" \
		--env PONY_TEST_HOST=$(2) \
		pony-test-$(1)
endef
$(foreach v,$(PYTHON_VERSIONS),$(foreach db,mysql postgres cockroach,$(eval $(call DOCKER_DB_RULE,$(v),$(db)))))

docker-up:
	docker compose up -d --wait

docker-down:
	docker compose down -v

clean:
	@$(foreach v,$(PYTHON_VERSIONS),docker rmi -f pony-test-$(v) 2>/dev/null || true;)
