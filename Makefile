# Copyright 2011-present the original author or authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SPRING_PROFILE?=ci
SHELL=/bin/bash -euo pipefail
COMPOSE_FILE=src/test/resources/docker-env/docker-compose.yml

IMAGE?=redis:8.6.2
export IMAGE

WORK_DIR=$(CURDIR)/work
export WORK_DIR

.PHONY: start stop clean clobber test all-tests logs status

start:
	@echo "Starting Redis infrastructure..."
	@mkdir -p work
	docker compose -f $(COMPOSE_FILE) up -d --wait redis-master redis-auth redis-cluster
	@echo "Initializing Sentinels and Cluster..."
	docker compose -f $(COMPOSE_FILE) up -d sentinel-init cluster-init
	@echo "Redis infrastructure is ready!"

stop:
	@echo "Stopping Redis infrastructure..."
	docker compose -f $(COMPOSE_FILE) down
	rm $(WORK_DIR)/*.sock || true

clean:
	@echo "Cleaning up Redis infrastructure..."
	docker compose -f $(COMPOSE_FILE) down -v --remove-orphans
	rm $(WORK_DIR)/*.sock || true

clobber: clean

test: start
	@sleep 2
	./mvnw clean test -U -P$(SPRING_PROFILE); \
	test_exit=$$?; \
	$(MAKE) clean; \
	exit $$test_exit

all-tests: start
	@sleep 1
	./mvnw clean test -U -DrunLongTests=true -P$(SPRING_PROFILE); \
	test_exit=$$?; \
	$(MAKE) clean; \
	exit $$test_exit

status:
	docker compose -f $(COMPOSE_FILE) ps

logs:
	docker compose -f $(COMPOSE_FILE) logs
