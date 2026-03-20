# Copyright 2011-2021 the original author or authors.
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

IMAGE?=redis:8.4.0
export IMAGE
SPRING_PROFILE?=ci
SHELL=/bin/bash -euo pipefail
COMPOSE_FILE=src/test/resources/docker-env/docker-compose.yml

.PHONY: start stop clean clobber test all-tests logs status health cluster-info

start:
	@echo "Starting Redis infrastructure..."
	docker compose -f $(COMPOSE_FILE) up -d --wait redis-master redis-replica-1 redis-replica-2 redis-auth sentinel-1 sentinel-2 sentinel-3 sentinel-auth cluster-node-0 cluster-node-1 cluster-node-2 cluster-node-3
	@echo "Initializing cluster..."
	docker compose -f $(COMPOSE_FILE) up -d cluster-init
	@echo "Redis infrastructure is ready!"

stop:
	@echo "Stopping Redis infrastructure..."
	docker compose -f $(COMPOSE_FILE) down

clean:
	@echo "Cleaning up Redis infrastructure..."
	docker compose -f $(COMPOSE_FILE) down -v --remove-orphans

clobber: clean

test: start
	@sleep 2
	./mvnw clean test -U -P$(SPRING_PROFILE) || (echo "Maven tests failed"; exit 1)
	$(MAKE) stop
	$(MAKE) clean

all-tests: start
	@sleep 1
	./mvnw clean test -U -DrunLongTests=true -P$(SPRING_PROFILE) || (echo "Maven tests failed"; exit 1)
	$(MAKE) stop
	$(MAKE) clean

status:
	docker compose -f $(COMPOSE_FILE) ps
