# Copyright 2011-2014 the original author or authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

REDIS_VERSION:=2.8.13

#######
# Redis
#######
.PRECIOUS: work/redis-%.conf

work/redis-%.conf:
	@mkdir -p $(@D)

	echo port $* >> $@
	echo daemonize yes >> $@
	echo pidfile $(shell pwd)/work/redis-$*.pid >> $@
	echo logfile $(shell pwd)/work/redis-$*.log >> $@
	echo save \"\" >> $@
	echo slaveof 127.0.0.1 6379 >> $@

# Handled separately because it's the master and all others are slaves
work/redis-6379.conf:
	@mkdir -p $(@D)

	echo port 6379 >> $@
	echo daemonize yes >> $@
	echo pidfile $(shell pwd)/work/redis-6379.pid >> $@
	echo logfile $(shell pwd)/work/redis-6379.log >> $@
	echo save \"\" >> $@

work/redis-%.pid: work/redis-%.conf work/redis/bin/redis-server
	work/redis/bin/redis-server $<

redis-start: work/redis-6379.pid work/redis-6380.pid work/redis-6381.pid

redis-stop: stop-6379 stop-6380 stop-6381

##########
# Sentinel
##########
.PRECIOUS: work/sentinel-%.conf

work/sentinel-%.conf:
	@mkdir -p $(@D)

	echo port $* >> $@
	echo daemonize yes >> $@
	echo pidfile $(shell pwd)/work/sentinel-$*.pid >> $@
	echo logfile $(shell pwd)/work/sentinel-$*.log >> $@
	echo save \"\" >> $@
	echo sentinel monitor mymaster 127.0.0.1 6379 2 >> $@

work/sentinel-%.pid: work/sentinel-%.conf work/redis-6379.pid work/redis/bin/redis-server
	work/redis/bin/redis-server $< --sentinel

sentinel-start: work/sentinel-26379.pid work/sentinel-26380.pid work/sentinel-26381.pid

sentinel-stop: stop-26379 stop-26380 stop-26381

########
# Global
########
clean:
	rm -rf work/*.conf work/*.log

clobber:
	rm -rf work

work/redis/bin/redis-cli work/redis/bin/redis-server:
	@mkdir -p work/redis

	curl -sSL https://github.com/antirez/redis/archive/$(REDIS_VERSION).tar.gz | tar xzf - -C work
	$(MAKE) -C work/redis-$(REDIS_VERSION) -j
	$(MAKE) -C work/redis-$(REDIS_VERSION) PREFIX=$(shell pwd)/work/redis install
	rm -rf work/redis-$(REDIS_VERSION)

start: redis-start sentinel-start

stop-%: work/redis/bin/redis-cli
	-work/redis/bin/redis-cli -p $* shutdown

stop: redis-stop sentinel-stop

test:
	$(MAKE) start
	sleep 2
	$(PWD)/gradlew clean build -DrunLongTest=true
	$(MAKE) stop
