# Copyright 2011-2018 the original author or authors.
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

REDIS_VERSION:=4.0.9
SPRING_PROFILE?=ci

#######
# Redis
#######
.PRECIOUS: work/redis-%.conf

work/redis-%.conf:
	@mkdir -p $(@D)

	echo port $* >> $@
	echo daemonize yes >> $@
	echo protected-mode no >> $@
	echo notify-keyspace-events Ex >> $@
	echo pidfile $(shell pwd)/work/redis-$*.pid >> $@
	echo logfile $(shell pwd)/work/redis-$*.log >> $@
	echo unixsocket $(shell pwd)/work/redis-$*.sock >> $@
	echo unixsocketperm 755 >> $@
	echo save \"\" >> $@
	echo slaveof 127.0.0.1 6379 >> $@

# Handled separately because it's the master and all others are slaves
work/redis-6379.conf:
	@mkdir -p $(@D)

	echo port 6379 >> $@
	echo daemonize yes >> $@
	echo protected-mode no >> $@
	echo notify-keyspace-events Ex >> $@
	echo pidfile $(shell pwd)/work/redis-6379.pid >> $@
	echo logfile $(shell pwd)/work/redis-6379.log >> $@
	echo unixsocket $(shell pwd)/work/redis-6379.sock >> $@
	echo unixsocketperm 755 >> $@
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
	echo protected-mode no >> $@
	echo bind 0.0.0.0 >> $@
	echo pidfile $(shell pwd)/work/sentinel-$*.pid >> $@
	echo logfile $(shell pwd)/work/sentinel-$*.log >> $@
	echo save \"\" >> $@
	echo sentinel monitor mymaster 127.0.0.1 6379 2 >> $@

work/sentinel-%.pid: work/sentinel-%.conf work/redis-6379.pid work/redis/bin/redis-server
	work/redis/bin/redis-server $< --sentinel

sentinel-start: work/sentinel-26379.pid work/sentinel-26380.pid work/sentinel-26381.pid

sentinel-stop: stop-26379 stop-26380 stop-26381


#########
# Cluster
#########
.PRECIOUS: work/cluster-%.conf

work/cluster-%.conf:
	@mkdir -p $(@D)

	echo port $* >> $@
	echo protected-mode no >> $@
	echo cluster-enabled yes  >> $@
	echo cluster-config-file $(shell pwd)/work/nodes-$*.conf  >> $@
	echo cluster-node-timeout 5  >> $@
	echo pidfile $(shell pwd)/work/cluster-$*.pid >> $@
	echo logfile $(shell pwd)/work/cluster-$*.log >> $@
	echo save \"\" >> $@

work/cluster-%.pid: work/cluster-%.conf work/redis/bin/redis-server
	work/redis/bin/redis-server $< &

cluster-start: work/cluster-7379.pid work/cluster-7380.pid work/cluster-7381.pid work/cluster-7382.pid

work/meet-%:
	-work/redis/bin/redis-cli -p $* cluster meet 127.0.0.1 7379

# Handled separately because this node is a slave
work/meet-7382:
	-work/redis/bin/redis-cli -p 7382 cluster meet 127.0.0.1 7379
	sleep 2
	-work/redis/bin/redis-cli -p 7382 cluster replicate $(shell work/redis/bin/redis-cli -p 7379 cluster myid)

cluster-meet: work/meet-7380 work/meet-7381 work/meet-7382

cluster-stop: stop-7379 stop-7380 stop-7381 stop-7382

cluster-slots:
	-work/redis/bin/redis-cli -p 7379 cluster addslots $(shell seq 0 5460)
	-work/redis/bin/redis-cli -p 7380 cluster addslots $(shell seq 5461 10922)
	-work/redis/bin/redis-cli -p 7381 cluster addslots $(shell seq 10923 16383)

cluster-init: cluster-start cluster-meet cluster-slots

########
# Global
########
clean:
	rm -rf work/*.conf work/*.log dump.rdb

clobber:
	rm -rf work

work/redis/bin/redis-cli work/redis/bin/redis-server:
	@mkdir -p work/redis

	curl -sSL https://github.com/antirez/redis/archive/$(REDIS_VERSION).tar.gz | tar xzf - -C work
	$(MAKE) -C work/redis-$(REDIS_VERSION) -j
	$(MAKE) -C work/redis-$(REDIS_VERSION) PREFIX=$(shell pwd)/work/redis install
	rm -rf work/redis-$(REDIS_VERSION)

start: redis-start sentinel-start cluster-init

stop-%: work/redis/bin/redis-cli
	-work/redis/bin/redis-cli -p $* shutdown

stop: redis-stop sentinel-stop cluster-stop

test:
	$(MAKE) start
	sleep 2
	mvn clean install -DrunLongTests=true -P$(SPRING_PROFILE)
	$(MAKE) stop
