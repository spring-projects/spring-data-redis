PATH := ./work/redis-git/src:${PATH}

################## CONFIG ##################

####################
#      REDIS       #
####################

define REDIS_6379_CONF
port 6379
daemonize yes
pidfile ./work/redis_6379.pid
logfile ./work/redis_6379.log
save ""
appendonly no
endef

define REDIS_6380_CONF
port 6380
daemonize yes
pidfile ./work/redis_6380.pid
logfile ./work/redis_6380.log
save ""
appendonly no
endef

define REDIS_6381_CONF
port 6381
daemonize yes
pidfile ./work/redis_6381.pid
logfile ./work/redis_6381.log
save ""
appendonly no
endef

####################
#     CLUSTER      #
####################

define CLUSTER_7379_CONF
port 7379
daemonize yes
pidfile ./work/cluster_7379.pid
logfile ./work/cluster_7379.log
save ""
appendonly no
cluster-enabled yes
cluster-config-file ./work/cluster_7379.conf
endef

define CLUSTER_7380_CONF
port 7380
daemonize yes
pidfile ./work/cluster_7380.pid
logfile ./work/cluster_7380.log
save ""
appendonly no
cluster-enabled yes
cluster-config-file ./work/cluster_7380.conf
endef

define CLUSTER_7381_CONF
port 7381
daemonize yes
pidfile ./work/cluster_7381.pid
logfile ./work/cluster_7381.log
save ""
appendonly no
cluster-enabled yes
cluster-config-file ./work/cluster_7381.conf
endef

####################
#     SENTINEL     #
####################

define SENTINEL_26380_CONF
port 26380
daemonize yes
sentinel monitor mymaster 127.0.0.1 6380 1
sentinel down-after-milliseconds mymaster 2000
sentinel failover-timeout mymaster 120000
sentinel parallel-syncs mymaster 1
pidfile ./work/sentinel_26380.pid
logfile ./work/sentinel_26380.log
endef

define SENTINEL_26381_CONF
port 26381
daemonize yes
sentinel monitor mymaster 127.0.0.1 6381 1
sentinel down-after-milliseconds mymaster 2000
sentinel failover-timeout mymaster 120000
sentinel parallel-syncs mymaster 1
pidfile ./work/sentinel_26381.pid
logfile ./work/sentinel_26381.log
endef

################## EXPORT ##################

export REDIS_6379_CONF
export REDIS_6380_CONF
export REDIS_6381_CONF

export SENTINEL_26380_CONF
export SENTINEL_26381_CONF

# export CLUSTER_7379_CONF
# export CLUSTER_7380_CONF
# export CLUSTER_7381_CONF

################## SCRIPT ##################

cleanup:
	#- rm -vf ./work/cluster_*.conf 2>/dev/null
	#- rm dump.rdb appendonly.aof - 2>/dev/null
	echo 'clean'

start: create-work-dir
	make cleanup
	echo "$$REDIS_6379_CONF" > ./work/redis_6379.conf && redis-server ./work/redis_6379.conf
	echo "$$REDIS_6380_CONF" > ./work/redis_6380.conf && redis-server ./work/redis_6380.conf
	echo "$$REDIS_6381_CONF" > ./work/redis_6381.conf && redis-server ./work/redis_6381.conf
	
	echo "$$SENTINEL_26380_CONF" > ./work/sentinel_26380.conf && redis-sentinel ./work/sentinel_26380.conf
	echo "$$SENTINEL_26381_CONF" > ./work/sentinel_26381.conf && redis-sentinel ./work/sentinel_26381.conf

	# echo "$$CLUSTER_7379_CONF" > ./work/cluster_7379.conf && redis-server ./work/cluster_7379.conf
	# echo "$$CLUSTER_7380_CONF" > ./work/cluster_7380.conf && redis-server ./work/cluster_7380.conf
	# echo "$$CLUSTER_7381_CONF" > ./work/cluster_7381.conf && redis-server ./work/cluster_7381.conf

stop:
	kill -9 `cat ./work/redis_6379.pid`
	kill -9 `cat ./work/redis_6380.pid`
	kill -9 `cat ./work/redis_6381.pid`
	
	kill -9 `cat ./work/sentinel_26380.pid`
	kill -9 `cat ./work/sentinel_26381.pid`

	# kill -9 `cat ./work/cluster_7379.pid`
	# kill -9 `cat ./work/cluster_7381.pid`
	# kill -9 `cat ./work/cluster_7382.pid`

test:
	make start
	sleep 2
	gradle clean build -DrunLongTest=true
	make stop

get-redis: create-work-dir
	[ ! -e work/redis-git ] && git clone https://github.com/antirez/redis.git ./work/redis-git && cd work/redis-git || true
	make -C work/redis-git -j4

create-work-dir:
	- mkdir -p work

travis-install: get-redis
	
