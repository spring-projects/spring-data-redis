#!/bin/sh

for i in 0 1 2 3; do
  cp /config/node-$i.conf /data/node-$i.conf
  redis-server /data/node-$i.conf --daemonize yes
done

for port in 7379 7380 7381 7382; do
  until redis-cli -p $port ping 2>/dev/null | grep -q PONG; do
    sleep 0.5
  done
done

redis-cli --cluster create \
  127.0.0.1:7379 \
  127.0.0.1:7380 \
  127.0.0.1:7381 \
  --cluster-replicas 0 \
  --cluster-yes

redis-cli -p 7382 cluster meet 127.0.0.1 7379

until redis-cli -p 7382 cluster info 2>/dev/null | grep -q "cluster_known_nodes:4"; do
  sleep 0.5
done

MASTER_ID=$(redis-cli -p 7379 cluster myid)
redis-cli -p 7382 cluster replicate "$MASTER_ID"

tail -f /dev/null