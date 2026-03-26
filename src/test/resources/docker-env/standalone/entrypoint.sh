#!/bin/sh

cp /config/master.conf /data/master.conf
cp /config/replica-1.conf /data/replica-1.conf
cp /config/replica-2.conf /data/replica-2.conf

for i in 1 2 3; do
  cp /config/sentinel-$i.conf /data/sentinel-$i.conf
done

redis-server /data/master.conf --daemonize yes

until redis-cli -p 6379 ping 2>/dev/null | grep -q PONG; do
  sleep 0.5
done

redis-server /data/replica-1.conf --daemonize yes
redis-server /data/replica-2.conf --daemonize yes

for port in 6380 6381; do
  until redis-cli -p $port ping 2>/dev/null | grep -q PONG; do
    sleep 0.5
  done
done

redis-sentinel /data/sentinel-1.conf --daemonize yes
redis-sentinel /data/sentinel-2.conf --daemonize yes

for port in 26379 26380; do
  until redis-cli -p $port ping 2>/dev/null | grep -q PONG; do
    sleep 0.5
  done
done

redis-sentinel /data/sentinel-3.conf
