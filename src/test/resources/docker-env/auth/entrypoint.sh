#!/bin/sh

cp /config/standalone-auth.conf /data/standalone-auth.conf
cp /config/sentinel-auth.conf /data/sentinel-auth.conf

redis-server /data/standalone-auth.conf --daemonize yes

until redis-cli -p 6382 -a foobared ping 2>/dev/null | grep -q PONG; do
  sleep 0.5
done

redis-sentinel /data/sentinel-auth.conf