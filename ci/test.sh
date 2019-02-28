#!/usr/bin/env bash

set -euo pipefail

[[ -d $PWD/maven && ! -d $HOME/.m2 ]] && ln -s $PWD/maven $HOME/.m2

rm -rf $HOME/.m2/repository/org/springframework/data/redis 2> /dev/null || :

cd spring-data-redis-github

ln -s /work  x

# Maven run from inside Makefile to interact with Redis server.
make test SPRING_PROFILE=spring5-next
