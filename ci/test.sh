#!/usr/bin/env bash

set -euo pipefail

[[ -d $PWD/maven && ! -d $HOME/.m2 ]] && ln -s $PWD/maven $HOME/.m2

rm -rf $HOME/.m2/repository/org/springframework/data/redis 2> /dev/null || :

cd spring-data-redis-github

ln -s /work  x

make test SPRING_PROFILE=spring5-next

./mvnw clean dependency:list test -P${PROFILE} -Dsort
