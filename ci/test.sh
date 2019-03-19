#!/bin/bash

set -uo pipefail

[[ -d $PWD/maven && ! -d $HOME/.m2 ]] && ln -s $PWD/maven $HOME/.m2

rm -rf $HOME/.m2/repository/org/springframework/data/redis 2> /dev/null || :

cd spring-data-redis-github

ln -sf /work

# Launch Redis in proper configuration
make start

./mvnw -U clean test -DrunLongTests=true -Pspring5-next

# Capture resulting exit code from maven (pass/fail)
RESULT=$?

# Shutdown Redis
make stop

exit $RESULT
