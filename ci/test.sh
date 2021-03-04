#!/bin/bash -x

set -euo pipefail

rm -f work

# Create link to directory with Redis binaries
cwd=$(pwd)

# Launch Redis in proper configuration
pushd /tmp && ln -s /work && make -f $cwd/Makefile start REDIS_VERSION=6.2.1 && popd

# Execute maven test
MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw clean test -P${PROFILE} -DrunLongTests=${LONG_TESTS:-false} -B

# Capture resulting exit code from maven (pass/fail)
RESULT=$?

# Shutdown Redis
pushd /tmp && make -f $cwd/Makefile stop && popd

# Return maven results
exit $RESULT