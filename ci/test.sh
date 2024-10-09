#!/bin/bash -x

set -euo pipefail

rm -f work

# Create link to directory with Redis binaries
cwd=$(pwd)

# Launch Redis in proper configuration
pushd /tmp && ln -s /work && make -f $cwd/Makefile start && popd

export JENKINS_USER=${JENKINS_USER_NAME}

# Execute maven test
MAVEN_OPTS="-Duser.name=${JENKINS_USER} -Duser.home=/tmp/jenkins-home" ./mvnw -s settings.xml -Ddevelocity.storage.directory=/tmp/jenkins-home/.develocity-root -Dmaven.repo.local=/tmp/jenkins-home/.m2/spring-data-redis clean test -P${PROFILE} -DrunLongTests=${LONG_TESTS:-false} -U -B

# Capture resulting exit code from maven (pass/fail)
RESULT=$?

# Shutdown Redis
pushd /tmp && make -f $cwd/Makefile stop && popd

# Return maven results
exit $RESULT
