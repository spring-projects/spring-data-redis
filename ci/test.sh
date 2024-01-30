#!/bin/bash -x

set -euo pipefail

rm -f work

# Create link to directory with Redis binaries
cwd=$(pwd)

# Launch Redis in proper configuration
pushd /tmp && ln -s /work && make -f $cwd/Makefile start && popd

export DEVELOCITY_CACHE_USERNAME=${DEVELOCITY_CACHE_USR}
export DEVELOCITY_CACHE_PASSWORD=${DEVELOCITY_CACHE_PSW}
export JENKINS_USER=${JENKINS_USER_NAME}

# The environment variable to configure access key is still GRADLE_ENTERPRISE_ACCESS_KEY
export GRADLE_ENTERPRISE_ACCESS_KEY=${DEVELOCITY_ACCESS_KEY}

# Execute maven test
MAVEN_OPTS="-Duser.name=${JENKINS_USER} -Duser.home=/tmp/jenkins-home" ./mvnw -s settings.xml clean test -P${PROFILE} -DrunLongTests=${LONG_TESTS:-false} -Dgradle.cache.local.enabled=false -Dgradle.cache.remote.enabled=false -U -B

# Capture resulting exit code from maven (pass/fail)
RESULT=$?

# Shutdown Redis
pushd /tmp && make -f $cwd/Makefile stop && popd

# Return maven results
exit $RESULT
