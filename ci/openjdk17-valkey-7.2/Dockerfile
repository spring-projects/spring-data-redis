ARG BASE
FROM ${BASE}
# Any ARG statements before FROM are cleared.
ARG VERSION
ENV VERSION=${VERSION}
ENV PROJECT=valkey
ENV GH_ORG=valkey-io

# Copy Spring Data Redis's Makefile into the container
COPY ./Makefile /

RUN set -eux; \
#	sed -i -e 's/http/https/g' /etc/apt/sources.list ; \
	apt-get update ; \
	apt-get install -y build-essential ; \
	make work/valkey/bin/valkey-cli work/valkey/bin/valkey-server VERSION=${VERSION}; \
	chmod -R o+rw work; \
	apt-get clean; \
	rm -rf /var/lib/apt/lists/*;
