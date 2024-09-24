ARG BASE
FROM ${BASE}
# Any ARG statements before FROM are cleared.
ARG VERSION
ENV VERSION=${VERSION}

# Copy Spring Data Redis's Makefile into the container
COPY ./Makefile /


RUN set -eux; \
#	sed -i -e 's/http/https/g' /etc/apt/sources.list ; \
	apt-get update ; \
	apt-get install -y build-essential curl; \
	make work/redis/bin/redis-cli work/redis/bin/redis-server VERSION=${VERSION}; \
	chmod -R o+rw work; \
	apt-get clean; \
	rm -rf /var/lib/apt/lists/*;
