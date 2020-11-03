FROM adoptopenjdk/openjdk15:latest

# Copy Spring Data Redis's Makefile into the container
COPY ./Makefile /

RUN set -eux; \
	apt-get update ; \
	apt-get install -y build-essential ; \
	make work/redis/bin/redis-cli work/redis/bin/redis-server; \
	chmod -R o+rw work; \
	apt-get clean; \
	rm -rf /var/lib/apt/lists/*;

