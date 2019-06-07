FROM adoptopenjdk/openjdk8:latest

RUN apt-get update

# Get the tools for building Redis
RUN apt-get install -y build-essential

# Copy Spring Data Redis's Makefile into the container
COPY ./Makefile /

# Build Redis inside the container so we don't have to build it during the job.
RUN make work/redis/bin/redis-cli work/redis/bin/redis-server

RUN chmod -R o+rw work

RUN apt-get clean \
 && rm -rf /var/lib/apt/lists/*
