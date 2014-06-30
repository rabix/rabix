FROM ubuntu:14.04

RUN apt-get update && apt-get upgrade -y --no-install-recommends && \
    apt-get install -y --no-install-recommends build-essential zlib1g-dev curl python

