FROM ubuntu:20.04
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
    && apt-get install -y \
        cmake \
        g++ \
        gcc \
        git \
        libboost1.71-all-dev \
        libhwloc-dev \
        libncurses5-dev \
        libpq-dev \
        libreadline-dev \
        libsqlite3-dev \
        libtbb-dev \
        lld \
        lsb-release \
        man \
        postgresql-server-dev-all \
        sudo \
    && apt-get clean

ENV OPOSSUM_HEADLESS_SETUP=true

COPY test/test_plugin.sh /test_plugin.sh
ENTRYPOINT ["/test_plugin.sh"]
