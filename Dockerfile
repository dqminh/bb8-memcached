FROM registry.cfdata.org/stash/plat/dockerfiles/debian-buster-rustlang/master:1.53.0-1-1 AS builder
COPY Cargo.toml Cargo.lock /build/
COPY src /build/src
WORKDIR /build
CMD cargo test
