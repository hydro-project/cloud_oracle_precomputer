FROM mcr.microsoft.com/devcontainers/rust:1-1-bullseye

# Install protobuf compiler
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get -y install --no-install-recommends protobuf-compiler