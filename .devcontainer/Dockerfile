FROM mcr.microsoft.com/devcontainers/rust:1-1-bullseye

# Install protobuf compiler
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends protobuf-compiler

# Install python dependencies including pip
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends python3-pip python3-setuptools python3-wheel \
    && python -m pip install --upgrade pip \ 
    && python -m pip install -r Requirements.txt