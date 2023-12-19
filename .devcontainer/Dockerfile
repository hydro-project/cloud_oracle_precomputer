ARG PYTHON_VERSION=3.9.16

FROM mcr.microsoft.com/devcontainers/rust:1-1-bullseye

# Install protobuf compiler
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends protobuf-compiler

USER vscode

# Install python dependencies including pip
# Install python environment manager
RUN curl https://pyenv.run | bash && \
    echo 'export PATH="$HOME/.pyenv/bin:$PATH"' >> ~/.bashrc && \
    echo 'eval "$($HOME/.pyenv/bin/pyenv init -)"' >> ~/.bashrc && \
    echo 'eval "$($HOME/.pyenv/bin/pyenv init -)"' >> ~/.bashrc

# Install setup python environment with optimized python
# env PYTHON_CONFIGURE_OPTS='--enable-optimizations --with-lto --enable-shared' PYTHON_CFLAGS='-march=native -mtune=native'
RUN $HOME/.pyenv/bin/pyenv install 3.9.16

RUN $HOME/.pyenv/bin/pyenv virtualenv 3.9.16 dev
RUN $HOME/.pyenv/bin/pyenv global 3.9.16
RUN $HOME/.pyenv/shims/pip install pip --upgrade
RUN $HOME/.pyenv/shims/pip install maturin
#RUN python3 -m pip install maturin