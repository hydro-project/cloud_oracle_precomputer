FROM mcr.microsoft.com/devcontainers/rust:1-1-bullseye

ARG PYTHON_VERSION=3.9.16

USER vscode
WORKDIR /home/vscode

# Install python dependencies including pip
# Install python environment manager
RUN curl https://pyenv.run | bash && \
    echo 'export PATH="$HOME/.pyenv/bin:$PATH"' >> ~/.bashrc && \
    echo 'eval "$($HOME/.pyenv/bin/pyenv init -)"' >> ~/.bashrc && \
    echo 'eval "$($HOME/.pyenv/bin/pyenv init -)"' >> ~/.bashrc

# Install setup python environment with optimized python
# env PYTHON_CONFIGURE_OPTS='--enable-optimizations --with-lto --enable-shared' PYTHON_CFLAGS='-march=native -mtune=native'
RUN $HOME/.pyenv/bin/pyenv install 3.9.16  && \
    $HOME/.pyenv/bin/pyenv virtualenv 3.9.16 dev && \
    $HOME/.pyenv/bin/pyenv global 3.9.16  && \
    $HOME/.pyenv/shims/pip install pip --upgrade  && \
    $HOME/.pyenv/shims/pip install maturin

# Copy in the project files
#COPY --chown=vscode:vscode . /home/vscode/sky-pie-precomputation

# Install protobuf from Github
COPY install_protobuf.sh /home/vscode
RUN sudo bash /home/vscode/install_protobuf.sh && \
    rm /home/vscode/install_protobuf.sh
