FROM mcr.microsoft.com/devcontainers/rust:1-1-bullseye

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

# Clone the skypie-precomputation repo
RUN git clone https://github.com/hydro-project/cloud_oracle_precomputer.git

WORKDIR /home/vscode/cloud_oracle_precomputer

RUN git checkout dev

# Install protobuf from Github
RUN sudo bash util/install_protobuf.sh

# Install dependencies for cvxpy
RUN sudo apt-get update && \
    sudo apt-get install -y libopenblas-dev cmake

# Install Python requirements
RUN $HOME/.pyenv/shims/pip install -r Requirements.txt

# Build and install the skypie-precomputation util packages
RUN $HOME/.pyenv/shims/pip install -e baselines && \
    $HOME/.pyenv/shims/pip install -e proto_messages

WORKDIR /home/vscode
RUN sudo rm -rf cloud_oracle_precomputer

# Install SkyPIE oracle
RUN git clone https://github.com/hydro-project/cloud_oracle_skypie.git
WORKDIR /home/vscode/cloud_oracle_skypie
RUN $HOME/.pyenv/shims/pip install -e .

WORKDIR /home/vscode
RUN sudo rm -rf cloud_oracle_skypie