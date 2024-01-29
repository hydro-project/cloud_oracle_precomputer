PB_VER="25.1"
PB_REL="https://github.com/protocolbuffers/protobuf/releases"

PB_OS="linux"
# Detect hardware architecture: x86_64 or arm64 -> aarch64
PB_ARCH=$(uname -m)
if [ "${PB_ARCH}" = "x86_64" ]; then
    echo ${PB_ARCH}
    #PB_ARCH="linux-x86_64"
elif [ "${PB_ARCH}" = "aarch64" ]; then
    echo ${PB_ARCH}
    PB_ARCH="aarch_64"
elif [ "${PB_ARCH}" = "arm64" ]; then
    PB_ARCH="aarch_64"
    echo ${PB_ARCH}
else
    echo "Unsupported architecture: ${PB_ARCH}"
    exit 1
fi

PB_URL="${PB_REL}/download/v${PB_VER}/protoc-${PB_VER}-${PB_OS}-${PB_ARCH}.zip"

mkdir -p protoc && pushd protoc && \
    curl -L ${PB_URL} -o protoc.zip && \
    head protoc.zip && \
    unzip protoc.zip && \
    popd && \
    sudo mv protoc/bin/* /usr/local/bin/ && \
    sudo mv protoc/include/* /usr/local/include/ && \
    rm -rf protoc
