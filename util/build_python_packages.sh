#!/bin/bash

# Build all components of the project
components=("baselines" "proto_messages")

# Build flags
MATURIN_FLAGS="-r"

# Path of this script
SCRIPT_PATH=$(dirname $(readlink -f $0))
ROOT_PATH=$(dirname $SCRIPT_PATH)

# Build all components
for component in ${components[@]}; do
    echo "Building ${component}..."
    maturin build ${MATURIN_FLAGS} -m ${ROOT_PATH}/${component}/Cargo.toml
done