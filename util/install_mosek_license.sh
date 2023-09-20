#!/bin/bash

# Install MOSEK license file

# Usage: ./install_mosek_license.sh <path to mosek.lic>
if [ "$#" -ne 1 ]; then
    echo "Usage: ./install_mosek_license.sh <path to mosek.lic>"
    exit 1
fi

mkdir -p ~/mosek
cp ${1} ~/mosek/mosek.lic