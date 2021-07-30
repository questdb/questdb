#!/bin/sh

BUILD_DIR="$(pwd)"
source ./vendor/install_openssl.sh 1.1.1b
sudo updatedb
source ./vendor/install_libpq.sh
sudo updatedb
sudo ldconfig
cd $BUILD_DIR
