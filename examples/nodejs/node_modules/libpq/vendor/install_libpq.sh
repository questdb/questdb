#!/bin/bash

set -e

OPENSSL_DIR="$(pwd)/openssl-1.1.1b"
POSTGRES_VERSION="11.3"
POSTGRES_DIR="$(pwd)/postgres-${POSTGRES_VERSION}"
TMP_DIR="/tmp/postgres"
JOBS="-j$(nproc || echo 1)"

if [ -d "${TMP_DIR}" ]; then
  rm -rf "${TMP_DIR}"
fi

mkdir -p "${TMP_DIR}"

curl https://ftp.postgresql.org/pub/source/v${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.tar.gz | \
  tar -C "${TMP_DIR}" -xzf -

cd "${TMP_DIR}/postgresql-${POSTGRES_VERSION}"

if [ -d "${POSTGRES_DIR}" ]; then
  rm -rf "${POSTGRES_DIR}"
fi
mkdir -p $POSTGRES_DIR

./configure --prefix=$POSTGRES_DIR --with-openssl --with-includes=${OPENSSL_DIR}/include --with-libraries=${OPENSSL_DIR}/lib --without-readline

cd src/interfaces/libpq; make; make install; cd -
cd src/bin/pg_config; make install; cd -
cd src/backend; make generated-headers; cd -
cd src/include; make install; cd -

export PATH="${POSTGRES_DIR}/bin:${PATH}"
export CFLAGS="-I${POSTGRES_DIR}/include"
export LDFLAGS="-L${POSTGRES_DIR}/lib"
export LD_LIBRARY_PATH="${POSTGRES_DIR}/lib:$LD_LIBRARY_PATH"
export PKG_CONFIG_PATH="${POSTGRES_DIR}/lib/pkgconfig:$PKG_CONFIG_PATH"
