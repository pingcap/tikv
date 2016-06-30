#!/bin/bash

set -e

echo "building etcd..."
mkdir -p ${DEPS_PATH}
mkdir -p ${BIN_PATH}

rm -rf ${DEPS_PATH}/src/github.com/coreos/etcd
git clone --depth=1 https://github.com/coreos/etcd.git ${DEPS_PATH}/src/github.com/coreos/etcd

cd ${DEPS_PATH}/src/github.com/coreos/etcd
export GOPATH=$DEPS_PATH
./build
cp -f ./bin/etcd $BIN_PATH