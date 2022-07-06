#!/bin/bash

set -eo pipefail

ARCH=$(uname -m)
PLATFORM=$(uname -s | tr '[:upper:]' '[:lower:]')
UNAME_PLATFORM=${PLATFORM}
BINARY=protoc
if [[ ${PLATFORM} == "darwin" ]]; then 
    PLATFORM=osx
    ARCH=universal_binary
elif [[ ${PLATFORM} == "linux" ]]; then
    case ${ARCH} in
    "arm64")
        ARCH=aarch_64
    ;;
    "amd64")
        ARCH=x86_64
    ;;
    *)
        echo "-> Architecture not supported"; exit 1;
    ;;
esac
fi

LATEST_RELEASE=$(curl -s https://api.github.com/repos/protocolbuffers/protobuf/releases/latest)
TAG=$(echo ${LATEST_RELEASE} | jq -r '.tag_name')
TAG_NO_V=$(echo ${TAG}|tr -d 'v')

PROTOC_PATH=build/${UNAME_PLATFORM}/${BINARY}
mkdir -p ${PROTOC_PATH}

curl -sL -o ${PROTOC_PATH}/${BINARY}.zip https://github.com/protocolbuffers/protobuf/releases/download/${TAG}/${BINARY}-${TAG_NO_V}-${PLATFORM}-${ARCH}.zip

BIN_PATH=${PROTOC_PATH}/bin/${BINARY}
cd ${PROTOC_PATH} && unzip ${BINARY}.zip
cd -
chmod +x ${BIN_PATH}
