#!/bin/bash

whoami
pwd

APP_PATH=/opt/oms
RELEASE_PATH=${APP_PATH}/latest
CFG_PATH=${RELEASE_PATH}/cfg/
VENV_PATH=${APP_PATH}/venv
PYTHON_PATH=${VENV_PATH}/bin/python

case "$1" in
    uat)
        CFG="${CFG_PATH}/instruments.nix.yml ${CFG_PATH}/oms.uat.yml"
        ;;
    prod)
        CFG="${CFG_PATH}/instruments.nix.yml ${CFG_PATH}/oms.prod.yml"
        ;;
    *)
        echo "Unknown environment:" $1 >&2
        exit 1
        ;;
esac

export PYTHONPATH=${RELEASE_PATH}/src:${RELEASE_PATH}/external/gateway/src:${RELEASE_PATH}/external/messaging/messaging_py:${RELEASE_PATH}/external/smartquant/src
pushd ${RELEASE_PATH} || return
. ${VENV_PATH}/bin/activate
$PYTHON_PATH ${RELEASE_PATH}/src/oms/bootstrap --log-level INFO -c ${CFG}
deactivate
popd || return
