#!/bin/sh

# SPDX-License-Identifier: MPL-2.0

set -e

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
APPS_DIR=$(CDPATH= cd -- "${SCRIPT_DIR}/../.." && pwd)
TEST_DIR_INPUT=${1:-${VSOCK_TEST_DIR:-${SCRIPT_DIR}}}

if [ -d "${TEST_DIR_INPUT}" ]; then
	TEST_DIR=$(CDPATH= cd -- "${TEST_DIR_INPUT}" && pwd)
else
	echo "Unknown test directory: ${TEST_DIR_INPUT}" >&2
	exit 1
fi

case "${TEST_DIR}" in
"${APPS_DIR}"/*)
	TEST_REL_DIR=${TEST_DIR#"${APPS_DIR}/"}
	;;
*)
	echo "Test directory must live under ${APPS_DIR}" >&2
	exit 1
	;;
esac

BUILD_DIR=${BUILD_DIR:-"${SCRIPT_DIR}/../../../../build/vsock_linux"}
BIN_DIR="${BUILD_DIR}/initramfs/test/${TEST_REL_DIR}"
HOST_BIN="${BIN_DIR}/run_host.sh"
GUEST_BIN="${BIN_DIR}/run_guest.sh"

make --no-print-directory -C "${TEST_DIR}" TEST_PLATFORM=linux \
	BUILD_DIR="${BUILD_DIR}"

if [ ! -x "${HOST_BIN}" ] || [ ! -x "${GUEST_BIN}" ]; then
	echo "Missing run_host.sh or run_guest.sh in ${BIN_DIR}" >&2
	exit 1
fi

VSOCK_TEST_BIND_CID="${VSOCK_TEST_BIND_CID:-1}" \
VSOCK_TEST_CONNECT_CID="${VSOCK_TEST_CONNECT_CID:-1}" \
	"${HOST_BIN}" &
HOST_PID=$!
trap 'kill "${HOST_PID}" 2>/dev/null || true; wait "${HOST_PID}" 2>/dev/null || true' EXIT

sleep "${VSOCK_TEST_HOST_START_DELAY_SEC:-0.5}"
VSOCK_TEST_PEER_CID="${VSOCK_TEST_PEER_CID:-1}" "${GUEST_BIN}"

wait "${HOST_PID}"
trap - EXIT
