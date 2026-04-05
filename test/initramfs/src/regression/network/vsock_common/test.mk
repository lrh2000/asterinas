# SPDX-License-Identifier: MPL-2.0

ifndef VSOCK_TEST_HOST_BIN
$(error Set VSOCK_TEST_HOST_BIN before including vsock_common/test.mk)
endif

ifndef VSOCK_TEST_GUEST_BIN
$(error Set VSOCK_TEST_GUEST_BIN before including vsock_common/test.mk)
endif

TEST_PLATFORM ?= asterinas

EXTRA_TARGETS = $(OBJ_OUTPUT_DIR)/run_guest.sh
SCRIPTS_FILTER := run_host.sh run_guest.sh run_linux_test.sh

ifeq ($(TEST_PLATFORM),asterinas)
C_OBJS_FILTER := $(VSOCK_TEST_HOST_BIN)
else
EXTRA_TARGETS += $(OBJ_OUTPUT_DIR)/run_host.sh
endif

include ../../common/Makefile

$(OBJ_OUTPUT_DIR)/run_host.sh: | $(OBJ_OUTPUT_DIR)
	@printf '%s\n' \
		'#!/bin/sh' \
		'' \
		'# SPDX-License-Identifier: MPL-2.0' \
		'' \
		'set -e' \
		'' \
		'SCRIPT_DIR=$$(CDPATH= cd -- "$$(dirname -- "$$0")" && pwd)' \
		'cd "$$SCRIPT_DIR"' \
		'' \
		'VSOCK_TEST_BIND_CID=$${VSOCK_TEST_BIND_CID:-2} VSOCK_TEST_CONNECT_CID=$${VSOCK_TEST_CONNECT_CID:-3} exec "./$(VSOCK_TEST_HOST_BIN)"' \
		> $@
	@chmod +x $@
	@echo "GEN <= $@"

$(OBJ_OUTPUT_DIR)/run_guest.sh: | $(OBJ_OUTPUT_DIR)
	@printf '%s\n' \
		'#!/bin/sh' \
		'' \
		'# SPDX-License-Identifier: MPL-2.0' \
		'' \
		'set -e' \
		'' \
		'SCRIPT_DIR=$$(CDPATH= cd -- "$$(dirname -- "$$0")" && pwd)' \
		'cd "$$SCRIPT_DIR"' \
		'' \
		'exec "./$(VSOCK_TEST_GUEST_BIN)"' \
		> $@
	@chmod +x $@
	@echo "GEN <= $@"
