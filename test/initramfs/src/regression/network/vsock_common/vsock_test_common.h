/* SPDX-License-Identifier: MPL-2.0 */

#ifndef VSOCK_TEST_COMMON_H
#define VSOCK_TEST_COMMON_H

/*
 * Shared definitions and implementation helpers used by both guest and host
 * sides of the vsock test framework.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/**
 * Names the environment variable that selects the control port.
 *
 * Set the same value on both the guest and the host.
 */
#define VSOCK_TEST_ENV_CONTROL_PORT "VSOCK_TEST_CONTROL_PORT"

/**
 * Provides the default control port used by both sides of the framework.
 */
#define VSOCK_TEST_DEFAULT_CONTROL_PORT 25000

/**
 * Describes one supported argument type on the control channel.
 */
enum vsock_test_arg_type {
	VSOCK_TEST_ARG_U32,
	VSOCK_TEST_ARG_SIZE,
	VSOCK_TEST_ARG_STR,
	VSOCK_TEST_ARG_BOOL,
};

#if defined(VSOCK_TEST_GUEST_IMPLEMENTATION) || \
	defined(VSOCK_TEST_HOST_IMPLEMENTATION)

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <linux/vm_sockets.h>

#define __VSOCK_TEST_CONTROL_LINE_LEN 4096

static uint32_t __vsock_test_parse_u32_env(const char *name, uint32_t fallback)
{
	const char *value = getenv(name);
	char *end = NULL;
	unsigned long parsed;

	if (value == NULL) {
		return fallback;
	}

	errno = 0;
	parsed = strtoul(value, &end, 10);
	if (errno != 0 || *end != '\0' || parsed > UINT32_MAX) {
		errno = EINVAL;
		return UINT32_MAX;
	}

	return (uint32_t)parsed;
}

static void __vsock_test_make_addr(struct sockaddr_vm *addr, uint32_t cid,
				   uint32_t port)
{
	memset(addr, 0, sizeof(*addr));
	addr->svm_family = AF_VSOCK;
	addr->svm_cid = cid;
	addr->svm_port = port;
}

static int __vsock_test_write_all(int fd, const void *buf, size_t len)
{
	const char *cursor = buf;

	while (len > 0) {
		ssize_t bytes_written = send(fd, cursor, len, MSG_NOSIGNAL);

		if (bytes_written < 0) {
			if (errno == EINTR) {
				continue;
			}
			return -1;
		}

		cursor += bytes_written;
		len -= (size_t)bytes_written;
	}

	return 0;
}

static int __vsock_test_send_line(int fd, const char *fmt, ...)
{
	char line[__VSOCK_TEST_CONTROL_LINE_LEN];
	va_list args;
	int len;

	va_start(args, fmt);
	len = vsnprintf(line, sizeof(line), fmt, args);
	va_end(args);
	if (len < 0 || (size_t)len >= sizeof(line)) {
		errno = EOVERFLOW;
		return -1;
	}

	return __vsock_test_write_all(fd, line, (size_t)len);
}

static int __vsock_test_read_line(int fd, char *buf, size_t buf_len)
{
	size_t index = 0;

	while (index + 1 < buf_len) {
		char ch;
		ssize_t bytes_read = read(fd, &ch, 1);

		if (bytes_read == 0) {
			if (index == 0) {
				return 0;
			}
			break;
		}
		if (bytes_read < 0) {
			if (errno == EINTR) {
				continue;
			}
			return -1;
		}
		if (ch == '\n') {
			break;
		}

		buf[index++] = ch;
	}

	if (index + 1 == buf_len) {
		errno = EOVERFLOW;
		return -1;
	}

	buf[index] = '\0';
	return 1;
}

#endif

#endif
