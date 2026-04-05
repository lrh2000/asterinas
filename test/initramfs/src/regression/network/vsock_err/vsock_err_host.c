// SPDX-License-Identifier: MPL-2.0

#define _GNU_SOURCE
#define VSOCK_TEST_HOST_IMPLEMENTATION

#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <linux/vm_sockets.h>

#include "vsock_err_scenarios.h"
#include "../vsock_common/vsock_test_host.h"

#define RESET_WAIT_MS 2000
#define HOLD_OPEN_DELAY_US 2000000
#define SHUTDOWN_PHASE_DELAY_US 1000000

static struct sockaddr_vm make_addr(uint32_t cid, uint32_t port)
{
	struct sockaddr_vm addr = { 0 };

	addr.svm_family = AF_VSOCK;
	addr.svm_cid = cid;
	addr.svm_port = port;
	return addr;
}

static int bind_listener(uint32_t port)
{
	int listener = socket(AF_VSOCK, SOCK_STREAM, 0);
	struct sockaddr_vm addr = make_addr(vsock_test_bind_cid(), port);

	if (listener < 0) {
		return -1;
	}
	if (bind(listener, (struct sockaddr *)&addr, sizeof(addr)) < 0 ||
	    listen(listener, 4) < 0) {
		close(listener);
		return -1;
	}

	return listener;
}

static int accept_one(int listener)
{
	for (;;) {
		int accepted = accept(listener, NULL, NULL);

		if (accepted >= 0) {
			return accepted;
		}
		if (errno != EINTR) {
			return -1;
		}
	}
}

static int connect_to_guest(uint32_t port)
{
	int sockfd = socket(AF_VSOCK, SOCK_STREAM, 0);
	struct sockaddr_vm addr = make_addr(vsock_test_connect_cid(), port);

	if (sockfd < 0) {
		return -1;
	}
	if (connect(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		close(sockfd);
		return -1;
	}

	return sockfd;
}

static int drain_socket(int sockfd)
{
	for (;;) {
		char buf[128];
		ssize_t bytes_read = read(sockfd, buf, sizeof(buf));

		if (bytes_read == 0) {
			return 0;
		}
		if (bytes_read < 0) {
			if (errno == EINTR) {
				continue;
			}
			if (errno == ECONNRESET) {
				return 0;
			}
			return -1;
		}
	}
}

static bool socket_looks_disconnected(int sockfd)
{
	struct pollfd pollfd = {
		.fd = sockfd,
		.events = POLLIN | POLLOUT | POLLERR | POLLHUP | POLLRDHUP,
	};

	/*
	 * AF_VSOCK exposes EOF/`EPIPE` with `SO_ERROR == 0` even on an RST.
	 * So this helper only checks that the connection is gone.
	 */

	if (poll(&pollfd, 1, 100) < 0) {
		return false;
	}
	if (pollfd.revents != (POLLIN | POLLOUT | POLLRDHUP)) {
		return false;
	}

	{
		char byte;
		ssize_t bytes_read = read(sockfd, &byte, sizeof(byte));

		if (bytes_read != 0) {
			return false;
		}
	}

	signal(SIGPIPE, SIG_IGN);
	if (write(sockfd, "x", 1) >= 0 || errno != EPIPE) {
		return false;
	}

	return true;
}

static bool third_connect_failed(uint32_t port)
{
	int sockfd = socket(AF_VSOCK, SOCK_STREAM | SOCK_NONBLOCK, 0);
	struct sockaddr_vm addr = make_addr(vsock_test_connect_cid(), port);
	struct pollfd pollfd = {
		.fd = sockfd,
		.events = POLLOUT | POLLERR | POLLHUP | POLLRDHUP,
	};
	int sock_err = 0;
	socklen_t len = sizeof(sock_err);

	if (sockfd < 0) {
		return false;
	}
	if (connect(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0 &&
	    errno != EINPROGRESS) {
		close(sockfd);
		return true;
	}
	if (poll(&pollfd, 1, 1000) < 0) {
		close(sockfd);
		return false;
	}
	if (getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &sock_err, &len) < 0) {
		close(sockfd);
		return false;
	}

	close(sockfd);
	return sock_err != 0;
}

VSOCK_HOST_SCENARIO(echo)
{
	VSOCK_HOST_BIND_ARGS(echo);
	int listener = -1;
	int accepted = -1;
	int ret = -1;

	listener = bind_listener(port);
	if (listener < 0) {
		goto out;
	}

	accepted = accept_one(listener);
	if (accepted < 0) {
		goto out;
	}

	for (;;) {
		char buf[4096];
		ssize_t bytes_read = read(accepted, buf, sizeof(buf));

		if (bytes_read == 0) {
			ret = 0;
			goto out;
		}
		if (bytes_read < 0) {
			if (errno == EINTR) {
				continue;
			}
			goto out;
		}
		if (write(accepted, buf, (size_t)bytes_read) != bytes_read) {
			goto out;
		}
	}

out:
	if (accepted >= 0) {
		close(accepted);
	}
	if (listener >= 0) {
		close(listener);
	}
	return ret;
}

VSOCK_HOST_SCENARIO(hold)
{
	VSOCK_HOST_BIND_ARGS(hold);
	int listener = -1;
	int accepted = -1;
	int ret = -1;

	listener = bind_listener(port);
	if (listener < 0) {
		goto out;
	}

	accepted = accept_one(listener);
	if (accepted < 0) {
		goto out;
	}

	usleep(HOLD_OPEN_DELAY_US);
	ret = 0;

out:
	if (accepted >= 0) {
		close(accepted);
	}
	if (listener >= 0) {
		close(listener);
	}
	return ret;
}

VSOCK_HOST_SCENARIO(send_shutdown)
{
	VSOCK_HOST_BIND_ARGS(send_shutdown);
	int listener = -1;
	int accepted = -1;
	int ret = -1;

	listener = bind_listener(port);
	if (listener < 0) {
		goto out;
	}

	accepted = accept_one(listener);
	if (accepted < 0) {
		goto out;
	}

	if (write(accepted, payload, strlen(payload)) !=
	    (ssize_t)strlen(payload)) {
		goto out;
	}
	if (shutdown(accepted, SHUT_WR) < 0) {
		goto out;
	}

	ret = drain_socket(accepted);

out:
	if (accepted >= 0) {
		close(accepted);
	}
	if (listener >= 0) {
		close(listener);
	}
	return ret;
}

VSOCK_HOST_SCENARIO(shutdown_read)
{
	VSOCK_HOST_BIND_ARGS(shutdown_read);
	int listener = -1;
	int accepted = -1;
	int ret = -1;

	listener = bind_listener(port);
	if (listener < 0) {
		goto out;
	}

	accepted = accept_one(listener);
	if (accepted < 0) {
		goto out;
	}
	if (shutdown(accepted, SHUT_RD) < 0) {
		goto out;
	}
	usleep(SHUTDOWN_PHASE_DELAY_US);
	if (shutdown(accepted, SHUT_WR) < 0) {
		goto out;
	}
	usleep(SHUTDOWN_PHASE_DELAY_US);

	ret = 0;

out:
	if (accepted >= 0) {
		close(accepted);
	}
	if (listener >= 0) {
		close(listener);
	}
	return ret;
}

VSOCK_HOST_SCENARIO(connect_addr)
{
	VSOCK_HOST_BIND_ARGS(connect_addr);
	int sockfd = -1;
	struct sockaddr_vm addr = { 0 };
	socklen_t addr_len = sizeof(addr);
	char line[64];
	int line_len;
	int ret = -1;

	sockfd = connect_to_guest(port);
	if (sockfd < 0) {
		goto out;
	}
	if (getsockname(sockfd, (struct sockaddr *)&addr, &addr_len) < 0) {
		goto out;
	}

	line_len = snprintf(line, sizeof(line), "ADDR %u %u\n", addr.svm_cid,
			    addr.svm_port);
	if (line_len < 0 || (size_t)line_len >= sizeof(line)) {
		errno = EOVERFLOW;
		goto out;
	}
	if (write(sockfd, line, (size_t)line_len) != line_len) {
		goto out;
	}

	ret = drain_socket(sockfd);

out:
	if (sockfd >= 0) {
		close(sockfd);
	}
	return ret;
}

VSOCK_HOST_SCENARIO(connect_expect_disconnect)
{
	VSOCK_HOST_BIND_ARGS(connect_expect_disconnect);
	int sockfd = -1;
	int elapsed_ms = 0;
	int ret = -1;

	sockfd = connect_to_guest(port);
	if (sockfd < 0) {
		goto out;
	}

	while (elapsed_ms <= RESET_WAIT_MS) {
		if (socket_looks_disconnected(sockfd)) {
			ret = 0;
			goto out;
		}

		usleep(100 * 1000);
		elapsed_ms += 100;
	}

	errno = ETIMEDOUT;

out:
	if (sockfd >= 0) {
		close(sockfd);
	}
	return ret;
}

VSOCK_HOST_SCENARIO(fill_backlog)
{
	VSOCK_HOST_BIND_ARGS(fill_backlog);
	int first_sockfd = -1;
	int second_sockfd = -1;
	int ret = -1;

	first_sockfd = connect_to_guest(port);
	if (first_sockfd < 0) {
		goto out;
	}
	second_sockfd = connect_to_guest(port);
	if (first_sockfd < 0) {
		goto out;
	}
	if (!third_connect_failed(port)) {
		errno = EPROTO;
		goto out;
	}

	ret = 0;

out:
	if (second_sockfd >= 0) {
		close(second_sockfd);
	}
	if (first_sockfd >= 0) {
		close(first_sockfd);
	}
	return ret;
}
