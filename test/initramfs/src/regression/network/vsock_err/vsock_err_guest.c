// SPDX-License-Identifier: MPL-2.0

#define _GNU_SOURCE
#define VSOCK_TEST_GUEST_IMPLEMENTATION

#include "../../common/test.h"

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <linux/vm_sockets.h>

#include "vsock_err_scenarios.h"
#include "../vsock_common/vsock_test_guest.h"

#define BOUND_PORT 25100
#define LISTEN_PORT 25101
#define COLLIDE_PORT 25102
#define AUTO_BIND_PORT VMADDR_PORT_ANY
#define INVALID_CID_PORT 25103

#define ECHO_PORT 25110
#define HOLD_PORT 25111
#define SEND_SHUTDOWN_PORT 25112
#define SHUTDOWN_READ_PORT 25113
#define GUEST_ACCEPT_PORT 25114
#define GUEST_RESET_PORT 25115
#define GUEST_BACKLOG_PORT 25116
#define REFUSED_PORT 25999

#define CONNECT_DELAY_US 200000
#define SHUTDOWN_PHASE_DELAY_US 1000000
#define PEER_SHUTDOWN_PAYLOAD "peer-shutdown"

#define POLL_SUPPORTED_EVENTS (POLLIN | POLLOUT | POLLERR | POLLHUP | POLLRDHUP)

#define POLL_CONNECTED POLLOUT
#define POLL_LOCAL_SHUT_RD (POLLIN | POLLOUT | POLLRDHUP)
#define POLL_LOCAL_SHUT_WR 0
#define POLL_LOCAL_SHUT_RDWR (POLLIN | POLLRDHUP | POLLHUP)
#define POLL_LOCAL_PEER_SHUT_RD (POLLIN | POLLOUT | POLLRDHUP)
#define POLL_LOCAL_PEER_SHUT_WR (POLLIN | POLLRDHUP | POLLHUP)
#define POLL_LOCAL_PEER_SHUT_RDWR (POLLIN | POLLRDHUP | POLLHUP)
#define POLL_PEER_SHUT_WR (POLLIN | POLLOUT | POLLRDHUP)
#define POLL_PEER_SHUT_RD POLLOUT
#define POLL_PEER_SHUT_RDWR (POLLIN | POLLOUT | POLLRDHUP)
#define POLL_CONNECT_REFUSED (POLLOUT | POLLERR)
#define POLL_CONNECT_REFUSED_FINI (POLLIN | POLLRDHUP | POLLHUP)

static struct sockaddr_vm make_addr(uint32_t cid, uint32_t port)
{
	struct sockaddr_vm addr = { 0 };

	addr.svm_family = AF_VSOCK;
	addr.svm_cid = cid;
	addr.svm_port = port;
	return addr;
}

static int new_socket(int flags)
{
	return socket(AF_VSOCK, SOCK_STREAM | flags, 0);
}

static int bind_socket(int flags, uint32_t port)
{
	int sockfd = new_socket(flags);
	struct sockaddr_vm addr = make_addr(VMADDR_CID_ANY, port);

	if (sockfd < 0) {
		return -1;
	}
	if (bind(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		close(sockfd);
		return -1;
	}

	return sockfd;
}

static int bind_listener(int flags, uint32_t port, int backlog)
{
	int listener = bind_socket(flags, port);

	if (listener < 0) {
		return -1;
	}
	if (listen(listener, backlog) < 0) {
		close(listener);
		return -1;
	}

	return listener;
}

static int connect_to_peer(uint32_t port, int flags)
{
	int sockfd = new_socket(flags);
	struct sockaddr_vm addr = make_addr(vsock_test_peer_cid(), port);

	if (sockfd < 0) {
		return -1;
	}

	usleep(CONNECT_DELAY_US);
	if (connect(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		if ((flags & SOCK_NONBLOCK) != 0 && errno == EINPROGRESS) {
			return sockfd;
		}

		close(sockfd);
		return -1;
	}

	return sockfd;
}

static int poll_revents(int fd, int timeout_ms)
{
	struct pollfd pollfd = {
		.fd = fd,
		.events = POLL_SUPPORTED_EVENTS,
	};

	if (poll(&pollfd, 1, timeout_ms) < 0) {
		return -1;
	}

	return pollfd.revents & POLL_SUPPORTED_EVENTS;
}

static int set_nonblocking(int fd)
{
	int flags = fcntl(fd, F_GETFL);

	if (flags < 0) {
		return -1;
	}

	return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int read_addr_line(int sockfd, uint32_t *cid, uint32_t *port)
{
	char line[64] = { 0 };
	unsigned int parsed_cid = 0;
	unsigned int parsed_port = 0;
	ssize_t bytes_read = read(sockfd, line, sizeof(line) - 1);

	if (bytes_read <= 0) {
		return -1;
	}
	if (sscanf(line, "ADDR %u %u", &parsed_cid, &parsed_port) != 2) {
		errno = EPROTO;
		return -1;
	}

	*cid = parsed_cid;
	*port = parsed_port;
	return 0;
}

FN_SETUP(vsock_init)
{
	CHECK(signal(SIGPIPE, SIG_IGN) != SIG_ERR);
	CHECK(vsock_test_guest_init());
}
END_SETUP()

/* Unconnected sockets: names, I/O, shutdown, and poll. */

FN_TEST(socket_names_before_connect)
{
	int unbound = TEST_SUCC(new_socket(SOCK_NONBLOCK));
	int bound = TEST_SUCC(bind_socket(SOCK_NONBLOCK, BOUND_PORT));
	int listener = TEST_SUCC(bind_listener(SOCK_NONBLOCK, LISTEN_PORT, 1));
	struct sockaddr_vm name = { 0 };
	socklen_t addr_len = sizeof(name);

	addr_len = sizeof(name);
	TEST_RES(getsockname(unbound, (struct sockaddr *)&name, &addr_len),
		 addr_len == sizeof(name) && name.svm_family == AF_VSOCK &&
			 name.svm_cid == VMADDR_CID_ANY &&
			 name.svm_port == VMADDR_PORT_ANY);

	addr_len = sizeof(name);
	TEST_RES(getsockname(bound, (struct sockaddr *)&name, &addr_len),
		 addr_len == sizeof(name) && name.svm_family == AF_VSOCK &&
			 name.svm_port == BOUND_PORT);

	addr_len = sizeof(name);
	TEST_RES(getsockname(listener, (struct sockaddr *)&name, &addr_len),
		 addr_len == sizeof(name) && name.svm_family == AF_VSOCK &&
			 name.svm_port == LISTEN_PORT);

	addr_len = sizeof(name);
	TEST_ERRNO(getpeername(unbound, (struct sockaddr *)&name, &addr_len),
		   ENOTCONN);
	addr_len = sizeof(name);
	TEST_ERRNO(getpeername(bound, (struct sockaddr *)&name, &addr_len),
		   ENOTCONN);
	addr_len = sizeof(name);
	TEST_ERRNO(getpeername(listener, (struct sockaddr *)&name, &addr_len),
		   ENOTCONN);

	TEST_SUCC(close(listener));
	TEST_SUCC(close(bound));
	TEST_SUCC(close(unbound));
}
END_TEST()

FN_TEST(send_and_recv_before_connect)
{
	int unbound = TEST_SUCC(new_socket(SOCK_NONBLOCK));
	int bound = TEST_SUCC(bind_socket(SOCK_NONBLOCK, BOUND_PORT + 1));
	int listener =
		TEST_SUCC(bind_listener(SOCK_NONBLOCK, LISTEN_PORT + 1, 1));
	char byte = 'z';

	TEST_ERRNO(send(unbound, &byte, 1, 0), ENOTCONN);
	TEST_ERRNO(send(unbound, &byte, 0, 0), ENOTCONN);
	TEST_ERRNO(write(unbound, &byte, 1), ENOTCONN);
	TEST_ERRNO(write(unbound, &byte, 0), ENOTCONN);
	TEST_ERRNO(recv(unbound, &byte, 1, 0), ENOTCONN);
	TEST_ERRNO(recv(unbound, &byte, 0, 0), ENOTCONN);
	TEST_ERRNO(read(unbound, &byte, 1), ENOTCONN);
	TEST_RES(read(unbound, &byte, 0), _ret == 0);

	TEST_ERRNO(send(bound, &byte, 1, 0), ENOTCONN);
	TEST_ERRNO(send(bound, &byte, 0, 0), ENOTCONN);
	TEST_ERRNO(write(bound, &byte, 1), ENOTCONN);
	TEST_ERRNO(write(bound, &byte, 0), ENOTCONN);
	TEST_ERRNO(recv(bound, &byte, 1, 0), ENOTCONN);
	TEST_ERRNO(recv(bound, &byte, 0, 0), ENOTCONN);
	TEST_ERRNO(read(bound, &byte, 1), ENOTCONN);
	TEST_RES(read(bound, &byte, 0), _ret == 0);

	TEST_ERRNO(send(listener, &byte, 1, 0), ENOTCONN);
	TEST_ERRNO(send(listener, &byte, 0, 0), ENOTCONN);
	TEST_ERRNO(write(listener, &byte, 1), ENOTCONN);
	TEST_ERRNO(write(listener, &byte, 0), ENOTCONN);
	TEST_ERRNO(recv(listener, &byte, 1, 0), ENOTCONN);
	TEST_ERRNO(recv(listener, &byte, 0, 0), ENOTCONN);
	TEST_ERRNO(read(listener, &byte, 1), ENOTCONN);
	TEST_RES(read(listener, &byte, 0), _ret == 0);

	TEST_SUCC(close(listener));
	TEST_SUCC(close(bound));
	TEST_SUCC(close(unbound));
}
END_TEST()

FN_TEST(shutdown_before_connect)
{
	int unbound = TEST_SUCC(new_socket(SOCK_NONBLOCK));
	int bound = TEST_SUCC(bind_socket(SOCK_NONBLOCK, BOUND_PORT + 2));
	int listener =
		TEST_SUCC(bind_listener(SOCK_NONBLOCK, LISTEN_PORT + 2, 1));

	TEST_ERRNO(shutdown(unbound, SHUT_RD), ENOTCONN);
	TEST_ERRNO(shutdown(unbound, SHUT_WR), ENOTCONN);
	TEST_ERRNO(shutdown(unbound, SHUT_RDWR), ENOTCONN);

	TEST_ERRNO(shutdown(bound, SHUT_RD), ENOTCONN);
	TEST_ERRNO(shutdown(bound, SHUT_WR), ENOTCONN);
	TEST_ERRNO(shutdown(bound, SHUT_RDWR), ENOTCONN);

	TEST_ERRNO(shutdown(listener, SHUT_RD), ENOTCONN);
	TEST_ERRNO(shutdown(listener, SHUT_WR), ENOTCONN);
	TEST_ERRNO(shutdown(listener, SHUT_RDWR), ENOTCONN);

	TEST_SUCC(close(listener));
	TEST_SUCC(close(bound));
	TEST_SUCC(close(unbound));
}
END_TEST()

FN_TEST(sendmsg_name_errors)
{
	int unbound = TEST_SUCC(new_socket(0));
	int listener = TEST_SUCC(bind_listener(0, LISTEN_PORT + 3, 1));
	struct vsock_test_scenario_handle handle;
	int connected;
	struct sockaddr_vm peer_addr =
		make_addr(vsock_test_peer_cid(), ECHO_PORT);
	char byte = 'q';
	struct iovec iov = {
		.iov_base = &byte,
		.iov_len = sizeof(byte),
	};
	struct msghdr msg = {
		.msg_name = &peer_addr,
		.msg_namelen = sizeof(peer_addr),
		.msg_iov = &iov,
		.msg_iovlen = 1,
	};

	TEST_ERRNO(sendmsg(unbound, &msg, 0), EOPNOTSUPP);
	TEST_ERRNO(sendmsg(listener, &msg, 0), EOPNOTSUPP);

	TEST_SUCC(VSOCK_TEST_START(echo, &handle, .port = ECHO_PORT));
	connected = TEST_SUCC(connect_to_peer(ECHO_PORT, 0));
	TEST_ERRNO(sendmsg(connected, &msg, 0), EISCONN);

	TEST_SUCC(close(connected));
	TEST_SUCC(vsock_test_wait(handle));
	TEST_SUCC(close(listener));
	TEST_SUCC(close(unbound));
}
END_TEST()

FN_TEST(bind_errors)
{
	int collide_first = TEST_SUCC(new_socket(0));
	int collide_second = TEST_SUCC(new_socket(0));
	int auto_bind = TEST_SUCC(new_socket(0));
	int invalid_cid = TEST_SUCC(new_socket(0));
	int short_addr = TEST_SUCC(new_socket(0));
	struct sockaddr_vm addr = make_addr(VMADDR_CID_ANY, COLLIDE_PORT);
	struct sockaddr_vm auto_name = { 0 };
	socklen_t addr_len = sizeof(auto_name);

	TEST_ERRNO(bind(short_addr, (struct sockaddr *)&addr, sizeof(addr) - 1),
		   EINVAL);

	TEST_SUCC(bind(collide_first, (struct sockaddr *)&addr, sizeof(addr)));
	TEST_ERRNO(bind(collide_first, (struct sockaddr *)&addr, sizeof(addr)),
		   EINVAL);
	TEST_ERRNO(bind(collide_second, (struct sockaddr *)&addr, sizeof(addr)),
		   EADDRINUSE);

	addr = make_addr(0x12345678, INVALID_CID_PORT);
	TEST_ERRNO(bind(invalid_cid, (struct sockaddr *)&addr, sizeof(addr)),
		   EADDRNOTAVAIL);

	addr = make_addr(VMADDR_CID_ANY, AUTO_BIND_PORT);
	TEST_SUCC(bind(auto_bind, (struct sockaddr *)&addr, sizeof(addr)));
	TEST_RES(getsockname(auto_bind, (struct sockaddr *)&auto_name,
			     &addr_len),
		 addr_len == sizeof(auto_name) &&
			 auto_name.svm_family == AF_VSOCK &&
			 auto_name.svm_port != VMADDR_PORT_ANY);

	TEST_SUCC(close(short_addr));
	TEST_SUCC(close(invalid_cid));
	TEST_SUCC(close(auto_bind));
	TEST_SUCC(close(collide_second));
	TEST_SUCC(close(collide_first));
}
END_TEST()

FN_TEST(listen_accept_and_poll_on_unconnected_sockets)
{
	int unbound = TEST_SUCC(new_socket(SOCK_NONBLOCK));
	int bound = TEST_SUCC(bind_socket(SOCK_NONBLOCK, BOUND_PORT + 3));
	int listener = TEST_SUCC(bind_socket(SOCK_NONBLOCK, LISTEN_PORT + 4));
	struct sockaddr_vm peer_addr =
		make_addr(vsock_test_peer_cid(), REFUSED_PORT);

	TEST_ERRNO(listen(unbound, 1), EINVAL);
	TEST_SUCC(listen(listener, 1));
	TEST_SUCC(listen(listener, 2));

	TEST_ERRNO(accept(unbound, NULL, NULL), EINVAL);
	TEST_ERRNO(accept(bound, NULL, NULL), EINVAL);
	TEST_ERRNO(accept(listener, NULL, NULL), EAGAIN);

	TEST_ERRNO(connect(listener, (struct sockaddr *)&peer_addr,
			   sizeof(peer_addr)),
		   EINVAL);

	TEST_RES(poll_revents(unbound, 0), _ret == POLLOUT);
	TEST_RES(poll_revents(bound, 0), _ret == POLLOUT);
	TEST_RES(poll_revents(listener, 0), _ret == 0);

	TEST_SUCC(close(listener));
	TEST_SUCC(close(bound));
	TEST_SUCC(close(unbound));
}
END_TEST()

/* Connected sockets: connect failure, API state checks, shutdown, and poll. */

FN_TEST(async_connect_refused_so_error)
{
	int sockfd = TEST_SUCC(new_socket(SOCK_NONBLOCK));
	struct sockaddr_vm addr =
		make_addr(vsock_test_peer_cid(), REFUSED_PORT);
	char byte = 'a';
	int sock_err = 0;
	socklen_t opt_len = sizeof(sock_err);

	TEST_ERRNO(connect(sockfd, (struct sockaddr *)&addr, sizeof(addr)),
		   EINPROGRESS);
	TEST_RES(poll_revents(sockfd, 1000), _ret == POLL_CONNECT_REFUSED);

	TEST_ERRNO(getpeername(sockfd, (struct sockaddr *)&addr, &opt_len),
		   ENOTCONN);

	TEST_ERRNO(bind(sockfd, (struct sockaddr *)&addr, sizeof(addr)),
		   EINVAL);
	TEST_ERRNO(listen(sockfd, 1), EINVAL);
	TEST_ERRNO(accept(sockfd, NULL, NULL), EINVAL);
	TEST_ERRNO(connect(sockfd, (struct sockaddr *)&addr, sizeof(addr)),
		   EALREADY);

	TEST_ERRNO(write(sockfd, &byte, 1), ENOTCONN);
	TEST_ERRNO(read(sockfd, &byte, 1), ENOTCONN);

	TEST_SUCC(shutdown(sockfd, SHUT_RD));
	TEST_SUCC(shutdown(sockfd, SHUT_WR));
	TEST_SUCC(shutdown(sockfd, SHUT_RDWR));

	TEST_RES(getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &sock_err, &opt_len),
		 opt_len == sizeof(sock_err) && sock_err == ECONNRESET);
	TEST_RES(getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &sock_err, &opt_len),
		 opt_len == sizeof(sock_err) && sock_err == 0);

	TEST_RES(poll_revents(sockfd, 1000), _ret == POLL_CONNECT_REFUSED_FINI);

	TEST_SUCC(close(sockfd));
}
END_TEST()

FN_TEST(connected_socket_api)
{
	struct vsock_test_scenario_handle handle;
	int sockfd;
	struct sockaddr_vm local_addr = { 0 };
	struct sockaddr_vm peer_addr = { 0 };
	socklen_t addr_len = sizeof(local_addr);
	char byte = 'a';

	TEST_SUCC(VSOCK_TEST_START(echo, &handle, .port = ECHO_PORT));
	sockfd = TEST_SUCC(connect_to_peer(ECHO_PORT, 0));

	addr_len = sizeof(local_addr);
	TEST_RES(getsockname(sockfd, (struct sockaddr *)&local_addr, &addr_len),
		 addr_len == sizeof(local_addr) &&
			 local_addr.svm_family == AF_VSOCK &&
			 local_addr.svm_port != VMADDR_PORT_ANY);

	addr_len = sizeof(peer_addr);
	TEST_RES(getpeername(sockfd, (struct sockaddr *)&peer_addr, &addr_len),
		 addr_len == sizeof(peer_addr) &&
			 peer_addr.svm_family == AF_VSOCK &&
			 peer_addr.svm_cid == vsock_test_peer_cid() &&
			 peer_addr.svm_port == ECHO_PORT);

	TEST_RES(poll_revents(sockfd, 0), _ret == POLL_CONNECTED);

	TEST_ERRNO(bind(sockfd, (struct sockaddr *)&peer_addr,
			sizeof(peer_addr)),
		   EINVAL);
	TEST_ERRNO(listen(sockfd, 1), EINVAL);
	TEST_ERRNO(accept(sockfd, NULL, NULL), EINVAL);
	TEST_ERRNO(connect(sockfd, (struct sockaddr *)&peer_addr,
			   sizeof(peer_addr)),
		   EISCONN);

	TEST_RES(read(sockfd, &byte, 0), _ret == 0);
	TEST_RES(recv(sockfd, &byte, 0, 0), _ret == 0);
	TEST_RES(write(sockfd, &byte, 0), _ret == 0);
	TEST_RES(send(sockfd, &byte, 0, 0), _ret == 0);

	TEST_RES(write(sockfd, &byte, sizeof(byte)), _ret == sizeof(byte));
	byte = '\0';
	TEST_RES(read(sockfd, &byte, sizeof(byte)),
		 _ret == sizeof(byte) && byte == 'a');

	TEST_SUCC(close(sockfd));
	TEST_SUCC(vsock_test_wait(handle));
}
END_TEST()

FN_TEST(local_shutdown_write_and_poll)
{
	struct vsock_test_scenario_handle handle;
	int sockfd;
	char byte = 'x';

	TEST_SUCC(VSOCK_TEST_START(hold, &handle, .port = HOLD_PORT));
	sockfd = TEST_SUCC(connect_to_peer(HOLD_PORT, 0));

	TEST_RES(poll_revents(sockfd, 0), _ret == POLL_CONNECTED);
	TEST_SUCC(shutdown(sockfd, SHUT_WR));
	TEST_RES(poll_revents(sockfd, 0), _ret == POLL_LOCAL_SHUT_WR);
	TEST_ERRNO(write(sockfd, &byte, 0), EPIPE);
	TEST_SUCC(set_nonblocking(sockfd));
	TEST_ERRNO(read(sockfd, &byte, sizeof(byte)), EAGAIN);

	TEST_SUCC(close(sockfd));
	TEST_SUCC(vsock_test_wait(handle));
}
END_TEST()

FN_TEST(local_shutdown_read_then_write_and_poll)
{
	struct vsock_test_scenario_handle handle;
	int sockfd;
	char byte = 'x';

	TEST_SUCC(VSOCK_TEST_START(hold, &handle, .port = HOLD_PORT + 1));
	sockfd = TEST_SUCC(connect_to_peer(HOLD_PORT + 1, 0));

	TEST_RES(poll_revents(sockfd, 0), _ret == POLL_CONNECTED);
	TEST_SUCC(shutdown(sockfd, SHUT_RD));
	TEST_RES(poll_revents(sockfd, 0), _ret == POLL_LOCAL_SHUT_RD);
	TEST_RES(read(sockfd, &byte, sizeof(byte)), _ret == 0);
	TEST_RES(write(sockfd, &byte, sizeof(byte)), _ret == sizeof(byte));

	TEST_SUCC(shutdown(sockfd, SHUT_WR));
	TEST_RES(poll_revents(sockfd, 0), _ret == POLL_LOCAL_SHUT_RDWR);
	TEST_ERRNO(write(sockfd, &byte, 0), EPIPE);

	TEST_SUCC(close(sockfd));
	TEST_SUCC(vsock_test_wait(handle));
}
END_TEST()

FN_TEST(peer_shutdown_write_and_poll)
{
	struct vsock_test_scenario_handle handle;
	int sockfd;
	char buf[sizeof(PEER_SHUTDOWN_PAYLOAD)] = { 0 };

	TEST_SUCC(VSOCK_TEST_START(send_shutdown, &handle,
				   .port = SEND_SHUTDOWN_PORT,
				   .payload = PEER_SHUTDOWN_PAYLOAD));
	sockfd = TEST_SUCC(connect_to_peer(SEND_SHUTDOWN_PORT, 0));

	/*
	 * The peer sends payload and then shuts down write immediately. Give the
	 * shutdown notification time to arrive before asserting the exact poll
	 * mask, otherwise the first wakeup may observe only the readable payload.
	 */
	usleep(CONNECT_DELAY_US);
	TEST_RES(poll_revents(sockfd, 1000), _ret == POLL_PEER_SHUT_WR);
	TEST_RES(read(sockfd, buf, strlen(PEER_SHUTDOWN_PAYLOAD)),
		 (size_t)_ret == strlen(PEER_SHUTDOWN_PAYLOAD) &&
			 memcmp(buf, PEER_SHUTDOWN_PAYLOAD,
				strlen(PEER_SHUTDOWN_PAYLOAD)) == 0);
	TEST_RES(poll_revents(sockfd, 0), _ret == POLL_PEER_SHUT_WR);
	TEST_RES(recv(sockfd, buf, sizeof(buf), 0), _ret == 0);

	TEST_SUCC(close(sockfd));
	TEST_SUCC(vsock_test_wait(handle));
}
END_TEST()

FN_TEST(peer_shutdown_read_then_write_and_poll)
{
	struct vsock_test_scenario_handle handle;
	int sockfd;
	char byte = 'x';

	TEST_SUCC(VSOCK_TEST_START(shutdown_read, &handle,
				   .port = SHUTDOWN_READ_PORT));
	sockfd = TEST_SUCC(connect_to_peer(SHUTDOWN_READ_PORT, 0));

	usleep(CONNECT_DELAY_US);
	TEST_RES(poll_revents(sockfd, 1000), _ret == POLL_PEER_SHUT_RD);
	TEST_ERRNO(write(sockfd, &byte, 0), EPIPE);
	TEST_SUCC(set_nonblocking(sockfd));
	TEST_ERRNO(read(sockfd, &byte, sizeof(byte)), EAGAIN);

	usleep(SHUTDOWN_PHASE_DELAY_US + CONNECT_DELAY_US);
	TEST_RES(poll_revents(sockfd, 1000), _ret == POLL_PEER_SHUT_RDWR);
	TEST_ERRNO(write(sockfd, &byte, 0), EPIPE);
	TEST_RES(read(sockfd, &byte, sizeof(byte)), _ret == 0);

	TEST_SUCC(close(sockfd));
	TEST_SUCC(vsock_test_wait(handle));
}
END_TEST()

FN_TEST(local_and_peer_shutdown_write_poll)
{
	struct vsock_test_scenario_handle handle;
	int sockfd;
	char byte = 'x';
	char buf[sizeof(PEER_SHUTDOWN_PAYLOAD)] = { 0 };

	TEST_SUCC(VSOCK_TEST_START(send_shutdown, &handle,
				   .port = SEND_SHUTDOWN_PORT + 1,
				   .payload = PEER_SHUTDOWN_PAYLOAD));
	sockfd = TEST_SUCC(connect_to_peer(SEND_SHUTDOWN_PORT + 1, 0));

	usleep(CONNECT_DELAY_US);
	TEST_RES(read(sockfd, buf, strlen(PEER_SHUTDOWN_PAYLOAD)),
		 (size_t)_ret == strlen(PEER_SHUTDOWN_PAYLOAD) &&
			 memcmp(buf, PEER_SHUTDOWN_PAYLOAD,
				strlen(PEER_SHUTDOWN_PAYLOAD)) == 0);
	TEST_SUCC(shutdown(sockfd, SHUT_WR));
	TEST_RES(poll_revents(sockfd, 1000), _ret == POLL_LOCAL_PEER_SHUT_WR);
	TEST_ERRNO(write(sockfd, &byte, 0), EPIPE);
	TEST_RES(read(sockfd, &byte, sizeof(byte)), _ret == 0);

	TEST_SUCC(close(sockfd));
	TEST_SUCC(vsock_test_wait(handle));
}
END_TEST()

FN_TEST(local_and_peer_shutdown_read_poll)
{
	struct vsock_test_scenario_handle handle;
	int sockfd;
	char byte = 'x';

	TEST_SUCC(VSOCK_TEST_START(shutdown_read, &handle,
				   .port = SHUTDOWN_READ_PORT + 1));
	sockfd = TEST_SUCC(connect_to_peer(SHUTDOWN_READ_PORT + 1, 0));

	TEST_SUCC(shutdown(sockfd, SHUT_RD));
	usleep(CONNECT_DELAY_US);
	TEST_RES(poll_revents(sockfd, 1000), _ret == POLL_LOCAL_PEER_SHUT_RD);
	TEST_RES(read(sockfd, &byte, sizeof(byte)), _ret == 0);
	TEST_ERRNO(write(sockfd, &byte, 0), EPIPE);

	usleep(SHUTDOWN_PHASE_DELAY_US + CONNECT_DELAY_US);
	TEST_SUCC(close(sockfd));
	TEST_SUCC(vsock_test_wait(handle));
}
END_TEST()

FN_TEST(local_and_peer_shutdown_read_write_poll)
{
	struct vsock_test_scenario_handle handle;
	int sockfd;
	char byte = 'x';

	TEST_SUCC(VSOCK_TEST_START(shutdown_read, &handle,
				   .port = SHUTDOWN_READ_PORT + 2));
	sockfd = TEST_SUCC(connect_to_peer(SHUTDOWN_READ_PORT + 2, 0));

	TEST_SUCC(shutdown(sockfd, SHUT_RDWR));
	usleep(SHUTDOWN_PHASE_DELAY_US + CONNECT_DELAY_US);

	TEST_RES(poll_revents(sockfd, 1000), _ret == POLL_LOCAL_PEER_SHUT_RDWR);
	TEST_ERRNO(write(sockfd, &byte, 0), EPIPE);
	TEST_RES(read(sockfd, &byte, sizeof(byte)), _ret == 0);

	TEST_SUCC(close(sockfd));
	TEST_SUCC(vsock_test_wait(handle));
}
END_TEST()

/* Guest listeners accepted by host-initiated connections. */

FN_TEST(accepted_socket_api)
{
	int listener =
		TEST_SUCC(bind_listener(SOCK_NONBLOCK, GUEST_ACCEPT_PORT, 1));
	int accepted;
	struct vsock_test_scenario_handle handle;
	struct sockaddr_vm local_addr = { 0 };
	struct sockaddr_vm peer_addr = { 0 };
	socklen_t addr_len = sizeof(local_addr);
	uint32_t reported_cid = 0;
	uint32_t expected_port = 0;

	addr_len = sizeof(peer_addr);
	TEST_ERRNO(getpeername(listener, (struct sockaddr *)&peer_addr,
			       &addr_len),
		   ENOTCONN);

	TEST_SUCC(VSOCK_TEST_START(connect_addr, &handle,
				   .port = GUEST_ACCEPT_PORT));
	TEST_RES(poll_revents(listener, 1000), _ret == POLLIN);

	accepted = TEST_SUCC(accept(listener, NULL, NULL));

	addr_len = sizeof(local_addr);
	TEST_RES(getsockname(accepted, (struct sockaddr *)&local_addr,
			     &addr_len),
		 addr_len == sizeof(local_addr) &&
			 local_addr.svm_family == AF_VSOCK &&
			 local_addr.svm_port == GUEST_ACCEPT_PORT);

	TEST_RES(read_addr_line(accepted, &reported_cid, &expected_port),
		 reported_cid == VMADDR_CID_ANY);

	addr_len = sizeof(peer_addr);
	TEST_RES(getpeername(accepted, (struct sockaddr *)&peer_addr,
			     &addr_len),
		 addr_len == sizeof(peer_addr) &&
			 peer_addr.svm_family == AF_VSOCK &&
			 peer_addr.svm_cid == vsock_test_peer_cid() &&
			 peer_addr.svm_port == expected_port);

	TEST_RES(poll_revents(accepted, 0), _ret == POLL_CONNECTED);

	TEST_ERRNO(bind(accepted, (struct sockaddr *)&peer_addr,
			sizeof(peer_addr)),
		   EINVAL);
	TEST_ERRNO(listen(accepted, 1), EINVAL);
	TEST_ERRNO(accept(accepted, NULL, NULL), EINVAL);
	TEST_ERRNO(connect(accepted, (struct sockaddr *)&peer_addr,
			   sizeof(peer_addr)),
		   EISCONN);

	TEST_SUCC(close(accepted));
	TEST_SUCC(close(listener));
	TEST_SUCC(vsock_test_wait(handle));
}
END_TEST()

FN_TEST(listener_close_drops_pending_connection)
{
	int listener =
		TEST_SUCC(bind_listener(SOCK_NONBLOCK, GUEST_RESET_PORT, 1));
	struct vsock_test_scenario_handle handle;

	/*
	 * AF_VSOCK exposes EOF/`EPIPE` with `SO_ERROR == 0` even on an RST.
	 * So this test only asserts that the pending connection disappears.
	 */
	TEST_SUCC(VSOCK_TEST_START(connect_expect_disconnect, &handle,
				   .port = GUEST_RESET_PORT));
	TEST_RES(poll_revents(listener, 1000), _ret == POLLIN);

	TEST_SUCC(close(listener));
	TEST_SUCC(vsock_test_wait(handle));
}
END_TEST()

FN_TEST(backlog_full_rejects_third_connection)
{
	int listener = TEST_SUCC(bind_listener(0, GUEST_BACKLOG_PORT, 1));
	struct vsock_test_scenario_handle handle;

	TEST_SUCC(VSOCK_TEST_START(fill_backlog, &handle,
				   .port = GUEST_BACKLOG_PORT));
	TEST_SUCC(vsock_test_wait(handle));
	TEST_SUCC(close(listener));
}
END_TEST()

FN_SETUP(vsock_fini)
{
	vsock_test_guest_fini();
}
END_SETUP()
