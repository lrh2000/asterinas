// SPDX-License-Identifier: MPL-2.0

#define VSOCK_TEST_HOST_IMPLEMENTATION

#include <sys/socket.h>
#include <unistd.h>

#include <linux/vm_sockets.h>

#include "example_scenarios.h"
#include "vsock_test_host.h"

VSOCK_HOST_SCENARIO(echo)
{
	VSOCK_HOST_BIND_ARGS(echo);
	struct sockaddr_vm addr = {
		.svm_family = AF_VSOCK,
		.svm_cid = vsock_test_bind_cid(),
		.svm_port = port,
	};
	int listener = socket(AF_VSOCK, SOCK_STREAM, 0);
	int accepted = -1;
	char buf[4096];
	ssize_t bytes_read;

	if (listener < 0) {
		return -1;
	}
	if (bind(listener, (struct sockaddr *)&addr, sizeof(addr)) < 0 ||
	    listen(listener, 1) < 0) {
		close(listener);
		return -1;
	}

	accepted = accept(listener, NULL, NULL);
	if (accepted < 0) {
		close(listener);
		return -1;
	}

	bytes_read = read(accepted, buf, sizeof(buf));
	if (bytes_read > 0 &&
	    write(accepted, buf, (size_t)bytes_read) != bytes_read) {
		bytes_read = -1;
	}

	close(accepted);
	close(listener);

	return bytes_read < 0 ? -1 : 0;
}

VSOCK_HOST_SCENARIO(hang)
{
	VSOCK_HOST_BIND_ARGS(hang);
	(void)request;

	for (;;) {
		pause();
	}

	return 0;
}
