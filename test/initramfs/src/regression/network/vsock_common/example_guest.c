// SPDX-License-Identifier: MPL-2.0

#define VSOCK_TEST_GUEST_IMPLEMENTATION

#include "../../common/test.h"

#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <linux/vm_sockets.h>

#include "example_scenarios.h"
#include "vsock_test_guest.h"

#define ECHO_PORT 25001
#define ECHO_PAYLOAD "hello-from-guest-over-vsock"
#define ECHO_CONNECT_DELAY_US 200000

FN_SETUP(vsock_init)
{
	CHECK(vsock_test_guest_init());
}
END_SETUP()

FN_TEST(run_echo_scenario)
{
	struct sockaddr_vm addr = {
		.svm_family = AF_VSOCK,
		.svm_cid = vsock_test_peer_cid(),
		.svm_port = ECHO_PORT,
	};
	struct vsock_test_scenario_handle handle;
	int sockfd = -1;
	char reply[sizeof(ECHO_PAYLOAD)] = { 0 };

	TEST_SUCC(VSOCK_TEST_START(echo, &handle, .port = ECHO_PORT));
	TEST_SUCC(usleep(ECHO_CONNECT_DELAY_US));

	sockfd = TEST_SUCC(socket(AF_VSOCK, SOCK_STREAM, 0));
	TEST_SUCC(connect(sockfd, (struct sockaddr *)&addr, sizeof(addr)));

	TEST_RES(write(sockfd, ECHO_PAYLOAD, strlen(ECHO_PAYLOAD)),
		 (size_t)_ret == strlen(ECHO_PAYLOAD));
	TEST_SUCC(shutdown(sockfd, SHUT_WR));

	TEST_RES(read(sockfd, reply, strlen(ECHO_PAYLOAD)),
		 (size_t)_ret == strlen(ECHO_PAYLOAD));
	TEST_RES(memcmp(reply, ECHO_PAYLOAD, strlen(ECHO_PAYLOAD)), _ret == 0);

	TEST_SUCC(close(sockfd));
	TEST_SUCC(vsock_test_wait(handle));
}
END_TEST()

FN_TEST(kill_hang_scenario)
{
	struct vsock_test_scenario_handle handle;

	TEST_SUCC(VSOCK_TEST_START(hang, &handle));
	TEST_SUCC(vsock_test_kill(handle));
}
END_TEST()

FN_SETUP(vsock_fini)
{
	vsock_test_guest_fini();
}
END_SETUP()
