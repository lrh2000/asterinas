/* SPDX-License-Identifier: MPL-2.0 */

#ifndef VSOCK_TEST_EXAMPLE_SCENARIOS_H
#define VSOCK_TEST_EXAMPLE_SCENARIOS_H

/*
 * Shared scenario definitions for the minimal guest-host vsock example.
 *
 * This header lists the scenarios used by the example and the arguments each
 * scenario accepts. `vsock_test_guest.h` uses it to generate guest-side
 * wrappers, and `vsock_test_host.h` uses it to generate host-side bind
 * helpers.
 */

#define VSOCK_TEST_SCENARIO_LIST(X)                             \
	X(echo, "Binds one echo server on the requested port.") \
	X(hang, "Blocks forever until the guest kills the scenario.")

#define VSOCK_TEST_FIELDS_echo(F) F(u32, port)

#define VSOCK_TEST_FIELDS_hang(F)

#endif
