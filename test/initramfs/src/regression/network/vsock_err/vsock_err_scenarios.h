/* SPDX-License-Identifier: MPL-2.0 */

#ifndef VSOCK_ERR_SCENARIOS_H
#define VSOCK_ERR_SCENARIOS_H

#define VSOCK_TEST_SCENARIO_LIST(X)                                                        \
	X(echo, "Accepts one connection and echoes its payload.")                          \
	X(hold,                                                                            \
	  "Accepts one connection and keeps it open briefly for guest-side state checks.") \
	X(send_shutdown, "Sends one payload and shuts down its write side.")               \
	X(shutdown_read,                                                                   \
	  "Shuts down read first, then write, on one accepted socket.")                    \
	X(connect_addr,                                                                    \
	  "Connects into one guest listener and reports its local address.")               \
	X(connect_expect_disconnect,                                                       \
	  "Connects into one guest listener and expects the pending connection to drop.")  \
	X(fill_backlog,                                                                    \
	  "Fills one guest listen backlog and checks the next connect path.")

#define VSOCK_TEST_FIELDS_echo(F) F(u32, port)

#define VSOCK_TEST_FIELDS_hold(F) F(u32, port)

#define VSOCK_TEST_FIELDS_send_shutdown(F) \
	F(u32, port)                       \
	F(str, payload)

#define VSOCK_TEST_FIELDS_shutdown_read(F) F(u32, port)

#define VSOCK_TEST_FIELDS_connect_addr(F) F(u32, port)

#define VSOCK_TEST_FIELDS_connect_expect_disconnect(F) F(u32, port)

#define VSOCK_TEST_FIELDS_fill_backlog(F) F(u32, port)

#endif
