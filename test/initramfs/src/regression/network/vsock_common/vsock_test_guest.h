/* SPDX-License-Identifier: MPL-2.0 */

#ifndef VSOCK_TEST_GUEST_H
#define VSOCK_TEST_GUEST_H

/*
 * Guest-side API for coordinated guest-host vsock tests.
 *
 * Overview
 * --------
 *
 * One guest test process talks to one host harness process over an AF_VSOCK
 * control connection. Through that connection, the guest can:
 * - start one named host scenario with typed arguments;
 * - wait for that scenario to finish;
 * - kill that scenario when the test needs to stop it early.
 *
 * The shared scenario header must be included before this header. It defines
 * the scenario names and arguments used by both sides. This header uses those
 * definitions to generate:
 * - one guest convenience struct per scenario:
 *   `struct vsock_test_<scenario>_args`;
 * - the typed wrappers `VSOCK_TEST_START(<scenario>, ...)` and
 *   `VSOCK_TEST_RUN(<scenario>, ...)`.
 *
 * Typical use
 * -----------
 *
 * 1. Include the shared scenario header before `vsock_test_guest.h`.
 * 2. Call `vsock_test_guest_init()` during test setup.
 * 3. Start or run a host scenario with `VSOCK_TEST_START(...)` or
 *    `VSOCK_TEST_RUN(...)`.
 * 4. Finish with `vsock_test_wait()` or `vsock_test_kill()`.
 *
 * Implementation model
 * --------------------
 *
 * This header is a single-header implementation. Define
 * `VSOCK_TEST_GUEST_IMPLEMENTATION` in exactly one guest translation unit
 * before including it:
 *
 *     #define VSOCK_TEST_GUEST_IMPLEMENTATION
 *     #include "example_scenarios.h"
 *     #include "vsock_test_guest.h"
 */

#ifndef VSOCK_TEST_SCENARIO_LIST
#error "Include the shared scenario header before vsock_test_guest.h."
#endif

#include <sys/types.h>

#include "vsock_test_common.h"

#define __VSOCK_TEST_GUEST_API static __attribute__((unused))

/**
 * Names the environment variable that points the guest to the host CID.
 *
 * Set this before `vsock_test_guest_init()` when the default peer CID is not
 * correct for the current test setup.
 */
#define VSOCK_TEST_ENV_PEER_CID "VSOCK_TEST_PEER_CID"

#define __VSOCK_TEST_FIELD_DECL_u32(field_name) uint32_t field_name;
#define __VSOCK_TEST_FIELD_DECL_size(field_name) size_t field_name;
#define __VSOCK_TEST_FIELD_DECL_str(field_name) const char *field_name;
#define __VSOCK_TEST_FIELD_DECL_bool(field_name) bool field_name;
#define __VSOCK_TEST_FIELD_DECL(field_type, field_name) \
	__VSOCK_TEST_FIELD_DECL_##field_type(field_name)

#define __VSOCK_TEST_DECLARE_ARGS_STRUCT(scenario_name, description_text)  \
	struct vsock_test_##scenario_name##_args {                         \
		unsigned char __reserved;                                  \
		VSOCK_TEST_FIELDS_##scenario_name(__VSOCK_TEST_FIELD_DECL) \
	};

VSOCK_TEST_SCENARIO_LIST(__VSOCK_TEST_DECLARE_ARGS_STRUCT)

/**
 * Identifies one running host-side scenario.
 *
 * Callers should treat this as opaque and only pass it back to
 * `vsock_test_wait()` or `vsock_test_kill()`.
 */
struct vsock_test_scenario_handle {
	pid_t child_pid;
};

/**
 * Stores one typed named argument for `vsock_test_start()`.
 */
struct vsock_test_arg {
	const char *name;
	enum vsock_test_arg_type type;
	union {
		uint32_t u32;
		size_t size;
		const char *str;
		bool boolean;
	} value;
};

/**
 * Stores one complete argument list for `vsock_test_start()`.
 */
struct vsock_test_args {
	size_t nr_args;
	const struct vsock_test_arg *args;
};

/**
 * Creates an empty argument list for one scenario with no fields.
 *
 * Example:
 *
 *     vsock_test_run("hang", VSOCK_TEST_NO_ARGS);
 */
#define VSOCK_TEST_NO_ARGS \
	((struct vsock_test_args){ .nr_args = 0, .args = NULL })

/**
 * Creates one `uint32_t` named argument.
 *
 * Example:
 *
 *     VSOCK_TEST_U32("port", 25001)
 */
#define VSOCK_TEST_U32(arg_name, arg_value) \
	((struct vsock_test_arg){           \
		.name = (arg_name),         \
		.type = VSOCK_TEST_ARG_U32, \
		.value.u32 = (arg_value),   \
	})

/**
 * Creates one `size_t` named argument.
 */
#define VSOCK_TEST_SIZE(arg_name, arg_value) \
	((struct vsock_test_arg){            \
		.name = (arg_name),          \
		.type = VSOCK_TEST_ARG_SIZE, \
		.value.size = (arg_value),   \
	})

/**
 * Creates one string named argument.
 *
 * The string is copied into the control message when the scenario starts, so
 * the caller only needs to keep `arg_value` valid until `vsock_test_start()`
 * or `vsock_test_run()` returns.
 */
#define VSOCK_TEST_STR(arg_name, arg_value) \
	((struct vsock_test_arg){           \
		.name = (arg_name),         \
		.type = VSOCK_TEST_ARG_STR, \
		.value.str = (arg_value),   \
	})

/**
 * Creates one boolean named argument.
 */
#define VSOCK_TEST_BOOL(arg_name, arg_value)  \
	((struct vsock_test_arg){             \
		.name = (arg_name),           \
		.type = VSOCK_TEST_ARG_BOOL,  \
		.value.boolean = (arg_value), \
	})

/**
 * Builds one complete argument list from explicit named arguments.
 *
 * Example:
 *
 *     vsock_test_start("echo", VSOCK_TEST_ARGS(VSOCK_TEST_U32("port", 25001)),
 *                      &handle);
 */
#define VSOCK_TEST_ARGS(...)                                            \
	((struct vsock_test_args){                                      \
		.nr_args = sizeof((const struct vsock_test_arg[]){      \
				   __VA_ARGS__ }) /                     \
			   sizeof(struct vsock_test_arg),               \
		.args = (const struct vsock_test_arg[]){ __VA_ARGS__ }, \
	})

/**
 * Initializes the guest-side framework singleton from environment variables.
 *
 * Call this once before any `vsock_test_*()` request. When
 * `VSOCK_TEST_ENV_PEER_CID` is unset, the framework defaults to the local CID
 * on Linux and to the host CID inside Asterinas.
 */
__VSOCK_TEST_GUEST_API int vsock_test_guest_init(void);

/**
 * Finalizes the guest-side framework singleton.
 *
 * This sends one best-effort shutdown request to the host harness and then
 * closes the control connection. Calling it more than once is allowed.
 */
__VSOCK_TEST_GUEST_API void vsock_test_guest_fini(void);

/**
 * Returns the peer CID selected for the current guest session.
 *
 * Call this after `vsock_test_guest_init()` when the guest test needs the
 * host-side CID for additional vsock operations.
 */
__VSOCK_TEST_GUEST_API uint32_t vsock_test_peer_cid(void);

/**
 * Starts one host-side scenario from a generic named argument list.
 *
 * On success, `handle` identifies the new host process. The test should later
 * call either `vsock_test_wait(handle)` or `vsock_test_kill(handle)`.
 */
__VSOCK_TEST_GUEST_API
int vsock_test_start(const char *scenario_name, struct vsock_test_args args,
		     struct vsock_test_scenario_handle *handle);

/**
 * Waits for one previously started host-side scenario to finish.
 *
 * Returns `0` only when the host scenario exits successfully.
 */
__VSOCK_TEST_GUEST_API
int vsock_test_wait(struct vsock_test_scenario_handle handle);

/**
 * Forcefully kills one previously started host-side scenario.
 *
 * Use this for scenarios that intentionally block forever, such as `hang`.
 */
__VSOCK_TEST_GUEST_API
int vsock_test_kill(struct vsock_test_scenario_handle handle);

/**
 * Starts one host-side scenario and waits for it immediately.
 */
__VSOCK_TEST_GUEST_API
int vsock_test_run(const char *scenario_name, struct vsock_test_args args);

#define __VSOCK_TEST_BUILD_ARG_u32(field_name) \
	VSOCK_TEST_U32(#field_name, __vsock_test_args.field_name),
#define __VSOCK_TEST_BUILD_ARG_size(field_name) \
	VSOCK_TEST_SIZE(#field_name, __vsock_test_args.field_name),
#define __VSOCK_TEST_BUILD_ARG_str(field_name) \
	VSOCK_TEST_STR(#field_name, __vsock_test_args.field_name),
#define __VSOCK_TEST_BUILD_ARG_bool(field_name) \
	VSOCK_TEST_BOOL(#field_name, __vsock_test_args.field_name),
#define __VSOCK_TEST_BUILD_ARG(field_type, field_name) \
	__VSOCK_TEST_BUILD_ARG_##field_type(field_name)

#define __VSOCK_TEST_BUILD_ARGS(scenario_name)                          \
	((struct vsock_test_args){                                      \
		.nr_args = sizeof((const struct vsock_test_arg[]){      \
				   VSOCK_TEST_FIELDS_##scenario_name(   \
					   __VSOCK_TEST_BUILD_ARG) }) / \
			   sizeof(struct vsock_test_arg),               \
		.args =                                                 \
			(const struct vsock_test_arg[]){                \
				VSOCK_TEST_FIELDS_##scenario_name(      \
					__VSOCK_TEST_BUILD_ARG) },      \
	})

/**
 * Starts one host-side scenario with the generated typed wrapper.
 *
 * Example:
 *
 *     struct vsock_test_scenario_handle handle;
 *
 *     VSOCK_TEST_START(echo, &handle, .port = 25001);
 */
#define VSOCK_TEST_START(scenario_name, handle_ptr, ...)                       \
	({                                                                     \
		const struct vsock_test_##scenario_name##_args __attribute__(( \
			unused)) __vsock_test_args = { 0, ##__VA_ARGS__ };     \
		vsock_test_start(#scenario_name,                               \
				 __VSOCK_TEST_BUILD_ARGS(scenario_name),       \
				 (handle_ptr));                                \
	})

/**
 * Runs one host-side scenario with the generated typed wrapper and
 * waits for it immediately.
 *
 * Example:
 *
 *     VSOCK_TEST_RUN(echo, .port = 25001);
 */
#define VSOCK_TEST_RUN(scenario_name, ...)                                     \
	({                                                                     \
		const struct vsock_test_##scenario_name##_args __attribute__(( \
			unused)) __vsock_test_args = { 0, ##__VA_ARGS__ };     \
		vsock_test_run(#scenario_name,                                 \
			       __VSOCK_TEST_BUILD_ARGS(scenario_name));        \
	})

#ifdef VSOCK_TEST_GUEST_IMPLEMENTATION

#define _GNU_SOURCE

#include <inttypes.h>
#include <signal.h>

#define __VSOCK_TEST_CONNECT_RETRY_MS 50
#define __VSOCK_TEST_CONNECT_TIMEOUT_MS 5000

struct __vsock_test_guest_state {
	int control_fd;
	uint32_t peer_cid;
	uint32_t control_port;
	bool initialized;
};

static struct __vsock_test_guest_state __vsock_test_guest_state = {
	.control_fd = -1,
};

static uint32_t __vsock_test_guest_default_peer_cid(void)
{
#ifdef __asterinas__
	return VMADDR_CID_HOST;
#else
	return VMADDR_CID_LOCAL;
#endif
}

static int __vsock_test_connect_with_retry(uint32_t cid, uint32_t port)
{
	struct sockaddr_vm addr;
	int elapsed_ms = 0;

	__vsock_test_make_addr(&addr, cid, port);

	while (elapsed_ms <= __VSOCK_TEST_CONNECT_TIMEOUT_MS) {
		int sockfd = socket(AF_VSOCK, SOCK_STREAM, 0);

		if (sockfd < 0) {
			return -1;
		}
		if (connect(sockfd, (struct sockaddr *)&addr, sizeof(addr)) ==
		    0) {
			errno = 0;
			return sockfd;
		}

		close(sockfd);
		if (errno != EINTR) {
			usleep(__VSOCK_TEST_CONNECT_RETRY_MS * 1000);
			elapsed_ms += __VSOCK_TEST_CONNECT_RETRY_MS;
		}
	}

	errno = ETIMEDOUT;
	return -1;
}

static char __vsock_test_to_hex_digit(unsigned int value)
{
	return (char)(value < 10 ? '0' + value : 'a' + (value - 10));
}

static char *__vsock_test_hex_encode_string(const char *value)
{
	size_t value_len;
	char *hex;

	if (value == NULL) {
		errno = EINVAL;
		return NULL;
	}

	value_len = strlen(value);
	if (value_len == 0) {
		hex = malloc(2);
		if (hex == NULL) {
			return NULL;
		}

		hex[0] = '-';
		hex[1] = '\0';
		return hex;
	}
	if (value_len > (SIZE_MAX - 1) / 2) {
		errno = EOVERFLOW;
		return NULL;
	}

	hex = malloc(value_len * 2 + 1);
	if (hex == NULL) {
		return NULL;
	}

	for (size_t index = 0; index < value_len; index++) {
		unsigned char byte = (unsigned char)value[index];

		hex[index * 2] = __vsock_test_to_hex_digit(byte >> 4);
		hex[index * 2 + 1] = __vsock_test_to_hex_digit(byte & 0x0f);
	}
	hex[value_len * 2] = '\0';
	return hex;
}

static int __vsock_test_send_arg(int fd, const struct vsock_test_arg *arg)
{
	switch (arg->type) {
	case VSOCK_TEST_ARG_U32:
		return __vsock_test_send_line(fd, "ARG %s u32 %" PRIu32 "\n",
					      arg->name, arg->value.u32);
	case VSOCK_TEST_ARG_SIZE:
		return __vsock_test_send_line(fd, "ARG %s size %" PRIuMAX "\n",
					      arg->name,
					      (uintmax_t)arg->value.size);
	case VSOCK_TEST_ARG_BOOL:
		return __vsock_test_send_line(fd, "ARG %s bool %u\n", arg->name,
					      arg->value.boolean ? 1U : 0U);
	case VSOCK_TEST_ARG_STR: {
		char *encoded = __vsock_test_hex_encode_string(arg->value.str);
		int ret;

		if (encoded == NULL) {
			return -1;
		}

		ret = __vsock_test_send_line(fd, "ARG %s str %s\n", arg->name,
					     encoded);
		free(encoded);
		return ret;
	}
	}

	errno = EINVAL;
	return -1;
}

static int __vsock_test_expect_status(int fd, int *status)
{
	char line[__VSOCK_TEST_CONTROL_LINE_LEN];
	int read_status = __vsock_test_read_line(fd, line, sizeof(line));

	if (read_status <= 0) {
		if (read_status == 0) {
			errno = EIO;
		}
		return -1;
	}
	if (sscanf(line, "OK %d", status) == 1) {
		return 0;
	}
	if (sscanf(line, "ERR %d", status) == 1) {
		errno = *status;
		return -1;
	}

	errno = EPROTO;
	return -1;
}

__VSOCK_TEST_GUEST_API int vsock_test_guest_init(void)
{
	int control_fd;

	if (__vsock_test_guest_state.initialized) {
		return 0;
	}

	signal(SIGPIPE, SIG_IGN);

	__vsock_test_guest_state.peer_cid = __vsock_test_parse_u32_env(
		VSOCK_TEST_ENV_PEER_CID, __vsock_test_guest_default_peer_cid());
	if (__vsock_test_guest_state.peer_cid == UINT32_MAX) {
		return -1;
	}

	__vsock_test_guest_state.control_port = __vsock_test_parse_u32_env(
		VSOCK_TEST_ENV_CONTROL_PORT, VSOCK_TEST_DEFAULT_CONTROL_PORT);
	if (__vsock_test_guest_state.control_port == UINT32_MAX) {
		return -1;
	}

	control_fd = __vsock_test_connect_with_retry(
		__vsock_test_guest_state.peer_cid,
		__vsock_test_guest_state.control_port);
	if (control_fd < 0) {
		return -1;
	}

	__vsock_test_guest_state.control_fd = control_fd;
	__vsock_test_guest_state.initialized = true;
	return 0;
}

__VSOCK_TEST_GUEST_API void vsock_test_guest_fini(void)
{
	if (!__vsock_test_guest_state.initialized) {
		return;
	}

	(void)__vsock_test_send_line(__vsock_test_guest_state.control_fd,
				     "QUIT\n");
	close(__vsock_test_guest_state.control_fd);
	__vsock_test_guest_state.control_fd = -1;
	__vsock_test_guest_state.initialized = false;
}

__VSOCK_TEST_GUEST_API uint32_t vsock_test_peer_cid(void)
{
	if (!__vsock_test_guest_state.initialized) {
		errno = ENOTCONN;
		return UINT32_MAX;
	}

	return __vsock_test_guest_state.peer_cid;
}

__VSOCK_TEST_GUEST_API
int vsock_test_start(const char *scenario_name, struct vsock_test_args args,
		     struct vsock_test_scenario_handle *handle)
{
	int child_pid;

	if (!__vsock_test_guest_state.initialized || handle == NULL ||
	    scenario_name == NULL) {
		errno = EINVAL;
		return -1;
	}
	if (__vsock_test_send_line(__vsock_test_guest_state.control_fd,
				   "START %s %zu\n", scenario_name,
				   args.nr_args) < 0) {
		return -1;
	}
	for (size_t index = 0; index < args.nr_args; index++) {
		if (__vsock_test_send_arg(__vsock_test_guest_state.control_fd,
					  &args.args[index]) < 0) {
			return -1;
		}
	}
	if (__vsock_test_expect_status(__vsock_test_guest_state.control_fd,
				       &child_pid) < 0) {
		return -1;
	}

	handle->child_pid = child_pid;
	return 0;
}

__VSOCK_TEST_GUEST_API
int vsock_test_wait(struct vsock_test_scenario_handle handle)
{
	int status = 0;

	if (!__vsock_test_guest_state.initialized || handle.child_pid <= 0) {
		errno = EINVAL;
		return -1;
	}
	if (__vsock_test_send_line(__vsock_test_guest_state.control_fd,
				   "WAIT %d\n", handle.child_pid) < 0) {
		return -1;
	}

	return __vsock_test_expect_status(__vsock_test_guest_state.control_fd,
					  &status);
}

__VSOCK_TEST_GUEST_API
int vsock_test_kill(struct vsock_test_scenario_handle handle)
{
	int status = 0;

	if (!__vsock_test_guest_state.initialized || handle.child_pid <= 0) {
		errno = EINVAL;
		return -1;
	}
	if (__vsock_test_send_line(__vsock_test_guest_state.control_fd,
				   "KILL %d\n", handle.child_pid) < 0) {
		return -1;
	}

	return __vsock_test_expect_status(__vsock_test_guest_state.control_fd,
					  &status);
}

__VSOCK_TEST_GUEST_API
int vsock_test_run(const char *scenario_name, struct vsock_test_args args)
{
	struct vsock_test_scenario_handle handle;

	if (vsock_test_start(scenario_name, args, &handle) < 0) {
		return -1;
	}

	return vsock_test_wait(handle);
}

#endif

#endif
