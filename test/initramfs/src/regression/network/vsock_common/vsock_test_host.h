/* SPDX-License-Identifier: MPL-2.0 */

#ifndef VSOCK_TEST_HOST_H
#define VSOCK_TEST_HOST_H

/*
 * Host-side API for coordinated guest-host vsock tests.
 *
 * Overview
 * --------
 *
 * The host harness owns the AF_VSOCK control listener. It accepts one guest
 * control connection, decodes typed scenario arguments, forks one child
 * process per started scenario, and exposes wait/kill lifecycle operations
 * back to the guest.
 *
 * Host test code only needs to provide the scenario implementations. The
 * framework owns `main()` and the control loop. A host scenario is kept small:
 * - declare it with `VSOCK_HOST_SCENARIO(<name>)`;
 * - bind the shared arguments with `VSOCK_HOST_BIND_ARGS(<name>)`;
 * - implement the host-side behavior needed by the guest test.
 *
 * The shared scenario header must be included before this header. It defines
 * the scenario names and arguments used by both sides, and this header uses
 * those definitions to generate the host-side bind helpers.
 *
 * Implementation model
 * --------------------
 *
 * This header is a single-header implementation. Define
 * `VSOCK_TEST_HOST_IMPLEMENTATION` in exactly one host translation unit before
 * including it:
 *
 *     #define VSOCK_TEST_HOST_IMPLEMENTATION
 *     #include "example_scenarios.h"
 *     #include "vsock_test_host.h"
 *
 * The translation unit that includes this header becomes the whole host test
 * program and receives the framework-owned `main()`.
 */

#ifndef VSOCK_TEST_SCENARIO_LIST
#error "Include the shared scenario header before vsock_test_host.h."
#endif

#include "vsock_test_common.h"

#define __VSOCK_TEST_HOST_API static __attribute__((unused))

/**
 * Names the environment variable that selects the CID used by host listeners.
 *
 * Set this before starting the host harness. Typical values are `1` for Linux
 * loopback testing and `2` for the Linux host that serves one Asterinas guest.
 */
#define VSOCK_TEST_ENV_BIND_CID "VSOCK_TEST_BIND_CID"

/**
 * Names the environment variable that selects the CID used when a host
 * scenario actively connects back into the guest.
 *
 * Host scenarios can read this through `vsock_test_connect_cid()`.
 */
#define VSOCK_TEST_ENV_CONNECT_CID "VSOCK_TEST_CONNECT_CID"

/**
 * Represents one decoded scenario request from the guest.
 *
 * Scenario callbacks receive this object and should access its contents only
 * through the helper functions below or through `VSOCK_HOST_BIND_ARGS(...)`.
 */
struct vsock_test_request;

#define __VSOCK_TEST_FIELD_BIND_u32(field_name)                          \
	uint32_t field_name;                                             \
	if (vsock_test_req_u32(request, #field_name, &field_name) < 0) { \
		return -1;                                               \
	}
#define __VSOCK_TEST_FIELD_BIND_size(field_name)                          \
	size_t field_name;                                                \
	if (vsock_test_req_size(request, #field_name, &field_name) < 0) { \
		return -1;                                                \
	}
#define __VSOCK_TEST_FIELD_BIND_str(field_name)                          \
	const char *field_name;                                          \
	if (vsock_test_req_str(request, #field_name, &field_name) < 0) { \
		return -1;                                               \
	}
#define __VSOCK_TEST_FIELD_BIND_bool(field_name)                          \
	bool field_name;                                                  \
	if (vsock_test_req_bool(request, #field_name, &field_name) < 0) { \
		return -1;                                                \
	}
#define __VSOCK_TEST_FIELD_BIND(field_type, field_name) \
	__VSOCK_TEST_FIELD_BIND_##field_type(field_name)

/**
 * Describes one registered host-side scenario.
 */
struct vsock_test_host_scenario {
	const char *name;
	const char *description;
	int (*run)(const struct vsock_test_request *request);
};

#define __VSOCK_TEST_DECLARE_SCENARIO_PROTO(scenario_name, description_text) \
	static int __vsock_test_host_scenario_##scenario_name(               \
		const struct vsock_test_request *request);

VSOCK_TEST_SCENARIO_LIST(__VSOCK_TEST_DECLARE_SCENARIO_PROTO)

/**
 * Defines one host-side scenario callback.
 *
 * Example:
 *
 *     VSOCK_HOST_SCENARIO(echo)
 *     {
 *         VSOCK_HOST_BIND_ARGS(echo);
 *         return run_echo_server(port);
 *     }
 */
#define VSOCK_HOST_SCENARIO(scenario_name)                     \
	static int __vsock_test_host_scenario_##scenario_name( \
		const struct vsock_test_request *request)

/**
 * Binds all shared arguments of one scenario to local variables.
 *
 * Call this at the beginning of a `VSOCK_HOST_SCENARIO(...)` body.
 */
#define VSOCK_HOST_BIND_ARGS(scenario_name) \
	VSOCK_TEST_FIELDS_##scenario_name(__VSOCK_TEST_FIELD_BIND)

/**
 * Returns whether the request contains one argument named `name`.
 */
__VSOCK_TEST_HOST_API
bool vsock_test_req_has(const struct vsock_test_request *request,
			const char *name);

/**
 * Returns one `uint32_t` request argument in `*value`.
 *
 * The call fails when the argument is missing or has a different type.
 */
__VSOCK_TEST_HOST_API
int vsock_test_req_u32(const struct vsock_test_request *request,
		       const char *name, uint32_t *value);

/**
 * Returns one `size_t` request argument in `*value`.
 *
 * The on-wire representation is wider than `size_t`, so this helper rejects
 * values that do not fit on the current host.
 */
__VSOCK_TEST_HOST_API
int vsock_test_req_size(const struct vsock_test_request *request,
			const char *name, size_t *value);

/**
 * Returns one string request argument in `*value`.
 *
 * The returned pointer remains valid until the scenario callback returns.
 */
__VSOCK_TEST_HOST_API
int vsock_test_req_str(const struct vsock_test_request *request,
		       const char *name, const char **value);

/**
 * Returns one boolean request argument in `*value`.
 */
__VSOCK_TEST_HOST_API
int vsock_test_req_bool(const struct vsock_test_request *request,
			const char *name, bool *value);

/**
 * Returns the bind CID selected for host listeners.
 *
 * Scenario implementations typically use this when they call `bind()`.
 */
__VSOCK_TEST_HOST_API uint32_t vsock_test_bind_cid(void);

/**
 * Returns the guest CID used by host-initiated data connections.
 */
__VSOCK_TEST_HOST_API uint32_t vsock_test_connect_cid(void);

/**
 * Returns the control port used by the host harness listener.
 */
__VSOCK_TEST_HOST_API uint32_t vsock_test_control_port(void);

/**
 * Runs the framework-owned host harness.
 *
 * Most test programs should not call this directly because the header already
 * provides `main()`. It is still exposed so callers can wrap it with custom
 * setup if they really need to.
 */
__VSOCK_TEST_HOST_API int
vsock_test_host_main_impl(const struct vsock_test_host_scenario *scenario_begin,
			  const struct vsock_test_host_scenario *scenario_end);

#define __VSOCK_TEST_DECLARE_SCENARIO_DESC(scenario_name, description_text) \
	{                                                                   \
		.name = #scenario_name,                                     \
		.description = description_text,                            \
		.run = __vsock_test_host_scenario_##scenario_name,          \
	},

static const struct vsock_test_host_scenario __vsock_test_host_scenarios[] = {
	VSOCK_TEST_SCENARIO_LIST(__VSOCK_TEST_DECLARE_SCENARIO_DESC)
};

#ifdef VSOCK_TEST_HOST_IMPLEMENTATION

#define _GNU_SOURCE

#include <inttypes.h>
#include <limits.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>

struct __vsock_test_request_arg {
	char *name;
	enum vsock_test_arg_type type;
	union {
		uint32_t u32;
		uint64_t size;
		char *str;
		bool boolean;
	} value;
};

struct vsock_test_request {
	char *scenario_name;
	size_t nr_args;
	struct __vsock_test_request_arg *args;
};

struct __vsock_test_host_config {
	uint32_t bind_cid;
	uint32_t connect_cid;
	uint32_t control_port;
	bool initialized;
};

struct __vsock_test_child_state {
	pid_t child_pid;
};

static struct __vsock_test_host_config __vsock_test_host_config;

static int __vsock_test_load_config(void)
{
	if (__vsock_test_host_config.initialized) {
		return 0;
	}

	__vsock_test_host_config.bind_cid = __vsock_test_parse_u32_env(
		VSOCK_TEST_ENV_BIND_CID, VMADDR_CID_ANY);
	if (__vsock_test_host_config.bind_cid == UINT32_MAX) {
		return -1;
	}

	__vsock_test_host_config.connect_cid = __vsock_test_parse_u32_env(
		VSOCK_TEST_ENV_CONNECT_CID, VMADDR_CID_LOCAL);
	if (__vsock_test_host_config.connect_cid == UINT32_MAX) {
		return -1;
	}

	__vsock_test_host_config.control_port = __vsock_test_parse_u32_env(
		VSOCK_TEST_ENV_CONTROL_PORT, VSOCK_TEST_DEFAULT_CONTROL_PORT);
	if (__vsock_test_host_config.control_port == UINT32_MAX) {
		return -1;
	}

	__vsock_test_host_config.initialized = true;
	return 0;
}

__VSOCK_TEST_HOST_API uint32_t vsock_test_bind_cid(void)
{
	if (__vsock_test_load_config() < 0) {
		return UINT32_MAX;
	}

	return __vsock_test_host_config.bind_cid;
}

__VSOCK_TEST_HOST_API uint32_t vsock_test_connect_cid(void)
{
	if (__vsock_test_load_config() < 0) {
		return UINT32_MAX;
	}

	return __vsock_test_host_config.connect_cid;
}

__VSOCK_TEST_HOST_API uint32_t vsock_test_control_port(void)
{
	if (__vsock_test_load_config() < 0) {
		return UINT32_MAX;
	}

	return __vsock_test_host_config.control_port;
}

static int __vsock_test_bind_and_listen(uint32_t cid, uint32_t port)
{
	int listener = socket(AF_VSOCK, SOCK_STREAM, 0);
	struct sockaddr_vm addr;

	if (listener < 0) {
		return -1;
	}

	__vsock_test_make_addr(&addr, cid, port);
	if (bind(listener, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		close(listener);
		return -1;
	}
	if (listen(listener, 4) < 0) {
		close(listener);
		return -1;
	}

	return listener;
}

static int __vsock_test_accept_one(int listener)
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

static const struct vsock_test_host_scenario *__vsock_test_find_scenario(
	const struct vsock_test_host_scenario *scenario_begin,
	const struct vsock_test_host_scenario *scenario_end, const char *name)
{
	for (const struct vsock_test_host_scenario *scenario = scenario_begin;
	     scenario < scenario_end; scenario++) {
		if (strcmp(scenario->name, name) == 0) {
			return scenario;
		}
	}

	return NULL;
}

static unsigned int __vsock_test_from_hex_digit(char ch)
{
	if (ch >= '0' && ch <= '9') {
		return (unsigned int)(ch - '0');
	}
	if (ch >= 'a' && ch <= 'f') {
		return (unsigned int)(ch - 'a' + 10);
	}
	if (ch >= 'A' && ch <= 'F') {
		return (unsigned int)(ch - 'A' + 10);
	}

	return UINT_MAX;
}

static char *__vsock_test_hex_decode_string(const char *value)
{
	size_t hex_len;
	char *decoded;

	if (strcmp(value, "-") == 0) {
		decoded = malloc(1);
		if (decoded == NULL) {
			return NULL;
		}

		decoded[0] = '\0';
		return decoded;
	}

	hex_len = strlen(value);
	if ((hex_len & 1U) != 0) {
		errno = EPROTO;
		return NULL;
	}

	decoded = malloc(hex_len / 2 + 1);
	if (decoded == NULL) {
		return NULL;
	}

	for (size_t index = 0; index < hex_len; index += 2) {
		unsigned int high = __vsock_test_from_hex_digit(value[index]);
		unsigned int low =
			__vsock_test_from_hex_digit(value[index + 1]);

		if (high == UINT_MAX || low == UINT_MAX) {
			free(decoded);
			errno = EPROTO;
			return NULL;
		}

		decoded[index / 2] = (char)((high << 4) | low);
	}
	decoded[hex_len / 2] = '\0';
	return decoded;
}

static int __vsock_test_parse_u32(const char *value, uint32_t *parsed)
{
	char *end = NULL;
	unsigned long raw;

	errno = 0;
	raw = strtoul(value, &end, 10);
	if (errno != 0 || *end != '\0' || raw > UINT32_MAX) {
		errno = EPROTO;
		return -1;
	}

	*parsed = (uint32_t)raw;
	return 0;
}

static int __vsock_test_parse_u64(const char *value, uint64_t *parsed)
{
	char *end = NULL;
	unsigned long long raw;

	errno = 0;
	raw = strtoull(value, &end, 10);
	if (errno != 0 || *end != '\0') {
		errno = EPROTO;
		return -1;
	}

	*parsed = (uint64_t)raw;
	return 0;
}

static void __vsock_test_request_fini(struct vsock_test_request *request)
{
	if (request == NULL) {
		return;
	}

	for (size_t index = 0; index < request->nr_args; index++) {
		free(request->args[index].name);
		if (request->args[index].type == VSOCK_TEST_ARG_STR) {
			free(request->args[index].value.str);
		}
	}

	free(request->args);
	free(request->scenario_name);
	request->args = NULL;
	request->scenario_name = NULL;
	request->nr_args = 0;
}

static int __vsock_test_parse_arg_line(struct __vsock_test_request_arg *arg,
				       char *line)
{
	char *save = NULL;
	char *command = strtok_r(line, " ", &save);
	char *name = strtok_r(NULL, " ", &save);
	char *type = strtok_r(NULL, " ", &save);
	char *value = strtok_r(NULL, " ", &save);

	if (command == NULL || name == NULL || type == NULL || value == NULL ||
	    strcmp(command, "ARG") != 0) {
		errno = EPROTO;
		return -1;
	}

	arg->name = strdup(name);
	if (arg->name == NULL) {
		return -1;
	}

	if (strcmp(type, "u32") == 0) {
		arg->type = VSOCK_TEST_ARG_U32;
		return __vsock_test_parse_u32(value, &arg->value.u32);
	}
	if (strcmp(type, "size") == 0) {
		arg->type = VSOCK_TEST_ARG_SIZE;
		return __vsock_test_parse_u64(value, &arg->value.size);
	}
	if (strcmp(type, "bool") == 0) {
		uint32_t boolean = 0;

		if (__vsock_test_parse_u32(value, &boolean) < 0 ||
		    boolean > 1U) {
			errno = EPROTO;
			return -1;
		}

		arg->type = VSOCK_TEST_ARG_BOOL;
		arg->value.boolean = boolean != 0;
		return 0;
	}
	if (strcmp(type, "str") == 0) {
		arg->type = VSOCK_TEST_ARG_STR;
		arg->value.str = __vsock_test_hex_decode_string(value);
		if (arg->value.str == NULL) {
			return -1;
		}

		return 0;
	}

	errno = EPROTO;
	return -1;
}

static int __vsock_test_read_start_request(int control_fd,
					   struct vsock_test_request *request,
					   char *line)
{
	char scenario_name[256];
	size_t nr_args = 0;

	if (sscanf(line, "START %255s %zu", scenario_name, &nr_args) != 2) {
		errno = EPROTO;
		return -1;
	}

	memset(request, 0, sizeof(*request));
	request->scenario_name = strdup(scenario_name);
	if (request->scenario_name == NULL) {
		return -1;
	}

	request->nr_args = nr_args;
	if (nr_args == 0) {
		return 0;
	}

	request->args = calloc(nr_args, sizeof(*request->args));
	if (request->args == NULL) {
		return -1;
	}

	for (size_t index = 0; index < nr_args; index++) {
		int read_status = __vsock_test_read_line(
			control_fd, line, __VSOCK_TEST_CONTROL_LINE_LEN);

		if (read_status <= 0) {
			if (read_status == 0) {
				errno = EIO;
			}
			return -1;
		}
		if (__vsock_test_parse_arg_line(&request->args[index], line) <
		    0) {
			return -1;
		}
	}

	return 0;
}

static const struct __vsock_test_request_arg *
__vsock_test_find_arg(const struct vsock_test_request *request,
		      const char *name)
{
	if (request == NULL || name == NULL) {
		errno = EINVAL;
		return NULL;
	}

	for (size_t index = 0; index < request->nr_args; index++) {
		if (strcmp(request->args[index].name, name) == 0) {
			return &request->args[index];
		}
	}

	errno = ENOENT;
	return NULL;
}

__VSOCK_TEST_HOST_API
bool vsock_test_req_has(const struct vsock_test_request *request,
			const char *name)
{
	for (size_t index = 0; index < request->nr_args; index++) {
		if (strcmp(request->args[index].name, name) == 0) {
			return true;
		}
	}

	return false;
}

__VSOCK_TEST_HOST_API
int vsock_test_req_u32(const struct vsock_test_request *request,
		       const char *name, uint32_t *value)
{
	const struct __vsock_test_request_arg *arg =
		__vsock_test_find_arg(request, name);

	if (arg == NULL) {
		return -1;
	}
	if (arg->type != VSOCK_TEST_ARG_U32) {
		errno = EINVAL;
		return -1;
	}

	*value = arg->value.u32;
	return 0;
}

__VSOCK_TEST_HOST_API
int vsock_test_req_size(const struct vsock_test_request *request,
			const char *name, size_t *value)
{
	const struct __vsock_test_request_arg *arg =
		__vsock_test_find_arg(request, name);

	if (arg == NULL) {
		return -1;
	}
	if (arg->type != VSOCK_TEST_ARG_SIZE) {
		errno = EINVAL;
		return -1;
	}
	if (arg->value.size > SIZE_MAX) {
		errno = EOVERFLOW;
		return -1;
	}

	*value = (size_t)arg->value.size;
	return 0;
}

__VSOCK_TEST_HOST_API
int vsock_test_req_str(const struct vsock_test_request *request,
		       const char *name, const char **value)
{
	const struct __vsock_test_request_arg *arg =
		__vsock_test_find_arg(request, name);

	if (arg == NULL) {
		return -1;
	}
	if (arg->type != VSOCK_TEST_ARG_STR) {
		errno = EINVAL;
		return -1;
	}

	*value = arg->value.str;
	return 0;
}

__VSOCK_TEST_HOST_API
int vsock_test_req_bool(const struct vsock_test_request *request,
			const char *name, bool *value)
{
	const struct __vsock_test_request_arg *arg =
		__vsock_test_find_arg(request, name);

	if (arg == NULL) {
		return -1;
	}
	if (arg->type != VSOCK_TEST_ARG_BOOL) {
		errno = EINVAL;
		return -1;
	}

	*value = arg->value.boolean;
	return 0;
}

static int __vsock_test_wait_child(pid_t child_pid)
{
	int wait_status = 0;

	if (waitpid(child_pid, &wait_status, 0) < 0) {
		return -1;
	}
	if (WIFEXITED(wait_status) && WEXITSTATUS(wait_status) == 0) {
		return 0;
	}
	if (WIFEXITED(wait_status)) {
		errno = WEXITSTATUS(wait_status);
		return -1;
	}
	if (WIFSIGNALED(wait_status)) {
		errno = EINTR;
		return -1;
	}

	errno = ECHILD;
	return -1;
}

static int __vsock_test_remove_child(struct __vsock_test_child_state *children,
				     size_t *nr_children, pid_t child_pid)
{
	for (size_t index = 0; index < *nr_children; index++) {
		if (children[index].child_pid != child_pid) {
			continue;
		}

		children[index] = children[*nr_children - 1];
		(*nr_children)--;
		return 0;
	}

	errno = ECHILD;
	return -1;
}

__VSOCK_TEST_HOST_API int
vsock_test_host_main_impl(const struct vsock_test_host_scenario *scenario_begin,
			  const struct vsock_test_host_scenario *scenario_end)
{
	struct __vsock_test_child_state *children = NULL;
	size_t nr_children = 0;
	int listener = -1;
	int control_fd = -1;
	int exit_code = 1;

	if (__vsock_test_load_config() < 0) {
		goto out;
	}

	signal(SIGPIPE, SIG_IGN);

	listener = __vsock_test_bind_and_listen(vsock_test_bind_cid(),
						vsock_test_control_port());
	if (listener < 0) {
		goto out;
	}

	control_fd = __vsock_test_accept_one(listener);
	if (control_fd < 0) {
		goto out;
	}

	for (;;) {
		char line[__VSOCK_TEST_CONTROL_LINE_LEN];
		int read_status =
			__vsock_test_read_line(control_fd, line, sizeof(line));

		if (read_status == 0) {
			exit_code = 0;
			break;
		}
		if (read_status < 0) {
			break;
		}
		if (strncmp(line, "START ", 6) == 0) {
			struct vsock_test_request request;
			const struct vsock_test_host_scenario *scenario;
			pid_t child_pid;

			if (__vsock_test_read_start_request(
				    control_fd, &request, line) < 0) {
				(void)__vsock_test_send_line(control_fd,
							     "ERR %d\n", errno);
				continue;
			}

			scenario = __vsock_test_find_scenario(
				scenario_begin, scenario_end,
				request.scenario_name);
			if (scenario == NULL) {
				__vsock_test_request_fini(&request);
				(void)__vsock_test_send_line(
					control_fd, "ERR %d\n", ENOENT);
				continue;
			}

			child_pid = fork();
			if (child_pid < 0) {
				int fork_errno = errno;

				__vsock_test_request_fini(&request);
				(void)__vsock_test_send_line(
					control_fd, "ERR %d\n", fork_errno);
				continue;
			}
			if (child_pid == 0) {
				int scenario_status = scenario->run(&request);
				int scenario_errno = errno;

				__vsock_test_request_fini(&request);
				close(control_fd);
				close(listener);
				if (scenario_status == 0) {
					_exit(0);
				}

				_exit(scenario_errno == 0 ? 1 : scenario_errno);
			}

			__vsock_test_request_fini(&request);
			children = realloc(children, (nr_children + 1) *
							     sizeof(*children));
			if (children == NULL) {
				kill(child_pid, SIGKILL);
				waitpid(child_pid, NULL, 0);
				exit_code = 1;
				goto out;
			}
			children[nr_children].child_pid = child_pid;
			nr_children++;
			if (__vsock_test_send_line(control_fd, "OK %d\n",
						   child_pid) < 0) {
				goto out;
			}
			continue;
		}
		if (strncmp(line, "WAIT ", 5) == 0) {
			int child_pid = 0;

			if (sscanf(line, "WAIT %d", &child_pid) != 1 ||
			    __vsock_test_remove_child(children, &nr_children,
						      child_pid) < 0) {
				(void)__vsock_test_send_line(
					control_fd, "ERR %d\n", ECHILD);
				continue;
			}
			if (__vsock_test_wait_child((pid_t)child_pid) < 0) {
				(void)__vsock_test_send_line(control_fd,
							     "ERR %d\n", errno);
				continue;
			}
			if (__vsock_test_send_line(control_fd, "OK 0\n") < 0) {
				goto out;
			}
			continue;
		}
		if (strncmp(line, "KILL ", 5) == 0) {
			int child_pid = 0;

			if (sscanf(line, "KILL %d", &child_pid) != 1 ||
			    __vsock_test_remove_child(children, &nr_children,
						      child_pid) < 0) {
				(void)__vsock_test_send_line(
					control_fd, "ERR %d\n", ECHILD);
				continue;
			}
			if (kill((pid_t)child_pid, SIGKILL) < 0 &&
			    errno != ESRCH) {
				(void)__vsock_test_send_line(control_fd,
							     "ERR %d\n", errno);
				continue;
			}
			(void)waitpid((pid_t)child_pid, NULL, 0);
			if (__vsock_test_send_line(control_fd, "OK 0\n") < 0) {
				goto out;
			}
			continue;
		}
		if (strcmp(line, "QUIT") == 0) {
			(void)__vsock_test_send_line(control_fd, "OK 0\n");
			exit_code = 0;
			break;
		}

		(void)__vsock_test_send_line(control_fd, "ERR %d\n", EPROTO);
	}

out:
	for (size_t index = 0; index < nr_children; index++) {
		if (kill(children[index].child_pid, SIGKILL) < 0 &&
		    errno != ESRCH) {
			continue;
		}
		(void)waitpid(children[index].child_pid, NULL, 0);
	}

	free(children);
	if (control_fd >= 0) {
		close(control_fd);
	}
	if (listener >= 0) {
		close(listener);
	}

	return exit_code;
}

#endif

/*
 * The framework-owned host entry point.
 *
 * Host test programs normally include this header in exactly one translation
 * unit that only defines scenarios. That file does not need to provide its own
 * `main()`.
 */
int main(void)
{
	return vsock_test_host_main_impl(
		__vsock_test_host_scenarios,
		__vsock_test_host_scenarios +
			sizeof(__vsock_test_host_scenarios) /
				sizeof(__vsock_test_host_scenarios[0]));
}

#endif
