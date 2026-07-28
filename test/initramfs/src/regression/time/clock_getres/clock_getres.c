// SPDX-License-Identifier: MPL-2.0

#define _GNU_SOURCE

#include "../../common/test.h"

#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

FN_TEST(clock_getres_realtime)
{
	struct timespec res = { 0 };
	TEST_RES(syscall(SYS_clock_getres, CLOCK_REALTIME, &res),
		 res.tv_sec == 0 && res.tv_nsec > 0 &&
			 res.tv_nsec < 1000000000);
}
END_TEST()

FN_TEST(clock_getres_monotonic)
{
	struct timespec res = { 0 };
	TEST_SUCC(syscall(SYS_clock_getres, CLOCK_MONOTONIC, &res));
}
END_TEST()

// A dynamic (per-process CPU) clock ID must be validated without reading it,
// so that unsupported dynamic clocks report an error instead of panicking.
FN_TEST(clock_getres_dynamic_cpu_clock)
{
	clockid_t clockid;
	CHECK(clock_getcpuclockid(getpid(), &clockid));

	struct timespec res = { 0 };
	TEST_SUCC(syscall(SYS_clock_getres, clockid, &res));
}
END_TEST()

FN_TEST(clock_getres_null_res)
{
	TEST_SUCC(syscall(SYS_clock_getres, CLOCK_REALTIME, NULL));
}
END_TEST()

FN_TEST(clock_getres_bad_res_efault)
{
	TEST_ERRNO(syscall(SYS_clock_getres, CLOCK_REALTIME, (void *)0xdead0000),
		   EFAULT);
}
END_TEST()

FN_TEST(clock_getres_invalid_clockid_einval)
{
	struct timespec res = { 0 };
	TEST_ERRNO(syscall(SYS_clock_getres, 999, &res), EINVAL);
}
END_TEST()
