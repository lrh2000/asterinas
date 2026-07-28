// SPDX-License-Identifier: MPL-2.0

#include "../../common/test.h"

#include <signal.h>

static volatile sig_atomic_t handled;

static void handler(int signum)
{
	handled = signum;
}

// After a signal handler returns, the interrupted code must resume. Where the
// kernel supplies the vDSO `rt_sigreturn` trampoline (e.g. AArch64), a wrong
// trampoline address makes the return livelock instead.
FN_TEST(signal_handler_returns)
{
	struct sigaction sa = { .sa_handler = handler };
	sigemptyset(&sa.sa_mask);

	TEST_SUCC(sigaction(SIGUSR1, &sa, NULL));
	TEST_SUCC(raise(SIGUSR1));
	TEST_RES(handled, handled == SIGUSR1);
}
END_TEST()
