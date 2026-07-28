// SPDX-License-Identifier: MPL-2.0

#define _GNU_SOURCE

#include "../../common/test.h"

#include <fcntl.h>
#include <sys/syscall.h>
#include <unistd.h>

#define A "rename_a"
#define B "rename_b"

static int mkfile(const char *path)
{
	int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
	if (fd < 0)
		return -1;
	return close(fd);
}

FN_SETUP(clean)
{
	unlink(A);
	unlink(B);
}
END_SETUP()

FN_TEST(renameat_syscall)
{
	CHECK(mkfile(A));
	TEST_SUCC(syscall(SYS_renameat, AT_FDCWD, A, AT_FDCWD, B));
	TEST_ERRNO(access(A, F_OK), ENOENT);
	TEST_SUCC(access(B, F_OK));
	unlink(A);
	unlink(B);
}
END_TEST()

FN_TEST(rename_wrapper)
{
	CHECK(mkfile(A));
	TEST_SUCC(rename(A, B));
	TEST_SUCC(access(B, F_OK));
	unlink(A);
	unlink(B);
}
END_TEST()

FN_TEST(rename_replaces_dest)
{
	CHECK(mkfile(A));
	CHECK(mkfile(B));
	TEST_SUCC(rename(A, B));
	TEST_ERRNO(access(A, F_OK), ENOENT);
	unlink(A);
	unlink(B);
}
END_TEST()

FN_TEST(renameat_nonexistent_enoent)
{
	TEST_ERRNO(syscall(SYS_renameat, AT_FDCWD, "no_such_file", AT_FDCWD, B),
		   ENOENT);
}
END_TEST()
