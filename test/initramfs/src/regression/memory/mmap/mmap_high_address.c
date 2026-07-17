// SPDX-License-Identifier: MPL-2.0

#define _GNU_SOURCE

#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#define PAGE_SIZE 4096
#define OLD_USER_LIMIT ((uintptr_t)0x0000800000000000)
#define LAST_MAPPABLE_PAGE ((uintptr_t)0x0000ffffffffe000)
#define TOP_GUARD_PAGE ((uintptr_t)0x0000fffffffff000)
#define LOW_48_BIT_LIMIT ((uintptr_t)0x0001000000000000)

#define INITIAL_VALUE UINT64_C(0x1111222233334444)
#define REMAPPED_VALUE UINT64_C(0x5555666677778888)
#define SCRATCH_VALUE UINT64_C(0xaaaabbbbccccdddd)
#define CHILD_VALUE UINT64_C(0x123456789abcdef0)

struct thread_arg {
	int cpu;
	volatile uint64_t *address;
	uint64_t expected;
};

static void fail(const char *operation)
{
	perror(operation);
	exit(EXIT_FAILURE);
}

static void *map_page(uintptr_t address)
{
	void *result = mmap((void *)address, PAGE_SIZE, PROT_READ | PROT_WRITE,
			    MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED_NOREPLACE,
			    -1, 0);
	if (result == MAP_FAILED)
		fail("mmap");
	if ((uintptr_t)result != address) {
		fprintf(stderr, "mmap returned unexpected address %p\n",
			result);
		exit(EXIT_FAILURE);
	}
	return result;
}

static void expect_unmappable(uintptr_t address)
{
	errno = 0;
	void *result = mmap((void *)address, PAGE_SIZE, PROT_NONE,
			    MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED_NOREPLACE,
			    -1, 0);
	if (result != MAP_FAILED || errno != ENOMEM) {
		fprintf(stderr,
			"mmap(%p) unexpectedly returned %p with errno %d\n",
			(void *)address, result, errno);
		exit(EXIT_FAILURE);
	}
}

static void *read_on_cpu(void *raw_arg)
{
	struct thread_arg *arg = raw_arg;
	cpu_set_t mask;

	CPU_ZERO(&mask);
	CPU_SET(arg->cpu, &mask);
	if (sched_setaffinity(0, sizeof(mask), &mask) != 0)
		fail("sched_setaffinity");

	if (*arg->address != arg->expected) {
		fprintf(stderr,
			"CPU %d read 0x%016lx at %p, expected 0x%016lx\n",
			arg->cpu, (unsigned long)*arg->address,
			(void *)arg->address, (unsigned long)arg->expected);
		exit(EXIT_FAILURE);
	}
	return NULL;
}

static void read_from_each_cpu(volatile uint64_t *address, uint64_t expected)
{
	long online_cpus = sysconf(_SC_NPROCESSORS_ONLN);
	if (online_cpus < 1)
		fail("sysconf");
	if (online_cpus > 4)
		online_cpus = 4;

	pthread_t threads[4];
	struct thread_arg args[4];
	for (int cpu = 0; cpu < online_cpus; cpu++) {
		args[cpu] = (struct thread_arg){
			.cpu = cpu,
			.address = address,
			.expected = expected,
		};
		int error = pthread_create(&threads[cpu], NULL, read_on_cpu,
					   &args[cpu]);
		if (error != 0) {
			errno = error;
			fail("pthread_create");
		}
	}
	for (int cpu = 0; cpu < online_cpus; cpu++) {
		int error = pthread_join(threads[cpu], NULL);
		if (error != 0) {
			errno = error;
			fail("pthread_join");
		}
	}
}

static void test_mapping_boundaries(void)
{
	volatile uint64_t *old_limit = map_page(OLD_USER_LIMIT);
	*old_limit = INITIAL_VALUE;
	if (*old_limit != INITIAL_VALUE) {
		fprintf(stderr, "high-address readback failed\n");
		exit(EXIT_FAILURE);
	}
	if (munmap((void *)old_limit, PAGE_SIZE) != 0)
		fail("munmap");

	volatile uint64_t *last_page = map_page(LAST_MAPPABLE_PAGE);
	*last_page = INITIAL_VALUE;
	if (*last_page != INITIAL_VALUE) {
		fprintf(stderr, "last-page readback failed\n");
		exit(EXIT_FAILURE);
	}
	if (munmap((void *)last_page, PAGE_SIZE) != 0)
		fail("munmap");

	expect_unmappable(TOP_GUARD_PAGE);
	expect_unmappable(LOW_48_BIT_LIMIT);
}

static void test_fork_isolation(void)
{
	volatile uint64_t *address = map_page(OLD_USER_LIMIT);
	*address = INITIAL_VALUE;

	pid_t child = fork();
	if (child < 0)
		fail("fork");
	if (child == 0) {
		*address = CHILD_VALUE;
		_exit(*address == CHILD_VALUE ? EXIT_SUCCESS : EXIT_FAILURE);
	}

	int status;
	if (waitpid(child, &status, 0) < 0)
		fail("waitpid");
	if (!WIFEXITED(status) || WEXITSTATUS(status) != EXIT_SUCCESS) {
		fprintf(stderr, "high-address child failed\n");
		exit(EXIT_FAILURE);
	}
	if (*address != INITIAL_VALUE) {
		fprintf(stderr, "fork did not preserve private high mapping\n");
		exit(EXIT_FAILURE);
	}
	if (munmap((void *)address, PAGE_SIZE) != 0)
		fail("munmap");
}

static void test_smp_tlb_invalidation(void)
{
	volatile uint64_t *address = map_page(OLD_USER_LIMIT);
	*address = INITIAL_VALUE;
	read_from_each_cpu(address, INITIAL_VALUE);

	if (munmap((void *)address, PAGE_SIZE) != 0)
		fail("munmap");

	volatile uint64_t *scratch = mmap(NULL, PAGE_SIZE,
					  PROT_READ | PROT_WRITE,
					  MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (scratch == MAP_FAILED)
		fail("mmap scratch");
	*scratch = SCRATCH_VALUE;

	address = map_page(OLD_USER_LIMIT);
	*address = REMAPPED_VALUE;
	read_from_each_cpu(address, REMAPPED_VALUE);

	if (munmap((void *)address, PAGE_SIZE) != 0)
		fail("munmap");
	if (munmap((void *)scratch, PAGE_SIZE) != 0)
		fail("munmap scratch");
}

int main(void)
{
	test_mapping_boundaries();
	test_fork_isolation();
	test_smp_tlb_invalidation();
	puts("48-bit high-address mmap regression passed");
	return EXIT_SUCCESS;
}
