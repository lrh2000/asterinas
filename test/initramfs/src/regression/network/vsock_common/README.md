# Vsock Guest-Host Test Framework

This directory contains the common support used by coordinated guest-host
vsock tests, plus a minimal example in the same directory.

The framework uses one AF_VSOCK control connection so the guest test can ask
the host to start named scenarios, wait for them to finish, or kill them when
needed. The current example keeps that setup deliberately small:

- `example_host.c` provides two host-side scenarios:
  - `echo`, which listens on one vsock port and echoes bytes back;
  - `hang`, which blocks until the guest asks the framework to kill it.
- `example_guest.c` drives those scenarios from the guest side and checks:
  - one echo round trip over AF_VSOCK;
  - one scenario kill path.

## Layout

- `vsock_test_guest.h` is the guest-side framework entry point.
- `vsock_test_host.h` is the host-side framework entry point.
- `vsock_test_common.h` holds the shared definitions and helpers used by both.
- `run_linux_test.sh` is the shared Linux runner for one guest-host test
  directory.
- `test.mk` generates `run_host.sh` and `run_guest.sh` in the build output for
  one test directory.
- `example_scenarios.h`, `example_guest.c`, and `example_host.c` form the
  minimal example in this directory.

## Linux Runner Contract

`run_linux_test.sh` is designed to be reusable by another test directory, such
as a future `test/initramfs/src/regression/network/vsock_err/`.

The target test directory only needs to follow this contract:

1. Its `Makefile` sets `VSOCK_TEST_HOST_BIN` and `VSOCK_TEST_GUEST_BIN`.
2. Its `Makefile` includes `../vsock_common/test.mk`.
3. It can be built with `make -C <test-dir> TEST_PLATFORM=linux`.

`test.mk` generates the `run_host.sh` and `run_guest.sh` entry points in the
build output, so the test directory does not need to keep copies of those
scripts in source control.

The current example lives in `vsock_common/` and follows the same contract. A
sibling test directory can use the same shape:

```make
VSOCK_TEST_HOST_BIN := vsock_err_host
VSOCK_TEST_GUEST_BIN := vsock_err_guest

include ../vsock_common/test.mk
```

That sibling directory can then reuse the same runner:

```sh
test/initramfs/src/regression/network/vsock_common/run_linux_test.sh \
  test/initramfs/src/regression/network/vsock_err
```

## Running The Example On Linux

Use the shared source-tree helper:

```sh
test/initramfs/src/regression/network/vsock_common/run_linux_test.sh
```

By default, it builds `test/initramfs/src/regression/network/vsock_common`,
starts `run_host.sh` with Linux loopback CID `1`, and then runs `run_guest.sh`
against the same loopback CID.

## Running With Asterinas

The guest example is included in the initramfs when basic tests are enabled.
To exercise it over AF_VSOCK, boot Asterinas with the virtio-vsock device
enabled:

```sh
make run_kernel ENABLE_REGRESSION_TEST=true VSOCK=on
```

To exercise the example end to end:

1. On the Linux host, build the host helper:

   ```sh
   make --no-print-directory -C test/initramfs/src/regression/network/vsock_common \
     TEST_PLATFORM=linux \
     BUILD_DIR="${PWD}/test/initramfs/build/vsock_framework_linux"
   ```

2. On the Linux host, start the built host harness:

   ```sh
   test/initramfs/build/vsock_framework_linux/initramfs/test/network/vsock_common/run_host.sh
   ```

3. Boot Asterinas with `make run_kernel ENABLE_BASIC_TEST=true VSOCK=on`.
4. Inside the guest shell, run:

   ```sh
   cd /test/network/vsock_common
   ./run_guest.sh
   ```

If a future sibling test such as `vsock_err/` should also be included in the
initramfs, add that directory to `test/initramfs/src/regression/network/Makefile`.
