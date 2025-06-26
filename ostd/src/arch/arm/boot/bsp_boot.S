/* SPDX-License-Identifier: MPL-2.0 */

// The boot routine executed by the bootstrap processor (BSP) on ARM.

KERNEL_VMA             = 0xffffffff00000000

.section ".boot", "awx", @progbits

# `_start` is also the entrypoint specified in the linker script.
.global _start
_start:
    b _start
