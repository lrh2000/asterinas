/* SPDX-License-Identifier: MPL-2.0 */

KERNEL_VMA             = 0xffffffff00000000

.section ".boot", "awx", @progbits

.global _start
_start:
    b _start
