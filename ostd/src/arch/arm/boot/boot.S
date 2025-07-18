/* SPDX-License-Identifier: MPL-2.0 */

TCR_TG1_4K             = 2 << 30
TCR_SH1_INNER          = 3 << 28
TCR_ORGN1_WBWA         = 1 << 26
TCR_IRGN1_WBWA         = 1 << 24
TCR_T1SZ_48            = 16 << 16

TCR_TG0_4K             = 0 << 14
TCR_SH0_INNER          = 3 << 12
TCR_ORGN0_WBWA         = 1 << 10
TCR_IRGN0_WBWA         = 1 << 8
TCR_T0SZ_48            = 16 << 0

MAIR_0WBWA             = 0xFF

SCTLR_I                = 1 << 12
SCTLR_C                = 1 << 2
SCTLR_M                = 1 << 0

PTE_TABLE              = 0x003
PTE_BLOCK              = 0x001
PTE_SH_INNER           = 0x300
PTE_ACCESS             = 0x400
PTE_SIZE               = 8

KERNEL_VMA             = 0xffffffff00000000

.section ".boot", "awx", @progbits

.global _start
_start:
    ldr     x0, =boot_l4pt
    msr     ttbr0_el1, x0
    msr     ttbr1_el1, x0

    ldr     x1, =boot_l3pt_linear_id
    orr     x1, x1, #PTE_TABLE
    str     x1, [x0]
    add     x0, x0, #(256 * PTE_SIZE)
    str     x1, [x0]

    ldr     x1, =boot_l3pt_kernel
    orr     x1, x1, #PTE_TABLE
    add     x0, x0, #(255 * PTE_SIZE)
    str     x1, [x0]

    mov     x0, #(TCR_TG1_4K | TCR_SH1_INNER | TCR_ORGN1_WBWA | TCR_IRGN1_WBWA | TCR_T1SZ_48)
    mov     x1, #(TCR_TG0_4K | TCR_SH0_INNER | TCR_ORGN0_WBWA | TCR_IRGN0_WBWA | TCR_T0SZ_48)
    orr     x0, x0, x1
    msr     tcr_el1, x0

    mov     x0, #MAIR_0WBWA
    msr     mair_el1, x0

    tlbi vmalle1
    dsb     sy
    isb

    mov     x0, #(SCTLR_I | SCTLR_C | SCTLR_M)
    msr     sctlr_el1, x0

    ldr     x0, =(boot_stack_top + KERNEL_VMA)
    mov     sp, x0
    ldr     x0, =_start_virt
    br x0

.balign 4096
boot_l4pt:
    .zero 512 * PTE_SIZE
boot_l3pt_linear_id:
    .quad (0 << 30) | PTE_BLOCK | PTE_SH_INNER | PTE_ACCESS
    .quad (1 << 30) | PTE_BLOCK | PTE_SH_INNER | PTE_ACCESS
    .quad (2 << 30) | PTE_BLOCK | PTE_SH_INNER | PTE_ACCESS
    .quad (3 << 30) | PTE_BLOCK | PTE_SH_INNER | PTE_ACCESS
    .zero 508 * PTE_SIZE
boot_l3pt_kernel:
    .zero 508 * PTE_SIZE
    .quad (0 << 30) | PTE_BLOCK | PTE_SH_INNER | PTE_ACCESS
    .quad (1 << 30) | PTE_BLOCK | PTE_SH_INNER | PTE_ACCESS
    .quad (2 << 30) | PTE_BLOCK | PTE_SH_INNER | PTE_ACCESS
    .quad 0

.section ".boot.stack", "awx", @nobits
boot_stack_bottom:
    .balign 4096
    .skip 0x40000
boot_stack_top:

.text
_start_virt:
    ldr     x0, =__cpu_local_start
    msr     tpidr_el1, x0

    b       arm_boot
