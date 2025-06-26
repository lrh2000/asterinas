// SPDX-License-Identifier: MPL-2.0 OR MIT
//
// The original source code is from [trapframe-rs](https://github.com/rcore-os/trapframe-rs),
// which is released under the following license:
//
// SPDX-License-Identifier: MIT
//
// Copyright (c) 2020 - 2024 Runji Wang
//
// We make the following new changes:
// * Implement the `trap_handler` of Asterinas.
//
// These changes are released under the following license:
//
// SPDX-License-Identifier: MPL-2.0

use core::arch::{asm, global_asm};

use crate::arch::cpu::context::GeneralRegs;

/// Saved registers on a trap.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
#[repr(C)]
pub struct RawUserContext {
    /// Trap num: Source and Kind
    pub trap_num: usize,
    /// Reserved for internal use
    pub __reserved: usize,
    /// Exception Link Register, elr_el1
    pub elr: usize,
    /// Saved Process Status Register, spsr_el1
    pub spsr: usize,
    /// Stack Pointer, sp_el0
    pub sp: usize,
    /// Software Thread ID Register, tpidr_el0
    pub tpidr: usize,
    /// General registers
    /// Must be last in this struct
    pub general: GeneralRegs,
}

global_asm!(include_str!("trap.S"));

/// Initializes interrupt handling for the current HART.
///
/// # Safety
///
/// This function will:
/// - Set `vbar_el1` to internal exception vector.
///
/// You **MUST NOT** modify these registers later.
pub(super) unsafe fn init() {
    unsafe {
        // Set the exception vector address
        asm!("msr vbar_el1, {}", in(reg) __vectors as usize);
    }
}

/// Trap frame of kernel interrupt
///
/// # Trap handler
///
/// You need to define a handler function like this:
///
/// ```no_run
/// use trapframe::TrapFrame;
///
/// #[no_mangle]
/// pub extern "C" fn trap_handler(tf: &mut TrapFrame) {
///     println!("TRAP! tf: {:#x?}", tf);
/// }
/// ```
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct TrapFrame {
    /// Trap num: Source and Kind
    pub trap_num: usize,
    /// Reserved for internal use
    pub __reserved: usize,
    /// Exception Link Register, elr_el1
    pub elr: usize,
    /// Saved Process Status Register, spsr_el1
    pub spsr: usize,
    /// Stack Pointer, sp_el1
    pub sp: usize,
    /// Software Thread ID Register, tpidr_el1
    pub tpidr: usize,
    /// General registers
    /// Must be last in this struct
    pub general: GeneralRegs,
}

impl RawUserContext {
    /// Goes to user space with the context, and comes back when a trap occurs.
    ///
    /// On return, the context will be reset to the status before the trap.
    /// Trap reason and error code will be placed at `trap_num`.
    pub(in crate::arch) fn run(&mut self) {
        // Return to userspace with interrupts disabled. Otherwise, interrupts
        // after switching `sscratch` will mess up the CPU state.
        crate::arch::irq::disable_local();
        unsafe { run_user(self) }
    }
}

unsafe extern "C" {
    unsafe fn __vectors();
    unsafe fn run_user(regs: &mut RawUserContext);
}
