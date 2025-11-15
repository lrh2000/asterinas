// SPDX-License-Identifier: MPL-2.0

//! Interrupt operations.

use core::arch::asm;

// FIXME: Mark this as unsafe. See
// <https://github.com/asterinas/asterinas/issues/1120#issuecomment-2748696592>.
pub(crate) fn enable_local() {
    unsafe { asm!("msr daifclr, 0b0010") };
}

/// Enables local IRQs and halts the CPU to wait for interrupts.
///
/// This method guarantees that no interrupts can occur in the middle. In other words, IRQs must
/// either have been processed before this method is called, or they must wake the CPU up from the
/// halting state.
//
// FIXME: Mark this as unsafe. See
// <https://github.com/asterinas/asterinas/issues/1120#issuecomment-2748696592>.
pub(crate) fn enable_local_and_halt() {
    enable_local();
    // TODO: We should put the CPU into the idle state.
}

pub(crate) fn disable_local() {
    unsafe { asm!("msr daifset, 0b0010") };
}

pub(crate) fn is_local_enabled() -> bool {
    let daif: usize;
    unsafe { asm!("mrs {}, daif", out(reg) daif) };
    daif & (1 << 7) == 0
}
