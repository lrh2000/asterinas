// SPDX-License-Identifier: MPL-2.0

//! Handles trap.

#[expect(clippy::module_inception)]
mod trap;

use spin::Once;
pub(super) use trap::RawUserContext;
pub use trap::TrapFrame;

use crate::arch::cpu::context::CpuException;

/// Initializes interrupt handling on ARM.
///
/// # Safety
///
/// On the current CPU, this function must be called
/// - only once and
/// - before any trap can occur.
pub(crate) unsafe fn init_on_cpu() {
    // SAFETY: The caller ensures the safety conditions.
    unsafe {
        trap::init_on_cpu();
    }
}

/// Handle traps (only from kernel).
// SAFETY: The name does not collide with other symbols.
#[unsafe(no_mangle)]
extern "C" fn trap_handler(f: &mut TrapFrame) {
    unimplemented!()
}
