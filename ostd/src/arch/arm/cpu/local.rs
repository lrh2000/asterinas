// SPDX-License-Identifier: MPL-2.0

//! Architecture dependent CPU-local information utilities.

use core::arch::asm;

pub(crate) fn get_base() -> u64 {
    let base;
    unsafe {
        asm!(
            "mrs {base}, tpidr_el1",
            base = out(reg) base,
            options(preserves_flags, nostack)
        );
    }
    base
}
