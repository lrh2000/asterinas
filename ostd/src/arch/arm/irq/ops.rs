// SPDX-License-Identifier: MPL-2.0

//! Interrupt operations.

// FIXME: Mark this as unsafe. See
// <https://github.com/asterinas/asterinas/issues/1120#issuecomment-2748696592>.
pub(crate) fn enable_local() {
    unimplemented!()
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
    unimplemented!()
}

pub(crate) fn disable_local() {
    unimplemented!()
}

/// Disables local IRQs and halts the CPU forever.
pub(crate) fn disable_local_and_halt() -> ! {
    unimplemented!()
}

pub(crate) fn is_local_enabled() -> bool {
    unimplemented!()
}
