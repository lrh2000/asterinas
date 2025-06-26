// SPDX-License-Identifier: MPL-2.0

//! Providing the ability to exit QEMU and return a value as debug result.

/// The exit code of QEMU.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QemuExitCode {
    /// The code that indicates a successful exit.
    Success,
    /// The code that indicates a failed exit.
    Failed,
}

/// Exit QEMU with the given exit code.
pub fn exit_qemu(exit_code: QemuExitCode) -> ! {
    loop {}
}
