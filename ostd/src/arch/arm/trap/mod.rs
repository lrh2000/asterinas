// SPDX-License-Identifier: MPL-2.0

//! Handles trap.

#[expect(clippy::module_inception)]
mod trap;

use spin::Once;
pub(super) use trap::RawUserContext;
pub use trap::TrapFrame;

use super::irq::IRQ_CHIP;
use crate::{
    arch::{
        cpu::context::{CpuException, CpuTrap},
        irq::{disable_local, enable_local, HwIrqLine},
    },
    cpu::{CpuId, PrivilegeLevel},
    ex_table::ExTable,
    irq::call_irq_callback_functions,
    mm::MAX_USERSPACE_VADDR,
};

/// Initializes interrupt handling on ARM.
pub(crate) unsafe fn init() {
    unsafe {
        self::trap::init();
    }
}

/// Handle traps (only from kernel).
#[no_mangle]
extern "C" fn trap_handler(f: &mut TrapFrame) {
    let trap = CpuTrap::new(f.trap_num);

    let exception = match trap {
        Some(CpuTrap::Exception(exception)) => exception,
        Some(CpuTrap::Interrupt) => {
            let irq_chip = IRQ_CHIP.get().unwrap();
            while let Some(hw_irq_line) = irq_chip.claim_interrupt() {
                call_irq_callback_functions(f, &hw_irq_line, PrivilegeLevel::Kernel);
            }
            return;
        }
        _ => panic!("Cannot handle kernel trap: {:?}, trapframe: {:#?}", trap, f),
    };

    match exception {
        CpuException::DataAbort { address, .. } if (0..MAX_USERSPACE_VADDR).contains(&address) => {
            handle_user_page_fault(f, &exception);
        }
        _ => {
            panic!(
                "Cannot handle kernel exception: {:?}, trapframe: {:#?}",
                exception, f
            );
        }
    }
}

#[expect(clippy::type_complexity)]
static USER_PAGE_FAULT_HANDLER: Once<fn(&CpuException) -> core::result::Result<(), ()>> =
    Once::new();

/// Injects a custom handler for page faults that occur in the kernel and
/// are caused by user-space address.
pub fn inject_user_page_fault_handler(
    handler: fn(info: &CpuException) -> core::result::Result<(), ()>,
) {
    USER_PAGE_FAULT_HANDLER.call_once(|| handler);
}

fn handle_user_page_fault(f: &mut TrapFrame, exception: &CpuException) {
    let handler = USER_PAGE_FAULT_HANDLER
        .get()
        .expect("Page fault handler is missing");

    let res = handler(exception);
    // Copying bytes by bytes can recover directly
    // if handling the page fault successfully.
    if res.is_ok() {
        return;
    }

    unimplemented!()
}
