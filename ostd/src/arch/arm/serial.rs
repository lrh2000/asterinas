// SPDX-License-Identifier: MPL-2.0

//! The console I/O.

use bitflags::bitflags;
use spin::Once;

use super::boot::DEVICE_TREE;
use crate::mm::paddr_to_vaddr;

struct Uart(*mut u32);

// SAFETY: For correctness purposes, the UART registers should not be accessed concurrently.
// However, doing so will not cause memory safety violations.
unsafe impl Send for Uart {}
unsafe impl Sync for Uart {}

bitflags! {
    struct Status: u32 {
        const TXFF = 1 << 5;
    }
}

impl Uart {
    // Reference: <https://developer.arm.com/documentation/ddi0183/g/programmers-model/summary-of-registers>.
    const OFFSET_UARTDR: usize = 0x000;
    const OFFSET_UARTFR: usize = 0x018;

    pub(self) fn new() -> Option<Self> {
        let node = DEVICE_TREE.get().unwrap().find_compatible(&["arm,pl011"])?;
        let addr = node.reg()?.next()?.starting_address as usize;
        Some(Self(paddr_to_vaddr(addr) as *mut u32))
    }

    pub(self) fn write_data(&self, data: u8) {
        unsafe {
            self.0
                .byte_add(Self::OFFSET_UARTDR)
                .write_volatile(data as u32);
        }
    }

    pub(self) fn read_status(&self) -> Status {
        let raw = unsafe { self.0.byte_add(Self::OFFSET_UARTFR).read_volatile() };
        Status::from_bits_truncate(raw)
    }
}

static UART: Once<Uart> = Once::new();

/// Initializes the serial port.
pub(crate) fn init() {
    // FIXME: Reserve the MMIO region in `io_memory_allocator`.
    UART.call_once(|| Uart::new().unwrap());
}

/// Sends a byte on the serial port.
pub(crate) fn send(data: u8) {
    let uart = UART.get().unwrap();

    // Note: It is the caller's responsibility to acquire the correct lock and ensure sequential
    // access to the UART registers.
    while uart.read_status().contains(Status::TXFF) {}
    uart.write_data(data);
}
