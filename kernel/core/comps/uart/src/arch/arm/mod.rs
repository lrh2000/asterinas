// SPDX-License-Identifier: MPL-2.0

use alloc::string::ToString;

use ostd::{
    arch::{
        boot::DEVICE_TREE,
        irq::{IRQ_CHIP, InterruptSourceInFdt, MappedIrqLine},
        serial::{Pl011Uart, SERIAL_PORT},
    },
    irq::IrqLine,
    sync::{LocalIrqDisabled, SpinLock},
};
use spin::Once;

use crate::{
    CONSOLE_NAME,
    console::{Uart, UartConsole},
};

/// IRQ line for UART serial.
static IRQ_LINE: Once<MappedIrqLine> = Once::new();

pub(super) fn init() {
    let Some(uart) = SERIAL_PORT.get() else {
        return;
    };

    let node = uart.lock().fdt_node();
    let intr_args = if let Some(prop) = node.property("interrupts")
        && let Ok(args) = prop
            .value
            .as_chunks::<{ size_of::<u32>() }>()
            .0
            .iter()
            .map(|chunk| u32::from_be_bytes(*chunk))
            .next_chunk()
    {
        args
    } else {
        ostd::info!("Failed to read 'interrupts' property from PL011 node");
        return;
    };

    // FIXME: We need to find the "interrupt-parent" property for the nearest ancestor. However,
    // there are no APIs to iterate ancestors. This workaround uses the "interrupt-parent" property
    // of the root node.
    let device_tree = DEVICE_TREE.get().unwrap();
    let intr_parent = if let Some(root) = device_tree.find_node("/")
        && let Some(parent) = root.property("interrupt-parent")
        && let Some(intr_parent) = parent.as_usize()
    {
        intr_parent as u32
    } else {
        ostd::info!("Failed to read 'interrupt-parent' property from PL011 node");
        return;
    };

    let Ok(mut irq_line) = IrqLine::alloc().and_then(|irq_line| {
        IRQ_CHIP.get().unwrap().map_fdt_pin_to(
            InterruptSourceInFdt {
                interrupt_parent: intr_parent as u32,
                arguments: intr_args,
            },
            irq_line,
        )
    }) else {
        ostd::info!("IRQ line is not available for PL011");
        return;
    };

    let uart_console = UartConsole::new(uart);

    aster_console::register_device(CONSOLE_NAME.to_string(), uart_console.clone());

    irq_line.on_active(move |_| uart_console.trigger_input_callbacks());
    IRQ_LINE.call_once(move || irq_line);
    uart.lock().enable_recv_interrupt();
    uart.flush();

    ostd::info!("Registered PL011 as a console");
}

impl Uart for &'static SpinLock<Pl011Uart, LocalIrqDisabled> {
    fn send(&self, buf: &[u8]) {
        let mut uart = self.lock();

        for byte in buf {
            // TODO: This is termios-specific behavior and should be part of the TTY implementation
            // instead of the serial console implementation. See the ONLCR flag for more details.
            if *byte == b'\n' {
                uart.send(b'\r');
            }
            uart.send(*byte);
        }
    }

    fn recv(&self, buf: &mut [u8]) -> usize {
        let mut uart = self.lock();

        for (i, byte) in buf.iter_mut().enumerate() {
            let Some(recv_byte) = uart.recv() else {
                return i;
            };
            *byte = recv_byte;
        }

        buf.len()
    }

    fn flush(&self) {
        let mut uart = self.lock();

        while uart.recv().is_some() {}
    }
}
