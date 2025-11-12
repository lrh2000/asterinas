// SPDX-License-Identifier: MPL-2.0

use alloc::vec::Vec;

pub(super) use ostd::arch::irq::MappedIrqLine;
use ostd::arch::{
    boot::DEVICE_TREE,
    irq::{InterruptSourceInFdt, IRQ_CHIP},
};

pub(super) fn probe_for_device() {
    // The device tree parsing logic here assumed a Linux-compatible device
    // tree.
    // Reference: <https://www.kernel.org/doc/Documentation/devicetree/bindings/virtio/mmio.txt>.
    let device_tree = DEVICE_TREE.get().unwrap();
    let mmio_nodes = device_tree.all_nodes().filter(|node| {
        node.compatible().is_some_and(|compatibles| {
            compatibles
                .all()
                .any(|compatible| compatible == "virtio,mmio")
        })
    });
    mmio_nodes.for_each(|node| {
        let mmio_region = node.reg().unwrap().next().unwrap();
        let mmio_start = mmio_region.starting_address as usize;
        let mmio_end = mmio_start + mmio_region.size.unwrap();

        let interrupt_property = node.property("interrupts").unwrap();
        let interrupt_arguments = interrupt_property
            .value
            .chunks_exact(size_of::<u32>())
            .map(|chunk| u32::from_be_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<_>>();
        let interrupt_source_in_fdt = InterruptSourceInFdt {
            interrupt_parent: 0x8002, // FIXME: How to get this value correctly?
            arguments: interrupt_arguments.as_slice(),
        };

        let _ = super::try_register_mmio_device(mmio_start..mmio_end, |irq_line| {
            IRQ_CHIP
                .get()
                .unwrap()
                .map_fdt_pin_to(interrupt_source_in_fdt, irq_line)
        });
    });
}
