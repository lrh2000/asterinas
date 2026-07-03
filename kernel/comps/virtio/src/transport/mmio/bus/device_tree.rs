// SPDX-License-Identifier: MPL-2.0

use ostd::arch::{
    boot::DEVICE_TREE,
    irq::{IRQ_CHIP, InterruptSourceInFdt},
};

pub(super) fn probe_from_device_tree() {
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

        let interrupt_source_in_fdt = InterruptSourceInFdt {
            // FIXME: We need to find the "interrupt-parent" property for the nearest ancestor.
            // However, there are no APIs to iterate ancestors. This workaround is for such device
            // trees (e.g., the ARM "virt" platform in QEMU).
            interrupt_parent: [node, device_tree.find_node("/").unwrap()]
                .iter()
                .find_map(|n| n.property("interrupt-parent"))
                .unwrap()
                .as_usize()
                .unwrap() as u32,
            arguments: node
                .property("interrupts")
                .unwrap()
                .value
                .chunks_exact(size_of::<u32>())
                .map(|chunk| u32::from_be_bytes(chunk.try_into().unwrap()))
                .next_chunk()
                .unwrap(),
        };

        let _ = super::try_register_mmio_device(mmio_start..mmio_end, |irq_line| {
            IRQ_CHIP
                .get()
                .unwrap()
                .map_fdt_pin_to(interrupt_source_in_fdt, irq_line)
        });
    });
}
