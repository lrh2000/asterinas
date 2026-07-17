// SPDX-License-Identifier: MPL-2.0

//! The ARM boot module defines the entrypoints of Asterinas.

pub(crate) mod smp;

use core::{arch::global_asm, ops::Range};

use fdt::Fdt;
use spin::Once;

use crate::{
    boot::{
        BootloaderAcpiArg, BootloaderFramebufferArg,
        memory_region::{MemoryRegion, MemoryRegionArray, MemoryRegionType},
    },
    mm::paddr_to_vaddr,
};

global_asm!(include_str!("bsp_boot.S"));

/// The Flattened Device Tree of the platform.
pub static DEVICE_TREE: Once<Fdt> = Once::new();

/// The physical address of [`DEVICE_TREE`].
static DEVICE_TREE_PADDR: Once<usize> = Once::new();

const BOOT_BLOCK_SIZE: usize = 1 << 30;

fn containing_boot_block(paddr: usize) -> Range<usize> {
    let start = paddr & !(BOOT_BLOCK_SIZE - 1);
    start..start + BOOT_BLOCK_SIZE
}

/// Returns the physical range covered by the temporary boot mapping for the DTB.
pub(crate) fn device_tree_boot_mapping_range() -> Range<usize> {
    containing_boot_block(*DEVICE_TREE_PADDR.get().unwrap())
}

fn parse_bootloader_name() -> &'static str {
    "Unknown"
}

fn parse_kernel_commandline() -> &'static str {
    DEVICE_TREE.get().unwrap().chosen().bootargs().unwrap_or("")
}

fn parse_initramfs() -> Option<&'static [u8]> {
    let (start, end) = parse_initramfs_range()?;

    let base_va = paddr_to_vaddr(start);
    let length = end - start;
    Some(unsafe { core::slice::from_raw_parts(base_va as *const u8, length) })
}

fn parse_acpi_arg() -> BootloaderAcpiArg {
    // TDDO: Add ACPI support for ARM, maybe.
    BootloaderAcpiArg::NotProvided
}

fn parse_framebuffer_info() -> Option<BootloaderFramebufferArg> {
    // TODO: Parse framebuffer info from device tree.
    None
}

fn parse_memory_regions(device_tree_paddr: usize) -> MemoryRegionArray {
    let mut regions = MemoryRegionArray::new();

    for region in DEVICE_TREE.get().unwrap().memory().regions() {
        if region.size.unwrap_or(0) > 0 {
            regions
                .push(MemoryRegion::new(
                    region.starting_address as usize,
                    region.size.unwrap(),
                    MemoryRegionType::Usable,
                ))
                .unwrap();
        }
    }

    if let Some(node) = DEVICE_TREE.get().unwrap().find_node("/reserved-memory") {
        for child in node.children() {
            if let Some(reg_iter) = child.reg() {
                for region in reg_iter {
                    regions
                        .push(MemoryRegion::new(
                            region.starting_address as usize,
                            region.size.unwrap(),
                            MemoryRegionType::Reserved,
                        ))
                        .unwrap();
                }
            }
        }
    }

    // Add the kernel region.
    regions.push(MemoryRegion::kernel()).unwrap();

    // Add the initramfs region.
    if let Some((start, end)) = parse_initramfs_range() {
        regions
            .push(MemoryRegion::new(
                start,
                end - start,
                MemoryRegionType::Module,
            ))
            .unwrap();
    }

    // Add the device tree region.
    regions
        .push(MemoryRegion::new(
            device_tree_paddr,
            DEVICE_TREE.get().unwrap().total_size(),
            MemoryRegionType::Module,
        ))
        .unwrap();

    regions.into_non_overlapping()
}

fn parse_initramfs_range() -> Option<(usize, usize)> {
    let chosen = DEVICE_TREE.get().unwrap().find_node("/chosen").unwrap();
    let initrd_start = chosen.property("linux,initrd-start")?.as_usize()?;
    let initrd_end = chosen.property("linux,initrd-end")?.as_usize()?;
    Some((initrd_start, initrd_end))
}

/// The entry point of the Rust code portion of Asterinas.
///
/// # Safety
///
/// - This function must be called only once at a proper timing in the BSP's boot assembly code.
/// - The caller must follow C calling conventions and put the right arguments in registers.
// SAFETY: The name does not collide with other symbols.
#[unsafe(no_mangle)]
unsafe extern "C" fn arm_boot(device_tree_paddr: usize) -> ! {
    DEVICE_TREE_PADDR.call_once(|| device_tree_paddr);
    let device_tree_ptr = paddr_to_vaddr(device_tree_paddr) as *const u8;
    let fdt = unsafe { Fdt::from_ptr(device_tree_ptr).unwrap() };
    DEVICE_TREE.call_once(|| fdt);

    use crate::boot::{EARLY_INFO, EarlyBootInfo, start_kernel};

    EARLY_INFO.call_once(|| EarlyBootInfo {
        bootloader_name: parse_bootloader_name(),
        kernel_cmdline: parse_kernel_commandline(),
        initramfs: parse_initramfs(),
        acpi_arg: parse_acpi_arg(),
        framebuffer_arg: parse_framebuffer_info(),
        memory_regions: parse_memory_regions(device_tree_paddr),
    });

    // SAFETY: The safety is guaranteed by the safety preconditions and the fact that we call it
    // once after setting up necessary resources.
    unsafe { start_kernel() };
}

#[cfg(ktest)]
mod test {
    use super::*;
    use crate::prelude::ktest;

    #[ktest]
    fn device_tree_boot_mapping_is_one_aligned_block() {
        assert_eq!(
            containing_boot_block(0x27fe0_0000),
            0x2_4000_0000..0x2_8000_0000
        );
        assert_eq!(containing_boot_block(0x4000_0000), 0x4000_0000..0x8000_0000);
    }
}
