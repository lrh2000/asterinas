// SPDX-License-Identifier: MPL-2.0

use alloc::fmt;
use core::{arch::asm, ops::Range};

use spin::Once;

use crate::{
    mm::{
        page_prop::{CachePolicy, PageFlags, PageProperty, PrivilegedPageFlags as PrivFlags},
        page_table::PageTableEntryTrait,
        DmaDirection, Paddr, PagingConstsTrait, PagingLevel, PodOnce, Vaddr, PAGE_SIZE,
    },
    Pod,
};

#[derive(Clone, Debug, Default)]
pub(crate) struct PagingConsts {}

impl PagingConstsTrait for PagingConsts {
    const BASE_PAGE_SIZE: usize = 4096;
    const NR_LEVELS: PagingLevel = 4;
    const ADDRESS_WIDTH: usize = 48;
    const VA_SIGN_EXT: bool = true;
    const HIGHEST_TRANSLATION_LEVEL: PagingLevel = 4;
    const PTE_SIZE: usize = size_of::<PageTableEntry>();
}

bitflags::bitflags! {
    #[derive(Pod)]
    #[repr(C)]
    /// Possible flags for a page table entry.
    pub(crate) struct PageTableFlags: usize {
        /// Specifies whether the mapped frame or page table is valid.
        const VALID =           1 << 0;
        /// Specifies whether the mapping does not points to a huge frame; this bit must also be
        /// set for all the valid last-level entries.
        const NON_HUGE =        1 << 1;
        /// Controls whether accesses from userspace (i.e. EL0) are permitted.
        const USER =            1 << 6;
        /// Controls whether writes to the mapped frames are disallowed.
        const NO_WRITE =        1 << 7;
        /// Whether the memory area represented by this entry is accessed.
        const ACCESSED =        1 << 10;
        /// Indicates that the mapping isn't present in all address spaces, so it is flushed from
        /// the TLB on an address space switch.
        const NON_GLOBAL =      1 << 11;

        /// Whether the memory area represented by this entry is modified.
        const DIRTY =           1 << 51;
        /// Forbid execute codes on the page.
        const NO_EXECUTE =      1 << 54;

        /// Ignored by the hardware. Free to use.
        const HIGH_IGN1 =       1 << 55;
        /// Ignored by the hardware. Free to use.
        const HIGH_IGN2 =       1 << 56;

        // Be careful that the following fields contain multiple bits!
        //
        /// Bit 2-4: Device memory, nGnRnE.
        const ATTR_DEVICE =     1 << 2;
        /// Bit 8-9: Inner shareability (effective only for Normal memory).
        const SH_INNER =        3 << 8;
    }
}

pub(crate) fn tlb_flush_addr(vaddr: Vaddr) {
    unsafe {
        asm!(
            "dsb ishst",
            "tlbi vaae1, {vpn}",
            vpn = in(reg) vaddr >> 12,
        );
    }
}

pub(crate) fn tlb_flush_addr_range(range: &Range<Vaddr>) {
    for vaddr in range.clone().step_by(PAGE_SIZE) {
        tlb_flush_addr(vaddr);
    }
}

pub(crate) fn tlb_flush_all_excluding_global() {
    unsafe {
        asm!("tlbi vmalle1", "dsb ish", "isb");
    }
}

pub(crate) fn tlb_flush_all_including_global() {
    // TODO: including global?
    unsafe {
        asm!("tlbi vmalle1", "dsb ish", "isb");
    }
}

/// # Safety
///
/// The caller must ensure that the virtual address range and DMA direction correspond correctly to
/// a DMA region.
pub(crate) unsafe fn sync_dma_range(range: Range<Vaddr>, direction: DmaDirection) {
    // FIXME: Implement this
}

#[derive(Clone, Copy, Pod, Default)]
#[repr(C)]
pub(crate) struct PageTableEntry(usize);

/// Activates the given root-level page table.
///
/// `_root_pt_cache` is ignored because it is currently not supported on ARM platforms.
///
/// # Safety
///
/// Changing the root-level page table is unsafe, because it's possible to violate memory safety by
/// changing the page mapping.
pub(crate) unsafe fn activate_page_table(root_paddr: Paddr, _root_pt_cache: CachePolicy) {
    unsafe {
        asm!(
            "msr ttbr0_el1, {root_paddr}",
            "msr ttbr1_el1, {root_paddr}",
            root_paddr = in(reg) root_paddr,
            options(nomem, nostack, preserves_flags),
        );
    }
}

pub(crate) fn current_page_table_paddr() -> Paddr {
    let root_paddr;
    unsafe {
        asm!(
            "mrs {root_paddr}, ttbr0_el1",
            root_paddr = out(reg) root_paddr,
            options(nomem, nostack, preserves_flags),
        );
    }
    root_paddr
}

impl PageTableEntry {
    const PHYS_ADDR_MASK: usize = 0x0000_FFFF_FFFF_F000;
    const PROP_MASK: usize =
        !Self::PHYS_ADDR_MASK & !PageTableFlags::VALID.union(PageTableFlags::NON_HUGE).bits();
}

/// Parse a bit-flag bits `val` in the representation of `from` to `to` in bits.
macro_rules! parse_flags {
    ($val:expr, $from:expr, $to:expr) => {
        ($val as usize & $from.bits() as usize) >> $from.bits().ilog2() << $to.bits().ilog2()
    };
}

impl PodOnce for PageTableEntry {}

impl PageTableEntryTrait for PageTableEntry {
    fn is_present(&self) -> bool {
        self.0 & PageTableFlags::VALID.bits() != 0
    }

    fn new_page(paddr: Paddr, level: PagingLevel, prop: PageProperty) -> Self {
        let flags = if level == 1 {
            PageTableFlags::VALID.bits() | PageTableFlags::NON_HUGE.bits()
        } else {
            PageTableFlags::VALID.bits()
        };
        let mut pte = Self(paddr & Self::PHYS_ADDR_MASK | flags);
        pte.set_prop(prop);
        pte
    }

    fn new_pt(paddr: Paddr) -> Self {
        let flags = (PageTableFlags::VALID | PageTableFlags::NON_HUGE).bits();
        Self(paddr & Self::PHYS_ADDR_MASK | flags)
    }

    fn paddr(&self) -> Paddr {
        self.0 & Self::PHYS_ADDR_MASK
    }

    fn prop(&self) -> PageProperty {
        let flags = (parse_flags!(self.0, PageTableFlags::VALID, PageFlags::R))
            | (parse_flags!(!self.0, PageTableFlags::NO_WRITE, PageFlags::W))
            | (parse_flags!(!self.0, PageTableFlags::NO_EXECUTE, PageFlags::X))
            | (parse_flags!(self.0, PageTableFlags::ACCESSED, PageFlags::ACCESSED))
            | (parse_flags!(self.0, PageTableFlags::DIRTY, PageFlags::DIRTY))
            | (parse_flags!(self.0, PageTableFlags::HIGH_IGN2, PageFlags::AVAIL2));
        let priv_flags = (parse_flags!(self.0, PageTableFlags::USER, PrivFlags::USER))
            | (parse_flags!(!self.0, PageTableFlags::NON_GLOBAL, PrivFlags::GLOBAL))
            | (parse_flags!(self.0, PageTableFlags::HIGH_IGN1, PrivFlags::AVAIL1));

        let cache = if self.0 & PageTableFlags::ATTR_DEVICE.bits() != 0 {
            CachePolicy::Uncacheable
        } else {
            CachePolicy::Writeback
        };

        PageProperty {
            flags: PageFlags::from_bits(flags as u8).unwrap(),
            cache,
            priv_flags: PrivFlags::from_bits(priv_flags as u8).unwrap(),
        }
    }

    #[expect(clippy::precedence)]
    fn set_prop(&mut self, prop: PageProperty) {
        if !self.is_present() {
            return;
        }

        let mut flags = parse_flags!(!prop.flags.bits(), PageFlags::W, PageTableFlags::NO_WRITE)
            | parse_flags!(!prop.flags.bits(), PageFlags::X, PageTableFlags::NO_EXECUTE)
            | PageTableFlags::ACCESSED.bits()
            | parse_flags!(prop.flags.bits(), PageFlags::DIRTY, PageTableFlags::DIRTY)
            | parse_flags!(
                prop.flags.bits(),
                PageFlags::ACCESSED,
                PageTableFlags::ACCESSED
            )
            | parse_flags!(prop.flags.bits(), PageFlags::DIRTY, PageTableFlags::DIRTY)
            | parse_flags!(
                prop.priv_flags.bits(),
                PrivFlags::USER,
                PageTableFlags::USER
            )
            | parse_flags!(
                !prop.priv_flags.bits(),
                PrivFlags::GLOBAL,
                PageTableFlags::NON_GLOBAL
            )
            | parse_flags!(
                prop.priv_flags.bits(),
                PrivFlags::AVAIL1,
                PageTableFlags::HIGH_IGN1
            )
            | parse_flags!(
                prop.flags.bits(),
                PageFlags::AVAIL2,
                PageTableFlags::HIGH_IGN2
            );

        flags |= PageTableFlags::SH_INNER.bits();
        match prop.cache {
            CachePolicy::Writeback => (),
            CachePolicy::Uncacheable => {
                // TODO: Currently Asterinas uses `Uncacheable` only for I/O
                // memory. Normal memory can also be `Noncacheable`, where the
                // attribute should not be set to `ATTR_DEVICE`.
                flags |= PageTableFlags::ATTR_DEVICE.bits()
            }
            _ => panic!("unsupported cache policy"),
        }

        self.0 = (self.0 & !Self::PROP_MASK) | flags;
    }

    fn is_last(&self, level: PagingLevel) -> bool {
        level == 1 || self.0 & PageTableFlags::NON_HUGE.bits() == 0
    }
}

impl fmt::Debug for PageTableEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut f = f.debug_struct("PageTableEntry");
        f.field("raw", &format_args!("{:#x}", self.0))
            .field("paddr", &format_args!("{:#x}", self.paddr()))
            .field("present", &self.is_present())
            .field(
                "flags",
                &PageTableFlags::from_bits_truncate(self.0 & !Self::PHYS_ADDR_MASK),
            )
            .field("prop", &self.prop())
            .finish()
    }
}

pub(crate) unsafe fn __memcpy_fallible(dst: *mut u8, src: *const u8, size: usize) -> usize {
    // TODO: Implement this fallible operation.
    unsafe { core::ptr::copy(src, dst, size) };
    0
}

pub(crate) unsafe fn __memset_fallible(dst: *mut u8, value: u8, size: usize) -> usize {
    // TODO: Implement this fallible operation.
    unsafe { core::ptr::write_bytes(dst, value, size) };
    0
}

pub(crate) unsafe fn __atomic_load_fallible(ptr: *const u32) -> u64 {
    // TODO: Implement this fallible operation.
    unsafe { core::intrinsics::atomic_load_relaxed(ptr) as u64 }
}

pub(crate) unsafe fn __atomic_cmpxchg_fallible(ptr: *mut u32, old_val: u32, new_val: u32) -> u64 {
    // TODO: Implement this fallible operation.
    unsafe { core::intrinsics::atomic_cxchg_relaxed_relaxed(ptr, old_val, new_val).0 as u64 }
}
