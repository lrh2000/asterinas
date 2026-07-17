// SPDX-License-Identifier: MPL-2.0

use core::{arch::asm, ops::Range};

pub(crate) use util::{
    __atomic_cmpxchg_fallible, __atomic_load_fallible, __memcpy_fallible, __memset_fallible,
};

use crate::mm::{
    PAGE_SIZE, Paddr, PagingConstsTrait, PagingLevel, PodOnce, Vaddr,
    dma::DmaDirection,
    page_prop::{
        CachePolicy, PageFlags, PageProperty, PageTableFlags, PrivilegedPageFlags as PrivFlags,
    },
    page_table::{PteScalar, PteTrait},
};

mod util;

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

/// The paging constants used by the non-sign-extended `TTBR0_EL1` region.
#[derive(Clone, Debug, Default)]
pub(crate) struct UserPagingConsts {}

impl PagingConstsTrait for UserPagingConsts {
    const BASE_PAGE_SIZE: usize = PagingConsts::BASE_PAGE_SIZE;
    const NR_LEVELS: PagingLevel = PagingConsts::NR_LEVELS;
    const ADDRESS_WIDTH: usize = PagingConsts::ADDRESS_WIDTH;
    const VA_SIGN_EXT: bool = false;
    const HIGHEST_TRANSLATION_LEVEL: PagingLevel = PagingConsts::HIGHEST_TRANSLATION_LEVEL;
    const PTE_SIZE: usize = PagingConsts::PTE_SIZE;
}

/// Whether userspace page tables contain the kernel's top-level mappings.
pub(crate) const USER_PAGE_TABLE_SHARES_KERNEL: bool = false;
/// The top-level entries managed by userspace page tables.
pub(crate) const USER_TOP_LEVEL_INDEX_RANGE: Range<usize> = 0..512;

bitflags::bitflags! {
    /// Possible flags for a page table entry.
    #[repr(C)]
    #[derive(Pod)]
    pub(crate) struct PteFlags: usize {
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
    // SAFETY: This invalidates the TLB, which doesn't affect the memory safety.
    unsafe {
        asm!(
            "dsb ishst",
            "tlbi vaae1, {vpn}",
            "dsb ish",
            "isb",
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
    // ARM does not provide a way to exclude global pages and flush all
    // other TLB entries. Therefore, we flush all, including global pages.
    tlb_flush_all_including_global();
}

pub(crate) fn tlb_flush_all_including_global() {
    // SAFETY: This invalidates the TLB, which doesn't affect the memory safety.
    unsafe { asm!("dsb ishst", "tlbi vmalle1", "dsb ish", "isb") };
}

pub(crate) fn can_sync_dma() -> bool {
    true
}

/// # Safety
///
/// The caller must ensure that
///  - the virtual address range and DMA direction correspond correctly to a
///    DMA region;
///  - `can_sync_dma()` is `true`.
pub(crate) unsafe fn sync_dma_range<D: DmaDirection>(range: Range<Vaddr>) {
    use core::sync::atomic::{AtomicUsize, Ordering};

    static CACHE_LINE_SIZE: AtomicUsize = AtomicUsize::new(0);

    let mut cache_line_size = CACHE_LINE_SIZE.load(Ordering::Relaxed);
    if cache_line_size == 0 {
        let dmin_line = {
            let ctr: usize;
            // SAFETY: It is safe to read the Cache Type Register (CTR).
            unsafe { asm!("mrs {}, ctr_el0", out(reg) ctr) };
            // DminLine, bits [19:16]: Log2 of the number of words in the smallest cache line.
            (ctr >> 16) & 0xf
        };
        // A word contains 4 bytes.
        cache_line_size = 4 << dmin_line;
        CACHE_LINE_SIZE.store(cache_line_size, Ordering::Relaxed);
    }

    for vaddr in range.step_by(cache_line_size) {
        // Performing cache maintenance operations is required for correctness
        // on systems with non-coherent DMA.
        // SAFETY: The caller ensures that the virtual address range corresponds
        // to a DMA region. So the underlying memory is untyped and the operations
        // are safe to perform.
        unsafe {
            match (D::CAN_READ_FROM_DEVICE, D::CAN_WRITE_TO_DEVICE) {
                (false, true) => asm!("dc ivac, {}", in(reg) vaddr),
                (true, false) => asm!("dc cvac, {}", in(reg) vaddr),
                (true, true) => asm!("dc civac, {}", in(reg) vaddr),
                _ => unreachable!(),
            }
        }
    }
}

/// Activates the given userspace root-level page table in `TTBR0_EL1`.
///
/// # Safety
///
/// Changing the root-level page table is unsafe, because it's possible to violate memory safety by
/// changing the page mapping.
pub(crate) unsafe fn activate_page_table(root_paddr: Paddr) {
    // SAFETY: The safety is upheld by the caller.
    unsafe {
        asm!(
            "msr ttbr0_el1, {root_paddr}",
            "isb",
            root_paddr = in(reg) root_paddr,
            options(nomem, nostack, preserves_flags),
        );
    }
    tlb_flush_all_excluding_global();
}

/// Activates the kernel root-level page table during CPU initialization.
///
/// # Safety
///
/// The caller must ensure that the root contains all mappings needed to
/// continue kernel execution and that this is the CPU's first managed root.
pub(crate) unsafe fn activate_kernel_page_table(root_paddr: Paddr) {
    // SAFETY: The safety is upheld by the caller.
    unsafe {
        asm!(
            // `TTBR0_EL1` must stop using the boot root before it is
            // dismissed. It remains a temporary kernel-root alias until the
            // first userspace root is activated on this CPU.
            "msr ttbr0_el1, {root_paddr}",
            "msr ttbr1_el1, {root_paddr}",
            "isb",
            root_paddr = in(reg) root_paddr,
            options(nomem, nostack, preserves_flags),
        );
    }
    tlb_flush_all_including_global();
}

pub(crate) fn current_page_table_paddr() -> Paddr {
    let root_paddr;
    // SAFETY: It is safe to read the root-level page table address.
    unsafe {
        asm!(
            "mrs {root_paddr}, ttbr0_el1",
            root_paddr = out(reg) root_paddr,
            options(nomem, nostack, preserves_flags),
        );
    }
    root_paddr
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Pod)]
pub(crate) struct PageTableEntry(usize);

/// Parses a bit-flag bits `val` in the representation of `from` to `to` in bits.
macro_rules! parse_flags {
    ($val:expr, $from:expr, $to:expr) => {
        (($val as usize & $from.bits() as usize) >> $from.bits().ilog2() << $to.bits().ilog2())
    };
}

impl PageTableEntry {
    const PHYS_ADDR_MASK: usize = 0x0000_FFFF_FFFF_F000;

    fn is_present(&self) -> bool {
        if self.0 & PteFlags::VALID.bits() != 0 {
            // Child page tables and readable pages.
            true
        } else if self.0 & PteFlags::SH_INNER.bits() != 0 {
            // Non-readable pages (`new_page()` always sets `SH_INNER`).
            true
        } else {
            // Nothing.
            false
        }
    }

    fn is_last(&self, level: PagingLevel) -> bool {
        level == 1 || self.0 & PteFlags::NON_HUGE.bits() == 0
    }

    fn paddr(&self) -> Paddr {
        self.0 & Self::PHYS_ADDR_MASK
    }

    fn prop(&self) -> PageProperty {
        let flags = parse_flags!(self.0, PteFlags::VALID, PageFlags::R)
            | parse_flags!(!self.0, PteFlags::NO_WRITE, PageFlags::W)
            | parse_flags!(!self.0, PteFlags::NO_EXECUTE, PageFlags::X)
            | parse_flags!(self.0, PteFlags::ACCESSED, PageFlags::ACCESSED)
            | parse_flags!(self.0, PteFlags::DIRTY, PageFlags::DIRTY)
            | parse_flags!(self.0, PteFlags::HIGH_IGN2, PageFlags::AVAIL2);

        let priv_flags = parse_flags!(self.0, PteFlags::USER, PrivFlags::USER)
            | parse_flags!(!self.0, PteFlags::NON_GLOBAL, PrivFlags::GLOBAL)
            | parse_flags!(self.0, PteFlags::HIGH_IGN1, PrivFlags::AVAIL1);

        let cache = if self.0 & PteFlags::ATTR_DEVICE.bits() != 0 {
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

    fn pt_flags(&self) -> PageTableFlags {
        let bits = PageTableFlags::empty().bits() as usize
            | parse_flags!(self.0, PteFlags::HIGH_IGN1, PageTableFlags::AVAIL1)
            | parse_flags!(self.0, PteFlags::HIGH_IGN2, PageTableFlags::AVAIL2);
        PageTableFlags::from_bits(bits as u8).unwrap()
    }

    fn new_page(paddr: Paddr, level: PagingLevel, prop: PageProperty) -> Self {
        // FIXME: To avoid the Access Flag Fault,
        // we set the ACCESSED bit to 1 all the time.
        let mut flags = PteFlags::ACCESSED.bits();
        if level == 1 {
            flags |= PteFlags::NON_HUGE.bits();
        }

        flags |= parse_flags!(prop.flags.bits(), PageFlags::R, PteFlags::VALID)
            | parse_flags!(!prop.flags.bits(), PageFlags::W, PteFlags::NO_WRITE)
            | parse_flags!(!prop.flags.bits(), PageFlags::X, PteFlags::NO_EXECUTE)
            | parse_flags!(prop.flags.bits(), PageFlags::ACCESSED, PteFlags::ACCESSED)
            | parse_flags!(prop.flags.bits(), PageFlags::DIRTY, PteFlags::DIRTY)
            | parse_flags!(prop.priv_flags.bits(), PrivFlags::USER, PteFlags::USER)
            | parse_flags!(
                !prop.priv_flags.bits(),
                PrivFlags::GLOBAL,
                PteFlags::NON_GLOBAL
            )
            | parse_flags!(
                prop.priv_flags.bits(),
                PrivFlags::AVAIL1,
                PteFlags::HIGH_IGN1
            )
            | parse_flags!(prop.flags.bits(), PageFlags::AVAIL2, PteFlags::HIGH_IGN2);

        flags |= PteFlags::SH_INNER.bits();
        match prop.cache {
            CachePolicy::Writeback => (),
            CachePolicy::Uncacheable => {
                // TODO: Currently Asterinas uses `Uncacheable` only for I/O
                // memory. Normal memory can also be `Noncacheable`, where the
                // attribute should not be set to `ATTR_DEVICE`.
                flags |= PteFlags::ATTR_DEVICE.bits();
            }
            _ => panic!("unsupported cache policy"),
        }

        debug_assert_eq!(
            paddr & !Self::PHYS_ADDR_MASK,
            0,
            "page physical address contains invalid bits"
        );
        Self(paddr | flags)
    }

    fn new_pt(paddr: Paddr, flags: PageTableFlags) -> Self {
        let flags = PteFlags::VALID.bits()
            | PteFlags::NON_HUGE.bits()
            | parse_flags!(flags.bits(), PageTableFlags::AVAIL1, PteFlags::HIGH_IGN1)
            | parse_flags!(flags.bits(), PageTableFlags::AVAIL2, PteFlags::HIGH_IGN2);

        debug_assert_eq!(
            paddr & !Self::PHYS_ADDR_MASK,
            0,
            "page table physical address contains invalid bits"
        );
        Self(paddr | flags)
    }
}

impl PodOnce for PageTableEntry {}

// SAFETY: The implementation is safe because:
//  - `from_usize` and `into_usize` are not overridden;
//  - `from_repr` and `repr` are correctly implemented;
//  - a zeroed PTE represents an absent entry.
unsafe impl PteTrait for PageTableEntry {
    fn from_repr(repr: &PteScalar, level: PagingLevel) -> Self {
        match repr {
            PteScalar::Absent => PageTableEntry(0),
            PteScalar::PageTable(paddr, flags) => Self::new_pt(*paddr, *flags),
            PteScalar::Mapped(paddr, prop) => Self::new_page(*paddr, level, *prop),
        }
    }

    fn to_repr(&self, level: PagingLevel) -> PteScalar {
        if !self.is_present() {
            return PteScalar::Absent;
        }

        if self.is_last(level) {
            PteScalar::Mapped(self.paddr(), self.prop())
        } else {
            PteScalar::PageTable(self.paddr(), self.pt_flags())
        }
    }
}
