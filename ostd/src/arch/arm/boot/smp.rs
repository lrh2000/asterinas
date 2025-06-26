// SPDX-License-Identifier: MPL-2.0

//! Multiprocessor Boot Support

use crate::{boot::smp::PerApRawInfo, mm::Paddr};

pub(crate) fn count_processors() -> Option<u32> {
    Some(1)
}

/// Brings up all application processors.
///
/// Following the x86 naming, all the harts that are not the bootstrapping hart
/// are "application processors".
///
/// # Safety
///
/// The caller must ensure that
///  1. we're in the boot context of the BSP,
///  2. all APs have not yet been booted, and
///  3. the arguments are valid to boot APs.
pub(crate) unsafe fn bringup_all_aps(
    _info_ptr: *const PerApRawInfo,
    _pr_ptr: Paddr,
    _num_cpus: u32,
) {
    unimplemented!()
}
