// SPDX-License-Identifier: MPL-2.0

use ostd::util::local::Local;

use crate::fs::cgroupfs::{fs::CgroupFsType, systree_node::CgroupSystem};

mod fs;
mod inode;
mod systree_node;

// This method should be called during kernel file system initialization,
// _after_ `aster_systree::init`.
pub(super) fn init() {
    let cgroupfs_type = Local::new(CgroupFsType);
    super::registry::register(cgroupfs_type, Some(CgroupSystem::singleton().clone() as _)).unwrap();
}
