// SPDX-License-Identifier: MPL-2.0

mod fs;
mod inode;
#[cfg(ktest)]
mod test;

use fs::SysFsType;
use ostd::util::local::Local;

// This method should be called during kernel file system initialization,
// _after_ `aster_systree::init`.
pub fn init() {
    let sysfs_type = Local::new(SysFsType);
    super::registry::register(sysfs_type, None).unwrap();
}
