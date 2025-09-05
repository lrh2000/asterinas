// SPDX-License-Identifier: MPL-2.0

mod fs;

use ostd::util::local::Local;

use crate::fs::overlayfs::fs::OverlayFsType;

pub(super) fn init() {
    let overlay_type = Local::new(OverlayFsType);
    super::registry::register(overlay_type, None).unwrap();
}
