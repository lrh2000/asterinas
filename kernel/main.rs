// SPDX-License-Identifier: MPL-2.0

#![no_std]
#![no_main]
// The `export_name` attribute for the `aster_main` entrypoint requires the removal of safety check.
// Please be aware that the kernel is not allowed to introduce any other unsafe operations.
// #![forbid(unsafe_code)]
extern crate aster_frame;

use aster_frame::early_println;

#[export_name = "aster_main"]
pub fn main() -> ! {
    aster_frame::init();
    early_println!("[kernel] finish init aster_frame");
    component::init_all(component::parse_metadata!()).unwrap();
    aster_nix::init();
    aster_nix::run_first_process();
}
