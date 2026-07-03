// SPDX-License-Identifier: MPL-2.0

pub(super) use ostd::arch::irq::MappedIrqLine;

pub(super) fn probe_for_device() {
    super::device_tree::probe_from_device_tree();
}
