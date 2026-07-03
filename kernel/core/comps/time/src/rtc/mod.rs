// SPDX-License-Identifier: MPL-2.0

use alloc::sync::Arc;

use crate::SystemTime;

/// Generic interface for RTC drivers.
pub trait Driver {
    /// Creates a RTC driver.
    /// Returns [`Some<Self>`] on success, [`None`] otherwise (e.g. platform unsupported).
    fn try_new() -> Option<Self>
    where
        Self: Sized;

    /// Reads RTC.
    fn read_rtc(&self) -> SystemTime;
}

macro_rules! declare_rtc_drivers {
    ( $( #[cfg $cfg:tt ] $module:ident :: $name:ident),* $(,)? ) => {
        pub fn init_rtc_driver() -> Arc<dyn Driver + Send + Sync> {
            // Iterate all possible drivers and pick one that can be initialized.
            $(
                #[cfg $cfg]
                if let Some(driver) = $module::$name::try_new() {
                    return Arc::new(driver);
                }
            )*

            ostd::warn!("No RTC device found, falling back to a dummy RTC");

            Arc::new(RtcDummy)
        }
    }
}

#[cfg(target_arch = "x86_64")]
mod cmos;
#[cfg(target_arch = "riscv64")]
mod goldfish;
#[cfg(target_arch = "loongarch64")]
mod loongson;
#[cfg(target_arch = "aarch64")]
mod pl031;

declare_rtc_drivers! {
    #[cfg(target_arch = "x86_64")] cmos::RtcCmos,
    #[cfg(target_arch = "riscv64")] goldfish::RtcGoldfish,
    #[cfg(target_arch = "loongarch64")] loongson::RtcLoongson,
    #[cfg(target_arch = "aarch64")] pl031::RtcPl031,
}

struct RtcDummy;

impl Driver for RtcDummy {
    fn try_new() -> Option<Self> {
        Some(Self)
    }

    fn read_rtc(&self) -> SystemTime {
        SystemTime {
            year: 1970,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            nanos: 0,
        }
    }
}

#[cfg(any(
    target_arch = "riscv64",
    target_arch = "loongarch64",
    target_arch = "aarch64"
))]
mod device_tree {
    use ostd::{arch::boot::DEVICE_TREE, io::IoMem, warn};

    /// Probes a RTC node from the device tree.
    ///
    /// The RTC node should have exactly one I/O memory region and the region should be available.
    /// Otherwise, this method will fail.
    pub(super) fn probe_from_device_tree(comptaible: &[&str]) -> Option<IoMem> {
        let device_tree = DEVICE_TREE.get().unwrap();

        let node = device_tree.find_compatible(comptaible)?;

        let Some(mut reg) = node.reg() else {
            warn!(
                "'{}' node should have exactly one `reg` property, but found zero `reg`s",
                node.name
            );
            return None;
        };
        let Some(region) = reg.next() else {
            warn!(
                "'{}' node should have exactly one `reg` property, but found zero `reg`s",
                node.name
            );
            return None;
        };
        if reg.next().is_some() {
            warn!(
                "'{}' node should have exactly one `reg` property, but found {} `reg`s",
                node.name,
                reg.count() + 2
            );
            return None;
        }

        let addr_start = region.starting_address as usize;
        let Some(addr_size) = region.size else {
            warn!("'{}' RTC register region is incomplete", node.name);
            return None;
        };
        let Some(addr_end) = addr_start.checked_add(addr_size) else {
            warn!("'{}' RTC register region size overflows", node.name);
            return None;
        };
        let Ok(io_mem) = IoMem::acquire(addr_start..addr_end) else {
            warn!("Failed to acquire '{}' RTC MMIO region", node.name);
            return None;
        };

        Some(io_mem)
    }
}
