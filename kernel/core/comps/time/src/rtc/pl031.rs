// SPDX-License-Identifier: MPL-2.0

//! PL031 RTC.
//!
//! This is a driver for ARM PrimeCell Real Time Clock (PL031).
//!
//! Reference: <https://developer.arm.com/documentation/ddi0224/b/Functional-Overview/ARM-PrimeCell-Real-Time-Clock--PL031--overview>

use chrono::DateTime;
use ostd::{io::IoMem, mm::VmIoOnce};

use crate::{SystemTime, rtc::Driver};

pub struct RtcPl031 {
    io_mem: IoMem,
}

impl Driver for RtcPl031 {
    fn try_new() -> Option<Self> {
        let io_mem = super::device_tree::probe_from_device_tree(&["arm,pl031"])?;

        Some(Self { io_mem })
    }

    fn read_rtc(&self) -> SystemTime {
        const RTCDR_OFFSET: usize = 0;

        let timestamp = self.io_mem.read_once::<u32>(RTCDR_OFFSET).unwrap();

        // This won't fail because the timestamp is a 32-bit integer.
        let time = DateTime::from_timestamp(timestamp as i64, 0).unwrap();
        SystemTime::from(time.naive_utc())
    }
}
