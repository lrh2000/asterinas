// SPDX-License-Identifier: MPL-2.0

mod buffer;
mod config;
pub mod device;
mod header;

use alloc::{
    boxed::Box,
    collections::{BTreeMap, BTreeSet},
    string::String,
    sync::Arc,
    vec::Vec,
};

use aster_softirq::{BottomHalfDisabled, Taskless};
pub use buffer::{RxBuffer, TxBuffer, TxBufferBuilder, new_tx_buffer_builder};
pub use device::{TxReservation, TxSubmit, VsockDevice};
pub use header::{
    VirtioVsockEvent, VirtioVsockEventId, VirtioVsockHdr, VirtioVsockOp, VirtioVsockShutdownFlags,
};
use ostd::sync::SpinLock;
use spin::Once;

pub const DEVICE_NAME: &str = "Virtio-Vsock";

type Callback = Arc<dyn Fn() + Send + Sync + 'static>;

pub trait TxCompletion: Send + Sync {
    fn on_pending_submit(self: Box<Self>) {}
}

struct DeviceEntry {
    device: Arc<VsockDevice>,
    recv_callbacks: Vec<Callback>,
    event_callbacks: Vec<Callback>,
}

struct Component {
    devices: SpinLock<BTreeMap<String, DeviceEntry>, BottomHalfDisabled>,
    rx_pending: SpinLock<BTreeSet<String>, BottomHalfDisabled>,
    tx_pending: SpinLock<BTreeSet<String>, BottomHalfDisabled>,
    event_pending: SpinLock<BTreeSet<String>, BottomHalfDisabled>,
    rx_taskless: Arc<Taskless>,
    tx_taskless: Arc<Taskless>,
    event_taskless: Arc<Taskless>,
}

static COMPONENT: Once<Component> = Once::new();

pub(crate) fn init() {
    buffer::init();
    COMPONENT.call_once(|| Component {
        devices: SpinLock::new(BTreeMap::new()),
        rx_pending: SpinLock::new(BTreeSet::new()),
        tx_pending: SpinLock::new(BTreeSet::new()),
        event_pending: SpinLock::new(BTreeSet::new()),
        rx_taskless: Taskless::new(process_pending_rx),
        tx_taskless: Taskless::new(process_pending_tx),
        event_taskless: Taskless::new(process_pending_event),
    });
}

pub fn register_device(name: String, device: Arc<VsockDevice>) {
    let component = COMPONENT.get().unwrap();
    component.devices.lock().insert(
        name,
        DeviceEntry {
            device,
            recv_callbacks: Vec::new(),
            event_callbacks: Vec::new(),
        },
    );
}

pub fn get_device(name: &str) -> Option<Arc<VsockDevice>> {
    let component = COMPONENT.get().unwrap();
    component
        .devices
        .lock()
        .get(name)
        .map(|entry| entry.device.clone())
}

pub fn all_devices() -> Vec<(String, Arc<VsockDevice>)> {
    let component = COMPONENT.get().unwrap();
    component
        .devices
        .lock()
        .iter()
        .map(|(name, entry)| (name.clone(), entry.device.clone()))
        .collect()
}

pub fn register_recv_callback(name: &str, callback: impl Fn() + Send + Sync + 'static) {
    let component = COMPONENT.get().unwrap();
    let mut devices = component.devices.lock();
    let Some(entry) = devices.get_mut(name) else {
        return;
    };
    entry.recv_callbacks.push(Arc::new(callback));
}

pub fn register_event_callback(name: &str, callback: impl Fn() + Send + Sync + 'static) {
    let component = COMPONENT.get().unwrap();
    let mut devices = component.devices.lock();
    let Some(entry) = devices.get_mut(name) else {
        return;
    };
    entry.event_callbacks.push(Arc::new(callback));
}

pub(crate) fn notify_recv(name: &str) {
    let component = COMPONENT.get().unwrap();
    let callbacks = component
        .devices
        .lock()
        .get(name)
        .map(|entry| entry.recv_callbacks.clone())
        .unwrap_or_default();

    for callback in callbacks {
        callback();
    }
}

pub(crate) fn notify_event(name: &str) {
    let component = COMPONENT.get().unwrap();
    let callbacks = component
        .devices
        .lock()
        .get(name)
        .map(|entry| entry.event_callbacks.clone())
        .unwrap_or_default();

    for callback in callbacks {
        callback();
    }
}

pub(crate) fn schedule_rx(name: &str) {
    let component = COMPONENT.get().unwrap();
    component.rx_pending.lock().insert(name.into());
    component.rx_taskless.schedule();
}

pub(crate) fn schedule_tx(name: &str) {
    let component = COMPONENT.get().unwrap();
    component.tx_pending.lock().insert(name.into());
    component.tx_taskless.schedule();
}

pub(crate) fn schedule_event(name: &str) {
    let component = COMPONENT.get().unwrap();
    component.event_pending.lock().insert(name.into());
    component.event_taskless.schedule();
}

fn process_pending_rx() {
    let component = COMPONENT.get().unwrap();
    let device_names = take_pending(&component.rx_pending);
    for device_name in device_names {
        if let Some(device) = get_device(&device_name) {
            device.process_rx();
        }
    }
}

fn process_pending_tx() {
    let component = COMPONENT.get().unwrap();
    let device_names = take_pending(&component.tx_pending);
    for device_name in device_names {
        if let Some(device) = get_device(&device_name) {
            device.process_tx();
        }
    }
}

fn process_pending_event() {
    let component = COMPONENT.get().unwrap();
    let device_names = take_pending(&component.event_pending);
    for device_name in device_names {
        if let Some(device) = get_device(&device_name) {
            device.process_event();
        }
    }
}

fn take_pending(pending: &SpinLock<BTreeSet<String>, BottomHalfDisabled>) -> Vec<String> {
    let mut pending = pending.lock();
    let device_names = pending.iter().cloned().collect();
    pending.clear();
    device_names
}
