// SPDX-License-Identifier: MPL-2.0

mod connection;
mod listener;
mod port;
mod space;

use alloc::{collections::VecDeque, sync::Arc};
use core::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use aster_softirq::{BottomHalfDisabled, Taskless};
pub(super) use connection::{Connection, ConnectionInner};
pub(super) use listener::{Listener, ListenerInner};
use ostd::sync::SpinLock;
pub(super) use port::BoundPort;
pub(super) use space::vsock_space;
use spin::Once;

// Reference: <https://elixir.bootlin.com/linux/v6.16.8/source/net/vmw_vsock/af_vsock.c#L136>
pub(super) const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
// Reference: <https://elixir.bootlin.com/linux/v6.16.8/source/net/vmw_vsock/virtio_transport_common.c#L82>
pub(super) const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(8);
// Reference: <https://elixir.bootlin.com/linux/v6.16.8/source/net/vmw_vsock/af_vsock.c#L137>
pub(super) const DEFAULT_RX_BUF_SIZE: usize = 256 * 1024;
// Reference: <https://elixir.bootlin.com/linux/v6.16.8/source/net/vmw_vsock/af_vsock.c#L138>
pub(super) const DEFAULT_PENDING_TX_BYTES: usize = 256 * 1024;
// Reference: <https://elixir.bootlin.com/linux/v6.16.8/source/include/linux/socket.h#L313>
pub(super) const MAX_BACKLOG: usize = 4096;
// Reference: <https://elixir.bootlin.com/linux/v6.16.8/source/include/linux/virtio_vsock.h#L114>
pub(super) const VIRTIO_VSOCK_MAX_PKT_BUF_SIZE: usize = 64 * 1024;
pub(super) const CREDIT_UPDATE_THRESHOLD: u32 =
    if DEFAULT_RX_BUF_SIZE / 4 < VIRTIO_VSOCK_MAX_PKT_BUF_SIZE {
        (DEFAULT_RX_BUF_SIZE / 4) as u32
    } else {
        VIRTIO_VSOCK_MAX_PKT_BUF_SIZE as u32
    };

struct Component {
    next_timer_generation: AtomicU64,
    pending_timer_events: SpinLock<VecDeque<connection::ConnectionTimerEvent>, BottomHalfDisabled>,
    timer_taskless: Arc<Taskless>,
}

static COMPONENT: Once<Component> = Once::new();

pub(super) fn init() {
    COMPONENT.call_once(|| Component {
        next_timer_generation: AtomicU64::new(1),
        pending_timer_events: SpinLock::new(VecDeque::new()),
        timer_taskless: Taskless::new(process_pending_timer_events),
    });

    let device_name = aster_virtio::device::vsock::DEVICE_NAME;
    if aster_virtio::device::vsock::get_device(device_name).is_none() {
        return;
    }

    aster_virtio::device::vsock::register_recv_callback(device_name, || {
        vsock_space().process_rx(device_name);
    });
    aster_virtio::device::vsock::register_event_callback(device_name, || {
        vsock_space().process_event(device_name);
    });
}

fn component() -> &'static Component {
    COMPONENT
        .get()
        .expect("vsock backend should be initialized")
}

fn next_timer_generation() -> u64 {
    component()
        .next_timer_generation
        .fetch_add(1, Ordering::Relaxed)
}

fn push_timer_event(event: connection::ConnectionTimerEvent) {
    let component = component();
    component.pending_timer_events.lock().push_back(event);
    component.timer_taskless.schedule();
}

fn process_pending_timer_events() {
    let component = component();
    let events = {
        let mut pending = component.pending_timer_events.lock();
        pending.drain(..).collect()
    };
    vsock_space().process_timer_events(events);
}
