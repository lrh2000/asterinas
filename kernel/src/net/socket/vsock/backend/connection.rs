// SPDX-License-Identifier: MPL-2.0

use core::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};

use aster_softirq::BottomHalfDisabled;
use aster_virtio::device::vsock::header::{
    VirtioVsockHdr, VirtioVsockOp, VirtioVsockShutdownFlags,
};
use ostd::sync::SpinLock;
use spin::once::Once;
use takeable::Takeable;

use super::{
    BoundPort, CREDIT_UPDATE_THRESHOLD, DEFAULT_CLOSE_TIMEOUT, DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_PENDING_TX_BYTES, DEFAULT_RX_BUF_SIZE,
};
use crate::{
    events::IoEvents,
    net::socket::{
        util::{SendRecvFlags, SockShutdownCmd},
        vsock::addr::VsockSocketAddr,
    },
    prelude::*,
    process::signal::Pollee,
    time::{Timer, clocks::JIFFIES_TIMER_MANAGER, timer::Timeout},
    util::{MultiRead, MultiWrite},
};

struct ReleasePendingBytes {
    connection: Arc<ConnectionInner>,
    bytes: usize,
}

impl TxCompletion for ReleasePendingBytes {
    fn on_pending_submit(self: Box<Self>) {
        self.connection.mark_credit_reported();
    }
}

impl Drop for ReleasePendingBytes {
    fn drop(&mut self) {
        self.connection.release_tx_bytes(self.bytes);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PendingSendAction {
    MarkCreditReported,
    ArmConnectTimeout,
    ArmCloseTimeout,
}

impl PendingSendAction {
    pub(super) fn apply_now(&self, connection: &ConnectionInner) {
        connection.mark_credit_reported();
        match self {
            Self::MarkCreditReported => {}
            Self::ArmConnectTimeout => connection.arm_connect_timeout(),
            Self::ArmCloseTimeout => connection.arm_close_timeout(),
        }
    }
}

pub(super) struct DeferredConnectionSend {
    conn_id: ConnId,
    action: PendingSendAction,
}

impl DeferredConnectionSend {
    pub(super) fn new(conn_id: ConnId, action: PendingSendAction) -> Self {
        Self { conn_id, action }
    }
}

impl TxCompletion for DeferredConnectionSend {
    fn on_pending_submit(self: Box<Self>) {
        super::space::vsock_space().apply_pending_send_action(self.conn_id, self.action);
    }
}

pub(in crate::net::socket::vsock) struct Connection {
    inner: Takeable<Arc<ConnectionInner>>,
}

impl Connection {
    pub(in crate::net::socket::vsock) fn new(inner: Arc<ConnectionInner>) -> Self {
        Self {
            inner: Takeable::new(inner),
        }
    }

    pub(in crate::net::socket::vsock) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr {
        self.inner.local_addr(guest_cid)
    }

    pub(in crate::net::socket::vsock) fn remote_addr(&self) -> VsockSocketAddr {
        self.inner.remote_addr()
    }

    pub(in crate::net::socket::vsock) fn has_result(&self) -> bool {
        self.inner.has_result()
    }

    pub(in crate::net::socket::vsock) fn init_pollee(&self, pollee: Pollee) {
        self.inner.init_pollee(pollee);
    }

    pub(in crate::net::socket::vsock) fn finish_connect(&mut self) -> Result<()> {
        let result = self.inner.finish_connect();
        if result.is_err() {
            super::space::vsock_space().remove_connection(&self.inner.conn_id());
        }
        result
    }

    pub(in crate::net::socket::vsock) fn into_inner(mut self) -> Option<ConnectionInner> {
        Arc::into_inner(self.inner.take())
    }

    pub(in crate::net::socket::vsock) fn try_recv(
        &mut self,
        writer: &mut dyn MultiWrite,
        flags: SendRecvFlags,
    ) -> Result<usize> {
        let read_len = self.inner.try_recv(writer, flags)?;
        if read_len == 0 {
            return Ok(0);
        }

        let guest_cid = super::space::vsock_space().guest_cid();
        if let Some(header) = self.inner.make_credit_update_header_if_needed(guest_cid) {
            let completion = Box::new(DeferredConnectionSend::new(
                self.inner.conn_id(),
                PendingSendAction::MarkCreditReported,
            ));
            if matches!(
                super::space::vsock_space().send_packet(header, Some(completion)),
                Ok(TxSubmit::SubmittedToQueue)
            ) {
                self.inner.mark_credit_reported();
            }
        }

        Ok(read_len)
    }

    pub(in crate::net::socket::vsock) fn shutdown(&mut self, cmd: SockShutdownCmd) -> Result<()> {
        let shutdown_action = self.inner.prepare_local_shutdown(cmd);
        if shutdown_action.shutdown_flags.is_empty() {
            return Ok(());
        }

        let header = self.inner.make_header(
            super::space::vsock_space().guest_cid(),
            VirtioVsockOp::Shutdown,
            0,
            shutdown_action.shutdown_flags.bits(),
        );
        let completion_action = if shutdown_action.arm_close_timeout {
            PendingSendAction::ArmCloseTimeout
        } else {
            PendingSendAction::MarkCreditReported
        };
        let completion = Box::new(DeferredConnectionSend::new(
            self.inner.conn_id(),
            completion_action,
        ));
        if matches!(
            super::space::vsock_space().send_packet(header, Some(completion))?,
            TxSubmit::SubmittedToQueue
        ) {
            self.inner.mark_credit_reported();
            if shutdown_action.arm_close_timeout {
                self.inner.arm_close_timeout();
            }
        }
        if shutdown_action.send_rst {
            super::space::vsock_space().remove_connection_instance(&self.inner);
            let rst_header = self.inner.make_header(
                super::space::vsock_space().guest_cid(),
                VirtioVsockOp::Rst,
                0,
                0,
            );
            let completion = Box::new(DeferredConnectionSend::new(
                self.inner.conn_id(),
                PendingSendAction::MarkCreditReported,
            ));
            if matches!(
                super::space::vsock_space().send_packet(rst_header, Some(completion)),
                Ok(TxSubmit::SubmittedToQueue)
            ) {
                self.inner.mark_credit_reported();
            }
        }
        self.inner.notify_pollee(shutdown_action.notify_events);
        Ok(())
    }

    pub(in crate::net::socket::vsock) fn check_io_events(&self) -> IoEvents {
        self.inner.check_io_events()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if !self.inner.is_usable() {
            return;
        }

        let inner = self.inner.take();
        let drop_action = inner.prepare_drop();
        let guest_cid = super::space::vsock_space().guest_cid();

        match drop_action.table_action {
            DropTableAction::Keep => {}
            DropTableAction::MoveToClosing => {
                super::space::vsock_space().move_connection_to_closing(inner.conn_id());
            }
            DropTableAction::Remove => {
                super::space::vsock_space().remove_connection_instance(&inner);
            }
        }

        if !drop_action.shutdown_flags.is_empty() {
            let shutdown_header = inner.make_header(
                guest_cid,
                VirtioVsockOp::Shutdown,
                0,
                drop_action.shutdown_flags.bits(),
            );
            let completion_action = if drop_action.arm_close_timeout {
                PendingSendAction::ArmCloseTimeout
            } else {
                PendingSendAction::MarkCreditReported
            };
            let completion = Box::new(DeferredConnectionSend::new(
                inner.conn_id(),
                completion_action,
            ));
            if matches!(
                super::space::vsock_space().send_packet(shutdown_header, Some(completion)),
                Ok(TxSubmit::SubmittedToQueue)
            ) {
                inner.mark_credit_reported();
                if drop_action.arm_close_timeout {
                    inner.arm_close_timeout();
                }
            }
        }
        if drop_action.send_rst {
            let rst_header = inner.make_header(guest_cid, VirtioVsockOp::Rst, 0, 0);
            let completion = Box::new(DeferredConnectionSend::new(
                inner.conn_id(),
                PendingSendAction::MarkCreditReported,
            ));
            if matches!(
                super::space::vsock_space().send_packet(rst_header, Some(completion)),
                Ok(TxSubmit::SubmittedToQueue)
            ) {
                inner.mark_credit_reported();
            }
        }
    }
}

pub(in crate::net::socket::vsock) struct ConnectionInner {
    conn_id: ConnId,
    bound_port: BoundPort,
    pollee: Once<Pollee>,
    state: SpinLock<ConnectionState, BottomHalfDisabled>,
    timer: SpinLock<Option<ConnectionTimerState>, BottomHalfDisabled>,
    available_tx_bytes: AtomicUsize,
    is_connect_result_ready: AtomicBool,
}

struct ConnectionState {
    phase: Phase,
    remote_addr: VsockSocketAddr,
    error: Option<Error>,
    rx_queue: RxQueue,
    credit: CreditState,
    shutdown: ShutdownState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Connecting,
    Connected,
    Closing,
    Closed,
    Reset,
}

struct CreditState {
    peer_buf_alloc: u32,
    peer_fwd_cnt: u32,
    local_fwd_cnt: u32,
    last_reported_fwd_cnt: u32,
    credit_request_pending: bool,
    tx_cnt: u32,
}

struct ShutdownState {
    local_read_closed: bool,
    local_write_closed: bool,
    peer_read_closed: bool,
    peer_write_closed: bool,
}

struct ConnectionTimerState {
    generation: u64,
    timer: Arc<Timer>,
}

pub(super) struct ConnectionTimerEvent {
    pub(super) conn_id: ConnId,
    pub(super) generation: u64,
}

pub(super) struct ShutdownAction {
    pub(super) remove_lookup_key: bool,
    pub(super) send_rst: bool,
    pub(super) notify_pollee: Option<Pollee>,
    pub(super) notify_events: IoEvents,
}

struct LocalShutdownAction {
    shutdown_flags: VirtioVsockShutdownFlags,
    notify_events: IoEvents,
    send_rst: bool,
    arm_close_timeout: bool,
}

pub(super) struct TimeoutAction {
    pub(super) notify_pollee: Option<Pollee>,
    pub(super) send_rst: bool,
}

enum DropTableAction {
    Keep,
    MoveToClosing,
    Remove,
}

struct DropAction {
    table_action: DropTableAction,
    shutdown_flags: VirtioVsockShutdownFlags,
    send_rst: bool,
    arm_close_timeout: bool,
}

struct RxQueue {
    packets: VecDeque<aster_virtio::device::vsock::RxBuffer>,
    used_bytes: usize,
    max_bytes: usize,
    read_offset: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ConnId {
    pub(super) local_port: u32,
    pub(super) peer_cid: u32,
    pub(super) peer_port: u32,
}

impl ConnectionInner {
    pub(super) fn new_connecting(bound_port: BoundPort, remote_addr: VsockSocketAddr) -> Arc<Self> {
        Self::new(bound_port, remote_addr, Phase::Connecting)
    }

    pub(super) fn new_connected(bound_port: BoundPort, remote_addr: VsockSocketAddr) -> Arc<Self> {
        Self::new(bound_port, remote_addr, Phase::Connected)
    }

    fn new(bound_port: BoundPort, remote_addr: VsockSocketAddr, phase: Phase) -> Arc<Self> {
        Arc::new(Self {
            conn_id: ConnId {
                local_port: bound_port.port(),
                peer_cid: remote_addr.cid,
                peer_port: remote_addr.port,
            },
            bound_port,
            pollee: Once::new(),
            state: SpinLock::new(ConnectionState {
                phase,
                remote_addr,
                error: None,
                rx_queue: RxQueue {
                    packets: VecDeque::new(),
                    used_bytes: 0,
                    max_bytes: DEFAULT_RX_BUF_SIZE,
                    read_offset: 0,
                },
                credit: CreditState {
                    peer_buf_alloc: 0,
                    peer_fwd_cnt: 0,
                    local_fwd_cnt: 0,
                    last_reported_fwd_cnt: 0,
                    credit_request_pending: false,
                    tx_cnt: 0,
                },
                shutdown: ShutdownState {
                    local_read_closed: false,
                    local_write_closed: false,
                    peer_read_closed: false,
                    peer_write_closed: false,
                },
            }),
            timer: SpinLock::new(None),
            available_tx_bytes: AtomicUsize::new(DEFAULT_PENDING_TX_BYTES),
            is_connect_result_ready: AtomicBool::new(phase == Phase::Connected),
        })
    }

    pub(super) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr {
        self.bound_port.local_addr(guest_cid)
    }

    pub(super) fn remote_addr(&self) -> VsockSocketAddr {
        self.state.lock().remote_addr
    }

    pub(super) const fn conn_id(&self) -> ConnId {
        self.conn_id
    }

    pub(in crate::net::socket::vsock) fn into_bound_port(self) -> BoundPort {
        self.bound_port
    }

    pub(super) fn has_result(&self) -> bool {
        self.is_connect_result_ready.load(Ordering::Acquire)
    }

    pub(super) fn init_pollee(&self, pollee: Pollee) {
        self.pollee.call_once(|| pollee);
    }

    pub(super) fn finish_connect(&self) -> Result<()> {
        let mut state = self.state.lock();
        if matches!(state.phase, Phase::Connecting) {
            return_errno_with_message!(Errno::EAGAIN, "the connection is pending");
        }

        if let Some(error) = state.error.take() {
            return Err(error);
        }

        Ok(())
    }

    pub(super) fn on_response(&self) -> Option<Pollee> {
        self.cancel_timer();
        self.state.lock().phase = Phase::Connected;
        self.is_connect_result_ready.store(true, Ordering::Release);
        self.pollee.get().cloned()
    }

    pub(super) fn on_rst(&self) -> Option<Pollee> {
        self.cancel_timer();
        let mut state = self.state.lock();
        state.phase = Phase::Reset;
        state.error = Some(Error::with_message(
            Errno::ECONNRESET,
            "the connection is reset",
        ));
        self.is_connect_result_ready.store(true, Ordering::Release);
        self.pollee.get().cloned()
    }

    pub(super) fn arm_connect_timeout(&self) {
        self.arm_timeout(DEFAULT_CONNECT_TIMEOUT);
    }

    pub(super) fn arm_close_timeout(&self) {
        self.arm_timeout(DEFAULT_CLOSE_TIMEOUT);
    }

    fn arm_timeout(&self, duration: Duration) {
        let generation = super::next_timer_generation();
        let conn_id = self.conn_id;
        let timer_manager = JIFFIES_TIMER_MANAGER
            .get()
            .expect("jiffies timer manager should be initialized");
        let timer = timer_manager.create_timer(move |_guard| {
            super::push_timer_event(ConnectionTimerEvent {
                conn_id,
                generation,
            });
        });
        timer.lock().set_timeout(Timeout::After(duration));

        let mut timer_state = self.timer.lock();
        if let Some(old_timer) = timer_state.replace(ConnectionTimerState { generation, timer }) {
            old_timer.timer.lock().cancel();
        }
    }

    pub(super) fn on_timeout(&self, generation: u64) -> Option<TimeoutAction> {
        let timer = {
            let mut timer_state = self.timer.lock();
            let active_timer = timer_state.take()?;
            if active_timer.generation != generation {
                *timer_state = Some(active_timer);
                return None;
            }
            active_timer.timer
        };
        timer.lock().cancel();

        let mut state = self.state.lock();
        if matches!(state.phase, Phase::Connecting) {
            state.phase = Phase::Reset;
            state.error = Some(Error::with_message(
                Errno::ETIMEDOUT,
                "the connection timed out",
            ));
            self.is_connect_result_ready.store(true, Ordering::Release);
            return Some(TimeoutAction {
                notify_pollee: self.pollee.get().cloned(),
                send_rst: false,
            });
        }

        if matches!(state.phase, Phase::Closing) {
            state.phase = Phase::Closed;
            return Some(TimeoutAction {
                notify_pollee: self.pollee.get().cloned(),
                send_rst: true,
            });
        }

        None
    }

    pub(super) fn on_shutdown(&self, flags: u32) -> ShutdownAction {
        let mut state = self.state.lock();
        let mut notify_events = IoEvents::empty();
        if flags & VirtioVsockShutdownFlags::SEND.bits() != 0 {
            state.shutdown.peer_write_closed = true;
            notify_events |= IoEvents::IN | IoEvents::RDHUP;
        }
        if flags & VirtioVsockShutdownFlags::RECEIVE.bits() != 0 {
            state.shutdown.peer_read_closed = true;
            notify_events |= IoEvents::OUT | IoEvents::HUP;
        }

        let peer_fully_closed = state.shutdown.peer_read_closed && state.shutdown.peer_write_closed;
        let local_fully_closed =
            state.shutdown.local_read_closed && state.shutdown.local_write_closed;
        let should_send_rst = peer_fully_closed
            && local_fully_closed
            && state.rx_queue.packets.is_empty()
            && state.rx_queue.read_offset == 0;
        if peer_fully_closed {
            state.phase = if should_send_rst {
                Phase::Closed
            } else {
                Phase::Closing
            };
        }

        ShutdownAction {
            remove_lookup_key: peer_fully_closed,
            send_rst: should_send_rst,
            notify_pollee: self.pollee.get().cloned(),
            notify_events,
        }
    }

    pub(super) fn on_credit_update(&self, buf_alloc: u32, fwd_cnt: u32) -> Option<Pollee> {
        let mut state = self.state.lock();
        state.credit.peer_buf_alloc = buf_alloc;
        state.credit.peer_fwd_cnt = fwd_cnt;
        state.credit.credit_request_pending = false;
        self.pollee.get().cloned()
    }

    pub(super) fn enqueue_rx_buffer(
        &self,
        buffer: aster_virtio::device::vsock::RxBuffer,
    ) -> Result<Option<Pollee>> {
        let mut state = self.state.lock();
        let packet_len = buffer.packet_len();
        let Some(new_used_bytes) = state.rx_queue.used_bytes.checked_add(packet_len) else {
            return_errno_with_message!(Errno::ENOMEM, "the receive queue is full");
        };
        if new_used_bytes > state.rx_queue.max_bytes {
            return_errno_with_message!(Errno::ENOMEM, "the receive queue is full");
        }

        state.rx_queue.used_bytes = new_used_bytes;
        state.rx_queue.packets.push_back(buffer);
        Ok(self.pollee.get().cloned())
    }

    pub(super) fn reserve_tx_bytes(&self, want: usize) -> Result<usize> {
        let mut current = self.available_tx_bytes.load(Ordering::Acquire);
        while current != 0 {
            let reserved = current.min(want);
            match self.available_tx_bytes.compare_exchange(
                current,
                current - reserved,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(reserved),
                Err(updated_current) => current = updated_current,
            }
        }

        return_errno_with_message!(Errno::EAGAIN, "the send queue is full")
    }

    pub(super) fn release_tx_bytes(&self, bytes: usize) {
        self.available_tx_bytes.fetch_add(bytes, Ordering::Release);
        self.notify_pollee(IoEvents::OUT);
    }

    pub(super) fn try_recv(
        &self,
        writer: &mut dyn MultiWrite,
        _flags: SendRecvFlags,
    ) -> Result<usize> {
        if let Some(error) = self.test_and_clear_error() {
            return Err(error);
        }

        let (buffer, read_offset) = {
            let mut state = self.state.lock();
            let Some(buffer) = state.rx_queue.packets.pop_front() else {
                if state.shutdown.local_read_closed {
                    return Ok(0);
                }
                if state.shutdown.peer_write_closed {
                    return Ok(0);
                }

                return_errno_with_message!(Errno::EAGAIN, "the receive buffer is empty");
            };

            (buffer, state.rx_queue.read_offset)
        };

        let mut packet = buffer.packet();
        packet.skip(read_offset);
        let read_len = writer.write(&mut packet)?;

        let mut state = self.state.lock();
        state.credit.local_fwd_cnt = state.credit.local_fwd_cnt.saturating_add(read_len as u32);

        let remaining = buffer.packet_len().saturating_sub(read_offset + read_len);
        if remaining == 0 {
            state.rx_queue.used_bytes = state
                .rx_queue
                .used_bytes
                .saturating_sub(buffer.packet_len());
            state.rx_queue.read_offset = 0;
        } else {
            state.rx_queue.read_offset = read_offset + read_len;
            state.rx_queue.packets.push_front(buffer);
        }

        Ok(read_len)
    }

    pub(super) fn make_credit_update_header_if_needed(
        &self,
        guest_cid: u32,
    ) -> Option<VirtioVsockHdr> {
        let state = self.state.lock();
        let reported_delta = state
            .credit
            .local_fwd_cnt
            .wrapping_sub(state.credit.last_reported_fwd_cnt);
        if reported_delta < CREDIT_UPDATE_THRESHOLD {
            return None;
        }

        Some(Self::build_header_from_state(
            guest_cid,
            &state,
            self.conn_id,
            VirtioVsockOp::CreditUpdate,
            0,
            0,
        ))
    }

    pub(super) fn make_header(
        &self,
        guest_cid: u32,
        op: VirtioVsockOp,
        len: u32,
        flags: u32,
    ) -> VirtioVsockHdr {
        let state = self.state.lock();
        Self::build_header_from_state(guest_cid, &state, self.conn_id, op, len, flags)
    }

    fn build_header_from_state(
        guest_cid: u32,
        state: &ConnectionState,
        conn_id: ConnId,
        op: VirtioVsockOp,
        len: u32,
        flags: u32,
    ) -> VirtioVsockHdr {
        let buf_alloc = state.rx_queue.max_bytes.min(u32::MAX as usize) as u32;
        VirtioVsockHdr::new(
            guest_cid as u64,
            state.remote_addr.cid as u64,
            conn_id.local_port,
            conn_id.peer_port,
            len,
            op,
            flags,
            buf_alloc,
            state.credit.local_fwd_cnt,
        )
    }

    pub(super) fn update_tx_cnt(&self, bytes: usize) {
        let mut state = self.state.lock();
        state.credit.tx_cnt = state.credit.tx_cnt.saturating_add(bytes as u32);
    }

    pub(super) fn mark_credit_reported(&self) {
        let mut state = self.state.lock();
        state.credit.last_reported_fwd_cnt = state.credit.local_fwd_cnt;
    }

    pub(super) fn make_credit_request_header_if_needed(
        &self,
        guest_cid: u32,
    ) -> Option<VirtioVsockHdr> {
        let mut state = self.state.lock();
        let peer_available = state.credit.peer_buf_alloc.saturating_sub(
            state
                .credit
                .tx_cnt
                .saturating_sub(state.credit.peer_fwd_cnt),
        );
        if peer_available != 0 || state.credit.credit_request_pending {
            return None;
        }

        state.credit.credit_request_pending = true;
        Some(Self::build_header_from_state(
            guest_cid,
            &state,
            self.conn_id,
            VirtioVsockOp::CreditRequest,
            0,
            0,
        ))
    }

    pub(super) fn rollback_credit_request(&self) {
        self.state.lock().credit.credit_request_pending = false;
    }

    pub(super) fn send_credit_available(&self) -> usize {
        let state = self.state.lock();
        let peer_available = state.credit.peer_buf_alloc.saturating_sub(
            state
                .credit
                .tx_cnt
                .saturating_sub(state.credit.peer_fwd_cnt),
        );
        peer_available as usize
    }

    pub(super) fn check_send_ready(&self) -> Result<()> {
        if let Some(error) = self.test_and_clear_error() {
            return Err(error);
        }

        let state = self.state.lock();
        if !matches!(state.phase, Phase::Connected) {
            return_errno_with_message!(Errno::ENOTCONN, "the connection is not established");
        }
        if state.shutdown.local_write_closed || state.shutdown.peer_read_closed {
            return_errno_with_message!(Errno::EPIPE, "the connection is closed for writing");
        }
        Ok(())
    }

    pub(super) fn check_io_events(&self) -> IoEvents {
        let state = self.state.lock();
        let mut events = IoEvents::empty();
        let peer_available = state.credit.peer_buf_alloc.saturating_sub(
            state
                .credit
                .tx_cnt
                .saturating_sub(state.credit.peer_fwd_cnt),
        );

        if !state.rx_queue.packets.is_empty()
            || state.shutdown.peer_write_closed
            || state.shutdown.local_read_closed
        {
            events |= IoEvents::IN;
        }
        if state.shutdown.local_write_closed
            || state.shutdown.peer_read_closed
            || (matches!(state.phase, Phase::Connected)
                && peer_available != 0
                && self.available_tx_bytes.load(Ordering::Relaxed) > 0)
        {
            events |= IoEvents::OUT;
        }
        if state.error.is_some() {
            events |= IoEvents::ERR;
        }
        if state.shutdown.peer_write_closed {
            events |= IoEvents::RDHUP;
        }
        if matches!(state.phase, Phase::Closed | Phase::Reset) {
            events |= IoEvents::HUP;
        }

        events
    }

    pub(super) fn test_and_clear_error(&self) -> Option<Error> {
        self.state.lock().error.take()
    }

    fn notify_pollee(&self, events: IoEvents) {
        let Some(pollee) = self.pollee.get() else {
            return;
        };
        pollee.notify(events);
    }

    fn cancel_timer(&self) {
        let timer = self
            .timer
            .lock()
            .take()
            .map(|timer_state| timer_state.timer);
        if let Some(timer) = timer {
            timer.lock().cancel();
        }
    }

    fn prepare_local_shutdown(&self, cmd: SockShutdownCmd) -> LocalShutdownAction {
        let mut notify_events = IoEvents::empty();
        let mut shutdown_flags = VirtioVsockShutdownFlags::empty();
        let mut state = self.state.lock();

        if cmd.shut_read() && !state.shutdown.local_read_closed {
            state.shutdown.local_read_closed = true;
            shutdown_flags |= VirtioVsockShutdownFlags::RECEIVE;
            notify_events |= IoEvents::IN | IoEvents::RDHUP;
        }

        if cmd.shut_write() && !state.shutdown.local_write_closed {
            state.shutdown.local_write_closed = true;
            shutdown_flags |= VirtioVsockShutdownFlags::SEND;
            notify_events |= IoEvents::OUT | IoEvents::HUP;
        }

        let local_fully_closed =
            state.shutdown.local_read_closed && state.shutdown.local_write_closed;
        let peer_fully_closed = state.shutdown.peer_read_closed && state.shutdown.peer_write_closed;
        let rx_queue_empty = state.rx_queue.packets.is_empty() && state.rx_queue.read_offset == 0;
        let send_rst = local_fully_closed && peer_fully_closed && rx_queue_empty;

        if local_fully_closed {
            state.phase = if send_rst {
                Phase::Closed
            } else {
                Phase::Closing
            };
        }

        LocalShutdownAction {
            shutdown_flags,
            notify_events,
            send_rst,
            arm_close_timeout: local_fully_closed && !send_rst,
        }
    }

    fn prepare_drop(&self) -> DropAction {
        let mut state = self.state.lock();
        match state.phase {
            Phase::Connecting => {
                state.phase = Phase::Closed;
                DropAction {
                    table_action: DropTableAction::Remove,
                    shutdown_flags: VirtioVsockShutdownFlags::empty(),
                    send_rst: true,
                    arm_close_timeout: false,
                }
            }
            Phase::Closed | Phase::Reset => DropAction {
                table_action: DropTableAction::Remove,
                shutdown_flags: VirtioVsockShutdownFlags::empty(),
                send_rst: false,
                arm_close_timeout: false,
            },
            Phase::Connected | Phase::Closing => {
                drop(state);
                let shutdown_action = self.prepare_local_shutdown(SockShutdownCmd::SHUT_RDWR);
                let peer_fully_closed = {
                    let state = self.state.lock();
                    state.shutdown.peer_read_closed && state.shutdown.peer_write_closed
                };
                let table_action = if shutdown_action.send_rst {
                    DropTableAction::Remove
                } else if peer_fully_closed {
                    DropTableAction::MoveToClosing
                } else {
                    DropTableAction::Keep
                };
                DropAction {
                    table_action,
                    shutdown_flags: shutdown_action.shutdown_flags,
                    send_rst: shutdown_action.send_rst,
                    arm_close_timeout: shutdown_action.arm_close_timeout,
                }
            }
        }
    }
}

impl Connection {
    pub(in crate::net::socket::vsock) fn try_send(
        &mut self,
        reader: &mut dyn MultiRead,
        _flags: SendRecvFlags,
    ) -> Result<usize> {
        if reader.is_empty() {
            return Ok(0);
        }

        self.inner.check_send_ready()?;

        let payload_len = reader.sum_lens().min(self.inner.send_credit_available());
        if payload_len == 0 {
            let guest_cid = super::space::vsock_space().guest_cid();
            if let Some(header) = self.inner.make_credit_request_header_if_needed(guest_cid) {
                let completion = Box::new(DeferredConnectionSend::new(
                    self.inner.conn_id(),
                    PendingSendAction::MarkCreditReported,
                ));
                match super::space::vsock_space().send_packet(header, Some(completion)) {
                    Ok(TxSubmit::SubmittedToQueue) => self.inner.mark_credit_reported(),
                    Ok(TxSubmit::QueuedInSoftwarePending) => {}
                    Err(_) => self.inner.rollback_credit_request(),
                }
            }
            return_errno_with_message!(Errno::EAGAIN, "the peer has no receive credit");
        }

        let guest_cid = super::space::vsock_space().guest_cid();
        let device =
            aster_virtio::device::vsock::get_device(aster_virtio::device::vsock::DEVICE_NAME)
                .ok_or_else(|| {
                    Error::with_message(Errno::ENODEV, "virtio-vsock device is unavailable")
                })?;
        let reservation = {
            let mut tx = device.lock_tx();
            tx.drain_used();
            tx.prepare_send()
        };
        let reserved = if matches!(reservation, TxReservation::Direct) {
            0
        } else {
            let reserved = self.inner.reserve_tx_bytes(payload_len)?;
            if reserved != payload_len {
                self.inner.release_tx_bytes(reserved);
                return_errno_with_message!(Errno::EAGAIN, "the pending send queue is full");
            }
            reserved
        };

        let packet = {
            let mut builder = match aster_virtio::device::vsock::new_tx_buffer_builder() {
                Ok(builder) => builder,
                Err(error) => {
                    if reserved != 0 {
                        self.inner.release_tx_bytes(reserved);
                    }
                    if matches!(reservation, TxReservation::Direct) {
                        let mut tx = device.lock_tx();
                        tx.cancel_prepared(reservation);
                    }
                    return Err(error.into());
                }
            };
            if let Err(error) = builder.append(|mut writer| {
                writer.limit(payload_len);
                Ok(reader.read(&mut writer)?)
            }) {
                if reserved != 0 {
                    self.inner.release_tx_bytes(reserved);
                }
                if matches!(reservation, TxReservation::Direct) {
                    let mut tx = device.lock_tx();
                    tx.cancel_prepared(reservation);
                }
                return Err(error.into());
            }

            builder.build(&self.inner.make_header(
                guest_cid,
                VirtioVsockOp::Rw,
                payload_len as u32,
                0,
            ))
        };

        let completion = (reserved != 0).then(|| {
            Box::new(ReleasePendingBytes {
                connection: Arc::clone(&self.inner),
                bytes: payload_len,
            }) as Box<dyn TxCompletion>
        });
        let submit = {
            let mut tx = device.lock_tx();
            tx.drain_used();
            tx.submit_prepared(reservation, packet, completion)
        };
        self.inner.update_tx_cnt(payload_len);
        if matches!(submit, TxSubmit::SubmittedToQueue) {
            self.inner.mark_credit_reported();
        }
        Ok(payload_len)
    }
}
