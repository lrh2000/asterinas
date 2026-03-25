// SPDX-License-Identifier: MPL-2.0

use core::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use aster_softirq::BottomHalfDisabled;
use aster_virtio::device::vsock::{
    header::{VirtioVsockHdr, VirtioVsockOp, VirtioVsockShutdownFlags},
    packet::RxPacket,
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

pub(in crate::net::socket::vsock) struct Connection {
    inner: Takeable<Arc<ConnectionInner>>,
}

pub(super) struct ConnectionInner {
    conn_id: ConnId,
    bound_port: BoundPort,
    remote_addr: VsockSocketAddr,
    pollee: Once<Pollee>,
    state: SpinLock<ConnectionState, BottomHalfDisabled>,
    timer: SpinLock<Option<ConnectionTimerState>, BottomHalfDisabled>,
    available_tx_bytes: AtomicUsize,
}

struct ConnectionState {
    phase: Phase,
    error: Option<Error>,
    rx_queue: RxQueue,
    credit: CreditState,
    shutdown: ShutdownState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Init,
    Connecting,
    Connected,
    Closing,
    Closed,
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

impl Connection {
    pub(super) fn new(inner: Arc<ConnectionInner>) -> Self {
        Self {
            inner: Takeable::new(inner),
        }
    }

    pub(super) fn init_pollee(&self, pollee: Pollee) {
        self.inner.pollee.call_once(move || pollee);
    }

    pub(in crate::net::socket::vsock) fn local_addr(&self) -> VsockSocketAddr {
        self.inner.bound_port.local_addr()
    }

    pub(in crate::net::socket::vsock) fn remote_addr(&self) -> VsockSocketAddr {
        self.inner.remote_addr
    }
}

pub(in crate::net::socket::vsock) enum ConnectResult {
    Connecting(Connection),
    Connected(Connection),
    Failed(BoundPort, Error),
}

impl Connection {
    pub(in crate::net::socket::vsock) fn has_connect_result(&self) -> bool {
        let state = self.inner.state.lock();
        match state.phase {
            Phase::Init => Arc::strong_count(&self.inner) == 1,
            Phase::Connecting => false,
            Phase::Connected | Phase::Closing | Phase::Closed => true,
        }
    }

    pub(in crate::net::socket::vsock) fn finish_connect(mut self) -> ConnectResult {
        let mut state = self.inner.state.lock();
        match state.phase {
            Phase::Init if Arc::strong_count(&self.inner) == 1 => {
                let error = state.error.take();
                drop(state);
                ConnectResult::Failed(
                    Arc::into_inner(self.inner.take())
                        .unwrap()
                        .into_bound_port(),
                    error.unwrap(),
                )
            }
            Phase::Init | Phase::Connecting => {
                drop(state);
                ConnectResult::Connecting(self)
            }
            Phase::Connected | Phase::Closing | Phase::Closed => {
                drop(state);
                ConnectResult::Connected(self)
            }
        }
    }
}

impl Connection {
    pub(in crate::net::socket::vsock) fn try_recv(
        &mut self,
        writer: &mut dyn MultiWrite,
        flags: SendRecvFlags,
    ) -> Result<usize> {
        let mut packet_pool = [const { None }; 8];

        let Some(mut packets) = self
            .inner
            .state
            .lock()
            .grab_packets_to_recv(&mut packet_pool[..], writer.sum_lens())?
        else {
            return Ok(0);
        };

        let result = packets.copy_to_userspace(writer);
        let recv_len = *result.as_ref().unwrap_or(&0);

        self.inner
            .state
            .lock()
            .ungrab_packets_and_finish_recv(&self.inner, packets, recv_len);

        result
    }
}

struct PoppedRxPackets<'a> {
    packets: &'a mut [Option<RxPacket>],
    read_offset: usize,
}

impl PoppedRxPackets<'_> {
    fn copy_to_userspace(&mut self, writer: &mut dyn MultiWrite) -> Result<usize> {
        let mut read_offset = self.read_offset;
        let mut total_write_len = 0;

        for (i, packet) in self.packets.iter().enumerate() {
            let packet = packet.as_ref().unwrap();

            let mut payload = packet.payload();
            payload.skip(read_offset);

            let write_len = writer.write(&mut payload)?;
            read_offset += write_len;
            total_write_len += write_len;

            if payload.has_remain() {
                self.packets = &mut self.packets[i..];
                self.read_offset = read_offset;
                return Ok(total_write_len);
            }

            read_offset = 0;
        }

        self.packets = &mut [];
        self.read_offset = 0;
        Ok(total_write_len)
    }
}

impl ConnectionState {
    fn grab_packets_to_recv<'a>(
        &mut self,
        packet_pool: &'a mut [Option<RxPacket>],
        max_bytes: usize,
    ) -> Result<Option<PoppedRxPackets<'a>>> {
        self.test_and_clear_error()?;

        let Some(packets) = self.pop_rx_packets(&mut packet_pool[..], max_bytes) else {
            if self.shutdown.local_read_closed || self.shutdown.peer_write_closed {
                return Ok(None);
            }
            return_errno_with_message!(Errno::EAGAIN, "the receive buffer is empty");
        };

        Ok(Some(packets))
    }

    fn pop_rx_packets<'a>(
        &mut self,
        packet_pool: &'a mut [Option<RxPacket>],
        mut max_bytes: usize,
    ) -> Option<PoppedRxPackets<'a>> {
        let mut read_offset = Some(0);
        let mut num_packets = 0;

        for packet_opt in packet_pool.iter_mut() {
            *packet_opt = self.rx_queue.packets.pop_front();
            let Some(packet_ref) = packet_opt.as_ref() else {
                break;
            };

            num_packets += 1;

            if read_offset.is_none() {
                read_offset = Some(self.rx_queue.read_offset);
                self.rx_queue.read_offset = 0;
            }

            let payload_len = packet_ref.payload().remain();
            if payload_len >= max_bytes {
                break;
            } else {
                max_bytes -= payload_len;
            }
        }

        if let Some(read_offset) = read_offset {
            Some(PoppedRxPackets {
                packets: &mut packet_pool[0..num_packets],
                read_offset,
            })
        } else {
            None
        }
    }

    fn ungrab_packets_and_finish_recv(
        &mut self,
        conn: &ConnectionInner,
        packets: PoppedRxPackets,
        recv_len: usize,
    ) {
        self.undo_pop_rx_packets(packets);

        self.rx_queue.used_bytes -= recv_len;
        self.credit.local_fwd_cnt += recv_len as u32;

        self.send_credit_update_header_if_needed(conn);
    }

    fn undo_pop_rx_packets(&mut self, packets: PoppedRxPackets) {
        debug_assert_eq!(self.rx_queue.read_offset, 0);

        if packets.packets.is_empty() {
            return;
        }

        debug_assert!(packets.read_offset < packets.packets[0].unwrap().payload().remain());

        for packet in packets.packets.iter().rev() {
            self.rx_queue.packets.push_front(packet.unwrap());
        }
        self.rx_queue.read_offset = packets.read_offset;
    }

    fn send_credit_update_header_if_needed(&mut self, conn: &ConnectionInner) {
        if self.credit.local_fwd_cnt - self.credit.last_reported_fwd_cnt < CREDIT_UPDATE_THRESHOLD {
            return;
        }

        self.send_packet(conn, VirtioVsockOp::CreditUpdate, 0);
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

impl Connection {
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

impl ConnectionState {
    fn test_and_clear_error(&mut self) -> Result<()> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }

        Ok(())
    }

    fn send_packet(&mut self, conn: &ConnectionInner, op: VirtioVsockOp, flags: u32) {
        let vsock_space = conn.bound_port.vsock_space();

        let buf_alloc = self.rx_queue.max_bytes.min(u32::MAX as usize) as u32;
        let header = VirtioVsockHdr::new(
            vsock_space.guest_cid(),
            conn.remote_addr.cid as u64,
            conn.conn_id.local_port,
            conn.conn_id.peer_port,
            0,
            op,
            flags,
            buf_alloc,
            self.credit.local_fwd_cnt,
        );

        vsock_space.send_packet(header);
        self.credit.last_reported_fwd_cnt = self.credit.local_fwd_cnt;
    }
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
    packets: VecDeque<RxPacket>,
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
            remote_addr,
            pollee: Once::new(),
            state: SpinLock::new(ConnectionState {
                phase,
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
        })
    }

    pub(super) const fn conn_id(&self) -> ConnId {
        self.conn_id
    }

    pub(super) fn into_bound_port(self) -> BoundPort {
        self.bound_port
    }

    pub(super) fn on_response(&self) -> Option<Pollee> {
        self.cancel_timer();
        self.state.lock().phase = Phase::Connected;
        self.pollee.get().cloned()
    }

    pub(super) fn on_rst(&self) -> Option<Pollee> {
        self.cancel_timer();
        let mut state = self.state.lock();
        state.phase = Phase::Closed;
        state.error = Some(Error::with_message(
            Errno::ECONNRESET,
            "the connection is reset",
        ));
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
            state.phase = Phase::Closed;
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

    pub(super) fn update_tx_cnt(&self, bytes: usize) {
        let mut state = self.state.lock();
        state.credit.tx_cnt = state.credit.tx_cnt.saturating_add(bytes as u32);
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
        if matches!(state.phase, Phase::Closed | Phase::Closed) {
            events |= IoEvents::HUP;
        }

        events
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
            Phase::Closed => DropAction {
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
