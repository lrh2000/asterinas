// SPDX-License-Identifier: MPL-2.0

use core::mem;

use aster_softirq::BottomHalfDisabled;
use aster_virtio::device::vsock::header::{VirtioVsockHdr, VirtioVsockOp};
use log::debug;
use ostd::sync::SpinLock;
use spin::Once;

use super::{
    BoundPort, Connection, ConnectionInner, Listener, ListenerInner, MAX_BACKLOG,
    connection::{ConnId, ConnectionTimerEvent, DeferredConnectionSend, PendingSendAction},
};
use crate::{
    events::IoEvents,
    net::socket::vsock::addr::{VMADDR_CID_ANY, VMADDR_PORT_ANY, VsockSocketAddr},
    prelude::*,
    process::signal::Pollee,
};

pub(in crate::net::socket::vsock) struct VsockSpace {
    ports: SpinLock<PortTable, BottomHalfDisabled>,
    sockets: SpinLock<SocketTable, BottomHalfDisabled>,
}

struct PortTable {
    next_ephemeral_port: u32,
    usage: BTreeMap<u32, usize>,
}

struct SocketTable {
    listeners: BTreeMap<u32, Arc<ListenerInner>>,
    connections: BTreeMap<ConnId, Arc<ConnectionInner>>,
    closing_connections: BTreeMap<ConnId, Vec<Arc<ConnectionInner>>>,
}

impl VsockSpace {
    const EPHEMERAL_PORT_START: u32 = 49152;

    fn next_ephemeral_port_after(port: u32) -> u32 {
        let mut next_port = if port == u32::MAX {
            Self::EPHEMERAL_PORT_START
        } else {
            port + 1
        };
        if next_port < Self::EPHEMERAL_PORT_START || next_port == VMADDR_PORT_ANY {
            next_port = Self::EPHEMERAL_PORT_START;
        }
        next_port
    }

    fn new() -> Self {
        Self {
            ports: SpinLock::new(PortTable {
                next_ephemeral_port: Self::EPHEMERAL_PORT_START,
                usage: BTreeMap::new(),
            }),
            sockets: SpinLock::new(SocketTable {
                listeners: BTreeMap::new(),
                connections: BTreeMap::new(),
                closing_connections: BTreeMap::new(),
            }),
        }
    }

    pub(in crate::net::socket::vsock) fn guest_cid(&self) -> u32 {
        aster_virtio::device::vsock::get_device(aster_virtio::device::vsock::DEVICE_NAME)
            .map(|device| device.guest_cid() as u32)
            .unwrap_or(VMADDR_CID_ANY)
    }

    pub(in crate::net::socket::vsock) fn bind_port(
        &self,
        addr: &VsockSocketAddr,
    ) -> Result<BoundPort> {
        if addr.port == VMADDR_PORT_ANY {
            return self.get_ephemeral_port();
        }

        let guest_cid = self.guest_cid();
        if addr.cid != VMADDR_CID_ANY && addr.cid != guest_cid {
            return_errno_with_message!(Errno::EADDRNOTAVAIL, "the vsock cid is not local");
        }

        let mut ports = self.ports.lock();
        if ports.usage.get(&addr.port).copied().unwrap_or(0) != 0 {
            return_errno_with_message!(Errno::EADDRINUSE, "the vsock port is already in use");
        }
        let usage = ports.usage.entry(addr.port).or_insert(0);
        *usage += 1;
        Ok(BoundPort::new(addr.port))
    }

    pub(in crate::net::socket::vsock) fn get_ephemeral_port(&self) -> Result<BoundPort> {
        let mut ports = self.ports.lock();
        let start_port = Self::next_ephemeral_port_after(ports.next_ephemeral_port.wrapping_sub(1));
        let mut current_port = start_port;

        loop {
            if ports.usage.get(&current_port).copied().unwrap_or(0) == 0 {
                ports.usage.insert(current_port, 1);
                ports.next_ephemeral_port = Self::next_ephemeral_port_after(current_port);
                return Ok(BoundPort::new(current_port));
            }

            current_port = Self::next_ephemeral_port_after(current_port);
            if current_port == start_port {
                return_errno_with_message!(
                    Errno::EADDRINUSE,
                    "no ephemeral vsock ports are available"
                );
            }
        }
    }

    pub(super) fn share_port(&self, port: u32) -> Result<BoundPort> {
        let mut ports = self.ports.lock();
        let usage = ports.usage.entry(port).or_insert(0);
        *usage += 1;
        Ok(BoundPort::new(port))
    }

    pub(super) fn put_bound_port(&self, port: u32) {
        let mut ports = self.ports.lock();
        let Some(usage) = ports.usage.get_mut(&port) else {
            return;
        };
        *usage = usage.saturating_sub(1);
        if *usage == 0 {
            ports.usage.remove(&port);
        }
    }

    pub(in crate::net::socket::vsock) fn new_listener(
        &self,
        bound_port: BoundPort,
        backlog: usize,
        pollee: Pollee,
    ) -> core::result::Result<Listener, (Error, BoundPort)> {
        let port = bound_port.port();
        let backlog = backlog.min(MAX_BACKLOG);
        let inner = Arc::new(ListenerInner::new(bound_port, backlog, pollee));
        let mut sockets = self.sockets.lock();
        if sockets.listeners.contains_key(&port) {
            let bound_port = Arc::into_inner(inner)
                .expect("new listener should not be shared before insertion")
                .into_bound_port();
            return Err((
                Error::with_message(Errno::EADDRINUSE, "the vsock listener already exists"),
                bound_port,
            ));
        }
        sockets.listeners.insert(port, inner.clone());

        Ok(Listener::new(inner))
    }

    pub(in crate::net::socket::vsock) fn new_connection(
        &self,
        bound_port: BoundPort,
        remote_addr: VsockSocketAddr,
        pollee: Pollee,
    ) -> core::result::Result<Connection, (Error, BoundPort)> {
        let port = bound_port.port();
        let inner = ConnectionInner::new_connecting(bound_port, remote_addr);
        inner.init_pollee(pollee);
        if let Err(error) = self.insert_connection(&inner) {
            let bound_port = Arc::into_inner(inner)
                .expect("new connection should not be shared before insertion")
                .into_bound_port();
            debug_assert_eq!(bound_port.port(), port);
            return Err((error, bound_port));
        }

        if let Err(error) = self.send_connection_control_packet(
            &inner,
            VirtioVsockOp::Request,
            0,
            PendingSendAction::ArmConnectTimeout,
        ) {
            let conn_id = inner.conn_id();
            self.remove_connection(&conn_id);
            let bound_port = Arc::into_inner(inner)
                .expect("new connection should be uniquely owned after failed request send")
                .into_bound_port();
            debug_assert_eq!(bound_port.port(), port);
            return Err((error, bound_port));
        }
        Ok(Connection::new(inner))
    }

    pub(super) fn insert_connection(&self, connection: &Arc<ConnectionInner>) -> Result<()> {
        let mut sockets = self.sockets.lock();
        let conn_id = connection.conn_id();
        if sockets.connections.contains_key(&conn_id) {
            return_errno_with_message!(Errno::EADDRINUSE, "the vsock connection already exists");
        }
        sockets.connections.insert(conn_id, connection.clone());
        Ok(())
    }

    pub(super) fn remove_connection(&self, conn_id: &ConnId) {
        self.sockets.lock().connections.remove(conn_id);
    }

    pub(super) fn remove_connection_instance(&self, connection: &Arc<ConnectionInner>) {
        let conn_id = connection.conn_id();
        let mut sockets = self.sockets.lock();
        sockets.connections.remove(&conn_id);
        let Some(connections) = sockets.closing_connections.get_mut(&conn_id) else {
            return;
        };
        connections.retain(|candidate| !Arc::ptr_eq(candidate, connection));
        if connections.is_empty() {
            sockets.closing_connections.remove(&conn_id);
        }
    }

    pub(super) fn apply_pending_send_action(&self, conn_id: ConnId, action: PendingSendAction) {
        let sockets = self.sockets.lock();
        if let Some(connection) = sockets.connections.get(&conn_id) {
            action.apply_now(connection);
            return;
        }

        let Some(connections) = sockets.closing_connections.get(&conn_id) else {
            return;
        };
        if connections.len() == 1 {
            action.apply_now(&connections[0]);
        }
    }

    pub(super) fn move_connection_to_closing(&self, conn_id: ConnId) {
        let mut sockets = self.sockets.lock();
        let Some(connection) = sockets.connections.remove(&conn_id) else {
            return;
        };
        sockets
            .closing_connections
            .entry(conn_id)
            .or_default()
            .push(connection);
    }

    pub(super) fn shutdown_listener(&self, listener: &Arc<ListenerInner>) {
        let drained_connections = {
            let mut sockets = self.sockets.lock();
            sockets.listeners.remove(&listener.bound_port.port());
            let drained_connections = listener.take_incoming_on_shutdown();
            for connection in &drained_connections {
                sockets.connections.remove(&connection.conn_id());
            }
            drained_connections
        };
        listener.notify_shutdown();
        for connection in drained_connections {
            self.reset_removed_connection(connection);
        }
    }

    pub(super) fn process_rx(&self, device_name: &str) {
        let Some(device) = aster_virtio::device::vsock::get_device(device_name) else {
            return;
        };
        loop {
            let buffer = {
                let mut rx = device.lock_rx();
                rx.pop_used()
            };
            let Some(buffer) = buffer else {
                break;
            };
            let header = {
                let mut reader = buffer.buf();
                let Ok(header) = reader.read_val::<VirtioVsockHdr>() else {
                    continue;
                };
                header
            };
            self.process_rx_packet(header, buffer);
        }
    }

    pub(super) fn process_event(&self, _device_name: &str) {
        self.guest_cid();
        let (connections, closing_connections) = {
            let mut sockets = self.sockets.lock();
            (
                mem::take(&mut sockets.connections),
                mem::take(&mut sockets.closing_connections),
            )
        };
        for connection in connections.into_values() {
            self.reset_removed_connection(connection);
        }

        for connection in closing_connections.into_values().flatten() {
            self.reset_removed_connection(connection);
        }
    }

    pub(super) fn process_timer_events(&self, events: Vec<ConnectionTimerEvent>) {
        for event in events {
            if self.process_timer_event(event) {
                continue;
            }
        }
    }

    fn process_rx_packet(
        &self,
        header: VirtioVsockHdr,
        buffer: aster_virtio::device::vsock::RxBuffer,
    ) {
        if !self.validate_rx_header(&header, &buffer) {
            self.send_raw_rst(&header);
            return;
        }

        let Some(op) = header.op() else {
            self.send_raw_rst(&header);
            return;
        };

        match op {
            VirtioVsockOp::Request => self.process_request(header),
            VirtioVsockOp::Response => self.process_response(header),
            VirtioVsockOp::Rst => {
                if let Some(connection) = self.remove_connection_for_rst(&header) {
                    self.reset_removed_connection(connection);
                }
            }
            VirtioVsockOp::Shutdown => self.process_shutdown_packet(header),
            VirtioVsockOp::Rw => self.process_rw_packet(header, buffer),
            VirtioVsockOp::CreditUpdate => self.process_credit_update(header),
            VirtioVsockOp::CreditRequest => {
                self.process_credit_request(header);
            }
        }
    }

    fn process_request(&self, header: VirtioVsockHdr) {
        let dst_port = header.dst_port();
        let listener = {
            let sockets = self.sockets.lock();
            sockets.listeners.get(&dst_port).cloned()
        };
        let Some(listener) = listener else {
            self.send_raw_rst(&header);
            return;
        };

        let Ok(bound_port) = self.share_port(listener.bound_port.port()) else {
            self.send_raw_rst(&header);
            return;
        };
        let remote_addr = VsockSocketAddr {
            cid: header.src_cid() as u32,
            port: header.src_port(),
        };
        let connection = ConnectionInner::new_connected(bound_port, remote_addr);
        let _ = connection.on_credit_update(header.buf_alloc(), header.fwd_cnt());

        if self.insert_connection(&connection).is_err()
            || listener.push_incoming(connection.clone()).is_err()
        {
            let conn_id = connection.conn_id();
            self.remove_connection(&conn_id);
            self.send_raw_rst(&header);
            return;
        }

        if let Err(error) = self.send_connection_control_packet(
            &connection,
            VirtioVsockOp::Response,
            0,
            PendingSendAction::MarkCreditReported,
        ) {
            debug!("failed to send vsock response packet: {:?}", error);
            self.reset_connection(connection);
        }
    }

    fn process_response(&self, header: VirtioVsockHdr) {
        let conn_id = Self::conn_id_from_header(&header);
        let (credit_pollee, response_pollee) = {
            let sockets = self.sockets.lock();
            let Some(connection) = sockets.connections.get(&conn_id) else {
                return;
            };
            (
                connection.on_credit_update(header.buf_alloc(), header.fwd_cnt()),
                connection.on_response(),
            )
        };
        if let Some(pollee) = credit_pollee {
            pollee.notify(IoEvents::OUT);
        }
        if let Some(pollee) = response_pollee {
            pollee.notify(IoEvents::OUT);
        }
    }

    fn process_shutdown_packet(&self, header: VirtioVsockHdr) {
        let guest_cid = self.guest_cid();
        let conn_id = Self::conn_id_from_header(&header);
        let (credit_pollee, shutdown_pollee, notify_events, rst_header) = {
            let mut sockets = self.sockets.lock();
            let Some(connection) = sockets.connections.remove(&conn_id) else {
                return;
            };
            let credit_pollee = connection.on_credit_update(header.buf_alloc(), header.fwd_cnt());
            let shutdown_action = connection.on_shutdown(header.flags());
            let rst_header = shutdown_action
                .send_rst
                .then(|| connection.make_header(guest_cid, VirtioVsockOp::Rst, 0, 0));

            if shutdown_action.remove_lookup_key {
                if !shutdown_action.send_rst {
                    sockets
                        .closing_connections
                        .entry(conn_id)
                        .or_default()
                        .push(connection);
                }
            } else {
                sockets.connections.insert(conn_id, connection);
            }

            (
                credit_pollee,
                shutdown_action.notify_pollee,
                shutdown_action.notify_events,
                rst_header,
            )
        };

        if let Some(pollee) = credit_pollee {
            pollee.notify(IoEvents::OUT);
        }
        if let Some(pollee) = shutdown_pollee {
            pollee.notify(notify_events);
        }
        if let Some(rst_header) = rst_header {
            let _ = self.send_packet(rst_header, None);
        }
    }

    fn process_rw_packet(
        &self,
        header: VirtioVsockHdr,
        buffer: aster_virtio::device::vsock::RxBuffer,
    ) {
        let conn_id = Self::conn_id_from_header(&header);
        let packet_action = {
            let mut sockets = self.sockets.lock();
            let Some(connection) = sockets.connections.remove(&conn_id) else {
                return self.send_raw_rst(&header);
            };
            let credit_pollee = connection.on_credit_update(header.buf_alloc(), header.fwd_cnt());
            match connection.enqueue_rx_buffer(buffer) {
                Ok(rx_pollee) => {
                    sockets.connections.insert(conn_id, connection);
                    Ok((credit_pollee, rx_pollee))
                }
                Err(error) => Err((connection, credit_pollee, error)),
            }
        };
        match packet_action {
            Ok((credit_pollee, rx_pollee)) => {
                if let Some(pollee) = credit_pollee {
                    pollee.notify(IoEvents::OUT);
                }
                if let Some(pollee) = rx_pollee {
                    pollee.notify(IoEvents::IN);
                }
            }
            Err((connection, credit_pollee, _error)) => {
                if let Some(pollee) = credit_pollee {
                    pollee.notify(IoEvents::OUT);
                }
                self.reset_removed_connection(connection);
                self.send_raw_rst(&header);
            }
        }
    }

    fn process_credit_update(&self, header: VirtioVsockHdr) {
        let conn_id = Self::conn_id_from_header(&header);
        let credit_pollee = {
            let sockets = self.sockets.lock();
            let Some(connection) = sockets.connections.get(&conn_id) else {
                return;
            };
            connection.on_credit_update(header.buf_alloc(), header.fwd_cnt())
        };
        if let Some(pollee) = credit_pollee {
            pollee.notify(IoEvents::OUT);
        }
    }

    fn process_credit_request(&self, header: VirtioVsockHdr) {
        let guest_cid = self.guest_cid();
        let conn_id = Self::conn_id_from_header(&header);
        let (credit_pollee, response_header) = {
            let sockets = self.sockets.lock();
            let Some(connection) = sockets.connections.get(&conn_id) else {
                return;
            };
            (
                connection.on_credit_update(header.buf_alloc(), header.fwd_cnt()),
                connection.make_header(guest_cid, VirtioVsockOp::CreditUpdate, 0, 0),
            )
        };
        if let Some(pollee) = credit_pollee {
            pollee.notify(IoEvents::OUT);
        }
        let completion = Box::new(DeferredConnectionSend::new(
            conn_id,
            PendingSendAction::MarkCreditReported,
        ));
        match self.send_packet(response_header, Some(completion)) {
            Ok(aster_virtio::device::vsock::TxSubmit::SubmittedToQueue) => {
                self.apply_pending_send_action(conn_id, PendingSendAction::MarkCreditReported);
            }
            Ok(aster_virtio::device::vsock::TxSubmit::QueuedInSoftwarePending) => {}
            Err(error) => {
                debug!("failed to send vsock credit update packet: {:?}", error);
            }
        }
    }

    fn remove_connection_for_rst(&self, header: &VirtioVsockHdr) -> Option<Arc<ConnectionInner>> {
        let conn_id = Self::conn_id_from_header(header);
        let mut sockets = self.sockets.lock();
        if let Some(connection) = sockets.connections.remove(&conn_id) {
            return Some(connection);
        }

        let (connection, remove_entry) = {
            let connections = sockets.closing_connections.get_mut(&conn_id)?;
            if connections.len() != 1 {
                return None;
            }
            let connection = connections.pop();
            let remove_entry = connections.is_empty();
            (connection, remove_entry)
        };
        let connection = connection?;
        if remove_entry {
            sockets.closing_connections.remove(&conn_id);
        }
        Some(connection)
    }

    fn send_connection_control_packet(
        &self,
        connection: &Arc<ConnectionInner>,
        op: VirtioVsockOp,
        flags: u32,
        pending_action: PendingSendAction,
    ) -> Result<()> {
        let header = connection.make_header(self.guest_cid(), op, 0, flags);
        let completion = Box::new(DeferredConnectionSend::new(
            connection.conn_id(),
            pending_action,
        ));
        let submit = self.send_packet(header, Some(completion))?;
        if matches!(
            submit,
            aster_virtio::device::vsock::TxSubmit::SubmittedToQueue
        ) {
            pending_action.apply_now(connection);
        }
        Ok(())
    }

    fn send_raw_rst(&self, header: &VirtioVsockHdr) {
        let rst_header = VirtioVsockHdr::new(
            self.guest_cid() as u64,
            header.src_cid(),
            header.dst_port(),
            header.src_port(),
            0,
            VirtioVsockOp::Rst,
            0,
            0,
            0,
        );
        let _ = self.send_packet(rst_header, None);
    }

    fn reset_connection(&self, connection: Arc<ConnectionInner>) {
        self.remove_connection_instance(&connection);
        self.reset_removed_connection(connection);
    }

    fn reset_removed_connection(&self, connection: Arc<ConnectionInner>) {
        let pollee = connection.on_rst();
        drop(connection);
        if let Some(pollee) = pollee {
            pollee.notify(IoEvents::ERR | IoEvents::IN | IoEvents::OUT);
        }
    }

    fn process_timer_event(&self, event: ConnectionTimerEvent) -> bool {
        let guest_cid = self.guest_cid();
        let active_result = {
            let mut sockets = self.sockets.lock();
            if let Some(connection) = sockets.connections.remove(&event.conn_id) {
                let timeout_action = connection.on_timeout(event.generation);
                if let Some(timeout_action) = timeout_action {
                    Some((connection, timeout_action))
                } else {
                    sockets.connections.insert(event.conn_id, connection);
                    None
                }
            } else {
                None
            }
        };
        if let Some((connection, timeout_action)) = active_result {
            let rst_header = timeout_action
                .send_rst
                .then(|| connection.make_header(guest_cid, VirtioVsockOp::Rst, 0, 0));
            drop(connection);
            if let Some(pollee) = timeout_action.notify_pollee {
                pollee.notify(IoEvents::ERR | IoEvents::IN | IoEvents::OUT);
            }
            if let Some(rst_header) = rst_header {
                let _ = self.send_packet(rst_header, None);
            }
            return true;
        }

        let closing_result = {
            let mut sockets = self.sockets.lock();
            let Some(connections) = sockets.closing_connections.remove(&event.conn_id) else {
                return false;
            };

            let mut remaining_connections = Vec::with_capacity(connections.len());
            let mut triggered = None;
            for connection in connections {
                let timeout_action = if triggered.is_none() {
                    connection.on_timeout(event.generation)
                } else {
                    None
                };
                if let Some(timeout_action) = timeout_action {
                    triggered = Some((connection, timeout_action));
                    continue;
                }
                remaining_connections.push(connection);
            }
            if !remaining_connections.is_empty() {
                sockets
                    .closing_connections
                    .insert(event.conn_id, remaining_connections);
            }
            triggered
        };
        if let Some((connection, timeout_action)) = closing_result {
            let rst_header = timeout_action
                .send_rst
                .then(|| connection.make_header(guest_cid, VirtioVsockOp::Rst, 0, 0));
            drop(connection);
            if let Some(pollee) = timeout_action.notify_pollee {
                pollee.notify(IoEvents::ERR | IoEvents::IN | IoEvents::OUT);
            }
            if let Some(rst_header) = rst_header {
                let _ = self.send_packet(rst_header, None);
            }
            return true;
        }

        false
    }

    fn conn_id_from_header(header: &VirtioVsockHdr) -> ConnId {
        ConnId {
            local_port: header.dst_port(),
            peer_cid: header.src_cid() as u32,
            peer_port: header.src_port(),
        }
    }

    pub(super) fn send_packet(
        &self,
        header: VirtioVsockHdr,
        completion: Option<Box<dyn aster_virtio::device::vsock::TxCompletion>>,
    ) -> Result<aster_virtio::device::vsock::TxSubmit> {
        let builder = aster_virtio::device::vsock::new_tx_buffer_builder()?;
        let packet = builder.build(&header);
        let device =
            aster_virtio::device::vsock::get_device(aster_virtio::device::vsock::DEVICE_NAME)
                .ok_or_else(|| {
                    Error::with_message(Errno::ENODEV, "virtio-vsock device is unavailable")
                })?;
        let mut tx = device.lock_tx();
        tx.drain_used();
        match tx.try_send(packet) {
            Ok(()) => Ok(aster_virtio::device::vsock::TxSubmit::SubmittedToQueue),
            Err(pending) => {
                match completion {
                    Some(completion) => pending.push_pending_tracked(completion),
                    None => pending.push_pending(),
                }
                Ok(aster_virtio::device::vsock::TxSubmit::QueuedInSoftwarePending)
            }
        }
    }

    fn validate_rx_header(
        &self,
        header: &VirtioVsockHdr,
        buffer: &aster_virtio::device::vsock::RxBuffer,
    ) -> bool {
        if header.type_ != 1 {
            return false;
        }
        if header.dst_cid() as u32 != self.guest_cid() {
            return false;
        }
        buffer.packet_len() == header.len as usize
    }
}

static VSOCK_SPACE: Once<VsockSpace> = Once::new();

pub(in crate::net::socket::vsock) fn vsock_space() -> &'static VsockSpace {
    VSOCK_SPACE.call_once(VsockSpace::new)
}
