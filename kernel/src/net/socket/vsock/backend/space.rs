// SPDX-License-Identifier: MPL-2.0

use aster_softirq::BottomHalfDisabled;
use aster_virtio::device::vsock::{
    device::VsockDevice,
    header::{VirtioVsockHdr, VirtioVsockOp},
    packet::{RxPacket, TxPacket},
};
use log::debug;
use ostd::sync::SpinLock;
use spin::Once;

use crate::{
    events::IoEvents,
    net::socket::vsock::{
        addr::VsockSocketAddr,
        backend::{
            BoundPort, Connection, ConnectionInner, Listener, ListenerInner, MAX_BACKLOG,
            connection::ConnId, port::PortTable,
        },
    },
    prelude::*,
    process::signal::Pollee,
};

pub(super) struct VsockSpace {
    device: Arc<VsockDevice>,
    ports: SpinLock<PortTable>,
    sockets: SpinLock<SocketTable, BottomHalfDisabled>,
}

struct SocketTable {
    listeners: BTreeMap<u32, Arc<ListenerInner>>,
    connections: BTreeMap<ConnId, Arc<ConnectionInner>>,
}

impl VsockSpace {
    fn new(device: Arc<VsockDevice>) -> Self {
        Self {
            device,
            ports: SpinLock::new(PortTable::new()),
            sockets: SpinLock::new(SocketTable {
                listeners: BTreeMap::new(),
                connections: BTreeMap::new(),
            }),
        }
    }

    pub(super) fn guest_cid(&self) -> u64 {
        self.device.guest_cid()
    }

    pub(super) fn lock_ports(&self) -> SpinLockGuard<'_, PortTable> {
        self.ports.lock()
    }

    pub(super) fn new_listener(
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
            debug_assert_eq!(bound_port.port(), port);
            return Err((
                Error::with_message(Errno::EADDRINUSE, "the vsock listener already exists"),
                bound_port,
            ));
        }
        sockets.listeners.insert(port, inner.clone());

        Ok(Listener::new(inner))
    }

    pub(super) fn new_connection(
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

        let connection = Connection::new(inner);
        // connection

        self.send_packet(header);

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

    pub(super) fn process_rx(&self) {
        let mut rx = self.device.lock_rx();

        while let Some(packet) = rx.recv() {
            self.process_rx_packet(packet);
        }
    }

    fn process_rx_packet(&self, packet: RxPacket) {
        let header = packet.header();

        if !self.validate_rx_header(&header, &packet) {
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
            VirtioVsockOp::Rw => self.process_rw_packet(header, packet),
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

        let bound_port = BoundPort::new_shared(listener.bound_port());
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
        sockets.connections.remove(&conn_id)
    }

    fn send_raw_rst(&self, header: &VirtioVsockHdr) {
        let rst_header = VirtioVsockHdr::new(
            self.guest_cid(),
            header.src_cid(),
            header.dst_port(),
            header.src_port(),
            0,
            VirtioVsockOp::Rst,
            0,
            0,
            0,
        );
        self.send_packet(rst_header);
    }

    pub(super) fn process_event(&self) {
        let connections = {
            let mut sockets = self.sockets.lock();
            core::mem::take(&mut sockets.connections)
        };

        for connection in connections.into_values() {
            self.reset_removed_connection(connection);
        }
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

    pub(super) fn process_timer_event(&self, event: ConnectionTimerEvent) -> bool {
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

        false
    }

    fn conn_id_from_header(header: &VirtioVsockHdr) -> ConnId {
        ConnId {
            local_port: header.dst_port(),
            peer_cid: header.src_cid() as u32,
            peer_port: header.src_port(),
        }
    }

    pub(super) fn send_packet(&self, header: VirtioVsockHdr) {
        let Ok(builder) = TxPacket::new_builder() else {
            log::warn!("failed to allocate vsock packet: {:?}", header);
            return;
        };
        let packet = builder.build(&header);

        let mut tx = self.device.lock_tx();
        match tx.try_send(packet) {
            Ok(()) => (),
            Err(pending) => {
                pending.push_pending(None);
            }
        }
    }

    fn validate_rx_header(&self, header: &VirtioVsockHdr, packet: &RxPacket) -> bool {
        if header.type_ != 1 {
            return false;
        }
        if header.dst_cid() != self.guest_cid() {
            return false;
        }
        packet.payload().remain() == header.len as usize
    }
}

static VSOCK_SPACE: Once<VsockSpace> = Once::new();

pub(super) fn vsock_space() -> Result<&'static VsockSpace> {
    VSOCK_SPACE
        .get()
        .ok_or_else(|| Error::with_message(Errno::ENODEV, "no virtio-vsock device is available"))
}

pub(super) fn init(device: Arc<VsockDevice>) {
    VSOCK_SPACE.call_once(move || VsockSpace::new(device));
}
