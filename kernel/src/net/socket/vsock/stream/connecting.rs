// SPDX-License-Identifier: MPL-2.0

use super::{connected::ConnectedStream, init::InitStream};
use crate::{
    events::IoEvents,
    net::socket::vsock::{
        addr::VsockSocketAddr,
        backend::{BoundPort, vsock_space},
    },
    prelude::*,
    process::signal::Pollee,
};

pub(super) struct ConnectingStream {
    connection: super::super::backend::Connection,
}

pub(super) enum ConnResult {
    Connecting(ConnectingStream),
    Connected(ConnectedStream),
    Failed(InitStream),
}

impl ConnectingStream {
    pub(super) fn new(
        bound_port: BoundPort,
        remote_addr: VsockSocketAddr,
        pollee: Pollee,
    ) -> core::result::Result<Self, (Error, BoundPort)> {
        vsock_space()
            .new_connection(bound_port, remote_addr, pollee)
            .map(|connection| Self { connection })
    }

    pub(super) fn has_result(&self) -> bool {
        self.connection.has_result()
    }

    pub(super) fn into_result(mut self) -> ConnResult {
        if !self.connection.has_result() {
            return ConnResult::Connecting(self);
        }

        match self.connection.finish_connect() {
            Ok(()) => ConnResult::Connected(ConnectedStream::new(self.connection, true)),
            Err(error) => {
                let inner = self
                    .connection
                    .into_inner()
                    .expect("failed connection should be uniquely owned");
                ConnResult::Failed(InitStream::new_connect_failed(
                    inner.into_bound_port(),
                    error,
                ))
            }
        }
    }

    pub(super) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr {
        self.connection.local_addr(guest_cid)
    }

    pub(super) fn remote_addr(&self) -> VsockSocketAddr {
        self.connection.remote_addr()
    }

    pub(super) fn check_io_events(&self) -> IoEvents {
        if self.connection.has_result() {
            self.connection.check_io_events()
        } else {
            IoEvents::empty()
        }
    }
}
