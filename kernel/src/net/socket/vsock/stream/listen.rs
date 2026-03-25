// SPDX-License-Identifier: MPL-2.0

use crate::{
    events::IoEvents,
    net::socket::vsock::{
        addr::VsockSocketAddr,
        backend::{BoundPort, Listener, vsock_space},
    },
    prelude::*,
    process::signal::Pollee,
};

pub(super) struct ListenStream {
    listener: Listener,
}

impl ListenStream {
    pub(super) fn new(
        bound_port: BoundPort,
        backlog: usize,
        pollee: Pollee,
    ) -> core::result::Result<Self, (BoundPort, Error)> {
        vsock_space()
            .new_listener(bound_port, backlog, pollee)
            .map(|listener| Self { listener })
            .map_err(|(error, bound_port)| (bound_port, error))
    }

    pub(super) fn try_accept(&self) -> Result<super::connected::ConnectedStream> {
        self.listener
            .try_accept()
            .map(|connection| super::connected::ConnectedStream::new(connection, false))
    }

    pub(super) fn set_backlog(&self, backlog: usize) {
        self.listener.set_backlog(backlog);
    }

    pub(super) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr {
        self.listener.local_addr(guest_cid)
    }

    pub(super) fn check_io_events(&self) -> IoEvents {
        self.listener.check_io_events()
    }
}
