// SPDX-License-Identifier: MPL-2.0

use crate::{
    events::IoEvents,
    net::socket::{
        util::{SendRecvFlags, SockShutdownCmd},
        vsock::addr::VsockSocketAddr,
    },
    prelude::*,
    process::signal::Pollee,
    util::{MultiRead, MultiWrite},
};

pub(super) struct ConnectedStream {
    connection: super::super::backend::Connection,
    is_new_connection: bool,
}

impl ConnectedStream {
    pub(super) fn new(
        connection: super::super::backend::Connection,
        is_new_connection: bool,
    ) -> Self {
        Self {
            connection,
            is_new_connection,
        }
    }

    pub(super) fn try_send(
        &mut self,
        reader: &mut dyn MultiRead,
        flags: SendRecvFlags,
    ) -> Result<usize> {
        self.connection.try_send(reader, flags)
    }

    pub(super) fn try_recv(
        &mut self,
        writer: &mut dyn MultiWrite,
        flags: SendRecvFlags,
    ) -> Result<usize> {
        self.connection.try_recv(writer, flags)
    }

    pub(super) fn shutdown(&mut self, cmd: SockShutdownCmd, _pollee: &Pollee) -> Result<()> {
        self.connection.shutdown(cmd)
    }

    pub(super) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr {
        self.connection.local_addr(guest_cid)
    }

    pub(super) fn remote_addr(&self) -> VsockSocketAddr {
        self.connection.remote_addr()
    }

    pub(super) fn init_pollee(&self, pollee: Pollee) {
        self.connection.init_pollee(pollee);
    }

    pub(super) fn finish_last_connect(&mut self) -> Result<()> {
        if !self.is_new_connection {
            return_errno_with_message!(Errno::EISCONN, "the socket is already connected");
        }

        self.is_new_connection = false;
        Ok(())
    }

    pub(super) fn check_io_events(&self) -> IoEvents {
        self.connection.check_io_events()
    }
}
