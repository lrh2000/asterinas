// SPDX-License-Identifier: MPL-2.0

use super::{connecting::ConnectingStream, listen::ListenStream};
use crate::{
    events::IoEvents,
    net::socket::{
        util::SocketAddr,
        vsock::{
            addr::{VMADDR_CID_ANY, VsockSocketAddr},
            backend::{BoundPort, vsock_space},
        },
    },
    prelude::*,
    process::signal::Pollee,
};

pub(super) struct InitStream {
    bound_port: Option<BoundPort>,
    is_connect_done: bool,
    last_connect_error: Option<Error>,
}

impl InitStream {
    pub(super) fn new() -> Self {
        Self {
            bound_port: None,
            is_connect_done: true,
            last_connect_error: None,
        }
    }

    pub(super) fn new_bound(bound_port: BoundPort) -> Self {
        Self {
            bound_port: Some(bound_port),
            is_connect_done: true,
            last_connect_error: None,
        }
    }

    pub(super) fn new_connect_failed(bound_port: BoundPort, error: Error) -> Self {
        Self {
            bound_port: Some(bound_port),
            is_connect_done: false,
            last_connect_error: Some(error),
        }
    }

    pub(super) fn bind(&mut self, addr: &VsockSocketAddr) -> Result<()> {
        if self.bound_port.is_some() {
            return_errno_with_message!(Errno::EINVAL, "the socket is already bound");
        }

        self.bound_port = Some(vsock_space().bind_port(addr)?);
        Ok(())
    }

    pub(super) fn connect(
        self,
        remote_addr: &VsockSocketAddr,
        pollee: Pollee,
    ) -> core::result::Result<ConnectingStream, (Error, Self)> {
        if remote_addr.cid != super::super::addr::VMADDR_CID_HOST {
            return Err((
                Error::with_message(Errno::EOPNOTSUPP, "only host vsock cid is supported"),
                self,
            ));
        }

        let bound_port = if let Some(bound_port) = self.bound_port {
            bound_port
        } else {
            match vsock_space().get_ephemeral_port() {
                Ok(bound_port) => bound_port,
                Err(error) => return Err((error, self)),
            }
        };

        ConnectingStream::new(bound_port, *remote_addr, pollee)
            .map_err(|(error, bound_port)| (error, Self::new_bound(bound_port)))
    }

    pub(super) fn finish_last_connect(&mut self) -> Result<()> {
        if self.is_connect_done {
            return Ok(());
        }

        self.is_connect_done = true;
        if let Some(error) = self.last_connect_error.take() {
            return Err(error);
        }

        return_errno_with_message!(
            Errno::ECONNABORTED,
            "the error code for the connection failure is not available"
        )
    }

    pub(super) fn listen(
        self,
        backlog: usize,
        pollee: Pollee,
    ) -> core::result::Result<ListenStream, (Error, Self)> {
        if !self.is_connect_done {
            return Err((
                Error::with_message(
                    Errno::EINVAL,
                    "the previous connection attempt is not fully consumed",
                ),
                self,
            ));
        }

        let Some(bound_port) = self.bound_port else {
            return Err((
                Error::with_message(Errno::EINVAL, "listen() without bind() is not implemented"),
                self,
            ));
        };

        ListenStream::new(bound_port, backlog, pollee)
            .map_err(|(bound_port, error)| (error, Self::new_bound(bound_port)))
    }

    pub(super) fn local_addr(&self, guest_cid: u32) -> Option<VsockSocketAddr> {
        self.bound_port
            .as_ref()
            .map(|bound_port| bound_port.local_addr(guest_cid))
    }

    pub(super) fn try_recv(&mut self) -> Result<(usize, SocketAddr)> {
        if self.is_connect_done {
            return_errno_with_message!(Errno::ENOTCONN, "the socket is not connected");
        }

        if let Some(error) = self.test_and_clear_error() {
            return Err(error);
        }

        Ok((
            0,
            VsockSocketAddr {
                cid: VMADDR_CID_ANY,
                port: 0,
            }
            .into(),
        ))
    }

    pub(super) fn try_send(&mut self) -> Result<usize> {
        if let Some(error) = self.test_and_clear_error() {
            return Err(error);
        }

        return_errno_with_message!(Errno::EPIPE, "the socket is not connected")
    }

    pub(super) fn check_io_events(&self) -> IoEvents {
        let mut events = IoEvents::OUT | IoEvents::HUP;
        if self.last_connect_error.is_some() {
            events |= IoEvents::ERR;
        }
        events
    }

    pub(super) fn test_and_clear_error(&mut self) -> Option<Error> {
        self.last_connect_error.take()
    }
}
