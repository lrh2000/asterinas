// SPDX-License-Identifier: MPL-2.0

use super::{endpoint::Endpoint, UnixStreamSocket};
use crate::{
    events::{IoEvents, Observer},
    net::socket::{unix::addr::UnixSocketAddrBound, SockShutdownCmd},
    prelude::*,
    process::signal::Poller,
};

pub(super) struct Connected {
    addr: Option<UnixSocketAddrBound>,
    peer: Weak<UnixStreamSocket>,
    local_endpoint: Endpoint,
}

impl Connected {
    pub(super) fn new(
        addr: Option<UnixSocketAddrBound>,
        peer: Weak<UnixStreamSocket>,
        local_endpoint: Endpoint,
    ) -> Self {
        Connected {
            addr,
            peer,
            local_endpoint,
        }
    }

    pub(super) fn addr(&self) -> Option<&UnixSocketAddrBound> {
        self.addr.as_ref()
    }

    pub(super) fn peer(&self) -> &Weak<UnixStreamSocket> {
        &self.peer
    }

    pub(super) fn try_write(&self, buf: &[u8]) -> Result<usize> {
        self.local_endpoint.try_write(buf)
    }

    pub(super) fn try_read(&self, buf: &mut [u8]) -> Result<usize> {
        self.local_endpoint.try_read(buf)
    }

    pub(super) fn shutdown(&self, cmd: SockShutdownCmd) -> Result<()> {
        self.local_endpoint.shutdown(cmd)
    }

    pub(super) fn poll(&self, mask: IoEvents, poller: Option<&mut Poller>) -> IoEvents {
        self.local_endpoint.poll(mask, poller)
    }

    pub(super) fn register_observer(
        &self,
        observer: Weak<dyn Observer<IoEvents>>,
        mask: IoEvents,
    ) -> Result<()> {
        self.local_endpoint.register_observer(observer, mask)
    }

    pub(super) fn unregister_observer(
        &self,
        observer: &Weak<dyn Observer<IoEvents>>,
    ) -> Option<Weak<dyn Observer<IoEvents>>> {
        self.local_endpoint.unregister_observer(observer)
    }
}
