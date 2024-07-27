// SPDX-License-Identifier: MPL-2.0

use super::{connected::Connected, endpoint::Endpoint, listener::push_incoming, UnixStreamSocket};
use crate::{
    events::{IoEvents, Observer},
    net::socket::unix::addr::{UnixSocketAddr, UnixSocketAddrBound, UnixSocketAddrKey},
    prelude::*,
    process::signal::{Pollee, Poller},
};

pub(super) struct Init {
    addr: Option<UnixSocketAddrBound>,
    pollee: Pollee,
    this: Weak<UnixStreamSocket>,
}

impl Init {
    pub(super) fn new(this: Weak<UnixStreamSocket>) -> Self {
        Self {
            addr: None,
            pollee: Pollee::new(IoEvents::empty()),
            this,
        }
    }

    pub(super) fn bind(&mut self, addr_to_bind: UnixSocketAddr) -> Result<()> {
        if self.addr.is_some() {
            return_errno_with_message!(Errno::EINVAL, "the socket is already bound to an address");
        }

        let bound_addr = addr_to_bind.bind()?;
        self.addr = Some(bound_addr);

        Ok(())
    }

    pub(super) fn connect(&self, remote_addr: UnixSocketAddrKey) -> Result<Connected> {
        let (this_end, remote_end) = Endpoint::new_pair();

        let remote_socket = push_incoming(&remote_addr, |real_remote_addr| {
            Arc::new(UnixStreamSocket::new_connected(
                Some(real_remote_addr.clone()),
                self.this.clone(),
                remote_end,
                false,
            ))
        })?;

        Ok(Connected::new(
            self.addr.clone(),
            Arc::downgrade(&remote_socket),
            this_end,
        ))
    }

    pub(super) fn addr(&self) -> Option<&UnixSocketAddrBound> {
        self.addr.as_ref()
    }

    pub(super) fn poll(&self, mask: IoEvents, poller: Option<&mut Poller>) -> IoEvents {
        self.pollee.poll(mask, poller)
    }

    pub(super) fn register_observer(
        &self,
        observer: Weak<dyn Observer<IoEvents>>,
        mask: IoEvents,
    ) -> Result<()> {
        self.pollee.register_observer(observer, mask);
        Ok(())
    }

    pub(super) fn unregister_observer(
        &self,
        observer: &Weak<dyn Observer<IoEvents>>,
    ) -> Option<Weak<dyn Observer<IoEvents>>> {
        self.pollee.unregister_observer(observer)
    }
}
