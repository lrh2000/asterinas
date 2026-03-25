// SPDX-License-Identifier: MPL-2.0

use core::sync::atomic::{AtomicUsize, Ordering};

use aster_softirq::BottomHalfDisabled;
use ostd::sync::SpinLock;

use super::{BoundPort, Connection, ConnectionInner, MAX_BACKLOG, vsock_space};
use crate::{events::IoEvents, prelude::*, process::signal::Pollee};

pub(in crate::net::socket::vsock) struct Listener {
    inner: Arc<ListenerInner>,
}

impl Listener {
    pub(in crate::net::socket::vsock) fn new(inner: Arc<ListenerInner>) -> Self {
        Self { inner }
    }

    pub(in crate::net::socket::vsock) fn try_accept(&self) -> Result<Connection> {
        self.inner.pop_incoming().map(Connection::new)
    }

    pub(in crate::net::socket::vsock) fn set_backlog(&self, backlog: usize) {
        self.inner.set_backlog(backlog);
    }

    pub(in crate::net::socket::vsock) fn local_addr(
        &self,
        guest_cid: u32,
    ) -> crate::net::socket::vsock::addr::VsockSocketAddr {
        self.inner.bound_port.local_addr(guest_cid)
    }

    pub(in crate::net::socket::vsock) fn check_io_events(&self) -> IoEvents {
        self.inner.check_io_events()
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        vsock_space().shutdown_listener(&self.inner);
    }
}

pub(in crate::net::socket::vsock) struct ListenerInner {
    pub(super) bound_port: BoundPort,
    pollee: Pollee,
    backlog: AtomicUsize,
    incoming_conns: SpinLock<Option<VecDeque<Arc<ConnectionInner>>>, BottomHalfDisabled>,
}

impl ListenerInner {
    pub(super) fn new(bound_port: BoundPort, backlog: usize, pollee: Pollee) -> Self {
        Self {
            bound_port,
            pollee,
            backlog: AtomicUsize::new(backlog),
            incoming_conns: SpinLock::new(Some(VecDeque::new())),
        }
    }

    pub(super) fn push_incoming(&self, connection: Arc<ConnectionInner>) -> Result<()> {
        {
            let mut incoming_conns = self.incoming_conns.lock();
            let Some(incoming_conns) = incoming_conns.as_mut() else {
                return_errno_with_message!(Errno::ECONNREFUSED, "the listener is shut down");
            };

            if incoming_conns.len() >= self.backlog.load(Ordering::Relaxed) {
                return_errno_with_message!(Errno::EAGAIN, "the listener backlog is full");
            }

            incoming_conns.push_back(connection);
        }

        self.pollee.notify(IoEvents::IN);
        Ok(())
    }

    pub(super) fn pop_incoming(&self) -> Result<Arc<ConnectionInner>> {
        let connection = {
            let mut incoming_conns = self.incoming_conns.lock();
            let Some(incoming_conns) = incoming_conns.as_mut() else {
                return_errno_with_message!(Errno::EINVAL, "the listener is shut down");
            };

            incoming_conns.pop_front().ok_or_else(|| {
                Error::with_message(Errno::EAGAIN, "no pending connection is available")
            })?
        };

        self.pollee.invalidate();
        Ok(connection)
    }

    pub(super) fn set_backlog(&self, backlog: usize) {
        self.backlog
            .store(backlog.min(MAX_BACKLOG), Ordering::Relaxed);
    }

    pub(super) fn take_incoming_on_shutdown(&self) -> VecDeque<Arc<ConnectionInner>> {
        self.incoming_conns.lock().take().unwrap_or_default()
    }

    pub(super) fn notify_shutdown(&self) {
        self.pollee.notify(IoEvents::IN | IoEvents::HUP);
    }

    pub(super) fn check_io_events(&self) -> IoEvents {
        let incoming_conns = self.incoming_conns.lock();
        let Some(incoming_conns) = incoming_conns.as_ref() else {
            return IoEvents::IN | IoEvents::HUP;
        };

        if incoming_conns.is_empty() {
            IoEvents::empty()
        } else {
            IoEvents::IN
        }
    }

    pub(super) fn into_bound_port(self) -> BoundPort {
        self.bound_port
    }
}
