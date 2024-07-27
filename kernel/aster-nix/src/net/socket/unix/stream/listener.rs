// SPDX-License-Identifier: MPL-2.0

use super::UnixStreamSocket;
use crate::{
    events::{IoEvents, Observer},
    net::socket::unix::addr::{UnixSocketAddrBound, UnixSocketAddrKey},
    prelude::*,
    process::signal::{Pollee, Poller},
};

pub(super) struct Listener {
    backlog: Arc<Backlog>,
}

impl Listener {
    pub(super) fn new(addr: UnixSocketAddrBound, backlog: usize) -> Result<Self> {
        let backlog = BACKLOG_TABLE.add_backlog(addr, backlog)?;
        Ok(Self { backlog })
    }

    pub(super) fn addr(&self) -> &UnixSocketAddrBound {
        self.backlog.addr()
    }

    pub(super) fn try_accept(&self) -> Result<Arc<UnixStreamSocket>> {
        self.backlog.pop_incoming()
    }

    pub(super) fn poll(&self, mask: IoEvents, poller: Option<&mut Poller>) -> IoEvents {
        self.backlog.poll(mask, poller)
    }

    pub(super) fn register_observer(
        &self,
        observer: Weak<dyn Observer<IoEvents>>,
        mask: IoEvents,
    ) -> Result<()> {
        self.backlog.register_observer(observer, mask)
    }

    pub(super) fn unregister_observer(
        &self,
        observer: &Weak<dyn Observer<IoEvents>>,
    ) -> Option<Weak<dyn Observer<IoEvents>>> {
        self.backlog.unregister_observer(observer)
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        BACKLOG_TABLE.remove_backlog(&self.backlog.addr().to_key())
    }
}

static BACKLOG_TABLE: BacklogTable = BacklogTable::new();

struct BacklogTable {
    backlog_sockets: RwLock<BTreeMap<UnixSocketAddrKey, Arc<Backlog>>>,
}

impl BacklogTable {
    const fn new() -> Self {
        Self {
            backlog_sockets: RwLock::new(BTreeMap::new()),
        }
    }

    fn add_backlog(&self, addr: UnixSocketAddrBound, backlog: usize) -> Result<Arc<Backlog>> {
        let addr_key = addr.to_key();

        let mut backlog_sockets = self.backlog_sockets.write();
        if backlog_sockets.contains_key(&addr_key) {
            return_errno_with_message!(Errno::EADDRINUSE, "the addr is already used");
        }
        let new_backlog = Arc::new(Backlog::new(addr, backlog));
        backlog_sockets.insert(addr_key, new_backlog.clone());
        Ok(new_backlog)
    }

    fn get_backlog(&self, addr_key: &UnixSocketAddrKey) -> Result<Arc<Backlog>> {
        let backlog_sockets = self.backlog_sockets.read();
        backlog_sockets
            .get(addr_key)
            .map(Arc::clone)
            .ok_or_else(|| Error::with_message(Errno::EINVAL, "the socket is not listened"))
    }

    fn push_incoming(
        &self,
        addr_key: &UnixSocketAddrKey,
        socket_creator: impl FnOnce(&UnixSocketAddrBound) -> Arc<UnixStreamSocket>,
    ) -> Result<Arc<UnixStreamSocket>> {
        let backlog = self.get_backlog(addr_key).map_err(|_| {
            Error::with_message(
                Errno::ECONNREFUSED,
                "no socket is listened at the remote address",
            )
        })?;

        let socket = socket_creator(backlog.addr());
        backlog.push_incoming(socket.clone())?;

        Ok(socket)
    }

    fn remove_backlog(&self, addr_key: &UnixSocketAddrKey) {
        self.backlog_sockets.write().remove(addr_key);
    }
}

struct Backlog {
    addr: UnixSocketAddrBound,
    pollee: Pollee,
    backlog: usize,
    incoming_sockets: Mutex<VecDeque<Arc<UnixStreamSocket>>>,
}

impl Backlog {
    fn new(addr: UnixSocketAddrBound, backlog: usize) -> Self {
        Self {
            addr,
            pollee: Pollee::new(IoEvents::empty()),
            backlog,
            incoming_sockets: Mutex::new(VecDeque::with_capacity(backlog)),
        }
    }

    fn addr(&self) -> &UnixSocketAddrBound {
        &self.addr
    }

    fn push_incoming(&self, socket: Arc<UnixStreamSocket>) -> Result<()> {
        let mut sockets = self.incoming_sockets.lock();
        if sockets.len() >= self.backlog {
            return_errno_with_message!(Errno::ECONNREFUSED, "incoming_sockets is full");
        }
        sockets.push_back(socket);
        self.pollee.add_events(IoEvents::IN);
        Ok(())
    }

    fn pop_incoming(&self) -> Result<Arc<UnixStreamSocket>> {
        let mut incoming_sockets = self.incoming_sockets.lock();
        let socket = incoming_sockets.pop_front();
        if incoming_sockets.is_empty() {
            self.pollee.del_events(IoEvents::IN);
        }
        socket
            .ok_or_else(|| Error::with_message(Errno::EAGAIN, "no pending connection is available"))
    }

    fn poll(&self, mask: IoEvents, poller: Option<&mut Poller>) -> IoEvents {
        // Lock to avoid any events may change pollee state when we poll
        let _lock = self.incoming_sockets.lock();
        self.pollee.poll(mask, poller)
    }

    fn register_observer(
        &self,
        observer: Weak<dyn Observer<IoEvents>>,
        mask: IoEvents,
    ) -> Result<()> {
        self.pollee.register_observer(observer, mask);
        Ok(())
    }

    fn unregister_observer(
        &self,
        observer: &Weak<dyn Observer<IoEvents>>,
    ) -> Option<Weak<dyn Observer<IoEvents>>> {
        self.pollee.unregister_observer(observer)
    }
}

pub(super) fn push_incoming(
    remote_addr_key: &UnixSocketAddrKey,
    remote_socket_creator: impl FnOnce(&UnixSocketAddrBound) -> Arc<UnixStreamSocket>,
) -> Result<Arc<UnixStreamSocket>> {
    BACKLOG_TABLE.push_incoming(remote_addr_key, remote_socket_creator)
}
