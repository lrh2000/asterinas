// SPDX-License-Identifier: MPL-2.0

use core::sync::atomic::{AtomicUsize, Ordering};

use super::UnixStreamSocket;
use crate::{
    events::{IoEvents, Observer},
    net::socket::{
        unix::addr::{UnixSocketAddrBound, UnixSocketAddrKey},
        SockShutdownCmd,
    },
    prelude::*,
    process::signal::{Pollee, Poller},
};

pub(super) struct Listener {
    backlog: Arc<Backlog>,
}

impl Listener {
    pub(super) fn new(
        addr: UnixSocketAddrBound,
        pollee: Pollee,
        backlog: usize,
        is_shutdown: bool,
    ) -> Self {
        let backlog = BACKLOG_TABLE
            .add_backlog(addr, pollee, backlog, is_shutdown)
            .unwrap();
        Self { backlog }
    }

    pub(super) fn addr(&self) -> &UnixSocketAddrBound {
        self.backlog.addr()
    }

    pub(super) fn try_accept(&self) -> Result<Arc<UnixStreamSocket>> {
        self.backlog.pop_incoming()
    }

    pub(super) fn listen(&self, backlog: usize) {
        self.backlog.set_backlog(backlog);
    }

    pub(super) fn shutdown(&self, cmd: SockShutdownCmd) {
        match cmd {
            SockShutdownCmd::SHUT_RD | SockShutdownCmd::SHUT_RDWR => self.backlog.shutdown(),
            SockShutdownCmd::SHUT_WR => (),
        }
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

    fn add_backlog(
        &self,
        addr: UnixSocketAddrBound,
        pollee: Pollee,
        backlog: usize,
        is_shutdown: bool,
    ) -> Option<Arc<Backlog>> {
        let addr_key = addr.to_key();
        let new_backlog = Arc::new(Backlog::new(addr, pollee, backlog, is_shutdown));

        let mut backlog_sockets = self.backlog_sockets.write();
        if backlog_sockets.contains_key(&addr_key) {
            return None;
        }
        backlog_sockets.insert(addr_key, new_backlog.clone());
        drop(backlog_sockets);

        Some(new_backlog)
    }

    fn push_incoming(
        &self,
        addr_key: &UnixSocketAddrKey,
        socket_creator: impl FnOnce(&UnixSocketAddrBound) -> Arc<UnixStreamSocket>,
    ) -> Result<Arc<UnixStreamSocket>> {
        let backlog = self
            .backlog_sockets
            .read()
            .get(addr_key)
            .cloned()
            .ok_or_else(|| {
                Error::with_message(
                    Errno::ECONNREFUSED,
                    "no socket is listening at the remote address",
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
    backlog: AtomicUsize,
    incoming_sockets: Mutex<Option<VecDeque<Arc<UnixStreamSocket>>>>,
}

impl Backlog {
    fn new(addr: UnixSocketAddrBound, pollee: Pollee, backlog: usize, is_shutdown: bool) -> Self {
        pollee.reset_events();

        let incoming_sockets = if is_shutdown {
            Some(VecDeque::with_capacity(backlog))
        } else {
            None
        };

        Self {
            addr,
            pollee,
            backlog: AtomicUsize::new(backlog),
            incoming_sockets: Mutex::new(incoming_sockets),
        }
    }

    fn addr(&self) -> &UnixSocketAddrBound {
        &self.addr
    }

    fn push_incoming(&self, socket: Arc<UnixStreamSocket>) -> Result<()> {
        let mut state = self.incoming_sockets.lock();

        let Some(sockets) = &mut *state else {
            return_errno_with_message!(
                Errno::ECONNREFUSED,
                "the listening socket is shut down for reading"
            );
        };

        if sockets.len() >= self.backlog.load(Ordering::Relaxed) {
            return_errno_with_message!(
                Errno::ECONNREFUSED,
                "the pending connection queue on the listening socket is full"
            );
        }

        sockets.push_back(socket);
        self.pollee.add_events(IoEvents::IN);

        Ok(())
    }

    fn pop_incoming(&self) -> Result<Arc<UnixStreamSocket>> {
        let mut state = self.incoming_sockets.lock();

        let Some(sockets) = &mut *state else {
            return_errno_with_message!(Errno::EINVAL, "the socket is shut down for reading");
        };

        let socket = sockets.pop_front();
        if sockets.is_empty() {
            self.pollee.del_events(IoEvents::IN);
        }

        socket
            .ok_or_else(|| Error::with_message(Errno::EAGAIN, "no pending connection is available"))
    }

    fn set_backlog(&self, backlog: usize) {
        self.backlog.store(backlog, Ordering::Relaxed);
    }

    fn shutdown(&self) {
        let mut state = self.incoming_sockets.lock();

        *state = None;
        self.pollee.del_events(IoEvents::IN);
    }

    fn poll(&self, mask: IoEvents, poller: Option<&mut Poller>) -> IoEvents {
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
