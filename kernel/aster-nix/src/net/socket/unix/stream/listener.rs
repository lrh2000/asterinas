// SPDX-License-Identifier: MPL-2.0

use keyable_arc::KeyableWeak;

use super::UnixStreamSocket;
use crate::{
    events::{IoEvents, Observer},
    fs::{path::Dentry, utils::Inode},
    net::socket::unix::addr::UnixSocketAddrBound,
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

static BACKLOG_TABLE: BacklogTable = BacklogTable::new();

struct BacklogTable {
    backlog_sockets: RwLock<BTreeMap<KeyableWeak<dyn Inode>, Arc<Backlog>>>,
    // TODO: For linux, there is also abstract socket domain that a socket addr is not bound to an inode.
}

impl BacklogTable {
    const fn new() -> Self {
        Self {
            backlog_sockets: RwLock::new(BTreeMap::new()),
        }
    }

    fn add_backlog(&self, addr: UnixSocketAddrBound, backlog: usize) -> Result<Arc<Backlog>> {
        let inode = {
            let UnixSocketAddrBound::Path(_, ref dentry) = addr else {
                todo!()
            };
            create_keyable_inode(dentry)
        };

        let mut backlog_sockets = self.backlog_sockets.write();
        if backlog_sockets.contains_key(&inode) {
            return_errno_with_message!(Errno::EADDRINUSE, "the addr is already used");
        }
        let new_backlog = Arc::new(Backlog::new(addr, backlog));
        backlog_sockets.insert(inode, new_backlog.clone());
        Ok(new_backlog)
    }

    fn get_backlog(&self, addr: &UnixSocketAddrBound) -> Result<Arc<Backlog>> {
        let inode = {
            let UnixSocketAddrBound::Path(_, dentry) = addr else {
                todo!()
            };
            create_keyable_inode(dentry)
        };

        let backlog_sockets = self.backlog_sockets.read();
        backlog_sockets
            .get(&inode)
            .map(Arc::clone)
            .ok_or_else(|| Error::with_message(Errno::EINVAL, "the socket is not listened"))
    }

    fn push_incoming(
        &self,
        addr: &UnixSocketAddrBound,
        socket: Arc<UnixStreamSocket>,
    ) -> Result<()> {
        let backlog = self.get_backlog(addr).map_err(|_| {
            Error::with_message(
                Errno::ECONNREFUSED,
                "no socket is listened at the remote address",
            )
        })?;

        backlog.push_incoming(socket)
    }

    fn remove_backlog(&self, addr: &UnixSocketAddrBound) {
        let UnixSocketAddrBound::Path(_, dentry) = addr else {
            todo!()
        };

        let inode = create_keyable_inode(dentry);
        self.backlog_sockets.write().remove(&inode);
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

fn create_keyable_inode(dentry: &Arc<Dentry>) -> KeyableWeak<dyn Inode> {
    let weak_inode = Arc::downgrade(dentry.inode());
    KeyableWeak::from(weak_inode)
}

pub(super) fn unregister_backlog(addr: &UnixSocketAddrBound) {
    BACKLOG_TABLE.remove_backlog(addr);
}

pub(super) fn push_incoming(
    remote_addr: &UnixSocketAddrBound,
    remote_socket: Arc<UnixStreamSocket>,
) -> Result<()> {
    BACKLOG_TABLE.push_incoming(remote_addr, remote_socket)
}
