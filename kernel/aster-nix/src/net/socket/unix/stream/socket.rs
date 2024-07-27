// SPDX-License-Identifier: MPL-2.0

use core::sync::atomic::AtomicBool;

use atomic::Ordering;
use takeable::Takeable;

use super::{connected::Connected, endpoint::Endpoint, init::Init, listener::Listener};
use crate::{
    events::{IoEvents, Observer},
    fs::{file_handle::FileLike, utils::StatusFlags},
    net::socket::{
        unix::{addr::UnixSocketAddrBound, UnixSocketAddr},
        util::{
            copy_message_from_user, copy_message_to_user, create_message_buffer,
            send_recv_flags::SendRecvFlags, socket_addr::SocketAddr, MessageHeader,
        },
        SockShutdownCmd, Socket,
    },
    prelude::*,
    process::signal::{Pollable, Poller},
    util::IoVec,
};

pub struct UnixStreamSocket {
    state: RwLock<Takeable<State>>,
    is_nonblocking: AtomicBool,
}

impl UnixStreamSocket {
    fn new_init(this: Weak<UnixStreamSocket>, is_nonblocking: bool) -> Self {
        Self {
            state: RwLock::new(Takeable::new(State::Init(Init::new(this)))),
            is_nonblocking: AtomicBool::new(is_nonblocking),
        }
    }

    pub(super) fn new_connected(
        addr: Option<UnixSocketAddrBound>,
        peer: Weak<UnixStreamSocket>,
        local_endpoint: Endpoint,
        is_nonblocking: bool,
    ) -> Self {
        Self {
            state: RwLock::new(Takeable::new(State::Connected(Connected::new(
                addr,
                None,
                peer,
                local_endpoint,
            )))),
            is_nonblocking: AtomicBool::new(is_nonblocking),
        }
    }
}

enum State {
    Init(Init),
    Listen(Listener),
    Connected(Connected),
}

impl UnixStreamSocket {
    pub fn new(is_nonblocking: bool) -> Arc<Self> {
        Arc::new_cyclic(|this| Self::new_init(this.clone(), is_nonblocking))
    }

    pub fn new_pair(is_nonblocking: bool) -> (Arc<Self>, Arc<Self>) {
        let (this_end, peer_end) = Endpoint::new_pair();

        let mut peer_storage = None;
        let this = Arc::new_cyclic(|this_weak| {
            let peer = Arc::new(Self::new_connected(
                None,
                this_weak.clone(),
                peer_end,
                is_nonblocking,
            ));
            let peer_weak = Arc::downgrade(&peer);
            peer_storage = Some(peer);
            Self::new_connected(None, peer_weak, this_end, is_nonblocking)
        });
        let peer = peer_storage.unwrap();

        (this, peer)
    }

    fn send(&self, buf: &[u8], flags: SendRecvFlags) -> Result<usize> {
        if self.is_nonblocking() {
            self.try_send(buf, flags)
        } else {
            self.wait_events(IoEvents::OUT, || self.try_send(buf, flags))
        }
    }

    fn try_send(&self, buf: &[u8], _flags: SendRecvFlags) -> Result<usize> {
        match self.state.read().as_ref() {
            State::Connected(connected) => connected.try_write(buf),
            _ => return_errno_with_message!(Errno::ENOTCONN, "the socket is not connected"),
        }
    }

    fn recv(&self, buf: &mut [u8], flags: SendRecvFlags) -> Result<usize> {
        if self.is_nonblocking() {
            self.try_recv(buf, flags)
        } else {
            self.wait_events(IoEvents::IN, || self.try_recv(buf, flags))
        }
    }

    fn try_recv(&self, buf: &mut [u8], _flags: SendRecvFlags) -> Result<usize> {
        match self.state.read().as_ref() {
            State::Connected(connected) => connected.try_read(buf),
            _ => return_errno_with_message!(Errno::ENOTCONN, "the socket is not connected"),
        }
    }

    fn try_accept(&self) -> Result<Arc<UnixStreamSocket>> {
        match self.state.read().as_ref() {
            State::Listen(listen) => listen.try_accept() as _,
            _ => return_errno_with_message!(Errno::EINVAL, "the socket is not listening"),
        }
    }

    fn is_nonblocking(&self) -> bool {
        self.is_nonblocking.load(Ordering::Relaxed)
    }

    fn set_nonblocking(&self, nonblocking: bool) {
        self.is_nonblocking.store(nonblocking, Ordering::Relaxed);
    }
}

impl Pollable for UnixStreamSocket {
    fn poll(&self, mask: IoEvents, poller: Option<&mut Poller>) -> IoEvents {
        let inner = self.state.read();
        match inner.as_ref() {
            State::Init(init) => init.poll(mask, poller),
            State::Listen(listen) => listen.poll(mask, poller),
            State::Connected(connected) => connected.poll(mask, poller),
        }
    }
}

impl FileLike for UnixStreamSocket {
    fn as_socket(self: Arc<Self>) -> Option<Arc<dyn Socket>> {
        Some(self)
    }

    fn read(&self, buf: &mut [u8]) -> Result<usize> {
        // TODO: Set correct flags
        let flags = SendRecvFlags::empty();
        self.recv(buf, flags)
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        // TODO: Set correct flags
        let flags = SendRecvFlags::empty();
        self.send(buf, flags)
    }

    fn status_flags(&self) -> StatusFlags {
        if self.is_nonblocking() {
            StatusFlags::O_NONBLOCK
        } else {
            StatusFlags::empty()
        }
    }

    fn set_status_flags(&self, new_flags: StatusFlags) -> Result<()> {
        self.set_nonblocking(new_flags.contains(StatusFlags::O_NONBLOCK));
        Ok(())
    }

    fn register_observer(
        &self,
        observer: Weak<dyn Observer<IoEvents>>,
        mask: IoEvents,
    ) -> Result<()> {
        match self.state.read().as_ref() {
            State::Init(init) => init.register_observer(observer, mask),
            State::Listen(listen) => listen.register_observer(observer, mask),
            State::Connected(connected) => connected.register_observer(observer, mask),
        }
    }

    fn unregister_observer(
        &self,
        observer: &Weak<dyn Observer<IoEvents>>,
    ) -> Option<Weak<dyn Observer<IoEvents>>> {
        match self.state.read().as_ref() {
            State::Init(init) => init.unregister_observer(observer),
            State::Listen(listen) => listen.unregister_observer(observer),
            State::Connected(connected) => connected.unregister_observer(observer),
        }
    }
}

impl Socket for UnixStreamSocket {
    fn bind(&self, socket_addr: SocketAddr) -> Result<()> {
        let addr = UnixSocketAddr::try_from(socket_addr)?;

        match self.state.write().as_mut() {
            State::Init(init) => init.bind(addr),
            State::Connected(connected) => connected.bind(addr),
            State::Listen(_) => {
                return_errno_with_message!(
                    Errno::EINVAL,
                    "the listening socket is already bound to an address"
                )
            }
        }
    }

    fn connect(&self, socket_addr: SocketAddr) -> Result<()> {
        let remote_addr = UnixSocketAddr::try_from(socket_addr)?.connect()?;

        // Note that the Linux kernel implementation locks the remote socket and checks to see if
        // it is listening first. This is different from our implementation, which locks the local
        // socket and checks the state of the local socket first.
        //
        // The difference may result in different error codes, but it's doubtful that this will
        // ever lead to real problems.
        //
        // See also <https://elixir.bootlin.com/linux/v6.10.4/source/net/unix/af_unix.c#L1527>.

        let mut state = self.state.write();

        state.borrow_result(|owned_state| {
            let init = match owned_state {
                State::Init(init) => init,
                State::Listen(listener) => {
                    return (
                        State::Listen(listener),
                        Err(Error::with_message(
                            Errno::EINVAL,
                            "the socket is listening",
                        )),
                    );
                }
                State::Connected(connected) => {
                    return (
                        State::Connected(connected),
                        Err(Error::with_message(
                            Errno::EISCONN,
                            "the socket is connected",
                        )),
                    );
                }
            };

            let connected = match init.connect(remote_addr) {
                Ok(connected) => connected,
                Err((err, init)) => {
                    return (State::Init(init), Err(err));
                }
            };

            (State::Connected(connected), Ok(()))
        })
    }

    fn listen(&self, backlog: usize) -> Result<()> {
        let mut state = self.state.write();

        state.borrow_result(|owned_state| {
            let init = match owned_state {
                State::Init(init) => init,
                State::Listen(listener) => {
                    return (
                        State::Listen(listener),
                        Err(Error::with_message(
                            Errno::EINVAL,
                            "the socket is listening",
                        )),
                    );
                }
                State::Connected(connected) => {
                    return (
                        State::Connected(connected),
                        Err(Error::with_message(
                            Errno::EINVAL,
                            "the socket is connected",
                        )),
                    );
                }
            };

            let listener = match init.listen(backlog) {
                Ok(listener) => listener,
                Err((err, init)) => {
                    return (State::Init(init), Err(err));
                }
            };

            (State::Listen(listener), Ok(()))
        })
    }

    fn accept(&self) -> Result<(Arc<dyn FileLike>, SocketAddr)> {
        let peer = if self.is_nonblocking() {
            self.try_accept()?
        } else {
            self.wait_events(IoEvents::IN, || self.try_accept())?
        };

        let peer_addr = peer.addr().unwrap();

        Ok((peer as _, peer_addr))
    }

    fn shutdown(&self, cmd: SockShutdownCmd) -> Result<()> {
        match self.state.read().as_ref() {
            State::Init(init) => init.shutdown(cmd),
            State::Listen(listen) => listen.shutdown(cmd),
            State::Connected(connected) => connected.shutdown(cmd),
        }

        Ok(())
    }

    fn addr(&self) -> Result<SocketAddr> {
        let addr = match self.state.read().as_ref() {
            State::Init(init) => init.addr().cloned(),
            State::Listen(listen) => Some(listen.addr().clone()),
            State::Connected(connected) => connected.addr().cloned(),
        };

        Ok(addr.into())
    }

    fn peer_addr(&self) -> Result<SocketAddr> {
        let weak_peer = match self.state.read().as_ref() {
            State::Connected(connected) => connected.peer().clone(),
            _ => return_errno_with_message!(Errno::ENOTCONN, "the socket is not connected"),
        };

        let peer = weak_peer
            .upgrade()
            .ok_or_else(|| Error::with_message(Errno::ENOTCONN, "the peer socket is closed"))?;

        peer.addr()
    }

    fn sendmsg(
        &self,
        io_vecs: &[IoVec],
        message_header: MessageHeader,
        flags: SendRecvFlags,
    ) -> Result<usize> {
        // TODO: Deal with flags
        debug_assert!(flags.is_all_supported());

        let MessageHeader {
            control_message, ..
        } = message_header;

        if control_message.is_some() {
            // TODO: Support sending control message
            warn!("sending control message is not supported");
        }

        let buf = copy_message_from_user(io_vecs);

        self.send(&buf, flags)
    }

    fn recvmsg(&self, io_vecs: &[IoVec], flags: SendRecvFlags) -> Result<(usize, MessageHeader)> {
        // TODO: Deal with flags
        debug_assert!(flags.is_all_supported());

        let mut buf = create_message_buffer(io_vecs);
        let received_bytes = self.recv(&mut buf, flags)?;

        let copied_bytes = {
            let message = &buf[..received_bytes];
            copy_message_to_user(io_vecs, message)
        };

        // TODO: Receive control message

        let message_header = MessageHeader::new(None, None);

        Ok((copied_bytes, message_header))
    }
}
