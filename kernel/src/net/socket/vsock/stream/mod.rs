// SPDX-License-Identifier: MPL-2.0

mod connected;
mod connecting;
mod init;
mod listen;

use core::sync::atomic::{AtomicBool, Ordering};

use connected::ConnectedStream;
use connecting::{ConnResult, ConnectingStream};
use init::InitStream;
use listen::ListenStream;
use takeable::Takeable;

use crate::{
    events::IoEvents,
    fs::{file::FileLike, pseudofs::SockFs, vfs::path::Path},
    net::socket::{
        Socket,
        private::SocketPrivate,
        util::{MessageHeader, SendRecvFlags, SockShutdownCmd, SocketAddr},
        vsock::addr::VsockSocketAddr,
    },
    prelude::*,
    process::signal::{PollHandle, Pollable, Pollee},
    util::{MultiRead, MultiWrite},
};

pub struct VsockStreamSocket {
    state: RwMutex<Takeable<State>>,
    is_nonblocking: AtomicBool,
    pollee: Pollee,
    pseudo_path: Path,
}

enum State {
    Init(InitStream),
    Connecting(ConnectingStream),
    Connected(ConnectedStream),
    Listen(ListenStream),
}

fn finish_failed_connect(mut init_stream: InitStream) -> (State, Result<()>) {
    let result = init_stream.finish_last_connect();
    (State::Init(init_stream), result)
}

impl VsockStreamSocket {
    pub fn new(is_nonblocking: bool) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            state: RwMutex::new(Takeable::new(State::Init(InitStream::new()))),
            is_nonblocking: AtomicBool::new(is_nonblocking),
            pollee: Pollee::new(),
            pseudo_path: SockFs::new_path(),
        }))
    }

    fn try_accept(&self) -> Result<(Arc<dyn FileLike>, SocketAddr)> {
        let state = self.state.read();
        let State::Listen(listen_stream) = state.as_ref() else {
            return_errno_with_message!(Errno::EINVAL, "the socket is not listening");
        };

        let connected = listen_stream.try_accept()?;
        let peer_addr = connected.remote_addr().into();
        let pollee = Pollee::new();
        connected.init_pollee(pollee.clone());
        let accepted = Arc::new(Self {
            state: RwMutex::new(Takeable::new(State::Connected(connected))),
            is_nonblocking: AtomicBool::new(false),
            pollee,
            pseudo_path: SockFs::new_path(),
        });
        Ok((accepted, peer_addr))
    }

    fn try_send(&self, reader: &mut dyn MultiRead, flags: SendRecvFlags) -> Result<usize> {
        let mut state = self.state.write();
        match state.as_mut() {
            State::Init(init_stream) => init_stream.try_send(),
            State::Connecting(connection_stream) => {
                if !connection_stream.has_result() {
                    return_errno_with_message!(Errno::EAGAIN, "the socket is connecting");
                }

                state.borrow_result(|owned_state| {
                    let State::Connecting(connection_stream) = owned_state else {
                        unreachable!();
                    };
                    match connection_stream.into_result() {
                        ConnResult::Connecting(connection_stream) => (
                            State::Connecting(connection_stream),
                            Err(Error::with_message(
                                Errno::EAGAIN,
                                "the socket is connecting",
                            )),
                        ),
                        ConnResult::Connected(mut connected_stream) => {
                            let result = connected_stream.try_send(reader, flags);
                            (State::Connected(connected_stream), result)
                        }
                        ConnResult::Failed(init_stream) => {
                            let (state, result) = finish_failed_connect(init_stream);
                            (state, result.map(|()| 0))
                        }
                    }
                })
            }
            State::Connected(connected_stream) => connected_stream.try_send(reader, flags),
            State::Listen(_) => {
                return_errno_with_message!(Errno::EPIPE, "the socket is not connected");
            }
        }
    }

    fn try_recv(&self, writer: &mut dyn MultiWrite, flags: SendRecvFlags) -> Result<usize> {
        let mut state = self.state.write();
        match state.as_mut() {
            State::Init(init_stream) => init_stream.try_recv().map(|(len, _)| len),
            State::Connecting(connection_stream) => {
                if !connection_stream.has_result() {
                    return_errno_with_message!(Errno::EAGAIN, "the socket is connecting");
                }

                state.borrow_result(|owned_state| {
                    let State::Connecting(connection_stream) = owned_state else {
                        unreachable!();
                    };
                    match connection_stream.into_result() {
                        ConnResult::Connecting(connection_stream) => (
                            State::Connecting(connection_stream),
                            Err(Error::with_message(
                                Errno::EAGAIN,
                                "the socket is connecting",
                            )),
                        ),
                        ConnResult::Connected(mut connected_stream) => {
                            let result = connected_stream.try_recv(writer, flags);
                            (State::Connected(connected_stream), result)
                        }
                        ConnResult::Failed(init_stream) => {
                            let (state, result) = finish_failed_connect(init_stream);
                            (state, result.map(|()| 0))
                        }
                    }
                })
            }
            State::Connected(connected_stream) => connected_stream.try_recv(writer, flags),
            State::Listen(_) => {
                return_errno_with_message!(Errno::ENOTCONN, "the socket is not connected");
            }
        }
    }

    fn check_io_events(&self) -> IoEvents {
        let state = self.state.read();
        match state.as_ref() {
            State::Init(init_stream) => init_stream.check_io_events(),
            State::Connecting(connecting_stream) => connecting_stream.check_io_events(),
            State::Connected(connected_stream) => connected_stream.check_io_events(),
            State::Listen(listen_stream) => listen_stream.check_io_events(),
        }
    }
}

impl Pollable for VsockStreamSocket {
    fn poll(&self, mask: IoEvents, poller: Option<&mut PollHandle>) -> IoEvents {
        self.pollee
            .poll_with(mask, poller, || self.check_io_events())
    }
}

impl SocketPrivate for VsockStreamSocket {
    fn is_nonblocking(&self) -> bool {
        self.is_nonblocking.load(Ordering::Relaxed)
    }

    fn set_nonblocking(&self, nonblocking: bool) {
        self.is_nonblocking.store(nonblocking, Ordering::Relaxed);
    }
}

impl Socket for VsockStreamSocket {
    fn bind(&self, socket_addr: SocketAddr) -> Result<()> {
        let mut state = self.state.write();
        let State::Init(init_stream) = state.as_mut() else {
            return_errno_with_message!(Errno::EINVAL, "the socket is already bound or connected");
        };

        init_stream.bind(&VsockSocketAddr::try_from(socket_addr)?)
    }

    fn connect(&self, socket_addr: SocketAddr) -> Result<()> {
        let remote_addr = VsockSocketAddr::try_from(socket_addr)?;

        let mut state = self.state.write();
        state.borrow_result(|owned_state| {
            let mut init_stream = match owned_state {
                State::Init(init_stream) => init_stream,
                State::Connecting(_) => {
                    return (
                        owned_state,
                        Err(Error::with_message(
                            Errno::EALREADY,
                            "the socket is connecting",
                        )),
                    );
                }
                State::Connected(_) | State::Listen(_) => {
                    return (
                        owned_state,
                        Err(Error::with_message(
                            Errno::EISCONN,
                            "the socket is connected",
                        )),
                    );
                }
            };

            if let Err(error) = init_stream.finish_last_connect() {
                return (State::Init(init_stream), Err(error));
            }

            match init_stream.connect(&remote_addr, self.pollee.clone()) {
                Ok(connecting_stream) => {
                    if self.is_nonblocking() {
                        (
                            State::Connecting(connecting_stream),
                            Err(Error::with_message(
                                Errno::EINPROGRESS,
                                "the socket is connecting",
                            )),
                        )
                    } else {
                        (State::Connecting(connecting_stream), Ok(()))
                    }
                }
                Err((error, init_stream)) => (State::Init(init_stream), Err(error)),
            }
        })?;

        if self.is_nonblocking() {
            return Ok(());
        }

        self.wait_events(IoEvents::OUT, None, || {
            let mut state = self.state.write();
            state.borrow_result(|owned_state| {
                let State::Connecting(connecting_stream) = owned_state else {
                    return (owned_state, Ok(()));
                };
                match connecting_stream.into_result() {
                    ConnResult::Connecting(connecting_stream) => (
                        State::Connecting(connecting_stream),
                        Err(Error::with_message(
                            Errno::EAGAIN,
                            "the socket is connecting",
                        )),
                    ),
                    ConnResult::Connected(mut connected_stream) => {
                        let result = connected_stream.finish_last_connect();
                        (State::Connected(connected_stream), result)
                    }
                    ConnResult::Failed(init_stream) => finish_failed_connect(init_stream),
                }
            })
        })
    }

    fn listen(&self, backlog: usize) -> Result<()> {
        let mut state = self.state.write();
        state.borrow_result(|owned_state| {
            let init_stream = match owned_state {
                State::Init(init_stream) => init_stream,
                State::Listen(ref listen_stream) => {
                    listen_stream.set_backlog(backlog);
                    return (owned_state, Ok(()));
                }
                State::Connecting(_) | State::Connected(_) => {
                    return (
                        owned_state,
                        Err(Error::with_message(
                            Errno::EINVAL,
                            "the socket is already connected",
                        )),
                    );
                }
            };

            match init_stream.listen(backlog, self.pollee.clone()) {
                Ok(listen_stream) => (State::Listen(listen_stream), Ok(())),
                Err((error, init_stream)) => (State::Init(init_stream), Err(error)),
            }
        })
    }

    fn accept(&self) -> Result<(Arc<dyn FileLike>, SocketAddr)> {
        self.block_on(IoEvents::IN, || self.try_accept())
    }

    fn shutdown(&self, cmd: SockShutdownCmd) -> Result<()> {
        let mut state = self.state.write();
        let State::Connected(connected_stream) = state.as_mut() else {
            return_errno_with_message!(Errno::EINVAL, "cannot shutdown a non-connected vsock");
        };

        connected_stream.shutdown(cmd, &self.pollee)
    }

    fn addr(&self) -> Result<SocketAddr> {
        let guest_cid = super::backend::vsock_space().guest_cid();
        let state = self.state.read();
        let local_addr = match state.as_ref() {
            State::Init(init_stream) => init_stream.local_addr(guest_cid),
            State::Connecting(connecting_stream) => Some(connecting_stream.local_addr(guest_cid)),
            State::Connected(connected_stream) => Some(connected_stream.local_addr(guest_cid)),
            State::Listen(listen_stream) => Some(listen_stream.local_addr(guest_cid)),
        };

        Ok(local_addr
            .unwrap_or(VsockSocketAddr {
                cid: guest_cid,
                port: 0,
            })
            .into())
    }

    fn peer_addr(&self) -> Result<SocketAddr> {
        let state = self.state.read();
        let peer_addr = match state.as_ref() {
            State::Connecting(connecting_stream) => connecting_stream.remote_addr(),
            State::Connected(connected_stream) => connected_stream.remote_addr(),
            State::Init(_) | State::Listen(_) => {
                return_errno_with_message!(Errno::ENOTCONN, "the socket is not connected")
            }
        };
        Ok(peer_addr.into())
    }

    fn sendmsg(
        &self,
        reader: &mut dyn MultiRead,
        _message_header: MessageHeader,
        flags: SendRecvFlags,
    ) -> Result<usize> {
        self.block_on(IoEvents::OUT, || self.try_send(reader, flags))
    }

    fn recvmsg(
        &self,
        writer: &mut dyn MultiWrite,
        flags: SendRecvFlags,
    ) -> Result<(usize, MessageHeader)> {
        let received = self.block_on(IoEvents::IN, || self.try_recv(writer, flags))?;
        Ok((received, MessageHeader::new(None, Vec::new())))
    }

    fn pseudo_path(&self) -> &Path {
        &self.pseudo_path
    }
}
