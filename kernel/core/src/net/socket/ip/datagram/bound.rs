// SPDX-License-Identifier: MPL-2.0

use aster_bigtcp::{
    errors::{
        IoError,
        udp::{RecvError, SendError},
    },
    wire::IpEndpoint,
};

use crate::{
    events::IoEvents,
    net::{
        iface::{BoundUdpPort, Iface, UdpSocket},
        socket::util::{RecvFlags, RecvOutput, SendFlags, datagram_common},
    },
    prelude::*,
    util::{MultiRead, MultiWrite},
};

pub(super) struct BoundDatagram {
    bound_socket: UdpSocket,
    remote_endpoint: Option<IpEndpoint>,
}

impl BoundDatagram {
    pub(super) fn new(bound_socket: UdpSocket) -> Self {
        Self {
            bound_socket,
            remote_endpoint: None,
        }
    }

    pub(super) fn iface(&self) -> &Arc<Iface> {
        self.bound_socket.iface()
    }

    pub(super) fn bound_port(&self) -> &BoundUdpPort {
        self.bound_socket.bound_port()
    }

    pub(super) fn try_recv(
        &mut self,
        writer: &mut dyn MultiWrite,
        flags: RecvFlags,
    ) -> Result<(RecvOutput, IpEndpoint)> {
        let result = self
            .bound_socket
            .recv(flags.receive_behavior(), |mut packet, endpoint| {
                let message_len = packet.remain();
                let copied_res = writer.write(&mut packet);
                (copied_res, endpoint, message_len)
            });

        match result {
            Ok((Ok(copied_len), endpoint, message_len)) => {
                let output = RecvOutput::new_for_packet(flags, copied_len, message_len);
                Ok((output, endpoint))
            }
            Ok((Err(err), _, _)) => Err(err.into()),
            Err(RecvError::Exhausted) => {
                return_errno_with_message!(Errno::EAGAIN, "the receive buffer is empty")
            }
        }
    }

    pub(super) fn try_send(
        &mut self,
        reader: &mut dyn MultiRead,
        remote: &IpEndpoint,
        _flags: SendFlags,
    ) -> Result<usize> {
        let message_len = reader.sum_lens();
        let result = self.bound_socket.send(message_len, *remote, |mut buffer| {
            match reader.read(&mut buffer) {
                Ok(copied_len) => {
                    debug_assert_eq!(message_len, copied_len);
                    Ok(())
                }
                Err((err, _)) => Err(err),
            }
        });

        match result {
            Ok(()) => Ok(message_len),
            Err(IoError::NoProgress) => {
                return_errno_with_message!(Errno::EAGAIN, "the socket buffer is full");
            }
            Err(IoError::Copy(err)) => Err(err.into()),
            Err(IoError::Socket(SendError::Unaddressable)) => {
                return_errno_with_message!(Errno::EINVAL, "the destination address is invalid");
            }
            Err(IoError::Socket(SendError::TooLarge)) => {
                return_errno_with_message!(Errno::EMSGSIZE, "the message is too large");
            }
            Err(IoError::Socket(SendError::NoMemory)) => {
                return_errno_with_message!(Errno::ENOMEM, "there is no enough memory");
            }
        }
    }
}

impl datagram_common::Bound for BoundDatagram {
    type Endpoint = IpEndpoint;

    fn local_endpoint(&self) -> Self::Endpoint {
        self.bound_socket.local_endpoint().unwrap()
    }

    fn remote_endpoint(&self) -> Option<&Self::Endpoint> {
        self.remote_endpoint.as_ref()
    }

    fn set_remote_endpoint(&mut self, endpoint: &Self::Endpoint) {
        self.remote_endpoint = Some(*endpoint)
    }

    fn check_io_events(&self) -> IoEvents {
        let mut events = IoEvents::empty();

        if self.bound_socket.can_recv() {
            events |= IoEvents::IN;
        }

        if self.bound_socket.can_send() {
            events |= IoEvents::OUT;
        }

        events
    }
}
