// SPDX-License-Identifier: MPL-2.0

use alloc::{collections::vec_deque::VecDeque, sync::Arc};
use core::sync::atomic::{AtomicUsize, Ordering};

use aster_softirq::BottomHalfDisabled;
use ostd::{
    mm::{Infallible, VmReader, VmWriter},
    sync::SpinLock,
};
use smoltcp::{
    iface::Context,
    wire::{IpAddress, IpEndpoint, IpRepr, UdpRepr},
};

use super::{
    ReceiveBehavior,
    common::{Inner, Socket, SocketBg},
};
use crate::{
    errors::{
        IoError,
        udp::{RecvError, SendError},
    },
    ext::Ext,
    iface::BoundUdpPort,
    packet::{ApplicationLayer, RxPacket},
    socket::{UDP_RECV_BUF_LEN, UDP_SEND_BUF_LEN, event::SocketEvents},
};

pub type UdpSocket<E> = Socket<UdpSocketInner, E>;

/// States needed by [`UdpSocketBg`].
pub struct UdpSocketInner {
    recv_queue: SpinLock<VecDeque<RxPacketWithSrc>, BottomHalfDisabled>,
    recv_mem: AtomicUsize,
    send_mem: AtomicUsize,
}

struct RxPacketWithSrc {
    packet: RxPacket<ApplicationLayer>,
    src_addr: IpEndpoint,
}

impl<E: Ext> Inner<E> for UdpSocketInner {
    type BoundPort = BoundUdpPort<E>;
    type Observer = E::UdpEventObserver;

    fn on_drop(this: &Arc<SocketBg<Self, E>>) {
        // A UDP socket can be removed immediately.
        this.bound.iface().common().remove_udp_socket(this);
    }
}

pub(crate) type UdpSocketBg<E> = SocketBg<UdpSocketInner, E>;

pub(crate) enum UdpProcessResult {
    NotProcessed(RxPacket<ApplicationLayer>),
    Processed,
    ProcessedContinue(RxPacket<ApplicationLayer>),
}

impl<E: Ext> UdpSocketBg<E> {
    /// Tries to process an incoming packet.
    pub(crate) fn process(
        &self,
        cx: &mut Context,
        ip_repr: &IpRepr,
        udp_repr: &UdpRepr,
        udp_payload: RxPacket<ApplicationLayer>,
    ) -> UdpProcessResult {
        if self.bound.port() != udp_repr.dst_port {
            return UdpProcessResult::NotProcessed(udp_payload);
        }

        let (to_recv, result) = if *self.bound.addr() == ip_repr.dst_addr() {
            (udp_payload, UdpProcessResult::Processed)
        } else if cx.is_broadcast(&ip_repr.dst_addr()) {
            match udp_payload.clone() {
                Ok(cloned) => (cloned, UdpProcessResult::ProcessedContinue(udp_payload)),
                Err(err) => {
                    ostd::error!("failed to allocate a network packet: {:?}", err);
                    return UdpProcessResult::ProcessedContinue(udp_payload);
                }
            }
        } else {
            return UdpProcessResult::NotProcessed(udp_payload);
        };

        let mut recv_queue = self.inner.recv_queue.lock();

        let new_mem = self.inner.recv_mem.load(Ordering::Relaxed) + to_recv.memory_usage();
        if new_mem > UDP_RECV_BUF_LEN {
            return result;
        }
        self.inner.recv_mem.store(new_mem, Ordering::Relaxed);

        recv_queue.push_back(RxPacketWithSrc {
            packet: to_recv,
            src_addr: IpEndpoint::new(ip_repr.src_addr(), udp_repr.src_port),
        });

        drop(recv_queue);

        self.notify_events(SocketEvents::CAN_RECV);

        result
    }

    fn account_recv_buffer(&self, delta: isize, _locked_queue: &mut VecDeque<RxPacketWithSrc>) {
        let old_mem = self.inner.recv_mem.load(Ordering::Relaxed);

        let (new_mem, is_overflowed) = old_mem.overflowing_add_signed(delta);
        debug_assert!(!is_overflowed);

        // No races due to `_locked_queue`. `recv_mem` is only modified under the queue lock.
        self.inner.recv_mem.store(new_mem, Ordering::Relaxed);
    }

    pub(crate) fn release_send_buffer(&self, memory_usage: usize) {
        self.inner
            .send_mem
            .fetch_sub(memory_usage, Ordering::Relaxed);

        self.notify_events(SocketEvents::CAN_SEND);
    }
}

impl<E: Ext> UdpSocket<E> {
    /// Binds to a specified endpoint.
    ///
    /// Polling the iface is _not_ required after this method succeeds.
    pub fn new_bind(
        bound: BoundUdpPort<E>,
        observer: E::UdpEventObserver,
    ) -> Result<Self, (BoundUdpPort<E>, smoltcp::socket::udp::BindError)> {
        let inner = UdpSocketInner {
            recv_queue: SpinLock::new(VecDeque::new()),
            recv_mem: AtomicUsize::new(0),
            send_mem: AtomicUsize::new(0),
        };

        let socket = Self::new(bound, inner);
        socket.init_observer(observer);
        socket
            .iface()
            .common()
            .register_udp_socket(socket.inner().clone());

        Ok(socket)
    }

    /// Sends some data.
    ///
    /// Polling the iface is _always_ required after this method succeeds.
    pub fn send<F, CopyErr>(
        &mut self,
        size: usize,
        dst_addr: IpEndpoint,
        f: F,
    ) -> Result<(), IoError<SendError, CopyErr>>
    where
        F: FnOnce(VmWriter<Infallible>) -> Result<(), CopyErr>,
    {
        if size > UDP_SEND_BUF_LEN {
            return Err(IoError::Socket(SendError::TooLarge));
        }

        // Note that this check is loose because we account for memory usage later via
        // `TxPacket::memory_usage`, which is guaranteed to be greater than `size`.
        //
        // We treat the socket buffer limit as a soft limit, so this is mostly okay. Additionally,
        // due to `&mut self`, only one packet will be added if the buffer limit is exceeded.
        if self.0.inner.send_mem.load(Ordering::Relaxed) + size > UDP_SEND_BUF_LEN {
            return Err(IoError::NoProgress);
        }

        let ip_repr = {
            if dst_addr.addr.is_unspecified() {
                return Err(IoError::Socket(SendError::Unaddressable));
            }

            let next_header = smoltcp::wire::IpProtocol::Udp;
            let payload_len = smoltcp::wire::UDP_HEADER_LEN + size;
            let hop_limit = 64;
            match (self.0.bound.addr(), dst_addr.addr) {
                (IpAddress::Ipv4(src_ipv4), IpAddress::Ipv4(dst_ipv4)) => {
                    IpRepr::Ipv4(smoltcp::wire::Ipv4Repr {
                        src_addr: *src_ipv4,
                        dst_addr: dst_ipv4,
                        next_header,
                        payload_len,
                        hop_limit,
                    })
                }
                (IpAddress::Ipv6(src_ipv6), IpAddress::Ipv6(dst_ipv6)) => {
                    IpRepr::Ipv6(smoltcp::wire::Ipv6Repr {
                        src_addr: *src_ipv6,
                        dst_addr: dst_ipv6,
                        next_header,
                        payload_len,
                        hop_limit,
                    })
                }
                (_, _) => return Err(IoError::Socket(SendError::Unaddressable)),
            }
        };

        let udp_repr = {
            if dst_addr.port == 0 {
                return Err(IoError::Socket(SendError::Unaddressable));
            }

            UdpRepr {
                src_port: self.0.bound.port(),
                dst_port: dst_addr.port,
            }
        };

        let iface = self.0.bound.iface();

        let packet = {
            let mut builder = iface
                .alloc_buffer_to_dst(dst_addr.addr, size)
                .map_err(|_| IoError::Socket(SendError::NoMemory))?
                .to_builder();
            f(builder.append()).map_err(IoError::Copy)?;
            builder.commit(size);
            builder.build()
        };

        self.0
            .inner
            .send_mem
            .fetch_add(packet.memory_usage(), Ordering::Relaxed);
        iface
            .common()
            .enqueue_udp_packet(&self.0, &ip_repr, &udp_repr, packet);

        Ok(())
    }

    /// Receives some data.
    ///
    /// Polling the iface is _not_ required after this method succeeds.
    pub fn recv<CopyFn, R>(
        &mut self,
        behavior: ReceiveBehavior,
        copy_fn: CopyFn,
    ) -> Result<R, RecvError>
    where
        CopyFn: FnOnce(VmReader<Infallible>, IpEndpoint) -> R,
    {
        let packet = {
            let mut recv_queue = self.0.inner.recv_queue.lock();
            let packet = recv_queue.pop_front().ok_or(RecvError::Exhausted)?;
            self.0
                .account_recv_buffer(-packet.packet.memory_usage().cast_signed(), &mut recv_queue);
            packet
        };

        let result = copy_fn(packet.packet.reader(), packet.src_addr);

        if !behavior.will_consume_data() {
            // No races due to `&mut self`. We're the only ones who can pop the received packets.
            let mut recv_queue = self.0.inner.recv_queue.lock();
            self.0
                .account_recv_buffer(packet.packet.memory_usage().cast_signed(), &mut recv_queue);
            recv_queue.push_front(packet);
        }

        Ok(result)
    }

    /// Returns whether it is possible to send some data.
    pub fn can_send(&self) -> bool {
        self.0.inner.send_mem.load(Ordering::Relaxed) < UDP_SEND_BUF_LEN
    }

    /// Returns whether it is possible to receive some data.
    pub fn can_recv(&self) -> bool {
        self.0.inner.recv_mem.load(Ordering::Relaxed) > 0
    }
}
