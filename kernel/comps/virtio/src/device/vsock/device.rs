// SPDX-License-Identifier: MPL-2.0

use alloc::{boxed::Box, collections::VecDeque, string::ToString, sync::Arc, vec::Vec};
use core::sync::atomic::{AtomicU64, Ordering};

use aster_softirq::BottomHalfDisabled;
use aster_util::slot_vec::SlotVec;
use log::debug;
use ostd::{
    arch::trap::TrapFrame,
    mm::{HasDaddr, HasSize, dma::DmaCoherent, io::util::HasVmReaderWriter},
    sync::{SpinLock, SpinLockGuard},
};

use super::{
    DEVICE_NAME, TxCompletion, VirtioVsockEvent, VirtioVsockEventId, VirtioVsockHdr,
    buffer::new_rx_buffer, config::VirtioVsockConfig,
};
use crate::{
    device::VirtioDeviceError,
    dma_buf::DmaBuf,
    queue::VirtQueue,
    transport::{ConfigManager, VirtioTransport},
};

pub struct VsockDevice {
    config_manager: ConfigManager<VirtioVsockConfig>,
    guest_cid: AtomicU64,
    tx: SpinLock<TxState, BottomHalfDisabled>,
    rx: SpinLock<RxState, BottomHalfDisabled>,
    event: SpinLock<EventState, BottomHalfDisabled>,
    transport: SpinLock<Box<dyn VirtioTransport>, BottomHalfDisabled>,
}

struct TxState {
    queue: VirtQueue,
    inflight: Vec<Option<SubmittedTx>>,
    pending: VecDeque<PendingTx>,
    reserved_descs: usize,
}

struct RxState {
    queue: VirtQueue,
    buffers: SlotVec<super::RxBuffer>,
}

struct EventState {
    queue: VirtQueue,
    buffers: Vec<Option<EventBuffer>>,
}

impl TxState {
    fn free_descs(&self) -> usize {
        self.queue
            .available_desc()
            .saturating_sub(self.reserved_descs)
    }
}

struct SubmittedTx {
    _packet: super::TxBuffer,
}

struct PendingTx {
    packet: super::TxBuffer,
    completion: Option<Box<dyn TxCompletion>>,
}

struct EventBuffer {
    dma: Arc<DmaCoherent>,
}

pub struct TxGuard<'a> {
    state: SpinLockGuard<'a, TxState, BottomHalfDisabled>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxReservation {
    Direct,
    Pending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxSubmit {
    SubmittedToQueue,
    QueuedInSoftwarePending,
}

pub struct TxPendingGuard<'a> {
    state: SpinLockGuard<'a, TxState, BottomHalfDisabled>,
    packet: super::TxBuffer,
}

pub struct RxGuard<'a> {
    state: SpinLockGuard<'a, RxState, BottomHalfDisabled>,
}

impl VsockDevice {
    const TX_QUEUE_INDEX: u16 = 1;
    const RX_QUEUE_INDEX: u16 = 0;
    const EVENT_QUEUE_INDEX: u16 = 2;
    const QUEUE_SIZE: u16 = 64;

    pub(crate) fn negotiate_features(features: u64) -> u64 {
        features
    }

    pub(crate) fn init(mut transport: Box<dyn VirtioTransport>) -> Result<(), VirtioDeviceError> {
        let config_manager = VirtioVsockConfig::new_manager(transport.as_ref());
        let guest_cid = VirtioVsockConfig::guest_cid(&config_manager);

        let mut rx_queue =
            VirtQueue::new(Self::RX_QUEUE_INDEX, Self::QUEUE_SIZE, transport.as_mut())?;
        let tx_queue = VirtQueue::new(Self::TX_QUEUE_INDEX, Self::QUEUE_SIZE, transport.as_mut())?;
        let event_queue = VirtQueue::new(
            Self::EVENT_QUEUE_INDEX,
            Self::QUEUE_SIZE,
            transport.as_mut(),
        )?;
        let mut rx_buffers = SlotVec::new();
        for index in 0..Self::QUEUE_SIZE {
            let buffer = new_rx_buffer().map_err(|_| VirtioDeviceError::QueueUnknownError)?;
            let token = rx_queue
                .add_dma_buf(&[], &[&buffer])
                .map_err(|_| VirtioDeviceError::QueueUnknownError)?;
            assert_eq!(token, index);
            assert_eq!(rx_buffers.put(buffer) as u16, index);
        }
        if rx_queue.should_notify() {
            rx_queue.notify();
        }

        let inflight = (0..Self::QUEUE_SIZE).map(|_| None).collect();
        let event_buffers = (0..Self::QUEUE_SIZE)
            .map(|_| Some(EventBuffer::new()))
            .collect::<Vec<_>>();
        let mut event_queue = event_queue;
        for (index, event_buffer) in event_buffers.iter().enumerate() {
            let event_buffer = event_buffer
                .as_ref()
                .expect("event buffers should be initialized before submission");
            let token = event_queue
                .add_dma_buf(&[], &[event_buffer])
                .map_err(|_| VirtioDeviceError::QueueUnknownError)?;
            assert_eq!(token, index as u16);
        }

        let device = Arc::new(Self {
            config_manager,
            guest_cid: AtomicU64::new(guest_cid),
            tx: SpinLock::new(TxState {
                queue: tx_queue,
                inflight,
                pending: VecDeque::new(),
                reserved_descs: 0,
            }),
            rx: SpinLock::new(RxState {
                queue: rx_queue,
                buffers: rx_buffers,
            }),
            event: SpinLock::new(EventState {
                queue: event_queue,
                buffers: event_buffers,
            }),
            transport: SpinLock::new(transport),
        });

        let mut transport = device.transport.lock();
        transport
            .register_queue_callback(
                Self::RX_QUEUE_INDEX,
                Box::new(move |_: &TrapFrame| super::schedule_rx(DEVICE_NAME)),
                true,
            )
            .unwrap();
        transport
            .register_queue_callback(
                Self::TX_QUEUE_INDEX,
                Box::new(move |_: &TrapFrame| super::schedule_tx(DEVICE_NAME)),
                true,
            )
            .unwrap();
        transport
            .register_queue_callback(
                Self::EVENT_QUEUE_INDEX,
                Box::new(move |_: &TrapFrame| super::schedule_event(DEVICE_NAME)),
                true,
            )
            .unwrap();
        transport
            .register_cfg_callback(Box::new(config_space_change))
            .unwrap();
        transport.finish_init();
        drop(transport);

        super::register_device(DEVICE_NAME.to_string(), device);
        Ok(())
    }

    pub fn lock_tx(&self) -> TxGuard<'_> {
        TxGuard {
            state: self.tx.lock(),
        }
    }

    pub fn lock_rx(&self) -> RxGuard<'_> {
        RxGuard {
            state: self.rx.lock(),
        }
    }

    pub fn guest_cid(&self) -> u64 {
        self.guest_cid.load(Ordering::Acquire)
    }

    pub(crate) fn process_rx(&self) {
        if self.rx.lock().queue.can_pop() {
            super::notify_recv(DEVICE_NAME);
        }
    }

    pub(crate) fn process_tx(&self) {
        let mut completions = Vec::new();
        let mut tx = self.lock_tx();
        tx.drain_used();
        let mut notified = false;
        let state = &mut tx.state;
        while state.free_descs() >= 1 {
            let Some(pending) = state.pending.pop_front() else {
                break;
            };
            let token = state
                .queue
                .add_dma_buf(&[&pending.packet], &[])
                .expect("pending tx submission should use one free descriptor");
            debug_assert!(state.inflight[token as usize].is_none());
            state.inflight[token as usize] = Some(SubmittedTx {
                _packet: pending.packet,
            });
            if let Some(completion) = pending.completion {
                completions.push(completion);
            }
            notified = true;
        }
        if notified && state.queue.should_notify() {
            state.queue.notify();
        }
        drop(tx);

        for completion in completions {
            completion.on_pending_submit();
        }
    }

    pub(crate) fn process_event(&self) {
        let mut event = self.event.lock();
        let mut has_transport_reset = false;
        while event.queue.can_pop() {
            let Ok((token, _len)) = event.queue.pop_used() else {
                break;
            };
            let event_buffer = event
                .buffers
                .get_mut(token as usize)
                .and_then(Option::take)
                .expect("used event token should have an event buffer");
            has_transport_reset |= event_buffer
                .read()
                .is_some_and(|event| matches!(event, VirtioVsockEventId::TransportReset));
            let new_token = event
                .queue
                .add_dma_buf(&[], &[&event_buffer])
                .expect("requeuing an event buffer should succeed");
            debug_assert_eq!(new_token, token);
            event.buffers[token as usize] = Some(event_buffer);
        }
        if event.queue.should_notify() {
            event.queue.notify();
        }
        drop(event);

        if has_transport_reset {
            let guest_cid = VirtioVsockConfig::guest_cid(&self.config_manager);
            self.guest_cid.store(guest_cid, Ordering::Release);
            super::notify_event(DEVICE_NAME);
        }
    }
}

impl<'a> TxGuard<'a> {
    fn free_descs(&self) -> usize {
        self.state
            .queue
            .available_desc()
            .saturating_sub(self.state.reserved_descs)
    }

    pub fn can_send(&self) -> bool {
        self.free_descs() >= 1
    }

    pub fn prepare_send(&mut self) -> TxReservation {
        if self.can_send() {
            self.state.reserved_descs += 1;
            TxReservation::Direct
        } else {
            TxReservation::Pending
        }
    }

    pub fn cancel_prepared(&mut self, reservation: TxReservation) {
        if matches!(reservation, TxReservation::Direct) {
            debug_assert!(self.state.reserved_descs > 0);
            self.state.reserved_descs -= 1;
        }
    }

    pub fn submit_prepared(
        &mut self,
        reservation: TxReservation,
        packet: super::TxBuffer,
        completion: Option<Box<dyn TxCompletion>>,
    ) -> TxSubmit {
        match reservation {
            TxReservation::Direct => {
                debug_assert!(self.state.reserved_descs > 0);
                self.state.reserved_descs -= 1;
                let token = self
                    .state
                    .queue
                    .add_dma_buf(&[&packet], &[])
                    .expect("reserved tx submission should use one free descriptor");
                debug_assert!(self.state.inflight[token as usize].is_none());
                self.state.inflight[token as usize] = Some(SubmittedTx { _packet: packet });
                if self.state.queue.should_notify() {
                    self.state.queue.notify();
                }
                TxSubmit::SubmittedToQueue
            }
            TxReservation::Pending => {
                self.state
                    .pending
                    .push_back(PendingTx { packet, completion });
                TxSubmit::QueuedInSoftwarePending
            }
        }
    }

    pub fn try_send(self, packet: super::TxBuffer) -> core::result::Result<(), TxPendingGuard<'a>> {
        let mut state = self.state;
        if state
            .queue
            .available_desc()
            .saturating_sub(state.reserved_descs)
            < 1
        {
            return Err(TxPendingGuard { state, packet });
        }

        let token = state
            .queue
            .add_dma_buf(&[&packet], &[])
            .expect("tx submission should use one free descriptor");
        debug_assert!(state.inflight[token as usize].is_none());
        state.inflight[token as usize] = Some(SubmittedTx { _packet: packet });
        if state.queue.should_notify() {
            state.queue.notify();
        }
        Ok(())
    }

    pub fn into_pending(self, packet: super::TxBuffer) -> TxPendingGuard<'a> {
        TxPendingGuard {
            state: self.state,
            packet,
        }
    }

    pub fn drain_used(&mut self) {
        while self.state.queue.can_pop() {
            let Ok((token, _len)) = self.state.queue.pop_used() else {
                break;
            };
            self.state.inflight[token as usize] = None;
        }
    }
}

impl<'a> TxPendingGuard<'a> {
    pub fn push_pending(mut self) {
        self.state.pending.push_back(PendingTx {
            packet: self.packet,
            completion: None,
        });
    }

    pub fn push_pending_tracked(mut self, completion: Box<dyn TxCompletion>) {
        self.state.pending.push_back(PendingTx {
            packet: self.packet,
            completion: Some(completion),
        });
    }
}

impl<'a> RxGuard<'a> {
    pub fn pop_used(&mut self) -> Option<super::RxBuffer> {
        if !self.state.queue.can_pop() {
            return None;
        }

        let (token, len) = self.state.queue.pop_used().ok()?;
        let mut buffer = self.state.buffers.remove(token as usize)?;
        let packet_len = (len as usize).checked_sub(VirtioVsockHdr::LEN)?;
        buffer.set_packet_len(packet_len);

        let replacement = new_rx_buffer().ok()?;
        let new_token = self.state.queue.add_dma_buf(&[], &[&replacement]).ok()?;
        debug_assert_eq!(new_token, token);
        debug_assert!(
            self.state
                .buffers
                .put_at(token as usize, replacement)
                .is_none()
        );

        if self.state.queue.should_notify() {
            self.state.queue.notify();
        }
        Some(buffer)
    }
}

impl EventBuffer {
    const LEN: usize = size_of::<VirtioVsockEvent>();

    fn new() -> Self {
        Self {
            dma: Arc::new(
                DmaCoherent::alloc(1, false).expect("allocating an event dma buffer should work"),
            ),
        }
    }

    fn read(&self) -> Option<VirtioVsockEventId> {
        let event: VirtioVsockEvent = self.dma.reader().read_val().ok()?;
        VirtioVsockEventId::try_from(event.id).ok()
    }
}

impl DmaBuf for EventBuffer {
    fn len(&self) -> usize {
        Self::LEN
    }
}

impl HasDaddr for EventBuffer {
    fn daddr(&self) -> ostd::mm::Daddr {
        self.dma.daddr()
    }
}

impl HasSize for EventBuffer {
    fn size(&self) -> usize {
        Self::LEN
    }
}

fn config_space_change(_: &TrapFrame) {
    debug!("virtio-vsock config change");
}
