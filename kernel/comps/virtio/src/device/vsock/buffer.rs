// SPDX-License-Identifier: MPL-2.0

use alloc::sync::Arc;

use aster_network::{
    RxBuffer as NetworkRxBuffer, TxBuffer as NetworkTxBuffer,
    TxBufferBuilder as NetworkTxBufferBuilder, dma_pool::DmaPool,
};
use ostd::{
    Result,
    mm::dma::{FromDevice, ToDevice},
};
use spin::Once;

use super::header::VirtioVsockHdr;

const RX_BUFFER_LEN: usize = 4096;
const TX_BUFFER_LEN: usize = 4096;

static RX_BUFFER_POOL: Once<Arc<DmaPool<FromDevice>>> = Once::new();
static TX_BUFFER_POOL: Once<Arc<DmaPool<ToDevice>>> = Once::new();

pub type TxBuffer = NetworkTxBuffer;
pub type TxBufferBuilder = NetworkTxBufferBuilder<VirtioVsockHdr>;
pub type RxBuffer = NetworkRxBuffer;

pub(super) fn init() {
    const POOL_INIT_SIZE: usize = 0;
    const POOL_HIGH_WATERMARK: usize = 64;

    RX_BUFFER_POOL
        .call_once(|| DmaPool::new(RX_BUFFER_LEN, POOL_INIT_SIZE, POOL_HIGH_WATERMARK, false));
    TX_BUFFER_POOL
        .call_once(|| DmaPool::new(TX_BUFFER_LEN, POOL_INIT_SIZE, POOL_HIGH_WATERMARK, false));
}

pub fn new_rx_buffer() -> Result<RxBuffer> {
    RxBuffer::new(size_of::<VirtioVsockHdr>(), RX_BUFFER_POOL.get().unwrap())
}

pub fn new_tx_buffer_builder() -> Result<TxBufferBuilder> {
    TxBuffer::new_builder::<VirtioVsockHdr>(TX_BUFFER_POOL.get().unwrap())
}
