# Asterinas Virtio-vsock 设计方案 v3

## 设计目标

本版设计以现有代码风格为第一优先级，主要对齐以下已有实现：

- `kernel/src/net/socket/ip/stream/`
- `kernel/src/net/socket/unix/stream/`
- `kernel/comps/network/`
- `kernel/comps/virtio/src/device/network/`
- `kernel/comps/softirq/src/taskless.rs`

因此本版不再引入与现有架构差异过大的命名和抽象，核心调整如下：

- socket 层状态名统一使用 `State`
- `State` 只保留 `Init` / `Connecting` / `Connected` / `Listen`
- “已 bind 但未 connect/listen”的状态放回 `InitStream` 内部，用 `Option<BoundPort>`
- 不再单列 `Closed`；连接关闭/重置由 `ConnectedStream` 内部状态表示
- `try_send` / `try_recv` / `try_accept` / `check_io_events` 等命名对齐现有 stream socket
- wrapper type 只在“拥有资源所有权”时使用，且默认不实现 `Clone`
- bottom half 使用 `Taskless`，而不是在 IRQ 中直接与 backend 做复杂交互

## 模块调整

在 `01-guide.md` 指定目录基础上，新增一个小的组件层用于解耦 virtio 与 kernel backend，理由是：

- `aster-virtio` 不能依赖 `aster-kernel`
- 现有仓库处理这类关系的方式是通过组件 crate 暴露设备注册/回调接口，例如 `aster-network`

因此建议目录变为：

```text
kernel/src/net/socket/vsock/
  mod.rs
  addr.rs
  stream/
    mod.rs
    init.rs
    connecting.rs
    connected.rs
    listen.rs
    observer.rs
  backend/
    mod.rs
    connection.rs
    listener.rs
    port.rs
    space.rs

kernel/comps/vsock/
  Cargo.toml
  src/
    lib.rs
    buffer.rs

kernel/comps/virtio/src/device/vsock/
  mod.rs
  config.rs
  header.rs
  device.rs
```

这里 `kernel/comps/vsock` 的职责只限于：

- 注册/获取 vsock 设备
- 暴露 `RxBuffer` / `TxBuffer`
- 注册发送/接收回调

真正的连接管理仍在 `kernel/src/net/socket/vsock/backend/`。

## socket 层设计

### `VsockStreamSocket`

对齐 `ip/stream`，整体形状如下：

```rust
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
```

这里不再引入 `Bound` / `Closed` 变体，原因与现有 `ip/stream` 一致：

- bind 只是 `InitStream` 的一个内部属性
- close/reset 不需要改变 `State` 大类，只需要改变 `ConnectedStream` 所拥有连接的内部状态

### `InitStream`

`InitStream` 保存未连接 socket 的全部状态：

```rust
pub(super) struct InitStream {
    bound_port: Option<BoundPort>,
    is_connect_done: bool,
    last_connect_error: AtomicBool,
}
```

更接近现有 TCP 的写法，区别只在于 `BoundPort` 来自 vsock backend。

建议 API：

```rust
impl InitStream {
    pub(super) fn new() -> Self;

    pub(super) fn new_bound(bound_port: BoundPort) -> Self;

    pub(super) fn new_refused(bound_port: BoundPort) -> Self;

    pub(super) fn bind(&mut self, addr: &VsockSocketAddr) -> Result<()>;

    pub(super) fn bound_port(&self) -> Option<&BoundPort>;

    pub(super) fn connect(
        self,
        remote_addr: &VsockSocketAddr,
        observer: StreamObserver,
    ) -> core::result::Result<ConnectingStream, (Error, Self)>;

    pub(super) fn listen(
        self,
        backlog: usize,
        observer: StreamObserver,
    ) -> core::result::Result<ListenStream, (Error, Self)>;

    pub(super) fn finish_last_connect(&mut self) -> Result<()>;

    pub(super) fn local_addr(&self, guest_cid: u32) -> Option<VsockSocketAddr>;

    pub(super) fn try_recv(&self) -> Result<(usize, SocketAddr)>;

    pub(super) fn try_send(&self) -> Result<usize>;

    pub(super) fn check_io_events(&self) -> IoEvents;

    pub(super) fn test_and_clear_error(&self) -> Option<Error>;
}
```

其中 `bound_port: Option<BoundPort>` 已经隐含了 bind ownership，因此不需要额外 `BoundState`。

### `ConnectingStream`

```rust
pub(super) struct ConnectingStream {
    connection: Connection,
    remote_addr: VsockSocketAddr,
}
```

建议 API：

```rust
impl ConnectingStream {
    pub(super) fn new(
        bound_port: BoundPort,
        remote_addr: VsockSocketAddr,
        observer: StreamObserver,
    ) -> core::result::Result<Self, (Error, BoundPort)>;

    pub(super) fn has_result(&self) -> bool;

    pub(super) fn into_result(self) -> ConnResult;

    pub(super) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr;

    pub(super) fn remote_addr(&self) -> VsockSocketAddr;

    pub(super) fn bound_port(&self) -> &BoundPort;

    pub(super) fn check_io_events(&self) -> IoEvents;
}

pub(super) enum ConnResult {
    Connecting(ConnectingStream),
    Connected(ConnectedStream),
    Refused(InitStream),
}
```

这与 `ip/stream/connecting.rs` 的职责一致：它只负责把“连接进行中”过渡到下一个 socket `State`。

### `ConnectedStream`

这里开始体现 wrapper type 的真正用途。

```rust
pub(super) struct ConnectedStream {
    connection: Connection,
    remote_addr: VsockSocketAddr,
    is_new_connection: bool,
}
```

注意：

- `Connection` 是 socket 层拥有的 wrapper
- `Connection` 不实现 `Clone`
- backend 中共享的是 `Arc<ConnectionInner>`

建议 API：

```rust
impl ConnectedStream {
    pub(super) fn new(
        connection: Connection,
        remote_addr: VsockSocketAddr,
        is_new_connection: bool,
    ) -> Self;

    pub(super) fn try_send(
        &mut self,
        reader: &mut dyn MultiRead,
        flags: SendRecvFlags,
    ) -> Result<usize>;

    pub(super) fn try_recv(
        &mut self,
        writer: &mut dyn MultiWrite,
        flags: SendRecvFlags,
    ) -> Result<usize>;

    pub(super) fn shutdown(&mut self, cmd: SockShutdownCmd, pollee: &Pollee) -> Result<()>;

    pub(super) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr;

    pub(super) fn remote_addr(&self) -> VsockSocketAddr;

    pub(super) fn bound_port(&self) -> &BoundPort;

    pub(super) fn finish_last_connect(&mut self) -> Result<()>;

    pub(super) fn init_observer(&self, observer: StreamObserver);

    pub(super) fn check_io_events(&self) -> IoEvents;

    pub(super) fn test_and_clear_error(&self) -> Option<Error>;
}
```

这里让 `try_send` / `try_recv` 使用 `&mut self`，原因是：

- 一个 `Connection` 只能属于一个 socket
- socket 的 `state` 锁拿住之后，不会有另一个 syscall 同时发送这个 connection
- 发送路径因此不需要 `prepare_send/commit_send` 这种人为拆分

### `ListenStream`

```rust
pub(super) struct ListenStream {
    listener: Listener,
}
```

建议 API：

```rust
impl ListenStream {
    pub(super) fn new(
        bound_port: BoundPort,
        backlog: usize,
        observer: StreamObserver,
    ) -> core::result::Result<Self, (BoundPort, Error)>;

    pub(super) fn try_accept(&self) -> Result<ConnectedStream>;

    pub(super) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr;

    pub(super) fn bound_port(&self) -> &BoundPort;

    pub(super) fn check_io_events(&self) -> IoEvents;
}
```

## wrapper type 的正确用法

### `BoundPort`

`BoundPort` 应该是一个拥有端口占用资源的 wrapper type：

```rust
pub(super) struct BoundPort {
    inner: Arc<BoundPortInner>,
}
```

但对外不实现 `Clone`。内部 `Arc` 仅用于：

- backend 表中保留内部对象
- connection / listener 引用同一端口资源

`Drop for BoundPort` 的语义：

- 当最后一个拥有该端口所有权的 socket 状态被销毁时，释放端口占用

更具体地说，`InitStream` / `ConnectingStream` / `ConnectedStream` / `ListenStream` 中各自保存这个 wrapper，谁最后 drop，谁触发释放。

### `Connection`

```rust
pub(super) struct Connection {
    inner: Arc<ConnectionInner>,
}
```

同样：

- 不实现 `Clone`
- socket 层通过 `Connection` 表达唯一 ownership
- backend 中直接保存 `Arc<ConnectionInner>`

这样 `ConnectedStream` drop 时可以明确触发：

- 从 backend 连接表注销
- 如果需要，发送 `RST` / `SHUTDOWN`
- 唤醒等待该连接资源的其他逻辑

### `Listener`

```rust
pub(super) struct Listener {
    inner: Arc<ListenerInner>,
}
```

`Listener` 也不实现 `Clone`，只在 `ListenStream` 中持有。backend 表里保存 `Arc<ListenerInner>`。

## backend 设计

### `VsockSpace`

backend 全局单例：

```rust
pub(super) struct VsockSpace {
    inner: SpinLock<VsockSpaceInner, BottomHalfDisabled>,
}

struct VsockSpaceInner {
    guest_cid: u32,
    next_ephemeral_port: u32,
    listeners: BTreeMap<u32, Arc<ListenerInner>>,
    connections: BTreeMap<ConnId, Arc<ConnectionInner>>,
}
```

API：

```rust
impl VsockSpace {
    pub(super) fn guest_cid(&self) -> u32;

    pub(super) fn bind_port(&self, addr: &VsockSocketAddr) -> Result<BoundPort>;

    pub(super) fn new_listener(
        &self,
        bound_port: BoundPort,
        backlog: usize,
        observer: StreamObserver,
    ) -> Result<Listener>;

    pub(super) fn new_connection(
        &self,
        bound_port: BoundPort,
        remote_addr: VsockSocketAddr,
        observer: StreamObserver,
    ) -> Result<Connection>;

    pub(super) fn poll(&self, device_name: &str);

    pub(super) fn process_rx(&self, device_name: &str);

    pub(super) fn process_tx(&self, device_name: &str);

    pub(super) fn process_event(&self, device_name: &str);
}
```

说明：

- `process_rx/process_tx/process_event` 在 taskless 中被调用
- `poll(device_name)` 用于主动触发对应设备发送/接收处理，形状上对齐 `iface.poll()`

### `ListenerInner`

```rust
pub(super) struct ListenerInner {
    local_port: u32,
    pollee: Pollee,
    backlog: AtomicUsize,
    incoming_conns: SpinLock<Option<VecDeque<Arc<ConnectionInner>>>, BottomHalfDisabled>,
}
```

这里直接参考 `unix/stream/listener.rs`：

- `incoming_conns` 是唯一的 accept backlog queue
- `Option<VecDeque<...>>` 直接编码“监听是否已 shutdown”

建议 API：

```rust
impl ListenerInner {
    pub(super) fn push_incoming(&self, conn: Arc<ConnectionInner>) -> Result<()>;
    pub(super) fn pop_incoming(&self) -> Result<Connection>;
    pub(super) fn set_backlog(&self, backlog: usize);
    pub(super) fn shutdown(&self);
    pub(super) fn check_io_events(&self) -> IoEvents;
}
```

`pop_incoming()` 返回 `Connection` wrapper，把 ownership 交给新 accepted socket。

### `ConnectionInner`

```rust
pub(super) struct ConnectionInner {
    conn_id: ConnId,
    bound_port: Arc<BoundPortInner>,
    pollee: Pollee,
    state: Atomic<ConnectionState>,
    tx_cnt: AtomicU32,
    peer_buf_alloc: AtomicU32,
    peer_fwd_cnt: AtomicU32,
    queued_tx_bytes: AtomicUsize,
    rx_queue: SpinLock<RxQueue, BottomHalfDisabled>,
    observer: StreamObserver,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConnectionState {
    Connecting,
    Connected,
    PeerClosed,
    LocalClosed,
    Closed,
    Reset,
}
```

这里仍然保留 `Connected` 大类中的细分关闭状态，但它们是连接内部状态，不映射为 socket `State`。

`rx_queue` 设计：

```rust
struct RxQueue {
    packets: VecDeque<Arc<RxBuffer>>,
    used_bytes: usize,
    max_bytes: usize,
    read_offset: usize,
}
```

这里不再发明 `RxPayload` / `ReceivedPacket` 一类中间对象，直接使用 `RxBuffer`：

- `RxBuffer` 已自带 DMA buffer 和长度信息
- header 可在消费时一次性读出
- payload 长度可从 header 和 `packet_len()` 得到

建议 API：

```rust
impl ConnectionInner {
    pub(super) fn check_io_events(&self) -> IoEvents;

    pub(super) fn test_and_clear_error(&self) -> Option<Error>;

    pub(super) fn finish_connect(&self) -> Result<()>;

    pub(super) fn try_send(
        &self,
        reader: &mut dyn MultiRead,
        flags: SendRecvFlags,
    ) -> Result<usize>;

    pub(super) fn try_recv(
        &self,
        writer: &mut dyn MultiWrite,
        flags: SendRecvFlags,
    ) -> Result<usize>;

    pub(super) fn shutdown(&self, cmd: SockShutdownCmd) -> Result<()>;

    pub(super) fn on_response(&self);

    pub(super) fn on_request(&self) -> Result<()>;

    pub(super) fn on_rst(&self);

    pub(super) fn on_shutdown(&self, flags: u32);

    pub(super) fn on_credit_update(&self, buf_alloc: u32, fwd_cnt: u32);

    pub(super) fn enqueue_rx_buffer(&self, buffer: Arc<RxBuffer>) -> Result<()>;

    pub(super) fn release_queued_tx(&self, bytes: usize);
}
```

## 发送路径

### 基本原则

发送时采用：

- socket 层串行化
- 直接拷贝到最终 `TxBuffer`
- 设备级 staging queue
- 连接级配额限制

不再使用 `prepare_send/commit_send`。

### 发送 API

`ConnectedStream::try_send()` 内部：

1. 通过 `ConnectionInner` 的 atomic 状态读取当前 peer credit 和本连接已占用的发送配额
2. 计算本次最多可发送的长度
3. 从 `aster_vsock::TxBufferBuilder` 分配一个 `TxBuffer`
4. 直接把用户数据从 `MultiRead` 写入这个 `TxBuffer`
5. 把 `TxBuffer` 交给设备发送

对应组件层建议接口：

```rust
pub trait AnyVsockDevice: Send + Sync + Debug {
    fn guest_cid(&self) -> u64;
    fn can_send(&self) -> bool;
    fn send(&mut self, packet: TxBuffer) -> Result<(), VsockDeviceError>;
    fn receive(&mut self) -> Result<RxBuffer, VsockDeviceError>;
    fn free_processed_tx_buffers(&mut self);
}
```

对齐 `aster_network::AnyNetworkDevice`。

### 设备级 staging queue

评审里指出，若每个 connection 都维护自己的待发队列，则：

- 设备 queue 重新可用时，很难决定按什么顺序从各 connection 分发
- 调度复杂度会明显上升

因此本版改为：

- 每个 device 维护一个 software staging queue
- 每个 connection 只维护“自己已占用多少已排队发送资源”的计数

设备内部：

```rust
struct PendingTx {
    packet: TxBuffer,
    resource: TxResource,
}

struct TxResource {
    connection: Arc<ConnectionInner>,
    bytes: usize,
}
```

当 `PendingTx` 完成发送并从 used queue 回收时，`TxResource` drop：

- 调 `connection.release_queued_tx(bytes)`
- 唤醒该连接的 `pollee` 上的 `OUT`

这样：

- 设备排队顺序是全局 FIFO
- 连接单独的发送配额限制仍然有效
- 资源回收点明确

### 连接级发送限制

每个 connection 增加：

```rust
queued_tx_bytes: AtomicUsize,
```

以及固定上限：

```rust
const MAX_QUEUED_TX_BYTES_PER_CONN: usize = 64 * 1024;
```

`try_send()` 只有在以下同时满足时才允许继续构造包：

- peer credit 允许
- `queued_tx_bytes < MAX_QUEUED_TX_BYTES_PER_CONN`

否则：

- 阻塞 socket 等待 `IoEvents::OUT`
- 或非阻塞返回 `EAGAIN`

## 接收路径

### 设备与 taskless 对接

不在 IRQ 中直接清理 queue 和更新 connection。

virtio vsock 设备初始化时创建三个 taskless：

```rust
rx_taskless: Arc<Taskless>,
tx_taskless: Arc<Taskless>,
event_taskless: Arc<Taskless>,
```

IRQ callback 只负责：

- 标记某条 queue 有 pending work
- `schedule()` 对应 taskless

真正执行在 taskless 中完成。

这与 `Taskless` 的设计初衷完全一致，也避免在硬中断里持有多个锁。

### RX taskless

`rx_taskless` 中循环：

1. 从 device 的 RX virtqueue `pop_used`
2. 得到 token 和长度
3. 取出对应 `RxBuffer`
4. 设置 `packet_len`
5. 重新补一个新的 `RxBuffer` 到相同 token
6. 读取 header
7. 路由到 `VsockSpace`

这里 helper API 应接受 token：

```rust
fn recycle_rx_buffer(&mut self, token: u16) -> Result<Arc<RxBuffer>>;
fn refill_rx_buffer(&mut self, token: u16) -> Result<()>;
```

而不是无参 `refill_rx_buffers()`。

### `VsockSpace` 路由

`process_rx()` 根据 header 的 `(dst_port, src_cid, src_port, op)`：

- `REQUEST`
  - 查 listener
  - 无 listener 或 backlog 满则发 `RST`
  - 否则创建 connection，入 listener backlog，并发 `RESPONSE`
- `RESPONSE`
  - 找到 connecting connection，改为 connected
- `RW`
  - 找到 connection，直接把 `Arc<RxBuffer>` 入 `rx_queue`
- `SHUTDOWN`
  - 更新 connection 内部状态并唤醒读写者
- `RST`
  - 标记 connection reset
- `CREDIT_UPDATE` / `CREDIT_REQUEST`
  - 更新 credit，必要时回包

## 组件层设计：`aster_vsock`

组件层直接参考 `aster_network`。

### `buffer.rs`

提供：

```rust
pub use buffer::{RxBuffer, TxBuffer, TxBufferBuilder};
pub mod dma_pool;
```

与现有 `kernel/comps/network/src/buffer.rs` 形状一致，只是 header 类型换成 `VirtioVsockHdr`。

### `lib.rs`

建议接口：

```rust
pub trait AnyVsockDevice: Send + Sync + Any + Debug {
    fn guest_cid(&self) -> u64;
    fn can_send(&self) -> bool;
    fn receive(&mut self) -> Result<RxBuffer, VsockDeviceError>;
    fn send(&mut self, packet: TxBuffer) -> Result<(), VsockDeviceError>;
    fn free_processed_tx_buffers(&mut self);
}

pub trait VsockDeviceCallback = Fn() + Send + Sync + 'static;

pub fn register_device(
    name: String,
    device: Arc<SpinLock<dyn AnyVsockDevice, BottomHalfDisabled>>,
);

pub fn get_device(
    name: &str,
) -> Option<Arc<SpinLock<dyn AnyVsockDevice, BottomHalfDisabled>>>;

pub fn register_recv_callback(name: &str, callback: impl VsockDeviceCallback);
pub fn register_send_callback(name: &str, callback: impl VsockDeviceCallback);
pub fn all_devices() -> Vec<(String, VsockDeviceRef)>;
```

这样 kernel backend 的接法就完全可以复制 `net/iface/init.rs` 的模式。

## virtio 设备设计

### `VsockDevice`

参考 virtio net，结构收紧为：

```rust
pub struct VsockDevice {
    config_manager: ConfigManager<VirtioVsockConfig>,
    tx_queue: VirtQueue,
    rx_queue: VirtQueue,
    event_queue: VirtQueue,
    tx_buffers: Vec<Option<PendingTx>>,
    rx_buffers: SlotVec<Arc<RxBuffer>>,
    event_buffers: Vec<Option<EventBuffer>>,
    transport: Box<dyn VirtioTransport>,
    rx_taskless: Arc<Taskless>,
    tx_taskless: Arc<Taskless>,
    event_taskless: Arc<Taskless>,
}
```

改动点：

- 不把 `transport` 放进额外 `SpinLock`，除非确实存在并发访问需求
- 不用 `BTreeMap<u16, ...>` 管 token；优先使用 `Vec<Option<_>>` 或 `SlotVec`，贴近现有 virtio net
- RX/TX/EVENT 分离，不在 IRQ 中跨 queue 交叉处理

### `init()`

```rust
impl VsockDevice {
    pub(crate) fn negotiate_features(features: u64) -> u64;

    pub(crate) fn init(mut transport: Box<dyn VirtioTransport>) -> Result<(), VirtioDeviceError>;
}
```

初始化流程：

1. 读取 config，得到 `guest_cid`
2. 初始化 3 条 virtqueue
3. 分配 RX / TX buffer pool
4. 预填充 RX / EVENT buffers
5. 创建 taskless
6. 注册 queue irq callback：
   - RX irq -> `rx_taskless.schedule()`
   - TX irq -> `tx_taskless.schedule()`
   - EVENT irq -> `event_taskless.schedule()`
7. `finish_init()`
8. `aster_vsock::register_device(...)`

### `AnyVsockDevice` 实现

```rust
impl AnyVsockDevice for VsockDevice {
    fn guest_cid(&self) -> u64;
    fn can_send(&self) -> bool;
    fn receive(&mut self) -> Result<RxBuffer, VsockDeviceError>;
    fn send(&mut self, packet: TxBuffer) -> Result<(), VsockDeviceError>;
    fn free_processed_tx_buffers(&mut self);
}
```

这里 `free_processed_tx_buffers()` 与 network 风格保持一致，供 send taskless 或上层回调复用。

## 锁与上下文

### 锁类型

对齐已有代码，尽量少种类：

```text
VsockStreamSocket.state                 -> RwMutex<Takeable<State>>
VsockSpace.inner                        -> SpinLock<_, BottomHalfDisabled>
ListenerInner.incoming_conns            -> SpinLock<_, BottomHalfDisabled>
ConnectionInner.rx_queue                -> SpinLock<_, BottomHalfDisabled>
aster_vsock 组件设备表                  -> SpinLock<_, BottomHalfDisabled>
VsockDevice pending/staging structures  -> SpinLock<_, BottomHalfDisabled>
```

设备级逻辑原则上都放在 taskless 中，因而不需要为了正常数据通路到处使用 `LocalIrqDisabled`。

### 锁顺序

syscall 正向路径：

1. `VsockStreamSocket.state`
2. `VsockSpace.inner`，如需要
3. `ConnectionInner.rx_queue` 或 `ListenerInner.incoming_conns`
4. 设备 `SpinLock`，如需要入队

taskless 逆向路径：

1. 设备 `SpinLock`
2. `VsockSpace.inner`
3. `ConnectionInner.rx_queue` 或 `ListenerInner.incoming_conns`

逆向路径不拿 socket 锁。socket 只在用户态 syscall 入口持有。

## 资源释放语义

### socket drop

`VsockStreamSocket` drop 时，根据当前 `State`：

- `Init`
  - drop `InitStream`
  - 若有 `BoundPort`，自动释放端口
- `Connecting`
  - drop `ConnectingStream`
  - `Connection` drop 通知 backend 取消连接
- `Connected`
  - drop `ConnectedStream`
  - `Connection` drop 触发连接关闭逻辑
- `Listen`
  - drop `ListenStream`
  - `Listener` drop 从 backend listener 表注销

### `Connection` drop

`Drop for Connection`：

- 把对应 `ConnectionInner` 从 `VsockSpace.connections` 移除
- 若连接曾建立，发 `RST` 或执行最小关闭流程
- 唤醒对应 `pollee`

由于 `Connection` 不可 clone，这个 drop 语义是稳定且清晰的。

## 测试重点

### 基础互通

- `socket(AF_VSOCK, SOCK_STREAM, 0)`
- `connect(VMADDR_CID_HOST, 1234)`
- `bind(*, 4321) + listen + accept`
- 双向 `read/write`

### 状态机与资源

- bind 后不 connect，drop 能释放端口
- connecting socket 被 drop，backend 表项清理正确
- accepted socket drop，不影响 listener 继续 accept
- connection reset 后仍保持 `State::Connected`，但内部错误语义正确

### 队列与并发

- RX taskless 与 `recvmsg()` 并发
- TX completion 释放 `queued_tx_bytes` 并唤醒阻塞发送者
- listener backlog 满时 `REQUEST -> RST`
- per-connection queued tx 上限生效

## 结论

这版设计的核心不是“重新发明一套 vsock 架构”，而是把 vsock 尽量塞进 Asterinas 已经存在的组织方式中：

- socket 层对齐 `ip/stream` 的 `State` 和 `try_*` 风格
- backlog 结构对齐 `unix/stream/listener`
- 设备注册与回调对齐 `aster_network`
- bottom half 机制直接使用 `Taskless`
- buffer 直接复用 `RxBuffer` / `TxBuffer` 思路，不再发明多余的 packet wrapper

在这个基础上，只有三处是新增设计：

- `aster_vsock` 组件层
- `BoundPort` / `Connection` / `Listener` 这些真正带 ownership 的 wrapper
- device 级 staging queue + connection 级发送资源记账

这三处正好对应评审指出的三个缺口：跨 crate 解耦、wrapper 语义、以及设备 queue 与 connection 唤醒的关系。
