# Asterinas Virtio-vsock 设计方案 v4

## 目标

本版在 [codex/06-plan.md](/root/asterinas/codex/06-plan.md) 基础上继续收敛，重点修正：

- 不再引入 `aster-vsock` 组件层，vsock 设备注册接口直接放在 `aster-virtio`
- `BoundPort` 改为轻量 ownership wrapper，不再包含 `Arc<...>`
- 端口占用统一由 `VsockSpace` 中的全局 `BTreeMap<port, usage_num>` 管理
- 去掉 `StreamObserver`，统一只用 `Pollee`
- 重新规划设备锁与 bottom half，避免正反向路径锁顺序不一致
- `taskless` 改成全局 TX/RX/EVENT 三类，而不是每个设备各自一组
- `ConnectionInner` 中经常一起访问的状态合并到同一把锁下
- `remote_addr` 不再在 `ConnectingStream` / `ConnectedStream` 中重复缓存

本版仍然遵守一个原则：尽量贴近现有代码组织，但不机械照抄。凡是新增字段或方法，都会说明其设计意图。

## 模块布局

目录回到更直接的方案：

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
  backend/
    mod.rs
    connection.rs
    listener.rs
    port.rs
    space.rs

kernel/comps/virtio/src/device/vsock/
  mod.rs
  config.rs
  header.rs
  device.rs
```

这里 `kernel/comps/virtio/src/device/vsock/mod.rs` 直接提供：

- `get_device()`
- `all_devices()`
- `register_recv_callback()`
- `register_send_callback()`
- `register_event_callback()`

原因很简单：当前只考虑 virtio-vsock，没有必要为了“未来可能有别的 vsock 设备”提前抽象出独立组件层。

## socket 层设计

### `VsockStreamSocket`

保持与 `ip/stream` 一致：

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

为什么仍用 `RwMutex<Takeable<State>>` 而不是普通 `Mutex`：

- 这和现有 `ip/stream`、`unix/stream` 一致
- `read` 路径可以复用现有 `read_updated_state()` / `write_updated_state()` 思路
- `Takeable<State>` 便于在状态转换时移动 ownership，而不是在多层 `Option` 里反复拆装

### `InitStream`

```rust
pub(super) struct InitStream {
    bound_port: Option<BoundPort>,
    is_connect_done: bool,
    is_conn_refused: AtomicBool,
}
```

字段说明：

- `bound_port`
  解释：
  保存 bind 得到的端口 ownership。`Init` 状态下 socket 是否已 bind，完全由它是否为 `Some` 决定。
- `is_connect_done`
  解释：
  直接沿用现有 TCP 的语义，处理异步 connect 拒绝后的“下一次 connect/recv/send/getsockopt 才看到最终错误”。
- `is_conn_refused`
  解释：
  首版只需要最关键的异步错误位，不必提前设计一整个错误枚举缓存。

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
        pollee: Pollee,
    ) -> core::result::Result<ConnectingStream, (Error, Self)>;

    pub(super) fn listen(
        self,
        backlog: usize,
        pollee: Pollee,
    ) -> core::result::Result<ListenStream, (Error, Self)>;

    pub(super) fn finish_last_connect(&mut self) -> Result<()>;

    pub(super) fn local_addr(&self, guest_cid: u32) -> Option<VsockSocketAddr>;

    pub(super) fn try_recv(&self) -> Result<(usize, SocketAddr)>;
    pub(super) fn try_send(&self) -> Result<usize>;
    pub(super) fn check_io_events(&self) -> IoEvents;
    pub(super) fn test_and_clear_error(&self) -> Option<Error>;
}
```

这里 `pollee` 直接传入，不再引入 `StreamObserver`。vsock backend 与 socket 都在 `kernel/` 内，看得到 `Pollee`，没有必要像 TCP + bigtcp 那样单独加 observer 适配层。

### `ConnectingStream`

```rust
pub(super) struct ConnectingStream {
    connection: Connection,
}
```

不再缓存 `remote_addr`，因为这本来就属于连接对象本身。

建议 API：

```rust
impl ConnectingStream {
    pub(super) fn new(
        bound_port: BoundPort,
        remote_addr: VsockSocketAddr,
        pollee: Pollee,
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

### `ConnectedStream`

```rust
pub(super) struct ConnectedStream {
    connection: Connection,
    is_new_connection: bool,
}
```

字段说明：

- `connection`
  解释：
  socket 层唯一拥有的连接 wrapper。它不 `Clone`，drop 时负责通知 backend 做连接回收。
- `is_new_connection`
  解释：
  保持与 TCP 现有行为一致，使 `connect()` 对“刚建立好的连接”只成功一次，后续返回 `EISCONN`。

建议 API：

```rust
impl ConnectedStream {
    pub(super) fn new(
        connection: Connection,
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
    pub(super) fn check_io_events(&self) -> IoEvents;
    pub(super) fn test_and_clear_error(&self) -> Option<Error>;
}
```

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
        pollee: Pollee,
    ) -> core::result::Result<Self, (BoundPort, Error)>;

    pub(super) fn try_accept(&self) -> Result<ConnectedStream>;
    pub(super) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr;
    pub(super) fn bound_port(&self) -> &BoundPort;
    pub(super) fn check_io_events(&self) -> IoEvents;
}
```

## `BoundPort` 与端口资源模型

这是本版最关键的收敛点。

### `BoundPort`

`BoundPort` 只保留轻量 ownership：

```rust
pub(super) struct BoundPort {
    port: u32,
}
```

它不含 `Arc`，也不需要 `Inner`。

设计思想：

- 真正的端口占用状态不放在 `BoundPort` 对象里
- 所有端口资源一律由 `VsockSpace` 的全局表管理
- `BoundPort` 只是“我拥有这个端口的一次使用计数”的证明

### `VsockSpace` 端口表

```rust
struct VsockSpaceInner {
    guest_cid: u32,
    next_ephemeral_port: u32,
    port_usage: BTreeMap<u32, usize>,
    listeners: BTreeMap<u32, Arc<ListenerInner>>,
    connections: BTreeMap<ConnId, Arc<ConnectionInner>>,
}
```

`port_usage` 的含义：

- key：port number
- value：当前这个 port 被多少个 `BoundPort` 持有

资源规则：

1. `bind()` / 自动分配 ephemeral port 时，先检查 `port_usage`
2. 若当前不允许复用，则返回 `EADDRINUSE`
3. 成功后 `usage += 1`，返回一个 `BoundPort { port }`
4. `BoundPort::drop()` 时调用 `VsockSpace::put_bound_port(port)`
5. 若 `usage -= 1` 后为 `0`，则从 `port_usage` 删除，这个 port 才真正释放

这样就能自然支持：

- 一个 listener 持有该 port
- listener accept 出来的若干 connected socket 也各自持有同一个 port
- listener 关闭后，accepted sockets 仍可继续使用该 port
- 最后一个使用者 drop 时，端口才释放

### `BoundPort` API

```rust
impl BoundPort {
    pub(super) fn port(&self) -> u32;
    pub(super) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr;
}

impl Drop for BoundPort {
    fn drop(&mut self);
}
```

这里的 `Drop` 会回到 `VsockSpace` 扣减 `usage_num`。

## `Connection` / `Listener` wrapper

### `Connection`

```rust
pub(super) struct Connection {
    inner: Arc<ConnectionInner>,
}
```

它仍然应该保留 `Arc`，原因和 `BoundPort` 不同：

- connection 需要在 backend connection table 中被查到
- RX/TX/event bottom half 需要通过表拿到它
- socket drop、设备收包、设备发送完成回收，这几个路径会共享同一连接对象

但这个 wrapper 仍然不实现 `Clone`。共享的是 `Arc<ConnectionInner>`，唯一 ownership 是 `Connection`。

### `Listener`

```rust
pub(super) struct Listener {
    inner: Arc<ListenerInner>,
}
```

同理：

- backend listener 表里需要保存 `Arc<ListenerInner>`
- `ListenStream` 拥有唯一的 `Listener` wrapper
- wrapper 不 `Clone`

## backend 设计

### `VsockSpace`

```rust
pub(super) struct VsockSpace {
    inner: SpinLock<VsockSpaceInner, BottomHalfDisabled>,
}

pub(super) fn vsock_space() -> &'static VsockSpace;
```

建议 API：

```rust
impl VsockSpace {
    pub(super) fn guest_cid(&self) -> u32;

    pub(super) fn bind_port(&self, addr: &VsockSocketAddr) -> Result<BoundPort>;
    pub(super) fn get_ephemeral_port(&self) -> Result<BoundPort>;
    pub(super) fn put_bound_port(&self, port: u32);

    pub(super) fn new_listener(
        &self,
        bound_port: BoundPort,
        backlog: usize,
        pollee: Pollee,
    ) -> Result<Listener>;

    pub(super) fn new_connection(
        &self,
        bound_port: BoundPort,
        remote_addr: VsockSocketAddr,
        pollee: Pollee,
    ) -> Result<Connection>;

    pub(super) fn insert_connection(&self, connection: &Arc<ConnectionInner>) -> Result<()>;
    pub(super) fn remove_connection(&self, conn_id: &ConnId);
    pub(super) fn remove_listener(&self, port: u32);
}
```

设计说明：

- `new_listener()` / `new_connection()` 拿走 `BoundPort` ownership
- `insert_connection()` 显式存在，是因为被动 accept 场景下 connection 创建和插表是同一事务
- `remove_connection()` / `remove_listener()` 是 `Drop` 路径使用的显式清理 API

### `ListenerInner`

与 `unix/stream/listener.rs` 对齐：

```rust
pub(super) struct ListenerInner {
    bound_port: u32,
    pollee: Pollee,
    backlog: AtomicUsize,
    incoming_conns: SpinLock<Option<VecDeque<Arc<ConnectionInner>>>, BottomHalfDisabled>,
}
```

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

### `ConnectionInner`

按 `07-review` 的建议，把经常一起访问的字段合并到一把锁里，而不是四散 atomic。

```rust
pub(super) struct ConnectionInner {
    conn_id: ConnId,
    bound_port: u32,
    pollee: Pollee,
    state: SpinLock<ConnectionState, BottomHalfDisabled>,
}

struct ConnectionState {
    phase: Phase,
    remote_addr: VsockSocketAddr,
    error: Option<Error>,
    rx_queue: RxQueue,
    credit: CreditState,
    shutdown: ShutdownState,
    tx_resources: TxResourceState,
}

enum Phase {
    Connecting,
    Connected,
    Closed,
    Reset,
}

struct CreditState {
    peer_buf_alloc: u32,
    peer_fwd_cnt: u32,
    tx_cnt: u32,
}

struct ShutdownState {
    local_read_closed: bool,
    local_write_closed: bool,
    peer_read_closed: bool,
    peer_write_closed: bool,
}

struct TxResourceState {
    queued_bytes: usize,
}

struct RxQueue {
    packets: VecDeque<Arc<RxBuffer>>,
    used_bytes: usize,
    max_bytes: usize,
    read_offset: usize,
}
```

为什么这样合并：

- `rx_queue`、`peer_buf_alloc`、`peer_fwd_cnt`、shutdown 状态在收包和收数时本来就常一起访问
- 全部拆成 atomic 并不会更清晰，反而更容易产生跨字段观察不一致
- 用一把 `SpinLock<ConnectionState>` 可以把协议状态变更保持成一个事务

### `ConnectionInner` API

```rust
impl ConnectionInner {
    pub(super) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr;
    pub(super) fn remote_addr(&self) -> VsockSocketAddr;
    pub(super) fn bound_port(&self) -> u32;

    pub(super) fn has_result(&self) -> bool;
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
    pub(super) fn on_rst(&self);
    pub(super) fn on_shutdown(&self, flags: u32);
    pub(super) fn on_credit_update(&self, buf_alloc: u32, fwd_cnt: u32);
    pub(super) fn enqueue_rx_buffer(&self, buffer: Arc<RxBuffer>) -> Result<()>;
    pub(super) fn release_queued_tx(&self, bytes: usize);

    pub(super) fn check_io_events(&self) -> IoEvents;
    pub(super) fn test_and_clear_error(&self) -> Option<Error>;
}
```

这里 `remote_addr()` 直接从连接状态里取，因此 `ConnectingStream` / `ConnectedStream` 无需再缓存一份。

## 发送路径

### 基本策略

仍然采用：

- socket 层串行化
- 直接写入最终 `TxBuffer`
- device 级 staging queue
- connection 级资源记账

但 `PendingTx` 不再试图直接保存 `ConnectionInner` 类型，而是通过一个回调 trait 释放资源。

### `PendingTx`

`PendingTx` 位于 virtio 模块内部：

```rust
struct PendingTx {
    packet: TxBuffer,
    completion: Box<dyn TxCompletion + Send + Sync>,
}

trait TxCompletion {
    fn complete(&self);
}
```

在 kernel backend 中定义一个实现：

```rust
pub(super) struct ConnectionTxCompletion {
    connection: Arc<ConnectionInner>,
    bytes: usize,
}

impl TxCompletion for ConnectionTxCompletion {
    fn complete(&self) {
        self.connection.release_queued_tx(self.bytes);
    }
}
```

这样：

- `PendingTx` 可以完全留在 virtio 模块内部
- virtio 不需要知道 `ConnectionInner` 的具体类型
- 发送完成时通过 trait 回调把资源还给连接

## virtio 设备注册与全局 taskless

### `virtio::device::vsock::mod.rs`

新增一个全局设备表，直接仿照 `aster_network`：

```rust
pub const DEVICE_NAME: &str = "virtio_vsock";

pub fn register_device(name: String, device: Arc<VsockDevice>);
pub fn get_device(name: &str) -> Option<Arc<VsockDevice>>;
pub fn all_devices() -> Vec<(String, Arc<VsockDevice>)>;

pub fn register_recv_callback(name: &str, callback: impl Fn() + Send + Sync + 'static);
pub fn register_send_callback(name: &str, callback: impl Fn() + Send + Sync + 'static);
pub fn register_event_callback(name: &str, callback: impl Fn() + Send + Sync + 'static);
```

设备少，且当前只考虑 virtio，不需要再多包一层 `AnyVsockDevice` trait。

### 全局 taskless

按 review 的建议，不为每个设备单独建 taskless，而是做成全局三类：

- `vsock_rx_taskless`
- `vsock_tx_taskless`
- `vsock_event_taskless`

IRQ callback 只做：

- 把设备名或设备引用加入全局 pending 列表
- `schedule()` 对应 taskless

例如：

```rust
static PENDING_RX_DEVICES: SpinLock<VecDeque<String>, BottomHalfDisabled>;
static PENDING_TX_DEVICES: SpinLock<VecDeque<String>, BottomHalfDisabled>;
static PENDING_EVENT_DEVICES: SpinLock<VecDeque<String>, BottomHalfDisabled>;
```

taskless handler 流程：

1. 取出 pending device 列表
2. 逐个 `get_device()`
3. 调设备的 `process_rx()` / `process_tx()` / `process_event()`
4. 再触发注册过的 callback

这样结构上更像 `aster_network` 的“全局软中断 + 设备列表”，同时保留了 `Taskless` 这个已有 bottom half 机制。

## `VsockDevice`

### 结构

```rust
pub struct VsockDevice {
    config_manager: ConfigManager<VirtioVsockConfig>,
    guest_cid: AtomicU64,

    rx_queue: SpinLock<VirtQueue, BottomHalfDisabled>,
    tx_queue: SpinLock<VirtQueue, BottomHalfDisabled>,
    event_queue: SpinLock<VirtQueue, BottomHalfDisabled>,

    rx_buffers: SpinLock<SlotVec<Arc<RxBuffer>>, BottomHalfDisabled>,
    tx_buffers: SpinLock<Vec<Option<PendingTx>>, BottomHalfDisabled>,
    event_buffers: SpinLock<Vec<Option<EventBuffer>>, BottomHalfDisabled>,

    transport: SpinLock<Box<dyn VirtioTransport>, BottomHalfDisabled>,
}
```

说明：

- queue 与 buffer 显式拆开，避免持有一个大 device lock 后在 RX 路径里又需要 TX 路径资源
- 锁粒度更小，便于统一正反向路径顺序
- `transport` 仍单独一把锁，因为 notify / config 访问是不同临界区

### 锁顺序

统一规定 device 内部锁顺序：

1. `rx_queue` / `tx_queue` / `event_queue`
2. 对应的 `rx_buffers` / `tx_buffers` / `event_buffers`
3. `transport`

backend 路径不允许在持有 `VsockSpace` 或 `ConnectionState` 锁时再拿 device lock。

也就是说：

- 正向发送：先算好包、释放 connection 锁，再进入 device
- 逆向接收：先从 device 拿出 `RxBuffer`、释放 device 锁，再进入 backend

这样正反两条路径完全不交叉持有 backend lock 和 device lock，自然不会死锁。

### 设备 API

```rust
impl VsockDevice {
    pub(crate) fn negotiate_features(features: u64) -> u64;
    pub(crate) fn init(mut transport: Box<dyn VirtioTransport>) -> Result<(), VirtioDeviceError>;

    pub fn guest_cid(&self) -> u64;
    pub fn can_send(&self) -> bool;

    pub fn send(&self, packet: TxBuffer, completion: Box<dyn TxCompletion + Send + Sync>)
        -> Result<()>;

    pub fn process_rx(&self);
    pub fn process_tx(&self);
    pub fn process_event(&self);
}
```

`process_rx()`：

- 取 used RX descriptor
- 恢复出 `Arc<RxBuffer>`
- 设置 packet len
- 重新填充同一个 token
- 读取 header
- 把 buffer 交给 backend

`process_tx()`：

- 回收 used TX descriptor
- 取出 `PendingTx`
- 调 `completion.complete()`
- 若 queue 重新变为可发送，触发 send callback

`process_event()`：

- 处理 transport reset
- 更新 `guest_cid`
- 触发 event callback

## 设备与 backend 对接

在 `kernel/src/net/socket/vsock/backend/mod.rs` 初始化时：

1. 从 `aster_virtio::device::vsock::get_device(DEVICE_NAME)` 取得设备
2. 注册 `recv/send/event` callback

建议接口：

```rust
pub fn init() {
    if aster_virtio::device::vsock::get_device(aster_virtio::device::vsock::DEVICE_NAME).is_none()
    {
        return;
    }

    aster_virtio::device::vsock::register_recv_callback(
        aster_virtio::device::vsock::DEVICE_NAME,
        || vsock_space().process_rx(aster_virtio::device::vsock::DEVICE_NAME),
    );

    aster_virtio::device::vsock::register_send_callback(
        aster_virtio::device::vsock::DEVICE_NAME,
        || vsock_space().process_tx(aster_virtio::device::vsock::DEVICE_NAME),
    );

    aster_virtio::device::vsock::register_event_callback(
        aster_virtio::device::vsock::DEVICE_NAME,
        || vsock_space().process_event(aster_virtio::device::vsock::DEVICE_NAME),
    );
}
```

## `VsockSpace::process_rx/tx/event`

建议 API：

```rust
impl VsockSpace {
    pub(super) fn process_rx(&self, device_name: &str);
    pub(super) fn process_tx(&self, device_name: &str);
    pub(super) fn process_event(&self, device_name: &str);
}
```

职责：

- `process_rx`
  从设备取 `RxBuffer`，按 header 路由到 listener/connection
- `process_tx`
  主要用于在设备 queue 可用时唤醒等待写的 socket
- `process_event`
  处理 transport reset，刷新 `guest_cid`，并标记现有连接错误

## 与现有代码保持一致的部分

以下设计点本版明确选择“尽量贴现有实现”：

- socket `State` 命名与分层
- `InitStream` 里用 `Option<BoundPort>`
- `is_connect_done` / `is_conn_refused` 的错误传播方式
- `try_send` / `try_recv` / `try_accept` / `check_io_events` 的命名
- `ListenStream::try_accept()` 返回 `ConnectedStream`
- `Pollee` 作为 poll/wakeup 的唯一入口

## 没有完全照抄的部分

以下设计是有意偏离已有 TCP/network 方案的：

- 不引入 `StreamObserver`
  原因：
  vsock backend 与 socket 层同在 kernel 内部，看得到 `Pollee`
- 不做独立 `aster-vsock` 组件
  原因：
  当前设备来源只有 virtio
- 不做 per-connection software TX queue
  原因：
  调度和唤醒复杂度太高
- `ConnectionState` 合并多字段到同一把锁
  原因：
  vsock 协议字段往往一起读写，事务一致性比拆 atomic 更重要

## 仍可能需要你拍板的点

当前还有一个设计点我认为可能存在两种都合理的实现，但先不阻塞文档：

- `transport reset` 后，已处于 `Listen` 的 socket 是否仅刷新 `guest_cid` 后继续可用，还是应该也报一轮可见错误

我先在文档里按“listener 继续可用、connected/connecting 连接报错”处理，因为这更符合 vsock 语义，也最容易和全局 `guest_cid` 更新模型兼容。如果你想要更强的显式错误语义，可以再定。

## 结论

本版的主线已经比较清晰：

- socket 层尽量复用 `ip/stream` 的状态机和命名
- `BoundPort` 回归轻量 ownership，对应的真实资源由 `VsockSpace.port_usage` 管
- `Connection` / `Listener` 才是需要 `Arc` 的共享对象
- 设备注册接口直接放在 `aster-virtio`
- bottom half 使用全局 taskless + pending device 列表
- device lock 与 backend lock 完全分离，避免死锁

这样既保留了和现有代码的整体一致性，也把几个 vsock 特有问题单独解决了：共享本地 port、发送完成回收、以及 virtio-only 的设备管理。*** End Patch
