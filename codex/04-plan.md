# Asterinas Virtio-vsock 细化设计

## 设计修正

相对 [codex/02-plan.md](/root/asterinas/codex/02-plan.md)，本版按 [codex/03-review.md](/root/asterinas/codex/03-review.md) 修正为以下原则：

- `Connection` 只表示“正在建立、已经建立、或者曾经建立”的连接，不再混入 `Init` / `Bound` / `Listening`
- `Socket` 的锁总是 syscall 正向路径最先拿到的锁
- IRQ / bottom half 路径不拿 `Mutex`，只拿 `SpinLock`
- 尽量避免额外拷贝：
  - 发送时从用户缓冲区直接拷入 packet buffer
  - 接收时尽量把收到的 packet payload 直接挂到 connection 的接收队列
- 明确每个队列的上限，避免用户态把内核内存打爆
- 明确 `Arc` 所有权与 wrapper type，避免把 listener/connection/socket 混用

## 总体模块

最终模块划分保持三层，但增加一个关键点：`virtio` 侧和 `kernel` backend 之间通过 trait 注册解耦，避免依赖环。

```text
kernel/src/net/socket/vsock/
  mod.rs
  addr.rs
  common.rs
  stream/
    mod.rs
    socket.rs
  backend/
    mod.rs
    space.rs
    listener.rs
    connection.rs
    port.rs
    queue.rs

kernel/comps/virtio/src/device/vsock/
  mod.rs
  config.rs
  header.rs
  packet.rs
  buffer.rs
  device.rs
```

## 核心类型

### 地址与 ID

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VsockSocketAddr {
    pub cid: u32,
    pub port: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnId {
    pub local_port: u32,
    pub peer_cid: u32,
    pub peer_port: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BoundPort(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GuestCid(u32);
```

约束：

- `BoundPort` 保证不是 `VMADDR_PORT_ANY`
- `ConnId` 不包含本地 CID，guest 当前只有一个有效 CID
- listener 表按 `BoundPort` 索引
- connection 表按 `ConnId` 索引

建议在 `addr.rs` 中提供：

```rust
pub const VMADDR_CID_ANY: u32 = u32::MAX;
pub const VMADDR_PORT_ANY: u32 = u32::MAX;
pub const VMADDR_CID_HYPERVISOR: u32 = 0;
pub const VMADDR_CID_LOCAL: u32 = 1;
pub const VMADDR_CID_HOST: u32 = 2;

impl VsockSocketAddr {
    pub fn is_any_cid(&self) -> bool;
    pub fn is_any_port(&self) -> bool;
}

pub fn normalize_bind_addr(addr: VsockSocketAddr, guest_cid: GuestCid) -> Result<VsockSocketAddr>;
pub fn validate_connect_addr(addr: VsockSocketAddr) -> Result<()>;
```

### wrapper type

用 wrapper 明确语义，避免误把同一个 `Arc<T>` 当成不同角色使用：

```rust
pub struct ListenerRef(Arc<Listener>);
pub struct ConnRef(Arc<Connection>);
```

只暴露必要方法：

```rust
impl Clone for ListenerRef { ... }
impl Clone for ConnRef { ... }

impl ListenerRef {
    pub fn addr(&self) -> VsockSocketAddr;
}

impl ConnRef {
    pub fn id(&self) -> ConnId;
    pub fn peer_addr(&self) -> VsockSocketAddr;
    pub fn local_addr(&self, guest_cid: GuestCid) -> VsockSocketAddr;
}
```

backend 内部可以通过 `ListenerRef` / `ConnRef` 传递对象，减少 API 误用空间。

## Socket 层

### `VsockStreamSocket`

```rust
pub struct VsockStreamSocket {
    state: Mutex<SocketState>,
    is_nonblocking: AtomicBool,
    pollee: Pollee,
    pseudo_path: Path,
}

enum SocketState {
    Init(InitState),
    Bound(BoundState),
    Listening(ListeningState),
    Connecting(ConnectingState),
    Connected(ConnectedState),
    Closed,
}

struct InitState;

struct BoundState {
    local_port: BoundPort,
}

struct ListeningState {
    listener: ListenerRef,
}

struct ConnectingState {
    conn: ConnRef,
}

struct ConnectedState {
    conn: ConnRef,
}
```

这里故意把 `Bound` / `Listening` 留在 socket 层，而不塞进 `Connection`。

### Socket API

```rust
impl VsockStreamSocket {
    pub fn new(is_nonblocking: bool) -> Result<Arc<Self>>;

    fn try_bind_locked(
        state: &mut SocketState,
        socket_addr: VsockSocketAddr,
        pollee: &Pollee,
    ) -> Result<()>;

    fn try_connect_locked(
        state: &mut SocketState,
        peer_addr: VsockSocketAddr,
        pollee: &Pollee,
    ) -> Result<ConnectAction>;

    fn try_listen_locked(
        state: &mut SocketState,
        backlog: usize,
        pollee: &Pollee,
    ) -> Result<()>;

    fn try_accept_locked(
        state: &mut SocketState,
    ) -> Result<(Arc<dyn FileLike>, SocketAddr)>;

    fn try_send_locked(
        state: &mut SocketState,
        reader: &mut dyn MultiRead,
        flags: SendRecvFlags,
    ) -> Result<usize>;

    fn try_recv_locked(
        state: &mut SocketState,
        writer: &mut dyn MultiWrite,
        flags: SendRecvFlags,
    ) -> Result<(usize, MessageHeader)>;

    fn try_shutdown_locked(
        state: &mut SocketState,
        cmd: SockShutdownCmd,
    ) -> Result<()>;
}

enum ConnectAction {
    ConnectedImmediately,
    InProgress(ConnRef),
}
```

说明：

- 所有 syscall 路径先拿 `state: Mutex<SocketState>`
- `SocketState` 内只保存轻量状态和 `ConnRef` / `ListenerRef`
- 真正的 backlog、rx queue、credit、协议状态都不放在 socket mutex 里

### 对应 `Socket` trait 的行为

```rust
impl Socket for VsockStreamSocket {
    fn bind(&self, addr: SocketAddr) -> Result<()>;
    fn connect(&self, addr: SocketAddr) -> Result<()>;
    fn listen(&self, backlog: usize) -> Result<()>;
    fn accept(&self) -> Result<(Arc<dyn FileLike>, SocketAddr)>;
    fn shutdown(&self, cmd: SockShutdownCmd) -> Result<()>;
    fn addr(&self) -> Result<SocketAddr>;
    fn peer_addr(&self) -> Result<SocketAddr>;
    fn sendmsg(...) -> Result<usize>;
    fn recvmsg(...) -> Result<(usize, MessageHeader)>;
    fn pseudo_path(&self) -> &Path;
}
```

## Backend 全局空间

### `VsockSpace`

```rust
pub struct VsockSpace {
    inner: SpinLock<VsockSpaceInner, BottomHalfDisabled>,
}

struct VsockSpaceInner {
    guest_cid: GuestCid,
    next_ephemeral_port: u32,
    listeners: BTreeMap<BoundPort, ListenerRef>,
    connections: BTreeMap<ConnId, ConnRef>,
}
```

导出单例：

```rust
pub fn vsock_space() -> &'static VsockSpace;
```

### `VsockSpace` API

```rust
impl VsockSpace {
    pub fn guest_cid(&self) -> GuestCid;

    pub fn bind_port(&self, requested_port: u32) -> Result<BoundPort>;

    pub fn create_listener(
        &self,
        local_port: BoundPort,
        backlog: usize,
        pollee: Pollee,
    ) -> Result<ListenerRef>;

    pub fn connect(
        &self,
        local_port: BoundPort,
        peer: VsockSocketAddr,
        pollee: Pollee,
    ) -> Result<ConnRef>;

    pub fn remove_listener(&self, local_port: BoundPort);

    pub fn remove_connection(&self, id: ConnId);

    pub fn on_packet(&self, packet: ReceivedPacket);

    pub fn on_transport_reset(&self, new_guest_cid: GuestCid);
}
```

说明：

- `connect()` 会创建 `Connection`，插入 connection 表，并请求 transport 发 `REQUEST`
- `on_packet()` 和 `on_transport_reset()` 由 virtio device 的 bottom half 调用
- `guest_cid()` 只读时仍走短时间 spin lock，保持一致性

## Listener 设计

### `Listener`

```rust
pub struct Listener {
    local_port: BoundPort,
    pollee: Pollee,
    inner: SpinLock<ListenerInner, BottomHalfDisabled>,
}

struct ListenerInner {
    backlog: usize,
    is_closed: bool,
    accept_queue: VecDeque<ConnRef>,
}
```

### Listener API

```rust
impl Listener {
    pub fn new(local_port: BoundPort, backlog: usize, pollee: Pollee) -> Self;

    pub fn local_port(&self) -> BoundPort;

    pub fn set_backlog(&self, backlog: usize);

    pub fn enqueue_incoming(&self, conn: ConnRef) -> Result<()>;

    pub fn dequeue_incoming(&self) -> Result<ConnRef>;

    pub fn close(&self);

    pub fn check_io_events(&self) -> IoEvents;
}
```

语义：

- `accept_queue.len() == backlog` 时，新的 `REQUEST` 直接被 backend 拒绝并回 `RST`
- `enqueue_incoming()` / `dequeue_incoming()` 只拿 listener 的 spin lock
- 监听 socket 的 `poll()` 依赖 `listener.pollee`

## Connection 设计

### `Connection`

```rust
pub struct Connection {
    id: ConnId,
    peer_addr: VsockSocketAddr,
    pollee: Pollee,
    inner: SpinLock<ConnectionInner, BottomHalfDisabled>,
}

struct ConnectionInner {
    state: ConnectionState,
    rx_queue: RxQueue,
    tx_credit: TxCredit,
    shutdown: ShutdownState,
    pending_request_response: Option<ConnectResult>,
}

enum ConnectionState {
    Connecting,
    Established,
    Reset,
    Closed,
}

enum ConnectResult {
    Response,
    Reset(Errno),
}

struct ShutdownState {
    local_read_closed: bool,
    local_write_closed: bool,
    peer_read_closed: bool,
    peer_write_closed: bool,
}

struct TxCredit {
    peer_buf_alloc: u32,
    peer_fwd_cnt: u32,
    tx_cnt: u32,
}
```

关键点：

- `Connection` 不再出现 `Init` / `Bound` / `Listening`
- `Connecting` 通过 `pending_request_response` 表达握手结果
- `Reset` / `Closed` 是连接终态

### 接收队列

```rust
struct RxQueue {
    packets: VecDeque<RxPacket>,
    bytes: usize,
    max_bytes: usize,
    fwd_cnt: u32,
}

struct RxPacket {
    payload: RxPayload,
    offset: usize,
    len: usize,
}
```

其中：

```rust
pub struct RxPayload(Arc<RxPayloadInner>);

struct RxPayloadInner {
    _buffer: Arc<ReceivedBuffer>,
    start: usize,
    end: usize,
}
```

这样收到的 payload 可以直接挂到 connection 队列，不必先拷一份到 `Vec<u8>`。

### Connection API

```rust
impl Connection {
    pub fn new_connecting(
        id: ConnId,
        peer_addr: VsockSocketAddr,
        pollee: Pollee,
    ) -> Self;

    pub fn new_passive(
        id: ConnId,
        peer_addr: VsockSocketAddr,
        pollee: Pollee,
    ) -> Self;

    pub fn id(&self) -> ConnId;

    pub fn peer_addr(&self) -> VsockSocketAddr;

    pub fn check_io_events(&self) -> IoEvents;

    pub fn finish_connect(&self) -> Result<()>;

    pub fn prepare_send(&self, max_len: usize) -> Result<SendBudget>;

    pub fn commit_send(&self, sent_len: usize) -> Result<TxPacketMeta>;

    pub fn queue_received(&self, packet: RxPayload) -> Result<()>;

    pub fn recv_into(&self, writer: &mut dyn MultiWrite) -> Result<usize>;

    pub fn shutdown(&self, cmd: SockShutdownCmd) -> Result<Option<ControlPacket>>;

    pub fn on_response(&self);

    pub fn on_reset(&self, errno: Errno);

    pub fn on_shutdown(&self, flags: VsockShutdownFlags);

    pub fn on_credit_update(&self, hdr: &VirtioVsockHdr);

    pub fn snapshot_credit(&self) -> CreditAdvertisement;
}

pub struct SendBudget {
    pub max_len: usize,
}

pub struct TxPacketMeta {
    pub id: ConnId,
    pub src_port: u32,
    pub dst_cid: u32,
    pub dst_port: u32,
    pub buf_alloc: u32,
    pub fwd_cnt: u32,
}

pub struct CreditAdvertisement {
    pub buf_alloc: u32,
    pub fwd_cnt: u32,
}
```

### `prepare_send()` / `commit_send()` 分拆原因

这是为了同时满足三点：

1. syscall 先拿 socket mutex
2. 不在 spin lock 下访问用户缓冲区
3. 尽量直接把用户数据拷进 packet buffer

发送流程是：

1. `sendmsg()` 拿 `Socket` mutex，确认当前是 `Connected`
2. 调 `conn.prepare_send(reader.sum_lens())`，只拿 connection spin lock，得到本次最多能发多少
3. 释放 connection spin lock，分配 `TxPacketBuffer`
4. 直接把用户数据拷进 `TxPacketBuffer` payload
5. 再调用 `conn.commit_send(copied_len)`，更新 `tx_cnt` 并拿到 header 所需元数据
6. 把 packet 提交给 transport

这样只有一次数据拷贝：用户缓冲区 -> packet buffer。

## 设备侧接口

### `virtio::device::vsock` 对 kernel 暴露的 trait

为避免依赖环，trait 定义在 `kernel/comps/virtio`，由 kernel backend 实现或消费。

```rust
pub trait VsockEventHandler: Send + Sync + 'static {
    fn on_packet(&self, packet: ReceivedPacket);
    fn on_transport_reset(&self, new_guest_cid: u64);
}

pub trait VsockTransport: Send + Sync + 'static {
    fn guest_cid(&self) -> u64;
    fn send(&self, packet: TxPacketBuffer) -> Result<()>;
    fn alloc_tx_packet(&self, payload_len: usize) -> Result<TxPacketBuffer>;
}

pub fn register_vsock_handler(handler: Arc<dyn VsockEventHandler>) -> Result<()>;
pub fn get_vsock_transport() -> Option<Arc<dyn VsockTransport>>;
```

kernel backend 的初始化流程：

```rust
pub fn init_vsock_backend() -> Result<()> {
    let transport = aster_virtio::device::vsock::get_vsock_transport()
        .ok_or_else(|| Error::with_message(Errno::ENODEV, "vsock device is not available"))?;
    let guest_cid = GuestCid(transport.guest_cid() as u32);
    vsock_space().init(guest_cid)?;
    aster_virtio::device::vsock::register_vsock_handler(Arc::new(VsockBackendHandler))?;
    Ok(())
}
```

### `ReceivedPacket` / `TxPacketBuffer`

```rust
pub struct ReceivedPacket {
    pub header: VirtioVsockHdr,
    pub payload: Option<RxPayload>,
}

pub struct TxPacketBuffer {
    header: VirtioVsockHdr,
    payload_len: usize,
    dma: TxDmaBuffer,
}

impl TxPacketBuffer {
    pub fn payload_writer(&mut self) -> TxPacketWriter<'_>;
    pub fn set_header(&mut self, header: VirtioVsockHdr);
}
```

说明：

- `alloc_tx_packet()` 返回尚未提交的 DMA-ready packet buffer
- `payload_writer()` 让 syscall 直接把用户数据写进最终发送 buffer
- device queue 满时，`send()` 返回 `EAGAIN` 风格错误，由 socket 层决定阻塞还是返回

## virtio 设备内部结构

### `VsockDevice`

```rust
pub struct VsockDevice {
    inner: SpinLock<VsockDeviceInner, LocalIrqDisabled>,
    transport: SpinLock<Box<dyn VirtioTransport>, LocalIrqDisabled>,
}

struct VsockDeviceInner {
    guest_cid: u64,
    rx_queue: VirtQueue,
    tx_queue: VirtQueue,
    event_queue: VirtQueue,
    tx_buffers: BTreeMap<u16, TxSubmitted>,
    rx_buffers: BTreeMap<u16, RxSubmitted>,
    event_buffers: BTreeMap<u16, EventSubmitted>,
}
```

这里 device 内部使用 `LocalIrqDisabled`，因为：

- queue completion 既可能来自正常上下文，也可能来自 IRQ
- virtqueue 自身属于设备临界区，不应该与 socket/backend 的 `BottomHalfDisabled` 锁混用

### 设备 API

```rust
impl VsockDevice {
    pub(crate) fn init(transport: Box<dyn VirtioTransport>) -> Result<()>;

    fn handle_rx_irq(&self);
    fn handle_tx_irq(&self);
    fn handle_event_irq(&self);

    fn drain_rx_used(&self) -> Vec<ReceivedPacket>;
    fn drain_event_used(&self) -> Vec<VsockEvent>;

    fn refill_rx_buffers(&self) -> Result<()>;
    fn refill_event_buffers(&self) -> Result<()>;
}
```

IRQ 中只做：

- 回收 used descriptor
- 重新补充 RX / EVENT buffer
- 把 `ReceivedPacket` / `VsockEvent` 推给 bottom half

bottom half 中再调用：

```rust
handler.on_packet(packet);
handler.on_transport_reset(new_guest_cid);
```

## 锁类型与锁顺序

### 锁类型

```text
VsockStreamSocket.state         -> Mutex<SocketState>
VsockSpace.inner                -> SpinLock<_, BottomHalfDisabled>
Listener.inner                  -> SpinLock<_, BottomHalfDisabled>
Connection.inner                -> SpinLock<_, BottomHalfDisabled>
VsockDevice.inner               -> SpinLock<_, LocalIrqDisabled>
VsockDevice.transport           -> SpinLock<_, LocalIrqDisabled>
```

### syscall 正向路径锁顺序

固定顺序：

1. `Socket.state: Mutex`
2. `VsockSpace.inner: SpinLock<_, BottomHalfDisabled>`，如果需要
3. `Listener.inner` 或 `Connection.inner`
4. `VsockDevice.inner`，如果最终要直接提交 TX

约束：

- syscall 可以在持有 `Socket.state` 时短时间拿 backend spin lock
- 不允许持有 backend spin lock 后再去拿 `Socket.state`
- 不允许在持有任何 spin lock 时访问用户地址空间

### 逆向收包路径锁顺序

RX IRQ -> bottom half 路径只允许：

1. `VsockSpace.inner`
2. `Listener.inner` 或 `Connection.inner`

逆向路径永远不拿 `Socket.state: Mutex`。

这样避免了评审指出的“设备逆向路径与 syscall 正向路径锁顺序相反”的问题。

## 队列设计

### listener accept queue

```rust
accept_queue: VecDeque<ConnRef>
```

- 锁：`Listener.inner`
- 上限：`backlog`
- 入队方：bottom half 处理 `REQUEST`
- 出队方：`accept()`

### connection rx queue

```rust
RxQueue {
    packets: VecDeque<RxPacket>,
    bytes: usize,
    max_bytes: 64 * 1024,
    fwd_cnt: u32,
}
```

- 锁：`Connection.inner`
- 入队方：bottom half 处理 `RW`
- 出队方：`recvmsg()`
- 当 `bytes == max_bytes` 时：
  - 不再接受额外 payload
  - 仅依赖 credit 让对端停发
  - 必要时发 `CREDIT_UPDATE`

### device 提交队列

virtqueue 本身就是设备提交队列，不再额外为每个 socket 建一层 software tx queue。最小实现中：

- `send()` 如果 TX queue 有空位，立即提交
- 没空位则返回可重试错误
- 阻塞 socket 由 `sendmsg()` 自行等待 `POLLOUT`

这样可以先避免额外软件队列和多重所有权复杂度。若后续性能需要，再增加全局 pending tx queue。

## 连接建立与关闭

### 主动连接

```rust
Socket::connect()
  -> lock Socket.state
  -> bind local ephemeral port if needed
  -> VsockSpace::connect(...)
  -> state = Connecting { conn }
  -> send REQUEST
  -> block_on(OUT, || conn.finish_connect())
  -> on success: state = Connected { conn }
```

### 被动连接

```rust
bottom half receive REQUEST
  -> lookup ListenerRef by local_port
  -> if no listener: send RST
  -> create ConnRef::new_passive(...)
  -> insert into connection table
  -> listener.enqueue_incoming(conn.clone())
  -> send RESPONSE
```

`accept()`：

```rust
Socket::accept()
  -> lock listening Socket.state
  -> listener.dequeue_incoming()
  -> create new VsockStreamSocket { ConnectedState { conn } }
```

### `shutdown()` / `close()`

```rust
Connection::shutdown(cmd) -> Result<Option<ControlPacket>>
```

返回值语义：

- `Some(ControlPacket::Shutdown(..))`：需要发送 `SHUTDOWN`
- `None`：状态未变化，不必发包

`close()` 的最低正确性要求：

- listener close 时，从 `VsockSpace.listeners` 删除，并让 accept queue 失效
- connected socket close 时：
  - 标记本端读写关闭
  - 尝试发 `SHUTDOWN`
  - 从 `VsockSpace.connections` 删除
  - 后续若再收到该连接的包，一律回 `RST`

首版不做复杂 linger，但会保证表项删除时机一致，不让旧连接污染新连接。

## 报文分发

### backend 报文处理入口

```rust
impl VsockSpace {
    pub fn on_packet(&self, packet: ReceivedPacket) {
        match packet.header.op() {
            VirtioVsockOp::Request => self.on_request(packet),
            VirtioVsockOp::Response => self.on_response(packet),
            VirtioVsockOp::Rst => self.on_rst(packet),
            VirtioVsockOp::Shutdown => self.on_shutdown(packet),
            VirtioVsockOp::Rw => self.on_rw(packet),
            VirtioVsockOp::CreditUpdate => self.on_credit_update(packet),
            VirtioVsockOp::CreditRequest => self.on_credit_request(packet),
        }
    }
}
```

内部辅助函数建议签名：

```rust
fn on_request(&self, packet: ReceivedPacket);
fn on_response(&self, packet: ReceivedPacket);
fn on_rst(&self, packet: ReceivedPacket);
fn on_shutdown(&self, packet: ReceivedPacket);
fn on_rw(&self, packet: ReceivedPacket);
fn on_credit_update(&self, packet: ReceivedPacket);
fn on_credit_request(&self, packet: ReceivedPacket);
```

`CreditRequest` 只需要立刻回一个带最新 `buf_alloc/fwd_cnt` 的 `CREDIT_UPDATE`。

## `Arc` 所有权规则

### 拥有者

- `VsockSpace.listeners` 持有 `ListenerRef`
- `VsockSpace.connections` 持有 `ConnRef`
- `SocketState::Listening` 持有 `ListenerRef`
- `SocketState::Connecting` / `Connected` 持有 `ConnRef`
- `Listener.accept_queue` 持有 `ConnRef`
- `RxQueue` 持有 `RxPayload`

### 不允许的反向引用

以下引用一律禁止：

- `Connection -> Arc<VsockStreamSocket>`
- `Listener -> Arc<VsockStreamSocket>`
- `Connection -> ListenerRef`

原因是：

- 设备逆向路径不应依赖 socket
- 避免 listener/connection/socket 之间形成 `Arc` 环

## 内存上限

为避免 guest 用户态打爆内核内存，首版固定上限：

- 单 connection `rx_queue.max_bytes = 64 KiB`
- 单 listener `accept_queue.len() <= backlog`
- virtio RX/TX DMA buffer 大小固定为一个 packet 上限
- 不额外引入无限增长的 software tx queue

如果用户态读得慢，credit 会自然收缩到 0，host 侧停止继续发送。

## 测试目标

### 最小互通

- guest `connect(VMADDR_CID_HOST, 1234)`
- guest `bind/listen/accept` on port `4321`
- guest/host 双向 `read/write`
- guest 主动 `shutdown`
- host 主动 `shutdown`

### 并发与边界

- 两个线程同时对同一个 socket `sendmsg()`，验证 `Socket.state: Mutex` 的串行化
- `accept()` 与收包并发，验证 backlog queue 无竞争
- `recvmsg()` 与 `RW` 收包并发，验证 rx queue 无竞争
- 重复 bind 同端口返回 `EADDRINUSE`
- 无 listener 时 `REQUEST` 被 `RST`
- 非阻塞 connect/accept/read/write 返回 `EAGAIN`
- transport reset 后已有连接失败，listener 保留

## 建议实现顺序

1. 地址类型、`SocketState`、`ConnId`、`BoundPort`
2. `VsockSpace` / `Listener` / `Connection` 基础 struct 和 wrapper
3. `virtio::device::vsock` trait 注册面与 `VsockDevice` 骨架
4. `connect/REQUEST/RESPONSE/RST`
5. `listen/accept`
6. `RW` 收发与 rx queue
7. credit、`shutdown`、`close`
8. transport reset、边界 errno、测试补全

## 结论

本版设计把类型、API、锁和队列粒度都收紧到了可编码层面，关键决策是：

- `Socket` 只管 syscall 入口状态，`Connection` 只管协议连接状态
- syscall 路径以 `Socket.state: Mutex` 为起点；逆向收包路径绝不反拿 `Socket` 锁
- `Connection` / `Listener` 的共享状态统一落在 `BottomHalfDisabled` 的 `SpinLock`
- 发送直接复制到最终 packet buffer，接收尽量把 payload 直接挂到 `rx_queue`
- `virtio` 与 kernel backend 通过 trait 注册解耦，避免依赖环

按这个结构实现后，代码可以自然满足评审里对锁顺序、上下文约束、引用计数语义和 API 颗粒度的要求。
