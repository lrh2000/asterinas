# Asterinas Virtio-vsock 设计方案 v5

## 目标与原则

本版综合以下输入后重新收敛：

- [codex/01-guide.md](/root/asterinas/codex/01-guide.md)
- [codex/03-review.md](/root/asterinas/codex/03-review.md)
- [codex/05-review.md](/root/asterinas/codex/05-review.md)
- [codex/07-review.md](/root/asterinas/codex/07-review.md)
- [codex/08-plan.md](/root/asterinas/codex/08-plan.md)
- [codex/09-review.md](/root/asterinas/codex/09-review.md)

目标仍然不变：只实现 guest 侧 `AF_VSOCK` `SOCK_STREAM` 的最小闭环：

- `bind`
- `listen` / `accept`
- `connect`
- `read` / `write`
- `shutdown` / `close`

不实现：

- socket options
- `SOCK_DGRAM` / `SOCK_SEQPACKET`
- 其它非 virtio 的 vsock 后端

本版重点修正四个方向：

1. 继续贴近已有代码风格，尤其是 `ip/stream` 的 `State` 和 `try_*` 命名。
2. `BoundPort` 继续保持轻量 ownership，端口占用由 `VsockSpace` 的全局表管理。
3. `VsockDevice` 的锁按 RX/TX/EVENT 三个方向聚合，不再过细拆分。
4. `process_tx` 完全隐藏在 virtio 组件内部，kernel backend 只关心 RX 和 EVENT。

## 模块布局

最终目录：

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
    port.rs
    listener.rs
    connection.rs
    space.rs

kernel/comps/virtio/src/device/vsock/
  mod.rs
  config.rs
  header.rs
  device.rs
```

`virtio/src/device/vsock/mod.rs` 只负责：

- 注册/获取设备
- 注册 RX / EVENT callback
- 维护全局 TX/RX/EVENT taskless 的 pending 设备列表

它不再对 kernel 暴露 `process_tx` 回调接口。

## socket 层

### `VsockStreamSocket`

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

沿用 `ip/stream` 的结构，不再新增 `Bound` / `Closed` 之类额外状态。

### `InitStream`

```rust
pub(super) struct InitStream {
    bound_port: Option<BoundPort>,
    is_connect_done: bool,
    is_conn_refused: AtomicBool,
}
```

设计说明：

- `bound_port`：
  保存本 socket 对本地 port 的一次 ownership。它是 bind 语义的一部分，不是 backend 共享状态。
- `is_connect_done` / `is_conn_refused`：
  直接复用现有 TCP stream 对“异步 connect 失败”这类场景的处理方式。

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

### `ConnectingStream`

```rust
pub(super) struct ConnectingStream {
    connection: Connection,
}
```

不缓存 `remote_addr`，也不单独持有 `BoundPort`；这两个信息都直接从 `Connection` 取。

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

最重要的约束：

- `try_send()` 和 `try_recv()` 采用 `&mut self`
- 原因不是“连接对象本身线程不安全”，而是 syscall 持有 socket state 锁后，本连接的发送路径已经独占
- 因此发送快路径不需要再为了同一连接的多线程竞争做额外锁设计

建议 API：

```rust
impl ConnectedStream {
    pub(super) fn new(connection: Connection, is_new_connection: bool) -> Self;

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

## `BoundPort` 与 port 资源

### `BoundPort`

```rust
pub(super) struct BoundPort {
    port: u32,
}
```

它依然是轻量 ownership wrapper，不含 `Arc`。

设计意图：

- `BoundPort` 表示“我对这个 port 持有一次占用”
- 它本身不保存共享状态
- 它的 `Drop` 只负责回到全局表把 `usage_num` 减一

### `VsockSpace` 中的 port 表

port 资源和 listener/connection 表拆成不同锁：

```rust
pub(super) struct VsockSpace {
    ports: SpinLock<PortTable, BottomHalfDisabled>,
    sockets: SpinLock<SocketTable, BottomHalfDisabled>,
}

struct PortTable {
    next_ephemeral_port: u32,
    usage: BTreeMap<u32, usize>,
}

struct SocketTable {
    guest_cid: u32,
    listeners: BTreeMap<u32, Arc<ListenerInner>>,
    connections: BTreeMap<ConnId, Arc<ConnectionInner>>,
}
```

这样拆分的原因：

- port 分配/释放与 listener/connection 查表是不同临界区
- `bind()` 和 `BoundPort::drop()` 只需要 port 表
- 收包路由只需要 socket 表
- 减少不必要的锁竞争

资源规则：

1. `bind_port()` / `get_ephemeral_port()` 在 `ports` 锁下检查 `usage`
2. 分配成功后 `usage += 1`
3. 返回 `BoundPort { port }`
4. `BoundPort::drop()` 调 `vsock_space().put_bound_port(port)`
5. `usage == 0` 时才真正释放该 port

这自然支持：

- listener 与 accepted socket share 同一 port
- listener 关闭后，已建立连接仍继续占住 port
- 最后一个使用者 drop 后再释放

本版把这个 ownership 放在 `ConnectionInner` / `ListenerInner` 里，而不是放在 wrapper 或 stream state 里：

- `InitStream` 在尚未进入 backend wrapper 之前直接持有 `BoundPort`
- `ConnectionInner` 持有一个 `BoundPort`
- `ListenerInner` 持有一个 `BoundPort`
- accepted connection 创建时，需要为 listener 的本地 port 再获取一个新的 `BoundPort`，并放入新的 `ConnectionInner`

这样语义更直接：

- 哪个 backend 对象代表一个仍然有效的 listener / connection，它就显式持有一次 `BoundPort`
- port ownership 和 backend 生命周期绑定，不会在 wrapper / inner 之间来回拆分
- `Connection refused` 时，可以在 backend 已经摘表后把 `Arc<ConnectionInner>` 直接 `into_inner`，再取回其中的 `BoundPort`

## `Connection` / `Listener` wrapper

这两个 wrapper 在本版里必须明确写清楚，因为它们承载的是 socket 层的唯一 ownership，而不是 backend 里的共享引用。

### `Connection`

```rust
pub(super) struct Connection {
    inner: Takeable<Arc<ConnectionInner>>,
}
```

语义：

- `Connection` 是 socket 层持有的 wrapper
- 它不实现 `Clone`
- 它对外仍然是唯一的 socket-side ownership，但真正的 port ownership 放在 `ConnectionInner`
- backend connection table、RX/EVENT 路径、TX completion 路径共享的是 `Arc<ConnectionInner>`
- 但某个具体 socket state 里持有的 `Connection` 只有一个，因此发送独占性仍然由 socket state 锁加 `&mut self` 保证
- `Takeable<Arc<ConnectionInner>>` 用来支持 `Drop` 和“建立失败后取回 inner”这两条路径共存

建议 API：

```rust
impl Connection {
    pub(super) fn new(inner: Arc<ConnectionInner>) -> Self;

    pub(super) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr;
    pub(super) fn remote_addr(&self) -> VsockSocketAddr;
    pub(super) fn bound_port(&self) -> &BoundPort;

    pub(super) fn has_result(&self) -> bool;
    pub(super) fn finish_connect(&mut self) -> Result<()>;
    pub(super) fn into_inner(self) -> Option<ConnectionInner>;

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

    pub(super) fn shutdown(&mut self, cmd: SockShutdownCmd) -> Result<()>;

    pub(super) fn check_io_events(&self) -> IoEvents;
    pub(super) fn test_and_clear_error(&self) -> Option<Error>;
}
```

`into_inner(self) -> Option<ConnectionInner>` 是这一版新增的关键 API。

语义约束：

- 只在连接已经确定进入 refused 终态后调用
- 调用前，backend connection table 中必须已经不存在该 `Arc<ConnectionInner>`
- 调用前，其它临时共享引用也必须已经释放
- 满足这两个条件后，`Arc<ConnectionInner>` 的 strong count 必然是 1，因此 `Arc::into_inner` 必须成功
- 成功后，socket state 转换代码再从返回的 `ConnectionInner` 中取出 `BoundPort`，构造 `InitStream::new_refused(bound_port)`

返回 `Option` 而不是复杂错误类型的原因：

- 失败分支本质上表示 plan 约束被破坏，而不是一个可恢复运行时分支
- caller 在 refused 路径上已经知道前置条件应当成立，通常直接 `unwrap()` 更清楚
- 这样 API 更贴近 `Arc::into_inner` 的真实语义，也避免伪造一个“可恢复错误”接口

这样做的好处是：

- `BoundPort` ownership 完整地留在 `ConnectionInner` 中，和 backend 状态绑定
- API 显式编码了“只有建立失败且 backend 已清理完毕，才能把连接退回 init state”
- `Connection` 本身仍然不需要知道 `InitStream`，保持 backend wrapper 与 socket state 分层清晰
- `Takeable` 让 `Drop` 路径与 `into_inner` 路径不会互相抢 ownership

这里 wrapper 上的 `&mut self` 很关键：

- 它把“单个 connection 的 syscall 发送路径独占”编码进 socket 层 API
- `ConnectionInner` 仍可以对 RX/EVENT/TX completion 提供 `&self` 回调接口
- 但 socket 层不应该绕过 wrapper 直接把共享的 `Arc<ConnectionInner>` 当作发送接口使用

`Drop for Connection` 的语义也必须明确：

- 从 `VsockSpace.sockets.connections` 中移除该连接
- 若连接尚未进入终态，向对端发起最小必要的关闭通知
- 唤醒本地等待该连接状态变化的读写/轮询方

这里不要求在 `drop` 时无条件发送某一种固定控制包；具体是 `SHUTDOWN`、`RST` 还是“仅本地回收”取决于当时 `ConnectionState.phase` 和 shutdown 状态。但“drop 会触发 backend 清理，而不是只减少一个本地引用”这一点必须在设计上写死。

更具体地，`Connection::drop` 必须分两个阶段：

1. backend 清理阶段
   - 持有 `VsockSpace.sockets`
   - 如有需要，短时间持有 `ConnectionInner.state`
   - 把连接标记为终态、从 connection table 摘掉、决定后续是否需要发送控制包
2. device 提交阶段
   - 释放所有 backend 锁
   - 若第一阶段决定需要发送 `SHUTDOWN` / `RST`，此时再进入 `VsockDevice.tx`

这样 `drop` 路径与正常 syscall 发送路径保持同一原则：

- backend lock 不和 device lock 同时持有
- backend 只负责做状态决定
- 真正的设备提交发生在释放 backend 锁之后

### `Listener`

```rust
pub(super) struct Listener {
    inner: Arc<ListenerInner>,
}
```

语义：

- `Listener` 只由 `ListenStream` 持有
- 它不实现 `Clone`
- 它对外仍然是唯一的 socket-side ownership，但真正的 port ownership 放在 `ListenerInner`
- backend listener table 里保存的是 `Arc<ListenerInner>`
- `Listener` 不需要 `into_inner` 之类的回收路径，因此不需要 `Takeable`

建议 API：

```rust
impl Listener {
    pub(super) fn new(inner: Arc<ListenerInner>) -> Self;

    pub(super) fn try_accept(&self) -> Result<Connection>;
    pub(super) fn set_backlog(&self, backlog: usize);
    pub(super) fn shutdown(&self);
    pub(super) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr;
    pub(super) fn bound_port(&self) -> &BoundPort;
    pub(super) fn check_io_events(&self) -> IoEvents;
}
```

`Drop for Listener` 的语义：

- 先把 listener 标记为 shutdown，不再接受新的 `REQUEST`
- 从 `VsockSpace.sockets.listeners` 中移除该 listener
- 唤醒 backlog 上阻塞的 `accept()` / 连接建立方

注意 accepted connection 已经转成独立的 `Connection` wrapper，不受 listener drop 影响；它们继续靠各自持有的 `BoundPort` 占住同一个本地 port。这个 `BoundPort` 不从 listener 中“借出”，而是 accept 路径额外为同一个 `port` 再获取一次 usage。

`Listener::drop` 也遵守同样的顺序要求：

1. 在 `VsockSpace.sockets` 和 `ListenerInner.incoming_conns` 下完成 shutdown 与摘表
2. 释放 backend 锁
3. 若需要对 backlog 中尚未完成握手的对象做额外通知或清理，在锁外继续进行

首版最小实现里，listener drop 本身不需要持锁发 device 控制包；但这里仍把顺序写清楚，避免后续扩展时把 device TX 重新带进 backend 锁作用域。

## backend

### `VsockSpace` API

```rust
impl VsockSpace {
    pub(super) fn guest_cid(&self) -> u32;

    pub(super) fn bind_port(&self, addr: &VsockSocketAddr) -> Result<BoundPort>;
    pub(super) fn get_ephemeral_port(&self) -> Result<BoundPort>;
    pub(super) fn share_port(&self, port: u32) -> Result<BoundPort>;
    pub(super) fn put_bound_port(&self, port: u32);

    pub(super) fn new_listener(
        &self,
        bound_port: BoundPort,
        backlog: usize,
        pollee: Pollee,
    ) -> core::result::Result<Listener, (Error, BoundPort)>;

    pub(super) fn new_connection(
        &self,
        bound_port: BoundPort,
        remote_addr: VsockSocketAddr,
        pollee: Pollee,
    ) -> core::result::Result<Connection, (Error, BoundPort)>;

    pub(super) fn insert_connection(&self, connection: &Arc<ConnectionInner>) -> Result<()>;
    pub(super) fn remove_connection(&self, conn_id: &ConnId);
    pub(super) fn remove_listener(&self, port: u32);

    pub(super) fn process_rx(&self, device_name: &str);
    pub(super) fn process_event(&self, device_name: &str);
}
```

注意这里已经去掉了 `process_tx()`，因为 TX completion 和 pending queue draining 完全在 virtio 组件内部处理。

这里新增 `share_port(port)`，专门表达“这个 port 已经被本地 listener/connection 使用，但我现在要为另一个逻辑 socket 再增加一次 usage”。

它和 `bind_port(addr)` 的区别是：

- `bind_port(addr)` 走普通 bind 语义检查
- `share_port(port)` 不重新做 bind 语义判断，只在 `ports` 锁下把该 port 的 `usage += 1`

accepted connection 应该通过 `share_port(listener.bound_port().port())` 获得自己的 `BoundPort`，先创建新的 `ConnectionInner`，再包装成新的 `Connection`

### `ListenerInner`

继续对齐 `unix/stream/listener.rs`：

```rust
pub(super) struct ListenerInner {
    bound_port: BoundPort,
    pollee: Pollee,
    backlog: AtomicUsize,
    incoming_conns: SpinLock<Option<VecDeque<Arc<ConnectionInner>>>, BottomHalfDisabled>,
}
```

建议 API：

```rust
impl ListenerInner {
    pub(super) fn push_incoming(&self, conn: Arc<ConnectionInner>) -> Result<()>;
    pub(super) fn pop_incoming(&self) -> Result<Arc<ConnectionInner>>;
    pub(super) fn set_backlog(&self, backlog: usize);
    pub(super) fn shutdown(&self);
    pub(super) fn check_io_events(&self) -> IoEvents;
}
```

这里 `pop_incoming()` 返回 `Arc<ConnectionInner>`，再由 `Listener` wrapper 把它包装成唯一的 `Connection` ownership 返回给新 socket。accepted connection 需要的 `BoundPort` 已经在对应 `ConnectionInner` 创建时准备好。

### `ConnectionInner`

本版做一个重要平衡：

- 大多数协议状态继续合并在一把 `SpinLock<ConnectionState>` 下
- 只有发送快路径必须跨 syscall / TX completion 两个上下文共享、且不值得每次重新拿锁的量，才用 atomic

结构如下：

```rust
pub(super) struct ConnectionInner {
    conn_id: ConnId,
    bound_port: BoundPort,
    pollee: Pollee,
    state: SpinLock<ConnectionState, BottomHalfDisabled>,
    available_tx_bytes: AtomicUsize,
}

struct ConnectionState {
    phase: Phase,
    remote_addr: VsockSocketAddr,
    error: Option<Error>,
    rx_queue: RxQueue,
    credit: CreditState,
    shutdown: ShutdownState,
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

struct RxQueue {
    packets: VecDeque<RxBuffer>,
    used_bytes: usize,
    max_bytes: usize,
    read_offset: usize,
}
```

这里 `BoundPort` 与 `conn_id.local_port` 分别承担不同职责：

- `bound_port` 表达 ownership，并在 inner drop 时释放一次 usage
- `conn_id.local_port` 表达查表和报文头所需的纯数值标识

这两个字段并不冗余，因为一个是 ownership 资源，一个是查表 key。

为什么单独保留 `available_tx_bytes: AtomicUsize`：

- 它是发送快路径最常访问的量
- `ConnectedStream::try_send(&mut self)` 已经保证单连接发送路径在 syscall 侧是独占的
- TX completion 在 virtio taskless 中异步归还额度
- 因此这里用 atomic 是合理的，不会引入复杂竞态，同时能避免每次发送再拿 `state` 锁

`available_tx_bytes` 的语义：

- 表示当前这个 connection 还允许再向 device staging queue 预占多少发送缓冲
- 发包前 CAS 减少
- 发送完成后由 completion 的 `Drop` 增加

建议 API：

```rust
impl ConnectionInner {
    pub(super) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr;
    pub(super) fn remote_addr(&self) -> VsockSocketAddr;

    pub(super) fn has_result(&self) -> bool;
    pub(super) fn finish_connect(&self) -> Result<()>;

    pub(super) fn on_response(&self);
    pub(super) fn on_rst(&self);
    pub(super) fn on_shutdown(&self, flags: u32);
    pub(super) fn on_credit_update(&self, buf_alloc: u32, fwd_cnt: u32);
    pub(super) fn enqueue_rx_buffer(&self, buffer: RxBuffer) -> Result<()>;

    pub(super) fn reserve_tx_bytes(&self, want: usize) -> Result<usize>;
    pub(super) fn release_tx_bytes(&self, bytes: usize);

    pub(super) fn try_recv(
        &self,
        writer: &mut dyn MultiWrite,
        flags: SendRecvFlags,
    ) -> Result<usize>;

    pub(super) fn shutdown(&self, cmd: SockShutdownCmd) -> Result<()>;

    pub(super) fn check_io_events(&self) -> IoEvents;
    pub(super) fn test_and_clear_error(&self) -> Option<Error>;
}
```

这里刻意不把 `ConnectionInner::try_send()` 暴露为 socket 层主接口。发送路径需要的独占语义由 `Connection::try_send(&mut self, ...)` 承担；`ConnectionInner` 只保留 RX/EVENT/TX completion 共享路径必须使用的接口，以及接收侧/状态侧那些不会破坏独占发送语义的方法。

## 发送路径

### 基本思路

保留早期版本里已经解释清楚的设计：

- syscall 先锁 socket state
- `ConnectedStream::try_send(&mut self)` 独占该 connection 的发送路径
- 从用户缓冲区直接拷入最终 `TxBuffer`
- 不做 `prepare_send/commit_send` 双阶段接口

发送流程：

1. 通过 `connection.reserve_tx_bytes(reader.sum_lens())` 取得本次最多可占用的发送额度
2. 读取 `ConnectionState.credit`，确定本次最多可写的 payload
3. 分配 `TxBuffer`
4. 直接把用户数据写入 `TxBuffer`
5. 构造一个 completion 对象，记录 `connection: Arc<ConnectionInner>` 和 `bytes`
6. 进入 device TX 路径提交或排入 pending

这里明确允许一个 in-flight race：

- 在步骤 2 完成、释放 `ConnectionInner.state` 之后，到步骤 6 真正提交到 device 之前，
  连接状态可能因为 `RST` / `SHUTDOWN` / transport reset 而变化
- 首版实现允许这个窗口内已经构造好的包继续进入 device queue，视为一个可接受的 in-flight packet race

理由：

- 这样可以保持锁顺序简单，不需要为了提交前 recheck 再次进入 backend 锁
- 这种 race 只影响极少数边界包，不破坏整体协议收敛
- 对端若已关闭或重置，最终仍会靠 `RST` / 错误状态完成一致化

### completion 设计

按 `09-review`，completion 不需要显式 `complete()`，靠 `Drop` 即可：

```rust
trait TxCompletion: Send + Sync {}

struct ConnectionTxCompletion {
    connection: Arc<ConnectionInner>,
    bytes: usize,
}

impl TxCompletion for ConnectionTxCompletion {}

impl Drop for ConnectionTxCompletion {
    fn drop(&mut self) {
        self.connection.release_tx_bytes(self.bytes);
    }
}
```

为什么这样更合适：

- TX completion 只应该发生一次
- Rust 编译器已经保证对象只会被 drop 一次
- 比手写 `complete()` 更不容易遗漏或重复调用

### 默认发送不需要 completion

review 提到一个关键点：默认并不应该强制构造 completion。

因此 TX API 分成两类：

- 控制包：`RST`、`RESPONSE`、`CREDIT_UPDATE` 等，默认不需要 completion
- 连接数据包：需要 completion 来归还 connection 的发送额度

## `VsockDevice`

### 结构

按 `09-review` 调整后，queue 和对应 buffer / pending 状态合并在同一把锁里：

```rust
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
}

struct RxState {
    queue: VirtQueue,
    buffers: SlotVec<RxBuffer>,
}

struct EventState {
    queue: VirtQueue,
    buffers: Vec<Option<EventBuffer>>,
}

struct SubmittedTx {
    packet: TxBuffer,
    completion: Option<Box<dyn TxCompletion>>,
}

struct PendingTx {
    packet: TxBuffer,
    completion: Option<Box<dyn TxCompletion>>,
}
```

为什么这样分：

- RX 路径总是需要同时访问 RX queue 和 RX buffer
- TX 路径总是需要同时访问 TX queue、inflight 和 pending
- 再细拆没有收益，只会制造锁顺序问题

### 显式 TX/RX 锁

为避免一批包处理时重复加锁，device 提供显式 guard：

```rust
impl VsockDevice {
    pub fn lock_tx(&self) -> TxGuard<'_>;
    pub fn lock_rx(&self) -> RxGuard<'_>;
    pub fn lock_event(&self) -> EventGuard<'_>;
}
```

#### `TxGuard`

```rust
pub struct TxGuard<'a> {
    device: &'a VsockDevice,
    state: SpinLockGuard<'a, TxState, BottomHalfDisabled>,
}
```

建议 API：

```rust
impl<'a> TxGuard<'a> {
    pub fn can_send(&self) -> bool;

    pub fn try_send(&mut self, packet: TxBuffer) -> Result<(), TxPendingGuard<'a>>;

    pub fn drain_used(&mut self);
}

pub struct TxPendingGuard<'a> {
    state: SpinLockGuard<'a, TxState, BottomHalfDisabled>,
    packet: TxBuffer,
}

impl<'a> TxPendingGuard<'a> {
    pub fn push_pending(self);
    pub fn push_pending_tracked(self, completion: Box<dyn TxCompletion>);
}
```

这个 API 的设计意图：

- 默认控制包只用 `try_send()`，不需要 completion
- 若 queue 满，`try_send()` 直接返回 `TxPendingGuard`
- 控制包可以直接 `push_pending()`
- 连接数据包则调用 `push_pending_tracked(completion)`
- 同一把 TX 锁在整个决策过程中保持持有，不会出现“发现满了再二次拿锁”的重复开销

这比简单的 `send(packet, completion)` 更贴 review 想表达的“completion 不是默认参数、TX lock 应该显式”的方向，而且 `queue full` 只有一种失败路径，因此没有必要额外引入 `SendTrackedError` 这类单变体错误类型。

#### `RxGuard`

```rust
pub struct RxGuard<'a> {
    device: &'a VsockDevice,
    state: SpinLockGuard<'a, RxState, BottomHalfDisabled>,
}

impl<'a> RxGuard<'a> {
    pub fn pop_used(&mut self) -> Option<RxBuffer>;
}
```

这样在 RX taskless 中可以一次持锁处理多个 used RX 包，避免每包重复进出锁。

### 设备 API

```rust
impl VsockDevice {
    pub(crate) fn negotiate_features(features: u64) -> u64;
    pub(crate) fn init(mut transport: Box<dyn VirtioTransport>) -> Result<(), VirtioDeviceError>;

    pub fn guest_cid(&self) -> u64;
    pub fn process_rx(&self);
    pub fn process_tx(&self);
    pub fn process_event(&self);
}
```

这里 `process_tx()` 只在 virtio 模块内部的 TX taskless 中调用，不对 kernel backend 暴露回调。

## bottom half 与 callback

### 全局 taskless

仍采用全局三类 taskless：

- RX
- TX
- EVENT

pending 列表仍按设备维度维护。

### 对 kernel 暴露的 callback

`virtio::device::vsock::mod.rs` 只保留：

```rust
pub fn register_recv_callback(name: &str, callback: impl Fn() + Send + Sync + 'static);
pub fn register_event_callback(name: &str, callback: impl Fn() + Send + Sync + 'static);
```

不再有 `register_send_callback()`。

原因：

- TX completion、pending drain、唤醒发送者这些逻辑都可以在 virtio 内部完成
- kernel backend 不需要感知“某个 TX 已完成”
- 暴露 TX callback 只会扩大模块边界

### backend 初始化

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

    aster_virtio::device::vsock::register_event_callback(
        aster_virtio::device::vsock::DEVICE_NAME,
        || vsock_space().process_event(aster_virtio::device::vsock::DEVICE_NAME),
    );
}
```

## 锁顺序

本版不把 syscall 正向发送写成一条从 socket 锁一直拿到 device 锁的长链，因为真实设计不是那样。它实际上分成两个阶段。

### syscall 正向路径: backend 决策阶段

1. `VsockStreamSocket.state`
2. `VsockSpace.ports` 或 `VsockSpace.sockets`
3. `ConnectionInner.state` 或 `ListenerInner.incoming_conns`

这个阶段只做：

- 检查 socket state 合法性
- 查询/更新 backend 状态
- 计算 credit 和可发送额度
- 构造待发送的 `TxBuffer`

到准备进入 device TX 之前，必须释放：

- `VsockSpace.sockets`
- `ConnectionInner.state`
- `ListenerInner.incoming_conns`

`ConnectionInner.available_tx_bytes` 通过 atomic 快路径访问，不计入锁顺序链。

### syscall 正向路径: device 提交阶段

1. `VsockStreamSocket.state`
2. `VsockDevice.tx`

这里 socket state 锁仍可能保持着，但 backend 的 spin lock 已经全部释放，所以不会和 RX/EVENT 逆向路径形成环。

### RX / EVENT taskless 逆向路径

1. `VsockDevice.rx` 或 `VsockDevice.event`
2. 释放 device 锁
3. `VsockSpace.sockets`
4. `ConnectionInner.state` 或 `ListenerInner.incoming_conns`

因此正反向路径的共同约束是：

- device lock 不与 backend lock 同时持有
- backend lock 只在 backend 决策阶段出现
- TX completion 归还额度只触碰 `available_tx_bytes` 和必要的唤醒逻辑，不反向持有 socket state 锁

## 需要保留的早期设计信息

这几条是前几版已经澄清、不能再丢掉的：

- `Connection` 只表示连接，不表示 `Init` / `Listen`
- `BoundPort` 需要 ownership 语义，drop 会释放一次 port usage
- `Connection` / `Listener` 是 wrapper，不可 clone；backend 用 `Arc<ConnectionInner>` / `Arc<ListenerInner>`
- `BoundPort` 放在 `ConnectionInner` / `ListenerInner` 中；只有 `Connection` wrapper 因为需要 `into_inner` 才使用 `Takeable<Arc<_>>`
- `Connection refused` 回到 `InitStream` 时，必须先确保 backend 已摘除对应 `Arc<ConnectionInner>`，再 `into_inner().unwrap()` 并取回 `BoundPort`
- 接收尽量直接使用 `RxBuffer`
- 发送直接从用户态拷到最终 `TxBuffer`
- 不能在原子上下文里访问用户内存
- backlog 和队列都必须有容量上限

## 测试目标

### 基础闭环

- guest -> host `connect/read/write`
- host -> guest `listen/accept/read/write`
- 双向 `shutdown`
- close 后资源回收

### 资源与并发

- 同一 listener accept 多个连接，`port_usage` 递增递减正确
- listener close 后 accepted connection 继续可用
- `connect` 被拒绝后，backend 摘表完成，`Connection::into_inner().unwrap()` 成功，并回退成 `InitStream::new_refused(bound_port)`
- TX queue 满时：
  - 控制包直接返回可重试错误
  - 数据包能通过 `TxPendingGuard` 进入 pending
- TX 完成后 completion drop 归还 `available_tx_bytes`
- 非阻塞发送在额度不足或 queue 满时返回 `EAGAIN`

### 事件

- 收到 `REQUEST` 时 backlog 满返回 `RST`
- 收到 `RST` 后连接错误对 `send/recv/poll` 可见
- transport reset 后刷新 `guest_cid`，连接报错

## 结论

本版相对 `08-plan` 的关键变化是：

- `VsockSpace` 的 port 和 socket 管理拆成两把锁
- `ConnectionInner` 恢复一个原子发送额度字段，用于发送快路径
- `VsockDevice` 的锁按 TX/RX/EVENT 三块聚合
- `tx_buffers` 明确同时保存 in-flight buffer，`pending` 也留在同一个 TX 锁里
- `process_tx()` 被彻底隐藏在 virtio 组件中
- 发送 API 改成显式 `lock_tx()` / `TxGuard` 风格，并支持 queue 满时用 guard 挂 pending

到这里，设计已经把你几轮 review 中最核心的矛盾点都收敛了：

- ownership 和共享状态分离
- socket 风格对齐已有代码
- device/backend 锁边界清晰
- TX queue 与 pending queue 的关系清晰
- TX completion 与连接资源归还机制清晰
