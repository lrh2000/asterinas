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
- `accepted connection` 的 socket `Pollee` 不在 backlog 入队时就要求存在，而是在 `accept()` 返回新 socket 前通过 `init_pollee()` 安装到 `ConnectionInner`

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
- backend connection table、RX/EVENT 路径、TX pending drain 路径共享的是 `Arc<ConnectionInner>`
- 但某个具体 socket state 里持有的 `Connection` 只有一个，因此发送独占性仍然由 socket state 锁加 `&mut self` 保证
- `Takeable<Arc<ConnectionInner>>` 用来支持 `Drop` 和“建立失败后取回 inner”这两条路径共存

建议 API：

```rust
impl Connection {
    pub(super) fn new(inner: Arc<ConnectionInner>) -> Self;

    pub(super) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr;
    pub(super) fn remote_addr(&self) -> VsockSocketAddr;
    pub(super) fn bound_port(&self) -> &BoundPort;
    pub(super) fn init_pollee(&self, pollee: Pollee);

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
- `Connection::init_pollee()` 允许 passive accept 路径在新 socket 创建后把它自己的 `Pollee` 绑定到已经排队的 connection 上

这里 wrapper 上的 `&mut self` 很关键：

- 它把“单个 connection 的 syscall 发送路径独占”编码进 socket 层 API
- `ConnectionInner` 仍可以对 RX/EVENT/TX pending drain 提供 `&self` 回调接口
- 但 socket 层不应该绕过 wrapper 直接把共享的 `Arc<ConnectionInner>` 当作发送接口使用

`shutdown()` / `drop` 的关闭语义在这一版里也必须写死，不再留给实现时猜：

- 关闭优先走 graceful path，而不是默认 abortive close
- `VIRTIO_VSOCK_OP_SHUTDOWN` 表示一个永久提示：
  - `RECEIVE` bit 表示“我之后不会再接收数据”
  - `SEND` bit 表示“我之后不会再发送数据”
- clean disconnect 的目标序列是：
  - 一侧发送一个或多个 `SHUTDOWN`
  - 双方都观测到对端不会再发/收后，由对端回 `RST`
  - 若在实现相关超时内等不到这个 `RST`，本端再主动发 `RST`

这与 virtio-vsock 规范和 Linux `virtio_transport_close()` 的行为保持一致：

- `shutdown()` 只发送本次新增关闭方向对应的 `SHUTDOWN` bits
- `close()` / `drop()` 若本地尚未完成双向 shutdown，则先补一个 `SHUTDOWN_MASK`
- 若 close 时已经收到对端的双向 `SHUTDOWN_MASK`，则立即回 `RST`
- 若本端已经发送完双向 `SHUTDOWN_MASK` 但迟迟等不到对端 `RST`，则在超时后主动发 `RST`

因此，“drop 会触发 backend 清理，而不是只减少一个本地引用”这一点必须在设计上写死；但清理过程不是“一律立刻发 `RST`”，而是先遵守 graceful close，再在必要时 fallback 到 `RST`。

更具体地，`Connection::drop` 必须分两个阶段：

1. backend 清理阶段
   - 持有 `VsockSpace.sockets`
   - 如有需要，短时间持有 `ConnectionInner.state`
   - 把连接标记为 closing 终态、决定后续是否需要发送 `SHUTDOWN` 或 `RST`
   - 不在持锁阶段直接碰 `VsockDevice.tx`
2. device 提交阶段
   - 释放所有 backend 锁
   - 若第一阶段决定需要发送 `SHUTDOWN` / `RST`，此时再进入 `VsockDevice.tx`

socket table 的摘表时机也要跟 graceful close 对齐：

- `Connecting` 上的连接在失败/拒绝/本地 close 时可以直接摘表
- `Connected` 上的连接在进入 close 流程后，不能立刻复用 `(local_port, peer_cid, peer_port)` 这组 tuple
- 因此首版实现里，已连接 connection 应保留在 connection table 中直到下面任一事件发生：
  - 收到对端 `RST`
  - close timeout 到期并由本端发送 `RST`
  - 已收到对端双向 `SHUTDOWN` 且本端确认连接进入最终关闭态

这样才能满足 virtio spec 对 clean disconnect 的要求：一端不能在另一端还在处理旧连接时，就重用同一个地址四元组建立新连接。

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

注意 accepted connection 已经转成独立的 `Connection` wrapper，不受 listener drop 影响；它们继续靠各自持有的 `BoundPort` 占住同一个本地 port。这个 `BoundPort` 不从 listener 中“借出”，而是 accept 路径额外为同一个 `port` 再获取一次 usage。与此同时，accepted socket 对应的 `Pollee` 也在 `accept()` 返回前安装到该 `ConnectionInner` 中。

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

注意这里已经去掉了 `process_tx()`，因为 TX pending drain 和 pending queue draining 完全在 virtio 组件内部处理。

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

这里 `pop_incoming()` 返回 `Arc<ConnectionInner>`，再由 `Listener` wrapper 把它包装成唯一的 `Connection` ownership 返回给新 socket。accepted connection 需要的 `BoundPort` 已经在对应 `ConnectionInner` 创建时准备好；对应的 socket `Pollee` 则在 `accept()` 创建新 socket 后通过 `Connection::init_pollee()` 安装。

### `ConnectionInner`

本版做一个重要平衡：

- 大多数协议状态继续合并在一把 `SpinLock<ConnectionState>` 下
- 只有发送快路径必须跨 syscall / TX pending drain 两个上下文共享、且不值得每次重新拿锁的量，才用 atomic

结构如下：

```rust
pub(super) struct ConnectionInner {
    conn_id: ConnId,
    bound_port: BoundPort,
    pollee: Once<Pollee>,
    state: SpinLock<ConnectionState, BottomHalfDisabled>,
    timer: SpinLock<Option<ConnectionTimerState>, BottomHalfDisabled>,
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
    local_fwd_cnt: u32,
    tx_cnt: u32,
}

struct ShutdownState {
    local_read_closed: bool,
    local_write_closed: bool,
    peer_read_closed: bool,
    peer_write_closed: bool,
}

struct ConnectionTimerState {
    generation: u64,
    timer: Arc<Timer>,
}

struct ConnectionTimerEvent {
    conn_id: ConnId,
    generation: u64,
}

struct RxQueue {
    packets: VecDeque<RxBuffer>,
    used_bytes: usize,
    max_bytes: usize,
    read_offset: usize,
}
```

这里把 `local_fwd_cnt` 放在 `CreditState` 里，而不是放进 `RxQueue`：

- `peer_buf_alloc` / `peer_fwd_cnt` / `tx_cnt` / `local_fwd_cnt` 都属于协议层 credit 视图
- `RxQueue` 只负责当前本地接收缓存的内容和读指针
- 每次用户态成功从 `try_recv()` 取走字节后，递增 `credit.local_fwd_cnt`
- 后续所有出站包头里的 `fwd_cnt` 都直接取自 `credit.local_fwd_cnt`

这样 credit 相关字段保持聚合，出站包头生成逻辑也不需要同时跨 `CreditState` 和 `RxQueue` 两处取值。

这里 `BoundPort` 与 `conn_id.local_port` 分别承担不同职责：

- `bound_port` 表达 ownership，并在 inner drop 时释放一次 usage
- `conn_id.local_port` 表达查表和报文头所需的纯数值标识

这两个字段并不冗余，因为一个是 ownership 资源，一个是查表 key。

这里把 `pollee` 设计成 `Once<Pollee>`，用于同时覆盖主动和被动两条建连路径：

- 主动 `connect()` 路径：`ConnectionInner` 创建时立即调用 `init_pollee(pollee)`
- 被动 `accept()` 路径：连接先进入 listener backlog，此时还没有 accepted socket 的 `Pollee`
- 等 `accept()` 真正创建出新的 `VsockStreamSocket` 后，再对该 `ConnectionInner` 调 `init_pollee(pollee)`

这样可以避免为了 accepted socket 额外引入“可替换 pollee”的复杂状态机，同时保持“一条已连接 connection 最终只绑定一个 socket pollee”。

`timer` 的设计约束：

- `ConnectionInner.timer` 外层 `Option` 表示“当前是否存在 active connection timer”
- 任一时刻最多只有一个 active timer；`connecting timeout` 和 `close timeout` 共用这一个 timer slot
- active timer 的身份只靠 `generation` 标识；它来自 backend 内的全局唯一递增计数器，因此不再额外缓存 `kind`
- timer callback 和后续 taskless 只传递值语义的 `ConnectionTimerEvent { conn_id, generation }`，不持有 `Arc/Weak<ConnectionInner>`

这里必须避免 `Arc/Weak<ConnectionInner>` 的原因是：

- `Connection refused` 路径要求 backend 摘表后，`Connection::into_inner().unwrap()` 能稳定成功
- 因此 timeout 机制绝不能在竞争窗口里临时增加 `ConnectionInner` 的 strong count
- 用 `ConnId + generation` 回查 socket table，可以完全绕开 refcount 干扰

为什么单独保留 `available_tx_bytes: AtomicUsize`：

- 它是发送快路径最常访问的量
- `ConnectedStream::try_send(&mut self)` 已经保证单连接发送路径在 syscall 侧是独占的
- TX pending drain 在 virtio taskless 中异步归还额度
- 因此这里用 atomic 是合理的，不会引入复杂竞态，同时能避免每次发送再拿 `state` 锁

`available_tx_bytes` 的语义：

- 表示当前这个 connection 还允许再占用多少 software pending queue 空间
- 只有当数据包因为 hardware queue 满而进入 device pending queue 时，才会消耗这部分额度
- pending 数据包后续被 `process_tx()` 从 software pending queue 推入 hardware queue，或被直接丢弃时，再归还这部分额度

建议 API：

```rust
impl ConnectionInner {
    pub(super) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr;
    pub(super) fn remote_addr(&self) -> VsockSocketAddr;
    pub(super) fn init_pollee(&self, pollee: Pollee);

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

`init_pollee()` 的约束：

- 底层用 `Once<Pollee>`，所以同一个 `ConnectionInner` 只能初始化一次
- 主动 `connect()` 和被动 `accept()` 都必须在各自路径上恰好调用一次
- backlog 中尚未被 accept 的 pending connection，不依赖自己的 `Pollee` 发通知；这阶段的可见事件仍由 listener 的 `Pollee` 表达
- 一旦 accepted socket 安装了自己的 `Pollee`，后续 `RX` / `RST` / `SHUTDOWN` / credit update 都通知这个连接的 `Pollee`

这里刻意不把 `ConnectionInner::try_send()` 暴露为 socket 层主接口。发送路径需要的独占语义由 `Connection::try_send(&mut self, ...)` 承担；`ConnectionInner` 只保留 RX/EVENT/TX pending drain 共享路径必须使用的接口，以及接收侧/状态侧那些不会破坏独占发送语义的方法。

`shutdown()` / `on_shutdown()` 的具体语义：

- `ConnectionInner::shutdown(cmd)`：
  - 只对本次新增关闭方向生效
  - 若 `cmd` 没有新增关闭任何方向，则返回成功，不重复发包
  - 锁外发送一个 `VIRTIO_VSOCK_OP_SHUTDOWN`，flags 仅包含这次新增关闭方向
- `ConnectionInner::on_shutdown(flags)`：
  - 更新 `peer_read_closed` / `peer_write_closed`
  - 若对端已经双向 `SHUTDOWN_MASK`：
    - 该连接应从 connection table 中摘除，避免旧连接长期占住 lookup key
    - 若本地也已经双向关闭，且 `rx_queue` 中已经没有未读数据，则进入“需要回 `RST` 并完成最终关闭”的路径
  - 无论单向还是双向 shutdown，都要通知本连接 `Pollee`

这与 Linux `virtio_transport_recv_connected()` 的行为保持同一方向：对端双向 `SHUTDOWN` 后，连接不再保留为可接收新数据的 established 连接；本地在适当时机回 `RST` 收尾。

统一的 connection timer 语义：

- `Connecting` 状态下，active timer 表示 connect timeout
- graceful close 流程中，active timer 表示 close timeout
- 这两者在状态机上互斥，因此共享同一个 timer slot 是成立的
- timeout handler 通过：
  - `ConnId` 在 `VsockSpace.sockets.connections` 中回查
  - `generation` 与 `ConnectionInner.timer` 当前 active generation 比对
  - 当前 `ConnectionState.phase` 和 shutdown 状态
  来决定这是 connect timeout 还是 close timeout

## 发送路径

### 基本思路

保留早期版本里已经解释清楚的设计：

- syscall 先锁 socket state
- `ConnectedStream::try_send(&mut self)` 独占该 connection 的发送路径
- 从用户缓冲区直接拷入最终 `TxBuffer`
- 不做 `prepare_send/commit_send` 双阶段接口

发送流程：

1. 读取 `ConnectionState.credit`，确定本次最多可写的 payload
2. 分配 `TxBuffer`
3. 直接把用户数据写入 `TxBuffer`
4. 进入 device TX 路径提交
5. 若 hardware queue 未满，包直接进入 hardware queue，本次发送立即成功，不占用 per-socket software pending 额度
6. 若 hardware queue 已满，则尝试通过 `connection.reserve_tx_bytes(payload_len)` 为该 connection 的 software pending queue 预留空间；成功后把包挂入 device pending queue

这里不再在“成功进入 hardware queue”的路径上构造 completion，也不再把 in-flight hardware queue 数据计入 `available_tx_bytes`。

理由：

- 对上层 socket 语义而言，只要包已经进入 virtio hardware queue，就视为已经发出
- software queue 长度控制的目的，是防止某个连接在 device queue 满时无限堆积本地待发数据
- 因此真正需要被限制和归还的是 device pending queue 中、尚未送入 hardware queue 的那部分数据，而不是已经进入 hardware queue 的数据

这里明确允许一个 in-flight race：

- 在步骤 2 完成、释放 `ConnectionInner.state` 之后，到步骤 6 真正提交到 device 之前，
  连接状态可能因为 `RST` / `SHUTDOWN` / transport reset 而变化
- 首版实现允许这个窗口内已经构造好的包继续进入 device queue，视为一个可接受的 in-flight packet race

理由：

- 这样可以保持锁顺序简单，不需要为了提交前 recheck 再次进入 backend 锁
- 这种 race 只影响极少数边界包，不破坏整体协议收敛
- 对端若已关闭或重置，最终仍会靠 `RST` / 错误状态完成一致化

### pending queue 记账

这里仍然需要保留一个 trait object，但它的语义已经收敛成“software pending queue 记账释放回调”，而不是 “hardware queue completion”。

```rust
trait TxCompletion: Send + Sync {}

struct PendingTx {
    packet: TxBuffer,
    completion: Option<Box<dyn TxCompletion>>,
}

struct ReleasePendingBytes {
    connection: Arc<ConnectionInner>,
    bytes: usize,
}

impl TxCompletion for ReleasePendingBytes {}

impl Drop for ReleasePendingBytes {
    fn drop(&mut self) {
        self.connection.release_tx_bytes(self.bytes);
    }
}
```

语义：

- `kernel/comps/virtio` 不能看到 `ConnectionInner`，因此挂在 pending 项里的对象必须是 trait object
- 控制包默认不带 `TxCompletion`
- 数据包只有在进入 software pending queue 时，才创建一个 `ReleasePendingBytes`
- 当 `process_tx()` 把这个 pending 包成功推入 hardware queue，或因为设备/连接终止而直接丢弃时，这个对象被 drop，并调用 `connection.release_tx_bytes(bytes)`

这样更贴合这里真正要限制的对象：

- 受限的是 per-socket software pending queue 长度
- 不是 hardware queue 中的 in-flight 数据长度

### 默认发送不需要额外 completion

review 提到一个关键点：默认并不应该强制构造 completion。

在本版里这条原则进一步收敛为：

- 控制包：`RST`、`RESPONSE`、`CREDIT_UPDATE` 等，默认不做 pending 记账
- 连接数据包：
  - 若立即进入 hardware queue，也不需要额外对象
  - 只有在进入 software pending queue 时，才附带一个 `TxCompletion` trait object

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

- 默认控制包只用 `try_send()`，不需要额外记账
- 若 queue 满，`try_send()` 直接返回 `TxPendingGuard`
- 控制包可以直接 `push_pending()`
- 连接数据包则调用 `push_pending_tracked(completion)`
- 同一把 TX 锁在整个决策过程中保持持有，不会出现“发现满了再二次拿锁”的重复开销

这比简单的“所有发送路径都附带统一记账参数”的接口更贴这里最终收敛出的语义：

- 成功进入 hardware queue 的路径不需要 completion
- 只有 software pending queue 才需要 per-socket 记账
- `queue full` 只有一种失败路径，因此没有必要额外引入 `SendTrackedError` 这类单变体错误类型

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

除此之外，backend 侧还需要一个独立的 `timer_taskless`：

- `virtio` 里的 RX/TX/EVENT taskless 只处理设备底半部
- `vsock backend` 里的 `timer_taskless` 只处理 connection timeout 事件
- timer callback 本身不做重逻辑，只把 `ConnectionTimerEvent` 放进 pending queue，再调度这个 `timer_taskless`

### 对 kernel 暴露的 callback

`virtio::device::vsock::mod.rs` 只保留：

```rust
pub fn register_recv_callback(name: &str, callback: impl Fn() + Send + Sync + 'static);
pub fn register_event_callback(name: &str, callback: impl Fn() + Send + Sync + 'static);
```

不再有 `register_send_callback()`。

原因：

- TX pending drain、唤醒发送者这些逻辑都可以在 virtio 内部完成
- kernel backend 不需要感知“某个 TX 已完成”
- 暴露 TX callback 只会扩大模块边界

## connection timeout

这里统一描述 `connecting timeout` 和 `close timeout`，不再只写后者。

connection timer 的 owner 与执行上下文：

- `TimerManager` 负责维护所有已 arm 的 timer，本身就是全局的 deadline 管理者
- `ConnectionInner` 只持有自己的 active timer
- `VsockSpace` 不再额外维护“所有等待超时 connection”的全局表
- `VsockSpace` / backend singleton 只维护：
  - `pending_timer_events: VecDeque<ConnectionTimerEvent>`
  - `timer_taskless: Arc<Taskless>`

这样职责分离很清楚：

- `TimerManager` 决定“什么时候到”
- `ConnectionInner.timer` 决定“这个 timer 属于谁、当前 generation 是多少”
- backend `timer_taskless` 决定“超时后协议状态怎么推进”

首版实现要求：

- 为每个 `ConnectionInner` 提供统一的 connection timer 机制
- timer callback 只投递 `ConnectionTimerEvent { conn_id, generation }`
- timer callback 不允许：
  - 持有 `Arc/Weak<ConnectionInner>`
  - 直接拿 backend/device 锁
  - 直接发送 `SHUTDOWN` / `RST`
- `timer_taskless` 处理事件时：
  - 先按 `ConnId` 回查 `VsockSpace.sockets.connections`
  - 查不到则直接丢弃
  - 查到后，再核对 `generation`
  - 只有匹配当前 active timer 的事件才真正处理

- `connecting timeout`：
  - 若连接仍处于 `Connecting`，则摘表、写入 `ETIMEDOUT`、置 connect result ready、通知 pollee
  - 若连接已被 `RESPONSE` / `RST` / refused 路径处理，则该 timer event 因 generation 不匹配或查表失败而失效
- `close timeout`：
  - 当 `close()` / `drop()` 发送了最终的 `SHUTDOWN_MASK` 后，若对端没有在实现相关超时内回 `RST`，本端必须主动发 `RST`
  - 这个超时是协议收敛所必需的；否则本地会一直保留一个 closing connection，占住 lookup key 和 port usage
- timeout handler 的职责仅限于：
  - 在 backend 锁下确认当前状态仍然需要处理这个 timeout
  - 做 connect timeout 或 close timeout 的状态推进决定
  - 若需要发 `RST`，释放 backend 锁后再实际发送
- 若在 timeout 到来前连接已经进入其它终态，则取消 timer；来不及取消的 stale event 也必须被 generation 检查挡掉

这里不要求首版复刻 Linux 的完整 `linger` 语义，但必须保留两条最小正确性：

- `connect` 能超时并返回 `ETIMEDOUT`
- graceful close 会等待对端 `RST`，超时后本端 `RST` 收尾

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

`ConnectionInner.available_tx_bytes` 通过 atomic 快路径访问，不计入锁顺序链。这里它表达的是 software pending queue 可用额度，而不是 hardware queue in-flight 额度。

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
- TX pending drain 归还额度只触碰 `available_tx_bytes` 和必要的唤醒逻辑，不反向持有 socket state 锁

## 需要保留的早期设计信息

这几条是前几版已经澄清、不能再丢掉的：

- `Connection` 只表示连接，不表示 `Init` / `Listen`
- `BoundPort` 需要 ownership 语义，drop 会释放一次 port usage
- `Connection` / `Listener` 是 wrapper，不可 clone；backend 用 `Arc<ConnectionInner>` / `Arc<ListenerInner>`
- `BoundPort` 放在 `ConnectionInner` / `ListenerInner` 中；只有 `Connection` wrapper 因为需要 `into_inner` 才使用 `Takeable<Arc<_>>`
- `Connection refused` 回到 `InitStream` 时，必须先确保 backend 已摘除对应 `Arc<ConnectionInner>`，再 `into_inner().unwrap()` 并取回 `BoundPort`
- connection timer 事件必须是值语义的 `ConnId + generation`，不能持有 `Arc/Weak<ConnectionInner>`
- `ConnectionInner.pollee` 用 `Once<Pollee>`；主动 connect 立即初始化，被动 accept 在创建新 socket 后通过 `init_pollee()` 初始化
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
- `connect timeout` 与 refused 竞争时，不会因为 timer 机制额外增加 `ConnectionInner` strong count，从而破坏 `into_inner().unwrap()`
- `accept` 返回后，新 socket 会初始化对应 `ConnectionInner` 的 `Pollee`，后续 RX/RST/shutdown 事件能正确唤醒它
- TX queue 满时：
  - 控制包直接返回可重试错误
  - 数据包能通过 `TxPendingGuard` 进入 pending
- pending 数据后续离开 software pending queue 时归还 `available_tx_bytes`
- 非阻塞发送在额度不足或 queue 满时返回 `EAGAIN`

### 事件

- 收到 `REQUEST` 时 backlog 满返回 `RST`
- 收到 `RST` 后连接错误对 `send/recv/poll` 可见
- `connect` 超时返回 `ETIMEDOUT`
- `shutdown(SHUT_WR)` 后对端看到 EOF / `peer_write_closed`
- 双向 graceful close 最终由 `RST` 收尾
- 若对端不回 `RST`，close timeout 到期后本端发送 `RST`
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
- software pending queue 与连接资源归还机制清晰
