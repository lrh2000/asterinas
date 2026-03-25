# Asterinas Virtio-vsock 设计方案

## 目标与范围

本次只实现 Asterinas guest 侧的 `AF_VSOCK` `SOCK_STREAM` 最小可用功能，支持 guest 与 host 之间的：

- `bind` / `listen` / `accept`
- `connect`
- 双向数据收发
- `shutdown` / `close`

明确不包含：

- `SOCK_DGRAM` / `SOCK_SEQPACKET`
- 大部分 `getsockopt` / `setsockopt`
- `/dev/vsock` ioctl
- guest 到 guest 路由
- 超出最小闭环所需的高级特性

实现必须遵守仓库现有边界：`kernel/` 保持纯 safe Rust，所有 virtio 设备交互放在 `kernel/comps/virtio/`。

## 现状与约束

仓库里已经有一部分 vsock 入口，但实现还未落地：

- `sys_socket()` 已接受 `AF_VSOCK + SOCK_STREAM`，并尝试创建 `VsockStreamSocket`
- `SocketAddr` 已支持 `SocketAddr::Vsock`
- `sockaddr_vm` 与 `VsockSocketAddr` 的用户态地址转换已存在
- `test/initramfs` 已有 guest 侧 `vsock_client` / `vsock_server` 测试程序
- `kernel/comps/virtio/src/device/socket/buffer.rs` 已提供可复用的 DMA buffer pool

同时我确认到两个关键现状：

- `kernel/src/net/socket/vsock/` 目录目前还不存在，需要从零建立
- `kernel/comps/virtio/src/device/` 还没有 `vsock/` 设备实现

因此，本次工作本质上是补齐一条新的 socket 栈，从 syscall 入口一直打到 virtio transport。

## 对外语义

用户态接口以 Linux `vsock(7)` 为准，只覆盖本次需要的子集：

- 地址为 `(cid, port)` 二元组
- 支持 `VMADDR_CID_ANY` 和 `VMADDR_PORT_ANY`
- guest 侧允许连接 `VMADDR_CID_HOST`
- `bind(VMADDR_CID_ANY, port)` 绑定到当前 guest CID
- 未绑定的主动连接 socket 在 `connect()` 时自动分配本地 ephemeral port
- `listen()` 后只接受发往本地 CID/本地端口的连接
- `shutdown(SHUT_WR)` 之后禁止继续发送
- 对端关闭发送方向后，本端 `read()` 在数据耗尽后返回 `0`
- 非阻塞语义、`poll`/`epoll` 可读写事件语义尽量对齐现有 TCP/Unix stream socket

本次不实现 Linux 全量错误细节，但会保证以下核心 errno 正确：

- `EADDRINUSE`
- `EADDRNOTAVAIL`
- `EINVAL`
- `EISCONN`
- `ENOTCONN`
- `ECONNREFUSED`
- `EAGAIN`
- `EPIPE`

## 协议依据

virtio-vsock 的最小协议流程直接按 Virtio 规范实现：

- 连接建立：`REQUEST -> RESPONSE`，若目标不存在或拒绝则返回 `RST`
- 数据传输：`RW`
- 流控：每个 stream 包都携带 `buf_alloc` / `fwd_cnt`，必要时发送 `CREDIT_REQUEST` / `CREDIT_UPDATE`
- 关闭：`SHUTDOWN` 表示单向或双向关闭，最终用 `RST` 完成连接终止
- 设备事件：处理 `TRANSPORT_RESET`，关闭已建立连接并刷新 guest CID

这里的 `SHUTDOWN` / `RST` / credit 行为来自 Virtio 1.2 第 5.10.6 节；CID 和 `VMADDR_CID_ANY/HOST` 的用户态语义参考 Linux `vsock(7)`。

## 总体架构

按 `01-guide.md` 的要求拆成三层：

1. `kernel/src/net/socket/vsock/stream/`
   - 对外实现 `Socket` trait
   - 管理阻塞/非阻塞、poll 事件、用户态读写、syscall 语义
   - 不直接操作 virtqueue

2. `kernel/src/net/socket/vsock/backend/`
   - 维护全局连接表、监听表、端口分配和连接状态机
   - 接收 virtio 层上送的包事件
   - 向 socket 实例分发连接建立、数据到达、对端关闭、连接重置等事件

3. `kernel/comps/virtio/src/device/vsock/`
   - 实现 virtio-vsock 设备驱动
   - 负责 config/queue/header/DMA buffer/中断/bottom half
   - 提供一个内核内部的 `VsockTransport` 抽象给 backend 调用

核心原则是：socket 层描述“文件语义”，backend 描述“连接语义”，virtio 层描述“设备语义”。

## 模块布局

建议新增如下文件：

```text
kernel/src/net/socket/vsock/
  mod.rs
  addr.rs
  common.rs
  backend/
    mod.rs
    addr.rs
    connect.rs
    listener.rs
    connection.rs
    manager.rs
    queue.rs
  stream/
    mod.rs
    init.rs
    connecting.rs
    connected.rs
    listen.rs
    observer.rs

kernel/comps/virtio/src/device/vsock/
  mod.rs
  config.rs
  header.rs
  packet.rs
  device.rs
  buffer.rs
```

命名和分层尽量贴近现有 `ip/stream` 与 `virtio/network`，这样可以复用 Asterinas 既有模式，降低接入成本。

## 数据模型

### 地址

`VsockSocketAddr` 保持简单：

```rust
pub struct VsockSocketAddr {
    pub cid: u32,
    pub port: u32,
}
```

建议在 `addr.rs` 中补充：

- `VMADDR_CID_ANY = u32::MAX`
- `VMADDR_PORT_ANY = u32::MAX`
- `VMADDR_CID_HYPERVISOR = 0`
- `VMADDR_CID_LOCAL = 1`
- `VMADDR_CID_HOST = 2`

以及辅助判断：

- 是否是通配 CID
- 是否是通配端口
- 是否允许作为本地 bind 地址
- 是否允许作为当前阶段的远端 connect 地址

最小实现中，本地 bind 只接受：

- `VMADDR_CID_ANY`
- 当前 guest CID

远端 connect 只保证支持：

- `VMADDR_CID_HOST`

`VMADDR_CID_LOCAL` 是否支持，取决于 virtio transport 是否实际能回环；首版先不承诺。

### backend 全局管理器

`VsockSpace` 作为全局单例，负责：

- 当前 `guest_cid`
- 监听端口表：`BindAddr -> Listener`
- 已建立/建立中的连接表：`ConnKey -> Connection`
- ephemeral port 分配器
- 指向 virtio device 的发送入口

建议 key 设计：

```rust
struct BindKey {
    local_port: u32,
}

struct ConnKey {
    local_port: u32,
    peer_cid: u32,
    peer_port: u32,
}
```

这里监听表只按本地端口索引，不按本地 CID 索引，原因是 guest 只有一个当前 CID；当 transport reset 导致 CID 改变时，只需更新 `guest_cid`，监听 socket 继续有效。

### listener

listener 维护：

- 绑定地址
- backlog 上限
- 已完成握手、等待 `accept()` 的连接队列
- pollee / wait queue

收到远端 `REQUEST` 时：

1. backend 查 listener
2. 若不存在，发送 `RST`
3. 若 backlog 满，发送 `RST`
4. 否则创建新的 backend connection，加入 accept 队列，并发送 `RESPONSE`

### connection

每条 stream 连接维护：

- 本地地址、对端地址
- 状态机
- 接收缓冲区
- 发送方向关闭标志
- 接收方向关闭标志
- 对端 credit 信息
- 本端 `buf_alloc` / `fwd_cnt`
- poll/pollee 事件源

建议状态机：

- `Init`
- `Bound`
- `Listening`
- `Connecting`
- `Connected`
- `PeerClosed`
- `Closing`
- `Closed`
- `Reset`

其中 socket 层状态和 backend 连接状态不必一一对应；socket 层只需要能表达 syscall 语义，backend 才需要精确反映协议阶段。

## 锁与并发

这是本次实现最容易出错的部分，必须在设计阶段先定规矩。

### 锁分层

建议固定锁顺序：

1. `VsockSpace` 全局表锁
2. `Listener` 内部锁
3. `Connection` 内部锁
4. `Socket` 自身状态锁

禁止反向获取，避免 `accept/connect/close` 与中断下半部交叉时死锁。

### 中断与 bottom half

virtio queue 完成中断里只做轻量工作：

- 回收 used descriptor
- 解析包头
- 将事件投递到 backend 处理队列
- 触发 bottom half / softirq

真正修改连接状态、推入接收缓冲区、唤醒等待者的逻辑，统一放到 bottom half 中执行。这样可以避免：

- 在原子上下文里拿 mutex
- 在原子上下文里访问用户态内存
- 在中断上下文里做复杂表操作

### 用户态读写

`sendmsg` / `recvmsg` 不直接持有 backend 自旋锁去读写用户缓冲区。推荐模式：

1. 先在 socket/backend 中确定可读写长度
2. 复制到中间 `Vec<u8>` 或内核缓冲区
3. 释放关键锁
4. 再执行用户态内存读写
5. 需要时重新进入 backend 更新 credit / fwd_cnt

这比“零拷贝但长时间持锁”更符合当前代码库的安全约束。

## virtio 设备设计

### 设备组成

virtio-vsock 使用三条队列：

- `RX`：接收来自 host 的包
- `TX`：发送到 host 的包
- `EVENT`：transport reset 等事件

设备初始化需要完成：

- 读取 `guest_cid`
- 协商 features
- 建立三条 virtqueue
- 预填充 RX / EVENT buffer
- 注册中断回调
- 注册到内核中的 vsock backend

### 包头与包类型

`header.rs` 定义 `virtio_vsock_hdr` 的 Rust 表达：

- `src_cid`
- `dst_cid`
- `src_port`
- `dst_port`
- `len`
- `type`
- `op`
- `flags`
- `buf_alloc`
- `fwd_cnt`

`op` 至少支持：

- `REQUEST`
- `RESPONSE`
- `RST`
- `SHUTDOWN`
- `RW`
- `CREDIT_UPDATE`
- `CREDIT_REQUEST`

最小实现中可以不支持 `SEQPACKET`，收到未知 `type/op` 时直接按规范回 `RST`。

### 发送路径

backend 不直接碰 virtqueue，而是调用 `VsockTransport::send(packet)`。

`send(packet)` 负责：

- 从 DMA pool 分配 TX buffer
- 填充 header 和 payload
- 投递到 TX queue
- 视情况 notify
- 在 completion 后回收 buffer

发送失败语义建议：

- 队列暂时满：向上返回“稍后重试”，socket 层映射为阻塞等待或 `EAGAIN`
- 设备永久不可用：连接转 `Reset`

### 接收路径

RX used buffer 完成后：

1. 提取 header/payload
2. 立刻补一个新的 RX buffer 回队列
3. 将解析出的只读 `ReceivedPacket` 投递给 backend

backend 根据 `(local_port, peer_cid, peer_port)` 路由到连接或 listener。

### transport reset

收到 `TRANSPORT_RESET` 时：

- 重新读取 `guest_cid`
- 所有已连接/连接中 socket 转为 `Reset`
- 唤醒阻塞中的 `connect/read/write/poll`
- listener 保留，但后续上报本地地址时使用新的 guest CID

这部分不是“锦上添花”，而是规范中明确要求的最小正确性。

## socket 层设计

### `VsockStreamSocket`

整体风格直接参考 `ip/stream/StreamSocket`：

- `state: RwLock<Takeable<State>>`
- `is_nonblocking: AtomicBool`
- `pollee: Pollee`
- `pseudo_path: Path`

状态可拆成：

- `Init`
- `Connecting`
- `Connected`
- `Listen`

这样可以最大化复用现有 socket 框架和 `SocketPrivate::block_on()` 模式。

### `bind()`

流程：

1. 校验地址族和地址长度
2. 若端口为 `ANY`，从 backend 分配 ephemeral port
3. 若 CID 为 `ANY`，解析为当前 guest CID
4. 调 backend 注册绑定
5. socket 进入 `Bound` 或留在 `Init+bound_addr` 形式

约束：

- 重复 bind 返回 `EINVAL`
- 绑定非本地 CID 返回 `EADDRNOTAVAIL`
- 端口冲突返回 `EADDRINUSE`

### `connect()`

流程：

1. 若未绑定，自动绑定 `(guest_cid, ephemeral_port)`
2. 在 backend 注册一条 `Connecting` 连接
3. 通过 transport 发送 `REQUEST`
4. 阻塞或非阻塞等待 `RESPONSE/RST`

结果：

- 收到 `RESPONSE` -> `Connected`
- 收到 `RST` -> `ECONNREFUSED`
- transport reset / device error -> `ECONNRESET` 或 `ENOTCONN`

### `listen()` / `accept()`

`listen()` 将已绑定 socket 转成 listener，并在 backend 注册 backlog 队列。

`accept()`：

1. 若 accept 队列为空，阻塞或返回 `EAGAIN`
2. 取出一个已建立 backend connection
3. 生成新的 `VsockStreamSocket::new_accepted(...)`
4. 返回新 fd 和对端 `SocketAddr::Vsock`

监听 socket 本身不参与数据收发。

### `sendmsg()` / `recvmsg()`

`sendmsg()`：

- 仅在 `Connected` 可用
- 尊重本端 `SHUT_WR` 和对端 `SHUT_RD`
- 若 peer credit 不足：
  - 阻塞 socket 等待 `CREDIT_UPDATE`
  - 非阻塞返回 `EAGAIN`
- 根据 payload 长度拆成一个或多个 `RW` 包

`recvmsg()`：

- 从连接接收缓冲区取数据
- 若缓冲区为空且对端未关闭，阻塞或 `EAGAIN`
- 若缓冲区为空且对端已关闭发送方向，返回 `0`
- 每次消费数据后推进本端 `fwd_cnt`，必要时发 `CREDIT_UPDATE`

首版不实现 `MSG_PEEK` 等复杂 flags；收到不支持的 flags 返回 `EOPNOTSUPP`。

### `shutdown()` / `close()`

`shutdown(SHUT_WR)`：

- 设置本端发送关闭
- 发 `SHUTDOWN`，携带 `SEND` 标志

`shutdown(SHUT_RD)`：

- 设置本端接收关闭
- 发 `SHUTDOWN`，携带 `RECEIVE` 标志
- 丢弃未读缓冲数据是否立即执行，可按 Linux 常见语义处理为“后续不再向用户交付”

`close()`：

- 若是 listener，直接从 backend 注销
- 若是 connected socket：
  - 若尚未发送双向 shutdown，先发对应 `SHUTDOWN`
  - 等待对端或定时后发送 `RST`

最小实现里可以不引入复杂 linger；但必须避免地址四元组在旧连接未清理时被过早复用。

## credit 流控方案

vsock stream 不能像“随便塞数据”那样实现，必须维护 credit。

每条连接维护：

- `tx_cnt`: 本端累计发出的 payload 字节数
- `peer_buf_alloc`
- `peer_fwd_cnt`
- `rx_buf_alloc`: 本端总接收缓冲容量
- `rx_forwarded_cnt`: 本端累计被应用读走的字节数

发送前可用 credit：

```text
peer_free = peer_buf_alloc - (tx_cnt - peer_fwd_cnt)
```

若 `peer_free == 0`：

- 发送 `CREDIT_REQUEST`
- 阻塞等待对端新的 `CREDIT_UPDATE` 或 piggyback 在其他包里的 credit 更新

本地接收缓冲建议首版固定容量，例如每连接 `64 KiB`。这足以支撑测试，同时把实现控制在可审查范围内。

## 与测试用例的对应

现有测试脚本已表达出两个最小场景：

1. guest client -> host server
   - guest 连接 `(VMADDR_CID_HOST, 1234)`
   - guest 发送命令并读取回包

2. host client -> guest server
   - guest 监听 `(*, 4321)`
   - host 连接 guest CID 的 `4321`
   - guest `accept/read/write`

因此首批回归测试应覆盖：

- `socket(AF_VSOCK, SOCK_STREAM, 0)`
- `bind/listen/accept`
- `connect`
- `read/write`
- host 主动关闭
- guest 主动关闭

建议补充的内核/集成测试：

- 非阻塞 `connect`
- 非阻塞 `accept/read/write`
- `bind(VMADDR_PORT_ANY)`
- 端口重复绑定返回 `EADDRINUSE`
- 无 listener 时 `connect` 返回 `ECONNREFUSED`
- 对端 `shutdown(SHUT_WR)` 后本端读到 EOF
- credit 耗尽后写阻塞，再由对端读取触发恢复
- transport reset 后 listener 保持、连接断开

## 分阶段实施

### 阶段 1：打通最小连接闭环

目标：

- virtio-vsock 设备探测、初始化、收发包
- backend 全局表和基础路由
- `connect` / `listen` / `accept`
- `RW` 数据收发
- `RST` 拒绝连接

这个阶段先把 `test/initramfs` 里的两个 C 程序跑通。

### 阶段 2：补齐正确关闭与流控

目标：

- `SHUTDOWN`
- `close`
- credit 管理
- 阻塞/非阻塞等待语义
- poll 事件正确性

没有这一阶段，系统可能“能跑 demo，但不稳定”。

### 阶段 3：补齐 reset 与边界错误

目标：

- `TRANSPORT_RESET`
- 更多 errno 对齐
- backlog、并发 accept/connect 的竞争测试
- 资源回收与泄漏检查

## 风险点

### 风险 1：在错误上下文里做重活

如果在 virtio 中断回调里直接改连接表、拿重锁、甚至唤醒复杂等待链，后续会很难 debug。必须从第一版起就采用“中断采集事件，bottom half 处理状态”的模式。

### 风险 2：socket 状态和协议状态脱节

如果 `connect()` 成功/失败只靠 socket 本地标志，不以 backend 事件驱动，会很容易出现：

- 已被 `RST` 的连接仍被认为可写
- `accept()` 取到未完全建立的连接
- `close()` 后旧连接留在表里

所以必须让 backend 成为单一事实来源。

### 风险 3：credit 实现不完整导致死锁

即使测试数据量不大，也不能完全跳过 credit 字段。最小可接受做法是：

- 所有 stream 包都带合法 `buf_alloc/fwd_cnt`
- peer credit 不足时不继续发 `RW`
- 本地读取后及时推进 `fwd_cnt`

否则与标准 host 实现互通时会出现隐蔽卡死。

### 风险 4：端口和地址复用时机错误

vsock 连接由 `(local_port, peer_cid, peer_port)` 唯一标识。若 `close()` 后过早释放表项，旧包到达可能会污染新连接。因此连接销毁必须由 backend 集中完成。

## 实施建议

建议按下面顺序提交：

1. 地址类型、模块骨架、syscall 接线修正
2. virtio-vsock 设备与 backend 骨架
3. `connect/listen/accept/RST`
4. `RW` 数据面
5. `shutdown/close/credit`
6. reset 与补充测试

每一步都应保持可编译，并优先让集成测试逐步变绿，而不是一次性堆大 patch。

## 结论

这项工作可以沿用 Asterinas 现有 TCP stream socket 和 virtio net 的组织方式，但不能简单复制 TCP 逻辑，因为 vsock 的核心约束在于：

- 地址空间是 `(cid, port)` 而不是 IP/port
- 连接建立与关闭完全由 virtio-vsock 包驱动
- stream 流控依赖 credit，而不是 host TCP 栈兜底

因此推荐方案是：

- 在 `kernel/src/net/socket/vsock/` 新建一套对齐现有 socket 框架的 stream 实现
- 在 `backend/` 中集中管理监听、连接、路由和状态机
- 在 `kernel/comps/virtio/src/device/vsock/` 中实现一个最小但完整的 virtio-vsock transport

这样能以最小范围改动接入现有内核结构，同时给后续补充 `SEQPACKET`、socket options 和更多 Linux 兼容语义留下清晰扩展点。
