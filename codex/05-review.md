你要多读一下已有的代码，尽量按照已有代码的风格设计（除非已有代码的涉及真的有扎实理由不能满足需要）

比如 kernel/src/net/socket/ip/stream/mod.rs 里面叫 State，你就不要叫 SocketState，然后那里 State 的划分是 Init、Connecting、Connected、Listen，你就先看看这样划分是不是能满足你的需要，你会发现其实你没有必要创建 Init 和 Bound 两个类型的，直接在里面用 Option 就好了，同理 Closed 类型也不需要存在，Connection 即使关了也会维持在 Connected 状态，这样就好了，不会引入问题，除非你能说出单列 Closed 有啥好处

其他很多地方也是 try_send_locked 等等方法，麻烦你多看看已有代码是怎么命名的以及实现在什么类型上的，比如 TCP 里的 try_send，不要自己造一些与现有架构不同的方法名

然后你对 wrapper type 的实现理解完全错误啊，如果你有 Connection(Arc<ConnectionInner>)，这样的好处是你要保证 Connection 是唯一的、不可 clone 的，被一个 socket 所拥有的，这样你 socket Drop的时候也会 Drop Connection，就能通知 backend 中断连接，而 backend 中可以有 Arc<ConnectionInner> 维护内部数据，重点是 Connection 不能提供 Clone，你实现 Clone 就完全不对了，而且 Connection 是暴露给外部用的（socket layer），而不是自己内部用的，内部可以直接用 Arc

BoundPort 应该具有这个 port 的 ownership，这样 Drop 的时候可以释放资源

RxPayload 里面先有个 Arc<RxPayloadInner>，再有个 Arc<ReceivedBuffer> 的意义是啥？不要这样使用 Arc，每使用一个 Arc 你需要说明这里为什么必须要有引用计数，如果这个类型保存数据（比如 buffer），不需要有引用计数，那么让外面的用户持有 Arc 即可，比如 Arc<RxPacket>

RxPayload、RxPayloadInner、ReceivedPacket 这设计都太冗余了，直接用 RxBuffer 不好吗，里面有些信息比如 payload_len 其实已经有了，还有些像 hdr 其实也没有必要 cache 到一个 field 里面，需要的时候从 buffer 里读取就是了，实际上一个 rx packet 也只需要读一次 header，所以并不会造成开销；类似发送也应该用 TxBuffer

prepare_send 和 commit_send 的设计是冗余的，你要注意一个 Connection 只能属于一个 Socket，然后 Socket 锁了 Mutex 才能发送，那么对应的发送接口应该要求 &mut self，并且整个 Mutex 获取之后，没有其他人能发送，那么直接通过 atomic variable 的操作就可以知道最多能发多少，不用先获取 spin lock 再释放

VsockEventHandler 和 VsockTransport 为什么需要在 Arc 里面？如果需要引用计数，说明原因，实际上 Box<dyn VsockEventHandler> 或者直接 VsockEventHandler { on_packet: fn(), } （避免 Box）应该够用，transport应该是 &'static VsockTransport，或者与与现有代码保持一致，用 get_device(), all_devices() 直接暴露设备更好，发包可以在设备上操作

你还没有考虑 bottom half怎么对接，需要和 taskless 交互

device queue 怎么决定 Connection 能不能发包？谁来通知阻塞的 connection？一个 device 可能对应多个 connection，而且每个 connection 的 tx limit 应该单独限制；但若是 Connection 逐个 TxQueue 的设计，你考虑下当设备的 tx queue 如果有空闲了，你怎么知道按什么顺序分发每个 connection 的新 packet？这个会很复杂，也许我们应该维护每个 Device 的 tx queue，但是用一个 trait 来记录消耗的资源，这样发包的时候 drop 一下，就可以增加对应 connection 的空闲空间并且唤醒阻塞的进程

VsockDevice 中结构和锁的划分为什么是现在这样，transport 为什么需要在中断处理中访问呢，不需要的话不应该标为 LocalIrqDisabled，为什么是在硬中断中清理 queue 而不是把设备记录一下就好？bottom half 中可以读取 queue 中的数据和 connection 交互；而且 rx queue 与 tx queue 绑定在一起不会有死锁问题吗（先 rx，处理，结果需要 tx），为什么 tx_buffers 和 rx_buffers 需要是 BTreeMap 呢？你看看 virtio net 中的怎么写的，尽量和已有代码保持一致啊

refill_rx_buffers 这种私有 helper API 其实你可以先不列的，显然你列出的这个版本并不具有合理性，一般拿走一个 rx buffer 都知道它的 index 是多少了，那么 refill 直接填充这个 index 就好了，怎么可能不接受参数
