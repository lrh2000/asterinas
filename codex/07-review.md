aster-network 存在是因为 network 设备可能有很多来源，virtio只是其中一中设备；vsock的话我们暂时不考虑除virtio之外的其他类型，没有必要过渡设计 aster-vsock 和 AnyVsockDevice，建议直接在 virtio/src/device/vsock/mod.rs 中添加 get_device/all_devices 等函数即可，

BoundPort 内部为啥需要 Arc<BoundPortInner> ？一般来说 Arc<ConnectionInner> 的存在是因为需要接受网络包，但是 bound port 是不能接受网络包的，Arc<BoundPortInner> 的存在似乎并无必要？从 bound 到 connection 理论上应该把 BoundPort 完整传进去，但如果 connecting 失败了还得把 ownership 拿回来，这块好好想想有什么方案可以优美解决？

PendingTx 应该要在 virtio 模块里保存吧，但是 virtio 模块里是看不到 ConnectionInner 的，所以这里可能需要通过 Box 保存一个 trait

StreamObserver 和 pollee 重了，保留 pollee 即可，StreamObserver 在 TCP 中用是因为 bigtcp 看不到 Pollee

锁顺序不一致会死锁，设备 spin lock 在正向位于最后一位而逆向中位于最后一位，这个考虑重新规划一下 vsock device 中的锁使用，可以拆开，这点不需要和 aster-network 下面的设计保持一致

taskless 必须是单独设备的吗，我觉得可以设计成全局的，就像 network 中的 recevie/send soft-IRQ，注意这样全局维护需要 TX/RX 的设备列表即可

ConnectionInner 中不需要无锁 atomic 访问的可以合并成一个带锁的，rx_queue、peer_buf_alloc、peer_fwd_cnt总是一起访问，合并到一个锁里会更好

在 vsock 的设计中，remote_addr 是不是很容易从 Connection 中低开销拿到？如果是的话，没必要在 ConnectionStream 和 ConnectingStream 中再 cache 一个单独的 remote_addr 字段了
