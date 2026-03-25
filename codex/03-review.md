注意以下几点：

 1. connection 只关心连接，正在建立、已经建立、或曾建立的连接，Init/Bound/Listening 均不应是 connection 的一部分
 2. 锁顺序存在问题，`Socket` 的锁应该总是最先拿到的，比如 syscall 先进来访问 `Socket`，注意要同时考虑 syscall 发包正向路径和从设备收包的逆向路径
 3. 锁的类型不明，正常 syscall 中可以用 Mutex，而 IRQ 或 bottom half 中都只能用 SpinLock （LocalIrqDisabled 或 BottomHalfDisabled），这也影响锁顺序，比如 `Socket` 的锁应该是 Mutex，那么它显然不能是最后一个被锁的
 4. 拷贝应该尽量减少，从 syscall 可以直接复制到 vsock packet 中，后续可以直接发出（放到 virtio queue中）、放到 backlog queue 或 socket queue 中等待，根据情况定，注意应限制队列长度避免用户态打爆内核内存
 5. 发送和接受避免条件竞争，多次锁 backlog queue 的 SpinLock，外围可以锁 Socket Mutex 以避免并发问题
 6. 细化设计，明确类型以及 API、API 参数等，明确队列设计包括队列以及其他数据维护用锁的粒度，明确引用计数使用（Arc），确保引用计数的使用符合其语义，适当通过 wrapper type 来避免错误使用
