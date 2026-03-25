VsockDevice 的锁分太细了，queue 和 buffer 没必要分成单独的锁，它们的锁总是要一起获取的

tx_buffers 中只有 pending 的 buffer，没有在 queue 中的 buffer，这个不行，你看看 virtio net 的设计，TX queue 里的 buffer 也要找地方存储，同样 tx queue, pending buffer, tx buffer 没必要分成单独的锁了

process_tx 是不是可以完全 hide 在 virtio component 中？有必要注册 callback 暴露给 kernel 用吗，因为 virtio 完全可以处理已经交给它的 pending

VsockDevice::send 的 API 应该优化，默认应该不需要构造 completion，当队列满之后返回一个特殊类型，通过特殊 guard 提供 completion 加入 pending 队列，避免重复加锁，也可以让锁显示，lock_tx / lock_rx；让 RX 锁显示可以避免接受多个包的时候重复加锁

Completion 在 drop 的时候更新信息就好了，Rust compiler 保证 drop 只能一次，不用单独 fn complete()

VsockSpace 中的 port 管理应该拆成单独的锁？Listener/Connection 可以在同一个锁下，不过 port 管理和它们没关系

你还得之前的版本你怎么设计 send 流程的吗，不要丢信息，更新设计时要考虑到这些设计，ConnectionInner 中应该有 atomic 变量追踪 TX 剩余 buffer 的大小，这样你 send 数据包不需要重复加锁了，这里没有 race 是 Connection 发送独占 socket mutex 保证的，这个信息你早期版本有解释，现在不应该把它丢掉，Connection send 也应该要求 &mut self
