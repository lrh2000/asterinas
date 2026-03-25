现在你的任务是在 Asterinas 中实现 vsock，你只需要实现 guest 接口以及提供与 host 沟通的最小化功能（如 listen / connect 建立连接、数据传输、关闭连接），你不需要实现其他的无关功能如 socket options

基本架构如下：
 - kernel/src/net/socket/vsock/stream/ 下实现 vsock socket，你可以参考 TCP socket 实现 kernel/src/net/socket/ip/stream/
 - kernel/src/net/socket/vsock/backend/ 下实现 vsock 连接管理，如连接状态，端口占用、监听信息，你可以参考 TCP 实现 kernel/libs/aster-bigtcp/
 - kernel/comps/virtio/src/device/vsock/ 下实现 vsock 数据包的收发接口，其他 virtio 实现可供参考

注意下面几点：
 1. 遵守 AGENTS.md 里指定的代码风格
 2. 服从内核开发的基本原则，如正确使用锁，避免条件竞争，避免在原子上下文中使用 mutex 或访问用户地址空间，利用 bottom half 进行网络数据处理与连接信息更新，等
 3. 遇到任何你不清楚的问题，你可以中断并询问我，请不要自行做不确定的决定
 4. virtio vsock 的信息你可以自行上网查询（例如参考 spec），或参考 ~/linux/net/vmw_vsock 中的 Linux 实现

输出一份设计文档保存在 codex/02-plan.md
