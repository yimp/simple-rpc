# Streaming RPC v0.3: 协议与生命周期

本文描述 **已经实现** 的 Linux/C11 轻量实现，不是 gRPC、HTTP/2 或基于 protobuf 的协议。

## 一条连接上的调用状态

```text
           Client                               Server
              |                                     |
              +--- STREAM_OPEN(mode, initial) ----->|
              |                                     | handler returns RPC_PENDING
              |     Server Streaming                | worker produces DATA
              |<------------- DATA -----------------+
              |<------------- DATA -----------------+
              |<------- STREAM_FINISH(status) -------+
              |                                     |
              |     Client Streaming                | worker consumes DATA
              +------------- DATA ----------------->|
              +------------- DATA ----------------->|
              +-------- STREAM_HALF_CLOSE -------->|
              |<------ DATA (0 or 1 response) ------+
              |<------- STREAM_FINISH(status) -------+
```

每个 `rpc_stream_read` 返回一条**完整的消息**；0 字节消息可用，`RPC_OK + len=0` 与成功结束 `RPC_STREAM_EOF` 不同。`STREAM_FINISH(status!=0)` 导致 `RPC_ERR_REMOTE`。如果在没有 FINISH 的情况下断开连接则报告 `RPC_ERR_CLOSED`，不能算作成功。

## 帧布局

固定 24 字节网络字节序 Header：`Magic(4)`、`Version(2)`、`Type(2)`、`RequestID(4)`、`MethodID(4)`、`Status(4)`、`PayloadLength(4)`。后面跟长度指定的字节负载，长度上限 1 MiB。Magic `0x53525043`、Version `1` 与 Unary 一致；流式增加 Type 3～6。`STREAM_OPEN` 的 Status 字段用于标记 `RPC_STREAM_SERVER=1` 或 `RPC_STREAM_CLIENT=2`；`STREAM_FINISH` 的 Status 字段是最终业务状态；其他流帧 Status 必须为 0。

无服务端 OPEN 确认帧：客户端发送 OPEN 后可直接开始写 DATA；TCP 保证它们按顺序到达。服务端校验 Method 是否注册、Mode 是否匹配，不匹配时尽可能返回 FINISH 错误状态。尚未提供跨版本能力协商。

## 两个方向是独立的

- Server Streaming：Client 只有一条 OPEN 携带的请求消息，之后 Server 发送多条 DATA、最后 FINISH。
- Client Streaming：Client 发送 OPEN（初始参数）、多条 DATA、HALF_CLOSE；Server 读取数据直到 EOF，可发送最多一条响应 DATA、最后 FINISH。
- 当前不能在一个 Stream 上同时双向发送 DATA。一个 `rpc_client *` 同时只有一个调用；同一连接中一个调用结束后可继续发起 Unary 或下一个 Stream。

## 有界缓冲与背压

1. **服务端输出**：Worker 的 `rpc_stream_write()` 把一条完整消息复制进 Stream 的发送队列；队列总共最多占用 2 条消息（含 Reactor 正在发送的消息），单消息最多 1 MiB。超限时使用 `pthread_cond_timedwait` 等待 Reactor 发送腾出空间。超时返回 `RPC_ERR_TIMEOUT`，不会自动丢弃其他已排队数据。
2. **服务端输入**：Reactor 最多把 2 条消息排入待消费队列，且最多另有一条正在解析；队列满后暂停该连接的 EPOLLIN。Worker 读出消息后唤醒 Reactor 恢复读。
3. **客户端**：当前提供串行同步读写，`poll()` 等待非阻塞 Socket，发送由内核 TCP 窗口控制。客户端不做应用层预读队列。
4. **跨线程**：Reactor 是服务端 Socket 的唯一读写者；Worker 只对 Stream 队列和条件变量操作，通过 `eventfd` 通知 Reactor。单个 Stream 的业务操作须串行，不能多个 Worker 同时写、读或完成。

因此流式接口可以避免缓冲整个大文件，但**不是**全局资源配额系统。应用仍需要限制活跃任务、Worker 排队和同时打开的连接数量。

## 异步请求所有权

- Handler 必须快速返回，不能直接执行可能等待的 `stream_read/write`。可将 `rpc_stream *` 提交给外部 Worker，返回 `RPC_PENDING`。
- Worker 最终调用 `rpc_stream_finish()` 一次；成功后不再访问 Stream。业务不调用 `retain/release`。
- 框架在 Handler 返回之前始终保持 Stream 有效，因此 Worker 可以抢先完成。
- 若客户端断开，框架唤醒受阻的 Worker；仍要求业务最终调用 `rpc_stream_finish()`，框架才能回收对象。`finish()` 成功只表示生命周期完成，不意味着对端收到。
- 服务端停止会关闭活跃连接，并等待所有 pending Stream 与 Unary 完成。业务不完成请求会让停止等待无限延长。
- `rpc_stream_destroy()` 仅由 Client 调用；如果未读到最终 FINISH，将关闭该 TCP 连接。框架没有应用层取消令牌或取消帧。

## API 返回规则

- 正常读消息：`RPC_OK (0)`，包括合法的空消息。
- 结束：`RPC_STREAM_EOF (1)`。Server 读到 Client HALF_CLOSE，或者 Client 读到成功的 Server FINISH。
- 远端失败：`RPC_ERR_REMOTE (-5)`，只有 Client 收到 FINISH 失败状态时返回。
- 其他负值：超时、非法调用、断连、网络错误等。每次操作的 `timeout_ms` 独立计时。
- `rpc_stream_write()` 只是复制数据到框架的有界队列（Server）或写入 Socket（Client），都**不**构成远端处理成功的证明。

## 未实现

双向流、单连接多路复用、TLS、认证、协议协商、单流取消、断点续传、跨线程同一 Stream 并发操作、全局内存/连接/任务资源限制。协议与 API 是实验版，可能继续演进。
