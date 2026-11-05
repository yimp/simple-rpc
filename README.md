# simple-rpc

用于学习与实验的 **Linux / C11** 轻量 TCP RPC 框架，只有 libc、pthread 等系统依赖。当前版本 **v0.3.0**。

支持普通 Unary（单请求 / 单响应）和两种流式 RPC：**Server Streaming**（一次请求、多条响应消息）、**Client Streaming**（多条请求消息、最多一条响应消息）。不同于完整的 gRPC，本项目不使用 protobuf / HTTP/2。

> **实验性项目：** 不提供 TLS、认证、生产级限流和资源配额。不要直接暴露至不受信任网络，也不要作为未经审计的生产级备份数据通道。

## 设计概要

- **服务端**：非阻塞 Socket + 单线程 `epoll` LT Reactor。Handler 在 Reactor 中进入，耗时逻辑交给用户 Worker，返回 `RPC_PENDING`。
- **客户端**：同步 API；底层非阻塞 Socket + `poll()`，各次读、写独立设置超时。
- **连接**：长连接复用，但**一条 TCP 连接同时只能执行一个 RPC（Unary 或 Stream）**；不支持多路复用，不支持同一个 `rpc_client *` 的并发操作。
- **协议**：24 字节二进制帧头，网络字节序。单条消息最多 **1 MiB**，**整条流的数据总量不受该限制**。
- **Stream**：不透明 `rpc_stream`，显式结束发送、服务端完成与客户端销毁。成功写入只表示数据已交给框架，不保证对端已收到。
- **背压**：服务端每条流最多缓存 **2 条待发送消息**（包含当前正在 Socket 上发送的消息）；接收侧最多排队 2 条消息，额外允许一条正在解析的消息。队列满时 Worker 等待、Reactor 适时暂停读。背压不会阻塞 Reactor。
- **异步生命周期**：业务不需要引用计数。异步 Handler 返回 `RPC_PENDING` 后，必须最终执行成功的 `reply()` 或 `stream_finish()`，即使客户端已断开。服务端停止等待未完成业务；业务忘记完成会造成无限等待。

## 源码模块

代码已由原先的单文件 `src/rpc.c` 拆分：

- `protocol.c`、`buffer.c`、`io.c`：网络协议、消息缓冲和公共 IO 工具。
- `server.c`、`connection.c`、`request.c`、`server_stream.c`：服务端 Reactor、连接、Unary 请求和流式收发。
- `client.c`、`client_stream.c`：同步 Unary 和流式客户端。
- `src/rpc_internal.h`：仅供 `src/` 使用的内部结构及跨模块函数；业务代码仍只需 `include/rpc.h`。

详见 [内部架构说明](docs/architecture.md)。此轮重构不改变对外 API、协议或线程模型。

## 编译和测试

要求 Linux、C11 编译器、CMake >= 3.16、pthread。

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure
```

测试包含 Unary 兼容、流的分帧及重组（超过单帧上限）、并行连接、正常与失败终止、Worker 提前完成、缓冲区背压、断连和服务端停止。`tests/test_stream.c` 包含更完整的流式案例。

同步 Unary 演示：两个终端运行 `./build/echo_server` 与 `./build/echo_client`。

流式演示：两个终端运行 `./build/stream_server` 与 `./build/stream_client`，输出示例：

```text
download: 3145728 bytes
upload: 3145728 bytes
```

`examples/stream_server.c` 演示使用业务自有的工作线程处理流式请求。示例为演示性质，实际使用时仍需要有界的 Worker 任务队列。

## 1. Unary（已有接口，兼容 v0.2）

```c
int echo(rpc_request *req, void *ud)
{
    int rc = rpc_request_set_response(
        req, rpc_request_data(req), rpc_request_size(req));
    return rc == RPC_OK ? RPC_DONE : rc;
}

rpc_server_register(server, METHOD_ECHO, echo, NULL);

rpc_buf result = {0};
int rc = rpc_call(client, METHOD_ECHO, data, size, &result, 3000);
rpc_buf_free(&result);
```

耗时 Handler 可返回 `RPC_PENDING`，由业务 Worker 调用 `rpc_request_reply()` 完成；`rpc_request` 不透明，业务无需调用 `retain/release`。

## 2. Server Streaming（备份读取、元数据扫描）

中心端作为 **RPC Client** 发起一个读取请求，源端 Agent 作为 **RPC Server** 持续发送数据消息：

```c
/* 注册时选择服务端流式模式，Handler 必须快速返回。 */
rpc_server_register_stream(server, METHOD_READ, RPC_STREAM_SERVER,
                           read_handler, worker_pool);

/* 客户端打开并连续读取完整消息。 */
rpc_stream *stream = NULL;
int rc = rpc_stream_open(client, METHOD_READ, RPC_STREAM_SERVER,
                         &range, sizeof(range), 5000, &stream);
if (rc == RPC_OK) {
    rpc_buf msg = {0};
    while ((rc = rpc_stream_read(stream, &msg, 5000)) == RPC_OK) {
        consume(msg.data, msg.len);
    }
    /* RPC_STREAM_EOF: 正常结束；RPC_ERR_REMOTE: 远端返回失败状态。 */
    rpc_buf_free(&msg);
    rpc_stream_destroy(stream);
}
```

服务端 Handler 应将 `rpc_stream *` 交给业务线程，返回 `RPC_PENDING`。业务线程可反复调用 `rpc_stream_write()`，最后调用 **一次** `rpc_stream_finish(stream, status)`。成功 finish 之后业务不得再访问 stream。

`rpc_stream_write()` 提交的是一条具有消息边界的记录，**不是** POSIX 的可能部分写入的 `write()`；超过 1 MiB 的逻辑业务数据需拆成多条消息。

## 3. Client Streaming（数据恢复、持续上传）

中心端发起恢复任务并连续发送数据，Agent 持续读取，最后返回完成确认：

```c
rpc_stream *stream = NULL;
int rc = rpc_stream_open(client, METHOD_RESTORE, RPC_STREAM_CLIENT,
                         &metadata, sizeof(metadata), 5000, &stream);
if (rc == RPC_OK) {
    while ((n = get_next_chunk(buf, sizeof(buf))) > 0) {
        rc = rpc_stream_write(stream, buf, (size_t)n, 5000);
        if (rc != RPC_OK) break;
    }
    if (rc == RPC_OK) rc = rpc_stream_finish_send(stream, 5000);
    if (rc == RPC_OK) {
        rpc_buf result = {0};
        rc = rpc_stream_read(stream, &result, 5000);
        /* 接收最终业务消息（如有），并继续读取直到 EOF / REMOTE。 */
        rpc_buf_free(&result);
    }
    rpc_stream_destroy(stream);
}
```

服务端的业务 Worker 调用 `rpc_stream_read()` 获取请求消息；收到 `RPC_STREAM_EOF` 代表客户端**正常半关闭请求方向**，然后最多写一条结果消息，并调用 `rpc_stream_finish()` 发送最终状态。

### 三种「结束」不能混淆

| API | 调用方 | 含义 |
|---|---|---|
| `rpc_stream_finish_send()` | Client Streaming 的客户端 | 请求数据已经发送完，**继续等响应** |
| `rpc_stream_finish()` | 服务端 Worker | 本次 RPC 的最终业务状态；成功后业务不再使用 Stream |
| `rpc_stream_destroy()` | 客户端 | 释放句柄；未完成的调用通过**关闭连接**取消 |

读取返回值：`RPC_OK` 表示收到一条完整消息（允许空消息）；`RPC_STREAM_EOF` 表示成功的消息序列结束；`RPC_ERR_REMOTE` 表示最终状态非 `RPC_STATUS_OK`；`RPC_ERR_TIMEOUT`、`RPC_ERR_CLOSED`、`RPC_ERR_PROTOCOL` 表示传输问题。`rpc_stream_remote_status()` 可在 EOF / REMOTE 后读取最终业务状态。

**注意：** 每次 `rpc_stream_read/write` 的 timeout 是该次操作的期限，而不是整个备份任务的统一期限。超时后的业务取消、重试、偏移检查点等应由调用方设计。

## Stream 协议简述

流式扩展保留 v1 的 24 字节帧头格式和旧 Unary 帧：

| 帧 Type | 方向 | 用途 |
|---|---|---|
| 1 `REQUEST` / 2 `RESPONSE` | Unary | 保持原有行为 |
| 3 `STREAM_OPEN` | Client → Server | 发起调用，Header `Status` 存储 Stream 模式，Payload 为初始请求 |
| 4 `STREAM_DATA` | 双向（受模式约束） | 一条完整数据消息，`Status=0` |
| 5 `STREAM_HALF_CLOSE` | Client → Server | Client Streaming 的请求发送完成，Payload 为空 |
| 6 `STREAM_FINISH` | Server → Client | RPC 最终状态在 `Status` 字段，Payload 为空 |

所有流帧都使用相同 Request ID、Method ID，单条 Payload 最多 1 MiB；当前连接不混用多个在途 Stream。Server Streaming 的请求方向在 `STREAM_OPEN` 后**隐式结束**。

细节、约束、状态机见 [docs/streaming.md](docs/streaming.md)。

## v0.3 边界与下一步

- 只支持 **Server Streaming** 和 **Client Streaming**；不支持双向流、一连接多路复用。
- Client Stream 由调用线程顺序操作；同一 Stream 也只允许**一个业务线程**执行 `read/write/finish`，并发访问必须自行串行化。
- Server Streaming 业务 Worker 的写、Client Streaming 业务 Worker 的读允许阻塞；**Reactor 线程严禁执行这些可能等待的操作**。
- 服务端独立设置的**应用层流队列**是有界的，但没有全局内存预算、连接总量或任务数限制；客户端阻塞发送依赖 TCP 背压。
- 取消暂时通过关闭 TCP 连接实现；没有单流取消帧、应用层 deadline 传播、自动重试或断点续传。
- 协议未提供版本能力协商；v0.2 服务端无法理解新 Stream 帧。Unary 帧保持兼容。
- `stream_finish()` 的成功只表示交还框架并排队最终状态，不保证对端收到；中途断连不能推断远端业务一定未完成。
- 停止服务端会唤醒等待 IO 的 Worker，但所有异步业务仍必须明确调用 `finish()`；有业务漏调用会无限等待。
- 当前重点是功能正确性与可理解性，暂不承诺高连接数下的性能。Worker 唤醒使用 `eventfd`，Reactor 会检查活跃连接，尚未做按连接精准唤醒的优化。
