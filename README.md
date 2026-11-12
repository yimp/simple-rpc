# simple-rpc

用于学习与实验的 **Linux / C11** 轻量 TCP RPC 框架，依赖 libc、pthread 和 Linux epoll。当前版本处于 v0.4 API 重构阶段。

支持 Unary、Server Streaming 和 Client Streaming。协议使用自定义 24 字节帧头，不依赖 protobuf / HTTP/2。

> 实验性项目：不提供 TLS、认证、生产级限流和全局资源配额。不要直接暴露至不受信任网络。

## 设计概要

- 服务端：非阻塞 Socket + 单线程 epoll Reactor。统一 Handler 在 Reactor 线程调用，耗时业务交给用户 Worker 并返回 RPC_PENDING。
- 客户端：同步 API；底层非阻塞 Socket + poll。
- 一条 TCP 连接同一时刻只承载一个 RPC；不支持多路复用。
- 单条消息最多 1 MiB，整条 Stream 的数据总量不受此限制。
- 服务端所有调用统一使用 rpc_request；服务端 Stream 状态嵌入 Request，由 Request 统一拥有生命周期。
- 客户端仍使用不透明 rpc_stream 句柄，表示客户端发起的一次流式调用。
- 服务端输入、输出队列有界，Worker 通过条件变量等待背压；Reactor 不执行阻塞式业务读写。

## 公共头文件

- include/rpc_common.h：公共错误码、状态码、Method 类型、rpc_buf。
- include/rpc_client.h：客户端连接、Unary 调用和客户端 Stream API。
- include/rpc_server.h：服务端注册、统一 Handler、Request 读写与完成 API。
- include/rpc.h：便利的 umbrella header，同时包含客户端和服务端 API。

客户端程序建议只 include rpc_client.h；服务端程序建议只 include rpc_server.h。rpc.h 保留给同时需要两侧 API 的程序。

## 统一 Method 注册

每个 Method 通过描述符声明调用模型和统一 Handler：

```c
static int echo(rpc_request *req, void *userdata)
{
    (void)userdata;
    int rc = rpc_request_set_response(
        req, rpc_request_data(req), rpc_request_size(req));
    return rc == RPC_OK ? RPC_HANDLER_DONE : rc;
}

const rpc_method_desc method = {
    .method = 1,
    .type = RPC_METHOD_UNARY,
    .handler = echo,
    .userdata = NULL
};
rpc_server_register(server, &method);
```

Handler 签名始终是：

```c
int handler(rpc_request *request, void *userdata);
```

- RPC_HANDLER_DONE：Handler 同步完成，框架按 Method 类型自动发送最终响应或结束流。
- RPC_PENDING：业务已将工作交给 Worker，Worker 必须最终调用一次 rpc_request_complete()。
- Handler 在 Reactor 线程运行，不得执行可能阻塞的 rpc_request_read/write。

## Unary

同步 Handler 使用 rpc_request_set_response()，然后返回 RPC_HANDLER_DONE。异步 Handler 返回 RPC_PENDING，由 Worker 调用 rpc_request_complete()。

```c
rpc_buf response = {0};
int rc = rpc_call(client, 1, data, size, &response, 3000);
rpc_buf_free(&response);
```

## Server Streaming

客户端一次请求，服务端持续发送消息：

```c
int read_file(rpc_request *req, void *userdata)
{
    submit_to_worker(req, userdata);
    return RPC_PENDING;
}

/* Worker */
while (read_next_chunk(buf, &len)) {
    if (rpc_request_write(req, buf, len, 5000) != RPC_OK)
        break;
}
rpc_request_complete(req, RPC_STATUS_OK, NULL, 0);
```

客户端使用 rpc_stream_open() 和 rpc_stream_read() 消费响应。

## Client Streaming

客户端持续发送消息，服务端读取到 EOF 后返回最多一条最终响应：

```c
rpc_buf msg = {0};
while (rpc_request_read(req, &msg, 5000) == RPC_OK) {
    process(msg.data, msg.len);
    rpc_buf_free(&msg);
}
rpc_request_complete(req, RPC_STATUS_OK, &result, sizeof(result));
```

客户端调用 rpc_stream_finish_send() 表示请求数据已发送完毕，然后读取最终响应和结束状态。

## 生命周期与背压

Request 是服务端一次调用的唯一生命周期所有者。异步 Worker 不需要 retain/release；RPC_PENDING 后必须完成一次。Worker 可能在 Handler 返回前完成，框架会等待 Handler 返回后再回收 Request。

rpc_request_write() 和 rpc_request_read() 可能阻塞，只能在业务 Worker 中调用。每条服务端流的收发队列最多缓存两条消息；队列满时 Worker 等待，Reactor 通过 eventfd 和 epoll 驱动 IO。客户端接口是同步的，调用线程负责读写。

客户端未完成的 Stream 通过 rpc_stream_destroy() 关闭连接取消。服务端断连会唤醒阻塞 Worker，但业务仍须调用 rpc_request_complete() 交还 Request 生命周期。

## 编译与测试

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure
```

示例：echo_server / echo_client（Unary）、async_server（异步 Unary）、stream_server / stream_client（下载和上传）。

双向流、单连接多路复用、TLS、认证、协议协商、单流取消、断点续传和全局资源配额暂不支持。
