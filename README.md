# simple-rpc

一个刻意保持简单的 **Linux / C11** TCP RPC 框架，适合作为理解和扩展 RPC 的起点。没有第三方运行时依赖。

## 设计

- **服务端**：非阻塞 Socket + `epoll`（LT 模式），单 Reactor 线程管理多个连接。
- **客户端**：同步 `rpc_call()`；底层非阻塞 Socket + `poll()`，控制单次调用的总超时。
- **业务执行**：统一的 `rpc_handler(rpc_request *, void *)`。返回 `RPC_DONE` 则框架立即应答；返回 `RPC_PENDING` 则交给业务稍后调用 `rpc_request_reply()`。
- **外部线程池**：框架不创建 Worker，也不要求用户 `retain/release`。用户可以将 `rpc_request *` 放入已有线程池或异步事件系统；后台线程调用 `reply()`，框架通过线程安全完成队列和 `eventfd` 唤醒 Reactor。
- **连接模型**：长连接复用、多连接并发；每个连接只处理一个未完成请求，不支持单连接多路复用。
- **协议**：固定 24 字节包头 + Payload，网络字节序，单帧最大 Payload 1 MiB；字段显式编解码。
- **安全边界**：这是受信任环境内的学习型实现，**不支持 TLS、认证、限流和生产级资源配额**；不要直接暴露到公网。

## 编译及运行

要求 Linux、CMake >= 3.16、C11 编译器、pthread。

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure
```

同步 Echo 演示：

```bash
# 终端 1
./build/echo_server
# 终端 2
./build/echo_client
```

异步服务端演示：`./build/async_server`（监听 `127.0.0.1:9001`，展示业务自有 Worker）；客户端示例连接 `9000`，可按需修改。

## API：同步 Handler

`rpc_request` 是**不透明对象**：用户不能修改其内部状态，也不需要参与引用计数。

```c
int echo_handler(rpc_request *req, void *userdata)
{
    (void)userdata;
    int rc = rpc_request_set_response(
        req, rpc_request_data(req), rpc_request_size(req));
    return rc == RPC_OK ? RPC_DONE : rc;
}

rpc_server *s = rpc_server_create();
rpc_server_register(s, 1, echo_handler, NULL);
rpc_server_listen(s, "127.0.0.1", 9000);
rpc_server_run(s);       /* 阻塞运行，直到 stop 且待处理请求完成 */
rpc_server_destroy(s);
```

同步 Handler **在 Reactor 线程执行，不能长时间阻塞**。`rpc_request_set_response()` 会复制字节；Handler 返回 `RPC_DONE` 后框架发送响应。返回其他负值时发送 `RPC_STATUS_HANDLER_ERROR`。

## API：异步 Handler

```c
int async_handler(rpc_request *req, void *userdata)
{
    my_pool *pool = userdata;
    if (my_pool_submit(pool, worker_task, req) != 0)
        return RPC_ERR_IO;   /* 任务没有接管 req，框架回复错误 */
    return RPC_PENDING;     /* 此后业务负责最终调用一次 reply */
}

void worker_task(void *arg)
{
    rpc_request *req = arg;
    const void *data = rpc_request_data(req);
    size_t size = rpc_request_size(req);

    int rc = rpc_request_reply(req, RPC_STATUS_OK, data, size);
    /* rc == RPC_OK 后，不能再访问 req，其他任务也不能访问。 */
    /* rc == RPC_ERR_NOMEM 时，业务必须重试或采取其他完成措施。 */
}
```

**异步完成契约**：

- `RPC_PENDING` 表示业务承担请求完成责任：**最终必须调用一次成功的 `rpc_request_reply()`**，不论客户端是否断开。
- `reply()` 可从任意 Worker 线程调用，也允许在 Handler 返回 `RPC_PENDING` 之前完成。`reply()` 复制响应字节并将结果排入完成队列；Socket 发送仍由 Reactor 执行。
- `reply()` 返回 `RPC_OK` 表示结果已交给框架，不代表对端收到；之后 `req` 不再属于业务。若返回 `RPC_ERR_NOMEM` 或输入无效，请求仍未完成，业务须修复或重试。
- 如果一个请求被多个任务访问，业务必须先自行汇合所有任务再 `reply()`，避免其他任务继续使用已完成的请求。
- 连接断开仅关闭 Socket，**不能释放业务仍在使用的异步请求**。当 Worker 最终 `reply()` 时，框架会丢弃不可投递的响应并回收请求。
- `rpc_server_stop()` 使 Reactor 关闭连接并等待所有已接管的 `RPC_PENDING` 请求完成，之后 `rpc_server_run()` 才返回。**漏调用 `reply()` 会导致停止无限等待**；框架无法从运行时间推断业务是否结束。
- `rpc_server_destroy()` 只能在 `rpc_server_run()` 返回后调用。若使用自有 Worker Pool，必须保证停止时不会再访问框架资源；可以先 join 工作线程再销毁 RPC 服务端。

## 客户端

```c
rpc_client *c = rpc_client_connect("127.0.0.1", 9000, 3000);
rpc_buf resp = {0};
int rc = rpc_call(c, 1, "hello", 5, &resp, 3000);
/* rc == RPC_OK 时使用 resp.data / resp.len */
rpc_buf_free(&resp);
rpc_client_close(c);
```

客户端不支持在同一个 `rpc_client *` 上并发发起调用。`RPC_ERR_REMOTE` 时可用 `rpc_client_last_status()` 查看远端状态。超时或传输故障后连接关闭；**超时不代表远端业务一定没有执行**。

## 帧格式（网络字节序）

| 偏移 | 字节数 | 字段 |
|---:|---:|---|
| 0 | 4 | Magic，`0x53525043`（`SRPC`） |
| 4 | 2 | Version = 1 |
| 6 | 2 | Type：1 请求，2 响应 |
| 8 | 4 | Request ID |
| 12 | 4 | Method ID |
| 16 | 4 | Status（请求必须为 0） |
| 20 | 4 | Payload Length，0～1 MiB |
| 24 | N | 业务 Payload，原样字节，不限定编码 |

客户端使用递增 Request ID；服务端响应回显 Request ID 与 Method ID。未知 Method 返回 `RPC_STATUS_NO_METHOD`。超过帧长上限的**入站帧**将导致连接断开。

## 当前边界

- 无内置线程池或自动 Executor：用户自行负责 Worker 调度、任务队列容量及背压。异步 Handler 的任务提交过程本身不能阻塞 Reactor。
- 无同连接多路复用：每个连接一次只处理一个 RPC。异步请求执行期间暂停该连接的新请求读取，最终回复发送后恢复读取。
- 无框架级请求取消和处理超时：长时间不 `reply()` 的请求会保持占用资源，且会阻止服务端正常停止。调用者应自己监控、限制和处理异常业务任务。
- 无流式 RPC、自动重试、TLS、身份认证。请勿直接用于不受信任的网络环境。
