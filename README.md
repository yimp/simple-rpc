# simple-rpc

一个刻意保持简单的 **Linux / C11** TCP RPC 框架，适合作为理解和扩展 RPC 的起点。无第三方运行时依赖。

## 设计

- **服务端**：非阻塞 Socket + `epoll`（LT 模式），单 IO 线程管理多连接，注册 `method_id -> handler`。
- **客户端**：同步 `rpc_call()`；底层非阻塞 Socket + `poll()`，控制整个调用的超时。
- **连接模型**：支持长连接复用和多连接并发；**同一个客户端连接一次只允许一个未完成请求**。客户端对象不支持多线程并发调用。
- **业务执行**：Handler 直接运行在事件循环线程上，**不能执行长时间阻塞操作**。线程池和流式 RPC 留待后续版本。
- **协议**：固定 24 字节包头 + Payload，网络字节序，最大 Payload 1 MiB；所有字段显式序列化，避免 C 结构体填充和字节序问题。
- **安全边界**：这是受信任环境内的学习型实现，**不支持 TLS、认证、限流及生产级资源配额**；不要直接暴露到公网。

## 编译与运行

要求 Linux、CMake >= 3.16、C11 编译器和 pthread（仅测试程序使用）。

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure

# 两个终端分别运行：
./build/echo_server
./build/echo_client
```

输出示例：`response: hello RPC`

## 对外接口

```c
rpc_server *s = rpc_server_create();
rpc_server_register(s, 1, echo_handler, NULL);
rpc_server_listen(s, "127.0.0.1", 9000);
rpc_server_run(s);       /* 阻塞执行事件循环 */
rpc_server_destroy(s);
```

Handler 类型为 `int (*)(const rpc_buf *request, rpc_buf *response, void *userdata)`。通过 `rpc_buf_append(response, data, len)` 构造响应，返回 0 表示成功。Handler 对 request 的指针不能在返回后继续持有。

```c
rpc_client *c = rpc_client_connect("127.0.0.1", 9000, 3000);
rpc_buf resp = {0};
int rc = rpc_call(c, 1, "hello", 5, &resp, 3000);
/* rc == RPC_OK 时使用 resp.data / resp.len */
rpc_buf_free(&resp);
rpc_client_close(c);
```

缺少 Method 或 Handler 错误时返回 `RPC_ERR_REMOTE`，可调用 `rpc_client_last_status(c)` 查看具体状态。`rpc_server_stop(s)` 可从另一个线程唤醒并停止服务器；先等待 `rpc_server_run()` 结束，再销毁服务器。

## 帧格式（网络字节序）

| 偏移 | 大小 | 字段 |
|---:|---:|---|
| 0 | 4 | Magic，`0x53525043`（`SRPC`） |
| 4 | 2 | Version，当前为 1 |
| 6 | 2 | Type：1 请求，2 响应 |
| 8 | 4 | Request ID |
| 12 | 4 | Method ID |
| 16 | 4 | Status（请求必须为 0） |
| 20 | 4 | Payload Length，0～1 MiB |
| 24 | N | 业务 Payload（原样字节，不限定编码） |

客户端使用递增 Request ID，服务器响应回显 Request ID 与 Method ID。未知 Method 返回 `RPC_STATUS_NO_METHOD`，Handler 非零返回 `RPC_STATUS_HANDLER_ERROR`。超过帧长上限的**入站帧**直接断开连接。

## 第一版的边界

- **没有线程池**：慢 Handler 会阻塞其他连接，需将业务限制为短小、非阻塞的处理。
- **没有单连接多路复用**：服务器可接受 TCP 层连续发来的多个请求帧，但逐条处理并按序回应；客户端要求同步串行调用。
- **没有自动重试**：调用超时或网络故障后连接关闭，不能确定远端是否已执行业务；应用层需自行决定重试语义。
- **调用超时**：按整个请求写入 + 响应读取的单一截止时间执行；主机名 DNS 查询不包括在连接超时保证内。
- **生命周期**：在 `rpc_server_run()` 运行期间不要并发调用 `rpc_server_destroy()` 或修改 Handler 注册表。
- **资源限制**：只限制单帧大小，尚未实现连接数量、总内存和慢客户端存活时间上限，后续应补齐。
- **平台限制**：当前使用 Linux `epoll`/`eventfd`，不支持 Windows、macOS 或 AIX。

## 后续可演进方向

1. Reactor + 固定大小 Worker Pool，把阻塞业务从 IO 线程分离。
2. 连接数 / 排队任务数限制、慢连接超时、可观测性。
3. 单连接多路复用、流式传输、取消及优雅停机。
