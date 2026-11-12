# Internal architecture

## Public headers

- `rpc_common.h`: shared status/error codes, method type and buffer.
- `rpc_client.h`: client connection, Unary call and client-side `rpc_stream`.
- `rpc_server.h`: method descriptor, unified Handler and server-side `rpc_request`.
- `rpc.h`: convenience umbrella header.

A client-only translation unit can include `rpc_client.h`; a server-only translation unit can include `rpc_server.h`.

## Server ownership model

`rpc_request` is the single server-side owner for one RPC invocation. Its common fields contain method metadata, initial request bytes, completion state and optional synchronous response. For streaming methods, the server-side queue/condition-variable state is embedded in the Request. The connection's `stream` pointer is only a Reactor-side alias to that embedded state.

The application always receives the same Handler signature:

```c
int handler(rpc_request *request, void *userdata);
```

- `RPC_HANDLER_DONE`: the handler completed synchronously; the framework performs method-specific finalization.
- `RPC_PENDING`: the application transferred completion responsibility to a worker, which must call `rpc_request_complete()` once.
- `rpc_request_read/write()` are valid only for the corresponding streaming method and may block. They must not run on the Reactor thread.

The Request is not freed until the Handler has returned and asynchronous completion has been accepted. For a stream, the framework also waits until queued output has been sent or the peer has disconnected. No application retain/release API is exposed.

## Modules

| Source | Responsibility |
|---|---|
| `buffer.c` | `rpc_buf` and error strings |
| `protocol.c` | 24-byte wire header and frame encoding |
| `io.c` | socket helpers and poll deadlines |
| `server.c` | listener, method registry, Reactor and shutdown |
| `connection.c` | connection framing and Reactor-owned socket IO |
| `request.c` | common Request metadata, Unary dispatch and completion queue |
| `server_stream.c` | embedded server stream queues, read/write, backpressure and finish |
| `client.c` | synchronous client connection and Unary |
| `client_stream.c` | client-side stream open/read/write/half-close/destroy |
| `rpc_internal.h` | private structures and cross-module declarations |

## Threading

- Reactor owns all server sockets and connection lifetimes.
- Handler starts on Reactor and must return quickly.
- User Workers own business execution; they exchange stream messages through bounded queues and condition variables.
- Client calls are synchronous and a client handle is not thread-safe.
- The framework does not create a Worker Pool.

## Compatibility

The 24-byte frame header and existing Unary request/response frames are unchanged. The public server registration and Handler APIs changed in this experimental release. The old `rpc.h` include remains as an umbrella header.
