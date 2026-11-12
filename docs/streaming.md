# Streaming RPC API v0.4

This document describes the current experimental Linux/C11 implementation.

## One Handler shape

All server methods use the same signature:

```c
int handler(rpc_request *request, void *userdata);
```

The registered `rpc_method_desc.type` determines the communication shape:

- `RPC_METHOD_UNARY`: one request and one response.
- `RPC_METHOD_SERVER_STREAM`: one initial request and multiple server-to-client DATA messages.
- `RPC_METHOD_CLIENT_STREAM`: one initial request, multiple client-to-server DATA messages, and at most one final server response message.
- `RPC_METHOD_BIDI_STREAM`: reserved, not implemented.

`RPC_HANDLER_DONE` means the Handler completed synchronously; the framework finalizes the method according to its type. `RPC_PENDING` means a Worker owns the remaining work and must call `rpc_request_complete()` exactly once.

## Server-side Request API

- `rpc_request_data/size()`: initial request payload.
- `rpc_request_write()`: Server Streaming only; submits one complete response message.
- `rpc_request_read()`: Client Streaming only; returns one complete request message.
- `rpc_request_set_response()`: synchronous final response for Unary or Client Streaming before returning `RPC_HANDLER_DONE`.
- `rpc_request_complete()`: asynchronous finalization. Unary uses the supplied response as the Unary payload; Server Streaming requires an empty payload; Client Streaming may include one final response message followed by FINISH.

Read returns `RPC_OK` for a message, including a zero-length message; `RPC_STREAM_EOF` for a normal client half-close; and a negative error for timeout, disconnect or misuse.

`rpc_request_read/write()` can wait and therefore must only run on a user Worker, never on the Reactor thread.

## Client-side Stream API

The client continues to use an opaque `rpc_stream` handle:

```c
rpc_stream_open(client, method, RPC_METHOD_SERVER_STREAM, ...);
rpc_stream_read(stream, &message, timeout_ms);
rpc_stream_destroy(stream);
```

For Client Streaming, the client writes messages, calls `rpc_stream_finish_send()`, then reads the optional final response message and FINISH status.

The client API is synchronous. Each client handle permits one outstanding call and is not thread-safe.

## Lifecycle

A server-side Request owns its stream state. The connection's stream pointer is an internal alias; the application never receives a server-side `rpc_stream`.

A successful `rpc_request_complete()` consumes the Request. Do not access it afterward. The framework waits for the Handler to return even if a Worker completes early. On disconnect, blocked stream operations wake with `RPC_ERR_CLOSED`; the Worker must still complete the Request so the framework can reclaim it.

## Backpressure

Server output and input queues are bounded to two messages. A producer Worker waits when the output queue is full; the Reactor wakes it after socket progress. A consumer Worker wakes the Reactor after removing an input message, allowing EPOLLIN to resume. Reactor never blocks on these queues.

The bound is per stream, not a global memory budget. Applications remain responsible for limiting concurrent calls and Worker queue depth.

## Wire protocol

The existing 24-byte header and Unary frame types 1/2 remain unchanged. Stream frame types 3-6 remain:

| Type | Direction | Meaning |
|---|---|---|
| 3 STREAM_OPEN | Client → Server | Start a streaming method; Status contains method type |
| 4 STREAM_DATA | Mode-dependent | One complete message |
| 5 STREAM_HALF_CLOSE | Client → Server | Client finished sending request messages |
| 6 STREAM_FINISH | Server → Client | Final status |

The protocol still supports only one active RPC per TCP connection. No multiplexing, BIDI, TLS, authentication, protocol negotiation, single-stream cancel frame or resume protocol is implemented.
