# Internal module layout

The public API remains in [`include/rpc.h`](../include/rpc.h). Everything in
`src/rpc_internal.h` is **private to the library**, including concrete definitions
of `rpc_server`, `rpc_client`, `rpc_request`, `rpc_stream` and `rpc_conn`.
Applications should not include the internal header or call `srpc_*` helpers.

| Source file | Responsibility |
|---|---|
| `src/buffer.c` | Reusable `rpc_buf` operations and public error strings |
| `src/protocol.c` | Network-byte-order 24-byte header, frame allocation and encoding |
| `src/io.c` | Shared POSIX socket helpers and `poll`-based deadlines |
| `src/client.c` | Client connection management and Unary `rpc_call()` |
| `src/client_stream.c` | Client streaming open/read/write/half-close/destroy |
| `src/server.c` | Listener, method registration, epoll Reactor and shutdown |
| `src/connection.c` | Reactor-owned connections, receive framing and socket writes |
| `src/request.c` | Unary request dispatch, async reply and completion queue |
| `src/server_stream.c` | Stream dispatch, bounded queues, backpressure and finish |
| `src/rpc_internal.h` | Shared private structures and cross-module declarations |

## Ownership and threading

- **Reactor thread** (`server.c`, `connection.c`): owns server sockets,
  `rpc_conn` lifetime, frame parsing, and sends data to peers. No Worker
  performs socket IO on a server connection.
- **Unary Worker** (`request.c`): `rpc_request_reply()` safely queues a
  completed response and wakes the Reactor through `eventfd`.
- **Stream Worker** (`server_stream.c`): `rpc_stream_read/write()` use a mutex,
  condition variable and bounded queues to exchange messages with the Reactor.
  Successful `rpc_stream_finish()` transfers completion responsibility back
  to the framework.
- **Client caller** (`client.c`, `client_stream.c`): synchronous calls use a
  nonblocking socket plus `poll()` internally. A `rpc_client` is **not**
  safe for simultaneous use by multiple threads.

`protocol.c` and `io.c` do not depend on server state. The core dependencies
of server-side calls are:

```text
server.c (Reactor loop)
  +-- connection.c (parse / send)
  |    +-- request.c (Unary dispatch)
  |    +-- server_stream.c (stream dispatch)
  +-- request.c (completion queue)
  +-- server_stream.c (stream readiness)

client.c ------------+--> protocol.c
client_stream.c ------+--> io.c
server modules -------+--> buffer.c
```

The `srpc_` prefix on non-public, cross-translation-unit functions prevents
common C identifiers (such as `dispatch` or `transfer`) from leaking into the
static-library symbol namespace without a prefix. Keep helpers `static` when
only one `.c` file uses them.

## Refactoring constraints

The v0.3 module split is intended to be **behavior-preserving**:

- Existing public signatures and the 24-byte wire header are unchanged.
- A TCP connection still serves one active RPC at a time.
- Client and server streaming continue to have the same per-message size limit,
  bounded server stream queues and completion semantics.
- No new built-in thread pool, TLS, multiplexing, or cancellation mechanism is
  introduced.

Tests remain in `tests/test_rpc.c` and `tests/test_stream.c`; run with
`ctest --test-dir build --output-on-failure` after building.
