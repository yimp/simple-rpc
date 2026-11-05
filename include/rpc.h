#ifndef SIMPLE_RPC_H
#define SIMPLE_RPC_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define RPC_MAX_PAYLOAD (1024u * 1024u)

/* All functions return RPC_OK (0) on success unless otherwise documented. */
enum {
    RPC_OK = 0,
    RPC_ERR_IO = -1,
    RPC_ERR_TIMEOUT = -2,
    RPC_ERR_PROTOCOL = -3,
    RPC_ERR_CLOSED = -4,
    RPC_ERR_REMOTE = -5,
    RPC_ERR_LIMIT = -6,
    RPC_ERR_INVALID = -7,
    RPC_ERR_NOMEM = -8,
    RPC_STREAM_EOF = 1  /* successful end of message sequence */
};

enum {
    RPC_STATUS_OK = 0,
    RPC_STATUS_NO_METHOD = 1,
    RPC_STATUS_HANDLER_ERROR = 2,
    RPC_STATUS_TOO_LARGE = 3
};

typedef struct rpc_buf {
    void *data;
    size_t len;
    size_t cap;
} rpc_buf;

typedef struct rpc_server rpc_server;
typedef struct rpc_client rpc_client;
typedef struct rpc_request rpc_request;

/* A handler is invoked on the Reactor thread. It must not block.
 * RPC_DONE: the framework sends the response set by rpc_request_set_response().
 * RPC_PENDING: the application MUST call rpc_request_reply() exactly once, even
 * when the peer has disconnected or the server has been stopped.
 * Any other return value produces RPC_STATUS_HANDLER_ERROR.
 *
 * For RPC_PENDING, the framework keeps req and its request bytes alive until
 * reply() has copied the result. No retain/release is needed or exposed.
 * Do not access req after a successful reply(), including from other tasks;
 * coordinate those tasks before replying. If reply() fails with NOMEM, the
 * caller still owns the pending completion obligation and may retry.
 * An asynchronous reply may race with the return of the handler itself.
 */
enum { RPC_DONE = 0, RPC_PENDING = 1 };
typedef int (*rpc_handler)(rpc_request *req, void *userdata);

/* Request bytes are read-only and owned by the framework. The pointer remains
 * valid until the handler returns RPC_DONE or a pending request is replied to.
 */
uint32_t rpc_request_method(const rpc_request *req);
const void *rpc_request_data(const rpc_request *req);
size_t rpc_request_size(const rpc_request *req);

/* Only call from the handler before returning RPC_DONE; copies the payload. */
int rpc_request_set_response(rpc_request *req, const void *data, size_t size);
/* Thread-safe completion for pending requests. Copies data before returning;
 * an RPC_OK return consumes req. Does not guarantee delivery to the peer.
 * The server event loop owns the socket and sends or discards the reply.
 */
int rpc_request_reply(rpc_request *req, uint32_t status,
                      const void *data, size_t size);

/* Streaming RPC: one active call per TCP connection, no multiplexing.
 * Frames carry complete messages (0..RPC_MAX_PAYLOAD), not raw TCP bytes.
 * The total number of messages/bytes in a stream is not capped.
 * These functions are not callable from the Reactor thread when they can wait.
 * Use RPC_PENDING and a user-owned worker for streaming handlers.
 */
typedef struct rpc_stream rpc_stream;

typedef enum rpc_stream_mode {
    RPC_STREAM_SERVER = 1, /* one initial request; many response messages */
    RPC_STREAM_CLIENT = 2  /* one initial request + many request messages; one response */
} rpc_stream_mode;

typedef int (*rpc_stream_handler)(rpc_stream *stream, void *userdata);

/* Register only before run; method IDs are shared with Unary handlers. */
int rpc_server_register_stream(rpc_server *server, uint32_t method,
                               rpc_stream_mode mode,
                               rpc_stream_handler handler, void *userdata);

/* Initiate a streaming method from a client. The initial request is copied
 * into the opening frame; timeout applies to opening frame transfer.
 * The rpc_client is exclusive until rpc_stream_destroy().
 */
int rpc_stream_open(rpc_client *client, uint32_t method,
                    rpc_stream_mode mode, const void *initial_request,
                    size_t initial_len, int timeout_ms, rpc_stream **out);

/* Write/read ONE complete message. Each operation has its own deadline.
 * Server write is safe from a worker thread and uses bounded queuing; when
 * full, it waits for the Reactor to send bytes. Server read waits for data.
 * Exactly one business thread may operate on a stream at any time; do not
 * call read/write/finish concurrently on the same stream. Never block the
 * Reactor thread inside a streaming Handler.
 * Client write/read use nonblocking sockets with poll internally.
 * read(): RPC_STREAM_EOF means a successful remote finish (or request-side
 * half-close when called by a server); RPC_ERR_REMOTE means final failure.
 * Zero-length messages are legal and returned as RPC_OK with len == 0.
 */
int rpc_stream_write(rpc_stream *stream, const void *data,
                     size_t len, int timeout_ms);
int rpc_stream_read(rpc_stream *stream, rpc_buf *message, int timeout_ms);

/* Client Streaming only: half-close request direction, then read response. */
int rpc_stream_finish_send(rpc_stream *stream, int timeout_ms);

/* Server only: complete the RPC, including the final remote status. A
 * successful call hands the stream back to the framework; do not touch it
 * afterward. Even a disconnected stream MUST be finished by user workers.
 * A handler may finish before returning RPC_PENDING.
 */
int rpc_stream_finish(rpc_stream *stream, uint32_t status);

/* Only meaningful after rpc_stream_read() returns STREAM_EOF or REMOTE. */
uint32_t rpc_stream_remote_status(const rpc_stream *stream);

/* Only on the client. An unfinished stream is cancelled by closing its
 * connection; a completed stream can be followed by another call.
 */
void rpc_stream_destroy(rpc_stream *stream);

/* Server-side initial request remains valid until successful finish(). */
const void *rpc_stream_request_data(const rpc_stream *stream);
size_t rpc_stream_request_size(const rpc_stream *stream);

int rpc_buf_append(rpc_buf *buf, const void *data, size_t len);
void rpc_buf_free(rpc_buf *buf);
const char *rpc_strerror(int result);

rpc_server *rpc_server_create(void);
/* Register before rpc_server_run(); duplicate or zero method IDs are rejected. */
int rpc_server_register(rpc_server *server, uint32_t method,
                        rpc_handler handler, void *userdata);
/* IPv4 bind address, e.g. "127.0.0.1" or "0.0.0.0"; 0 chooses a free port. */
int rpc_server_listen(rpc_server *server, const char *host, uint16_t port);
uint16_t rpc_server_port(const rpc_server *server);
/* Blocks until rpc_server_stop() AND all pending handlers have replied.
 * Stop closes client connections and discards late replies, but cannot cancel
 * user tasks. A missing reply can therefore prevent shutdown indefinitely.
 */
int rpc_server_run(rpc_server *server);
/* Thread-safe while the server exists; wakes a blocked event loop. */
void rpc_server_stop(rpc_server *server);
/* Call after rpc_server_run() returns, never concurrently with run/stop.
 * External worker pools must not access any request or server after destroy.
 */
void rpc_server_destroy(rpc_server *server);

/* Connect using IPv4 or hostname; timeout_ms must be positive. DNS resolution
 * (getaddrinfo) is synchronous and not included in the timeout guarantee. */
rpc_client *rpc_client_connect(const char *host, uint16_t port,
                               int timeout_ms);
/* A client is NOT thread-safe: at most one outstanding call per connection.
 * timeout_ms is a total deadline for writing the request and reading the reply.
 * A timeout/I/O/protocol failure makes the connection unusable (closed).
 * The response buffer may be reused; response->len is reset for each call.
 */
int rpc_call(rpc_client *client, uint32_t method,
             const void *request, size_t request_len,
             rpc_buf *response, int timeout_ms);
/* Valid after RPC_ERR_REMOTE; returns a RPC_STATUS_* value. */
uint32_t rpc_client_last_status(const rpc_client *client);
void rpc_client_close(rpc_client *client);

#ifdef __cplusplus
}
#endif
#endif
