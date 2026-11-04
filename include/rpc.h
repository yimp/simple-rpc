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
    RPC_ERR_NOMEM = -8
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

/* Handlers run on the server's event-loop thread: keep them short and nonblocking.
 * A zero return means success; any other return produces HANDLER_ERROR.
 * Request memory is valid only during the callback.
 */
typedef int (*rpc_handler)(const rpc_buf *request, rpc_buf *response,
                           void *userdata);

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
/* Blocks until rpc_server_stop(); handles multiple client connections. */
int rpc_server_run(rpc_server *server);
/* Thread-safe while the server exists; wakes a blocked event loop. */
void rpc_server_stop(rpc_server *server);
/* Call after rpc_server_run() returns, never concurrently with run/stop. */
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
