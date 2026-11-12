#ifndef SIMPLE_RPC_SERVER_H
#define SIMPLE_RPC_SERVER_H
#include "rpc_common.h"
#ifdef __cplusplus
extern "C" {
#endif
enum { RPC_HANDLER_DONE=0, RPC_PENDING=1 };
#define RPC_DONE RPC_HANDLER_DONE
typedef int (*rpc_handler)(rpc_request *request, void *userdata);
typedef struct rpc_method_desc {
    uint32_t method;
    rpc_method_type type;
    rpc_handler handler;
    void *userdata;
} rpc_method_desc;
/* Handler runs on the Reactor and must not block.
 * RPC_HANDLER_DONE: synchronous completion; framework finalizes by method type.
 * RPC_PENDING: a Worker owns completion and must call rpc_request_complete once.
 * A Worker may complete before Handler returns; Request remains valid until both
 * Handler return and accepted completion. */
int rpc_server_register(rpc_server *server, const rpc_method_desc *method);
uint32_t rpc_request_method(const rpc_request *request);
rpc_method_type rpc_request_type(const rpc_request *request);
const void *rpc_request_data(const rpc_request *request);
size_t rpc_request_size(const rpc_request *request);
/* Synchronous final response before returning RPC_HANDLER_DONE.
 * Valid for Unary and Client Streaming; not Server Streaming. */
int rpc_request_set_response(rpc_request *request, const void *data, size_t size);
/* Server-side stream operations. They may block; call only from a Worker.
 * read: Client Streaming. write: Server Streaming. */
int rpc_request_read(rpc_request *request, rpc_buf *message, int timeout_ms);
int rpc_request_write(rpc_request *request, const void *data,
                      size_t len, int timeout_ms);
/* Async completion: Unary payload; Server Streaming requires empty payload;
 * Client Streaming permits one final response message followed by final status.
 * A successful call consumes Request; do not access it afterward. */
int rpc_request_complete(rpc_request *request, uint32_t status,
                         const void *response, size_t response_len);
/* Deprecated Unary-only alias. */
int rpc_request_reply(rpc_request *request, uint32_t status,
                      const void *response, size_t response_len);
rpc_server *rpc_server_create(void);
int rpc_server_listen(rpc_server *server, const char *host, uint16_t port);
uint16_t rpc_server_port(const rpc_server *server);
int rpc_server_run(rpc_server *server);
void rpc_server_stop(rpc_server *server);
void rpc_server_destroy(rpc_server *server);
#ifdef __cplusplus
}
#endif
#endif
