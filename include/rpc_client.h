#ifndef SIMPLE_RPC_CLIENT_H
#define SIMPLE_RPC_CLIENT_H
#include "rpc_common.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct rpc_stream rpc_stream;
/* Compatibility aliases for the original v0.3 stream-open names. */
typedef rpc_method_type rpc_stream_mode;
#define RPC_STREAM_SERVER RPC_METHOD_SERVER_STREAM
#define RPC_STREAM_CLIENT RPC_METHOD_CLIENT_STREAM
rpc_client *rpc_client_connect(const char *host, uint16_t port, int timeout_ms);
int rpc_call(rpc_client *client, uint32_t method, const void *request,
             size_t request_len, rpc_buf *response, int timeout_ms);
uint32_t rpc_client_last_status(const rpc_client *client);
void rpc_client_close(rpc_client *client);
int rpc_stream_open(rpc_client *client, uint32_t method, rpc_method_type type,
                    const void *initial_request, size_t initial_len,
                    int timeout_ms, rpc_stream **out);
int rpc_stream_write(rpc_stream *stream, const void *data,
                     size_t len, int timeout_ms);
int rpc_stream_read(rpc_stream *stream, rpc_buf *message, int timeout_ms);
int rpc_stream_finish_send(rpc_stream *stream, int timeout_ms);
uint32_t rpc_stream_remote_status(const rpc_stream *stream);
void rpc_stream_destroy(rpc_stream *stream);
#ifdef __cplusplus
}
#endif
#endif
