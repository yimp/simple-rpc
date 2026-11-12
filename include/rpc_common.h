#ifndef SIMPLE_RPC_COMMON_H
#define SIMPLE_RPC_COMMON_H
#include <stddef.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
#define RPC_MAX_PAYLOAD (1024u * 1024u)
enum { RPC_OK=0, RPC_ERR_IO=-1, RPC_ERR_TIMEOUT=-2, RPC_ERR_PROTOCOL=-3,
       RPC_ERR_CLOSED=-4, RPC_ERR_REMOTE=-5, RPC_ERR_LIMIT=-6,
       RPC_ERR_INVALID=-7, RPC_ERR_NOMEM=-8, RPC_STREAM_EOF=1 };
enum { RPC_STATUS_OK=0, RPC_STATUS_NO_METHOD=1, RPC_STATUS_HANDLER_ERROR=2,
       RPC_STATUS_TOO_LARGE=3, RPC_STATUS_METHOD_TYPE_MISMATCH=4 };
typedef enum rpc_method_type {
    RPC_METHOD_UNARY=1, RPC_METHOD_SERVER_STREAM=2,
    RPC_METHOD_CLIENT_STREAM=3, RPC_METHOD_BIDI_STREAM=4
} rpc_method_type;
typedef struct rpc_buf { void *data; size_t len; size_t cap; } rpc_buf;
typedef struct rpc_server rpc_server;
typedef struct rpc_client rpc_client;
typedef struct rpc_request rpc_request;
int rpc_buf_append(rpc_buf *buf, const void *data, size_t len);
void rpc_buf_free(rpc_buf *buf);
const char *rpc_strerror(int result);
#ifdef __cplusplus
}
#endif
#endif
