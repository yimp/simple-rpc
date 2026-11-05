#define _POSIX_C_SOURCE 200809L
#include "rpc_internal.h"

#include <stdlib.h>
#include <string.h>

/* Public buffer utilities and error descriptions. */

int rpc_buf_append(rpc_buf *buf, const void *data, size_t len)
{
    if (!buf || (len && !data)) return RPC_ERR_INVALID;
    if (buf->len > RPC_MAX_PAYLOAD || len > RPC_MAX_PAYLOAD - buf->len)
        return RPC_ERR_LIMIT;
    if (len == 0) return RPC_OK;
    size_t needed = buf->len + len;
    if (needed > buf->cap) {
        size_t cap = buf->cap ? buf->cap : 256;
        while (cap < needed) {
            if (cap > RPC_MAX_PAYLOAD / 2) {
                cap = RPC_MAX_PAYLOAD;
                break;
            }
            cap *= 2;
        }
        void *new_data = realloc(buf->data, cap);
        if (!new_data) return RPC_ERR_NOMEM;
        buf->data = new_data;
        buf->cap = cap;
    }
    memcpy((uint8_t *)buf->data + buf->len, data, len);
    buf->len = needed;
    return RPC_OK;
}

void rpc_buf_free(rpc_buf *buf)
{
    if (!buf) return;
    free(buf->data);
    *buf = (rpc_buf){0};
}

const char *rpc_strerror(int error)
{
    switch (error) {
    case RPC_OK: return "success";
    case RPC_ERR_IO: return "I/O error";
    case RPC_ERR_TIMEOUT: return "timeout";
    case RPC_ERR_PROTOCOL: return "protocol error";
    case RPC_ERR_CLOSED: return "connection closed";
    case RPC_ERR_REMOTE: return "remote handler error";
    case RPC_ERR_LIMIT: return "payload too large";
    case RPC_ERR_INVALID: return "invalid argument or state";
    case RPC_ERR_NOMEM: return "out of memory";
    case RPC_STREAM_EOF: return "end of stream";
    default: return "unknown error";
    }
}

