#define _POSIX_C_SOURCE 200809L
#include "rpc_internal.h"

#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

/* Synchronous client-side streaming calls. */

static int client_stream_break(rpc_stream *st, int rc)
{
    if (st->client->fd >= 0) {
        close(st->client->fd);
        st->client->fd = -1;
    }
    return rc;
}

int rpc_stream_open(rpc_client *c, uint32_t method, rpc_method_type mode,
                    const void *initial, size_t initial_len,
                    int timeout_ms, rpc_stream **out)
{
    if (!c || !out || !method || timeout_ms <= 0 || c->busy ||
        (initial_len && !initial) ||
        (mode != RPC_METHOD_SERVER_STREAM && mode != RPC_METHOD_CLIENT_STREAM))
        return RPC_ERR_INVALID;
    if (c->fd < 0) return RPC_ERR_CLOSED;
    if (initial_len > RPC_MAX_PAYLOAD) return RPC_ERR_LIMIT;
    *out = NULL;
    rpc_stream *st = calloc(1, sizeof(*st));
    if (!st) return RPC_ERR_NOMEM;
    st->client = c;
    st->mode = mode;
    st->method = method;
    st->id = ++c->next_id;
    if (st->id == 0) st->id = ++c->next_id;
    uint8_t header[RPC_HEADER_SIZE];
    srpc_make_header(header, RPC_STREAM_OPEN, st->id, method, mode,
                (uint32_t)initial_len);
    int64_t deadline = srpc_clock_ms() + timeout_ms;
    int rc = srpc_transfer(c->fd, header, sizeof(header), 1, deadline);
    if (rc == RPC_OK && initial_len)
        rc = srpc_transfer(c->fd, (void *)initial, initial_len, 1, deadline);
    if (rc != RPC_OK) {
        client_stream_break(st, rc);
        free(st);
        return rc;
    }
    c->busy = true;
    *out = st;
    return RPC_OK;
}

int rpc_stream_write(rpc_stream *st, const void *data,
                     size_t len, int timeout_ms)
{
    if (!st || !st->client || (len && !data) || timeout_ms <= 0)
        return RPC_ERR_INVALID;
    if (len > RPC_MAX_PAYLOAD) return RPC_ERR_LIMIT;
    return srpc_client_stream_write(st, data, len, timeout_ms);
}

int srpc_client_stream_write(rpc_stream *st, const void *data,
                             size_t len, int timeout_ms)
{
    if (st->client->fd < 0) return RPC_ERR_CLOSED;
    if (st->mode != RPC_METHOD_CLIENT_STREAM || st->client_send_closed ||
        st->client_final_received)
        return RPC_ERR_INVALID;
    uint8_t header[RPC_HEADER_SIZE];
    srpc_make_header(header, RPC_STREAM_DATA, st->id, st->method, 0, (uint32_t)len);
    int64_t deadline = srpc_clock_ms() + timeout_ms;
    int rc = srpc_transfer(st->client->fd, header, sizeof(header), 1, deadline);
    if (rc == RPC_OK && len)
        rc = srpc_transfer(st->client->fd, (void *)data, len, 1, deadline);
    return rc == RPC_OK ? RPC_OK : client_stream_break(st, rc);
}

int rpc_stream_finish_send(rpc_stream *st, int timeout_ms)
{
    if (!st || !st->client || st->mode != RPC_METHOD_CLIENT_STREAM ||
        st->client_send_closed || timeout_ms <= 0)
        return RPC_ERR_INVALID;
    if (st->client->fd < 0) return RPC_ERR_CLOSED;
    uint8_t header[RPC_HEADER_SIZE];
    srpc_make_header(header, RPC_STREAM_HALF_CLOSE, st->id, st->method, 0, 0);
    int64_t deadline = srpc_clock_ms() + timeout_ms;
    int rc = srpc_transfer(st->client->fd, header, sizeof(header), 1, deadline);
    if (rc == RPC_OK) st->client_send_closed = true;
    else client_stream_break(st, rc);
    return rc;
}

int rpc_stream_read(rpc_stream *st, rpc_buf *msg, int timeout_ms)
{
    if (!st || !st->client || !msg || timeout_ms <= 0)
        return RPC_ERR_INVALID;
    return srpc_client_stream_read(st, msg, timeout_ms);
}

int srpc_client_stream_read(rpc_stream *st, rpc_buf *msg, int timeout_ms)
{
    rpc_client *c = st->client;
    msg->len = 0;
    if (c->fd < 0) return RPC_ERR_CLOSED;
    if (st->client_final_received) return st->remote_status == RPC_STATUS_OK ?
                                      RPC_STREAM_EOF : RPC_ERR_REMOTE;
    if (st->mode == RPC_METHOD_CLIENT_STREAM && !st->client_send_closed)
        return RPC_ERR_INVALID;
    msg->len = 0;
    uint8_t header[RPC_HEADER_SIZE];
    int64_t deadline = srpc_clock_ms() + timeout_ms;
    int rc = srpc_transfer(c->fd, header, sizeof(header), 0, deadline);
    if (rc != RPC_OK) return client_stream_break(st, rc);
    uint16_t type = srpc_get16(header + 6);
    uint32_t len = srpc_get32(header + 20);
    if (srpc_get32(header) != RPC_MAGIC || srpc_get16(header + 4) != RPC_VERSION ||
        srpc_get32(header + 8) != st->id || srpc_get32(header + 12) != st->method ||
        len > RPC_MAX_PAYLOAD ||
        (type != RPC_STREAM_DATA && type != RPC_STREAM_FINISH) ||
        (type == RPC_STREAM_FINISH && len != 0) ||
        (type == RPC_STREAM_DATA && srpc_get32(header + 16) != 0) ||
        (type == RPC_STREAM_DATA && st->mode == RPC_METHOD_CLIENT_STREAM &&
         st->client_data_count))
        return client_stream_break(st, RPC_ERR_PROTOCOL);
    if (type == RPC_STREAM_FINISH) {
        st->client_final_received = true;
        st->remote_status = srpc_get32(header + 16);
        c->last_status = st->remote_status;
        return st->remote_status == RPC_STATUS_OK ? RPC_STREAM_EOF : RPC_ERR_REMOTE;
    }
    if (len > msg->cap) {
        void *buf = realloc(msg->data, len);
        if (!buf) return client_stream_break(st, RPC_ERR_NOMEM);
        msg->data = buf;
        msg->cap = len;
    }
    if (len) {
        rc = srpc_transfer(c->fd, msg->data, len, 0, deadline);
        if (rc != RPC_OK) return client_stream_break(st, rc);
    }
    msg->len = len;
    ++st->client_data_count;
    return RPC_OK;
}

uint32_t rpc_stream_remote_status(const rpc_stream *st)
{
    return st && st->client && st->client_final_received ? st->remote_status : 0;
}

void rpc_stream_destroy(rpc_stream *st)
{
    if (!st || !st->client) return;
    rpc_client *c = st->client;
    /* Unfinished calls cannot leave their frames on a reusable connection. */
    if (!st->client_final_received && c->fd >= 0) {
        close(c->fd);
        c->fd = -1;
    }
    c->busy = false;
    free(st);
}

