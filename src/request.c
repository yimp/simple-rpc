#define _POSIX_C_SOURCE 200809L
#include "rpc_internal.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

uint32_t rpc_request_method(const rpc_request *req)
{
    return req ? req->method : 0;
}
rpc_method_type rpc_request_type(const rpc_request *req)
{
    return req ? req->type : (rpc_method_type)0;
}
const void *rpc_request_data(const rpc_request *req)
{
    return req ? req->data : NULL;
}
size_t rpc_request_size(const rpc_request *req)
{
    return req ? req->size : 0;
}

int rpc_request_set_response(rpc_request *req, const void *data, size_t size)
{
    if (!req || (size && !data) || atomic_load(&req->completed) ||
        (req->type != RPC_METHOD_UNARY &&
         req->type != RPC_METHOD_CLIENT_STREAM))
        return RPC_ERR_INVALID;
    if (size > RPC_MAX_PAYLOAD) return RPC_ERR_LIMIT;
    req->response.len = 0;
    return rpc_buf_append(&req->response, data, size);
}

static int request_complete_unary(rpc_request *req, uint32_t status,
                                  const void *data, size_t size)
{
    if (!req || req->type != RPC_METHOD_UNARY || (size && !data))
        return RPC_ERR_INVALID;
    if (size > RPC_MAX_PAYLOAD) return RPC_ERR_LIMIT;
    uint8_t *frame = srpc_encode_reply(req->request_id, req->method,
                                       status, data, size);
    if (!frame) return RPC_ERR_NOMEM;
    bool expected = false;
    if (!atomic_compare_exchange_strong(&req->completed, &expected, true)) {
        free(frame);
        return RPC_ERR_INVALID;
    }
    req->reply_frame = frame;
    req->reply_len = RPC_HEADER_SIZE + size;
    rpc_server *server = req->server;
    pthread_mutex_lock(&server->completion_lock);
    if (server->completed_tail)
        server->completed_tail->completed_next = req;
    else
        server->completed_head = req;
    server->completed_tail = req;
    uint64_t one = 1;
    (void)write(server->wake_fd, &one, sizeof(one));
    pthread_mutex_unlock(&server->completion_lock);
    return RPC_OK;
}

int rpc_request_complete(rpc_request *req, uint32_t status,
                         const void *response, size_t response_len)
{
    if (!req || (response_len && !response)) return RPC_ERR_INVALID;
    if (response_len > RPC_MAX_PAYLOAD) return RPC_ERR_LIMIT;
    if (req->type == RPC_METHOD_UNARY)
        return request_complete_unary(req, status, response, response_len);
    if (!req->has_stream) return RPC_ERR_INVALID;
    return srpc_request_stream_complete(req, status, response, response_len);
}

int rpc_request_reply(rpc_request *req, uint32_t status,
                      const void *response, size_t response_len)
{
    if (!req || req->type != RPC_METHOD_UNARY) return RPC_ERR_INVALID;
    return rpc_request_complete(req, status, response, response_len);
}

void srpc_free_request(rpc_request *req)
{
    if (!req) return;
    if (req->has_stream) srpc_stream_cleanup(&req->stream);
    free(req->data);
    rpc_buf_free(&req->response);
    free(req->reply_frame);
    free(req);
}

int srpc_dispatch(rpc_conn *c)
{
    rpc_server *server = c->server;
    rpc_request *req = calloc(1, sizeof(*req));
    if (!req) return -1;
    req->server = server;
    req->conn = c;
    req->request_id = c->request_id;
    req->method = c->method;
    req->type = RPC_METHOD_UNARY;
    req->data = c->payload;
    req->size = c->payload_len;
    atomic_init(&req->completed, false);
    c->payload = NULL;
    c->payload_len = c->payload_read = c->header_read = 0;
    c->active = req;

    method_entry *entry = NULL;
    for (method_entry *e = server->methods; e; e = e->next)
        if (e->method == req->method) { entry = e; break; }

    int result = entry && entry->type == RPC_METHOD_UNARY
                     ? entry->fn(req, entry->userdata)
                     : RPC_HANDLER_DONE;
    if (result == RPC_PENDING || atomic_load(&req->completed)) {
        ++server->pending_count;
        return srpc_conn_events(c, 0);
    }

    uint32_t status = !entry ? RPC_STATUS_NO_METHOD :
                      entry->type != RPC_METHOD_UNARY ?
                          RPC_STATUS_METHOD_TYPE_MISMATCH :
                      result == RPC_HANDLER_DONE ? RPC_STATUS_OK :
                          RPC_STATUS_HANDLER_ERROR;
    size_t size = status == RPC_STATUS_OK ? req->response.len : 0;
    uint8_t *frame = srpc_encode_reply(req->request_id, req->method, status,
                                       req->response.data, size);
    c->active = NULL;
    req->conn = NULL;
    srpc_free_request(req);
    if (!frame) return -1;
    return srpc_send_response(c, frame, RPC_HEADER_SIZE + size);
}

void srpc_drain_completions(rpc_server *server, int deliver)
{
    pthread_mutex_lock(&server->completion_lock);
    rpc_request *list = server->completed_head;
    server->completed_head = server->completed_tail = NULL;
    pthread_mutex_unlock(&server->completion_lock);
    while (list) {
        rpc_request *req = list;
        list = req->completed_next;
        rpc_conn *conn = req->conn;
        if (deliver && conn && !conn->closed && conn->active == req) {
            conn->active = NULL;
            req->conn = NULL;
            uint8_t *frame = req->reply_frame;
            size_t len = req->reply_len;
            req->reply_frame = NULL;
            if (srpc_send_response(conn, frame, len) < 0)
                srpc_close_conn(server, conn);
        } else if (conn && conn->active == req) {
            conn->active = NULL;
            req->conn = NULL;
        }
        srpc_free_request(req);
        --server->pending_count;
    }
}
