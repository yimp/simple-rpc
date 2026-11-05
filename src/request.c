#define _POSIX_C_SOURCE 200809L
#include "rpc_internal.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Unary request and asynchronous completion lifecycle. */

uint32_t rpc_request_method(const rpc_request *req)
{
    return req ? req->method : 0;
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
    if (!req || (size && !data)) return RPC_ERR_INVALID;
    if (size > RPC_MAX_PAYLOAD) return RPC_ERR_LIMIT;
    /* This function is only called by the synchronous handler. */
    req->response.len = 0;
    return rpc_buf_append(&req->response, data, size);
}

int rpc_request_reply(rpc_request *req, uint32_t status,
                      const void *data, size_t size)
{
    if (!req || (size && !data)) return RPC_ERR_INVALID;
    if (size > RPC_MAX_PAYLOAD) return RPC_ERR_LIMIT;
    uint8_t *frame = srpc_encode_reply(req->request_id, req->method,
                                  status, data, size);
    if (!frame) return RPC_ERR_NOMEM;
    bool expected = false;
    if (!atomic_compare_exchange_strong(&req->replied, &expected, true)) {
        free(frame);
        return RPC_ERR_INVALID;
    }
    /* Publish the completed frame. Only the Reactor reads this queue. */
    req->reply_frame = frame;
    req->reply_len = RPC_HEADER_SIZE + size;
    rpc_server *s = req->server;
    pthread_mutex_lock(&s->completion_lock);
    if (s->completed_tail) s->completed_tail->completed_next = req;
    else s->completed_head = req;
    s->completed_tail = req;
    /* Keep the server alive until the wakeup is posted: shutdown drains the
     * queue under this mutex before releasing its own resources. */
    uint64_t one = 1;
    (void)write(s->wake_fd, &one, sizeof(one));
    pthread_mutex_unlock(&s->completion_lock);
    return RPC_OK;
}

void srpc_free_request(rpc_request *req)
{
    if (!req) return;
    free(req->data);
    rpc_buf_free(&req->response);
    free(req->reply_frame);
    free(req);
}

int srpc_dispatch(rpc_conn *c)
{
    rpc_server *s = c->server;
    rpc_request *req = calloc(1, sizeof(*req));
    if (!req) return -1;
    req->server = s;
    req->conn = c;
    req->request_id = c->request_id;
    req->method = c->method;
    req->data = c->payload;
    req->size = c->payload_len;
    atomic_init(&req->replied, false);
    c->payload = NULL;
    c->payload_len = 0;
    c->payload_read = 0;
    c->header_read = 0;
    c->active = req;

    method_entry *entry = NULL;
    for (method_entry *e = s->methods; e; e = e->next) {
        if (e->method == req->method) {
            entry = e;
            break;
        }
    }
    int result = entry && entry->fn ? entry->fn(req, entry->userdata) : RPC_DONE;
    if (result == RPC_PENDING || atomic_load(&req->replied)) {
        /* The reply may already be queued before the handler has returned. */
        ++s->pending_count;
        return srpc_conn_events(c, 0); /* No EPOLLIN until the reply is sent. */
    }
    uint32_t status = !entry ? RPC_STATUS_NO_METHOD :
                      result == RPC_DONE ? RPC_STATUS_OK : RPC_STATUS_HANDLER_ERROR;
    size_t size = status == RPC_STATUS_OK ? req->response.len : 0;
    uint8_t *frame = srpc_encode_reply(req->request_id, req->method, status,
                                  req->response.data, size);
    c->active = NULL;
    srpc_free_request(req);
    if (!frame) return -1;
    return srpc_send_response(c, frame, RPC_HEADER_SIZE + size);
}

void srpc_drain_completions(rpc_server *s, int deliver)
{
    pthread_mutex_lock(&s->completion_lock);
    rpc_request *list = s->completed_head;
    s->completed_head = s->completed_tail = NULL;
    pthread_mutex_unlock(&s->completion_lock);
    while (list) {
        rpc_request *req = list;
        list = req->completed_next;
        rpc_conn *c = req->conn;
        if (deliver && c && !c->closed && c->active == req) {
            c->active = NULL;
            req->conn = NULL;
            uint8_t *frame = req->reply_frame;
            size_t len = req->reply_len;
            req->reply_frame = NULL;
            if (srpc_send_response(c, frame, len) < 0) srpc_close_conn(s, c);
        } else if (c && c->active == req) {
            c->active = NULL;
            req->conn = NULL;
        }
        srpc_free_request(req);
        --s->pending_count;
    }
}

