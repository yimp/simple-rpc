#define _POSIX_C_SOURCE 200809L
#include "rpc_internal.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <sys/epoll.h>
#include <unistd.h>

/* Server-side stream state is embedded in and owned by rpc_request. */
static void stream_wake(rpc_server *server)
{
    uint64_t one = 1;
    (void)write(server->wake_fd, &one, sizeof(one));
}
static int stream_init_server(rpc_stream *st)
{
    if (pthread_mutex_init(&st->lock, NULL) != 0) return -1;
    pthread_condattr_t attr;
    if (pthread_condattr_init(&attr) != 0) {
        pthread_mutex_destroy(&st->lock);
        return -1;
    }
    int ok = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) == 0 &&
             pthread_cond_init(&st->cond, &attr) == 0;
    pthread_condattr_destroy(&attr);
    if (!ok) pthread_mutex_destroy(&st->lock);
    return ok ? 0 : -1;
}
void srpc_stream_cleanup(rpc_stream *st)
{
    if (!st) return;
    srpc_frame_list_free(st->in_head);
    srpc_frame_list_free(st->out_head);
    if (st->server) {
        pthread_cond_destroy(&st->cond);
        pthread_mutex_destroy(&st->lock);
    }
}
void srpc_stream_fail(rpc_stream *st)
{
    if (!st || !st->server) return;
    pthread_mutex_lock(&st->lock);
    st->failed = true;
    pthread_cond_broadcast(&st->cond);
    pthread_mutex_unlock(&st->lock);
}
static int stream_wait_until(rpc_stream *st, int64_t deadline)
{
    struct timespec ts = {.tv_sec=deadline/1000,
                          .tv_nsec=(deadline%1000)*1000000L};
    int rc = pthread_cond_timedwait(&st->cond, &st->lock, &ts);
    return rc == ETIMEDOUT ? RPC_ERR_TIMEOUT :
           rc == 0 || rc == EINTR ? RPC_OK : RPC_ERR_IO;
}
static void stream_out_push(rpc_stream *st, stream_frame *frame)
{
    if (st->out_tail) st->out_tail->next = frame;
    else st->out_head = frame;
    st->out_tail = frame;
    ++st->out_count;
    st->out_bytes += frame->len;
}

int rpc_request_write(rpc_request *req, const void *data,
                      size_t len, int timeout_ms)
{
    if (!req || !req->has_stream || req->type != RPC_METHOD_SERVER_STREAM ||
        (len && !data) || timeout_ms <= 0)
        return len > RPC_MAX_PAYLOAD ? RPC_ERR_LIMIT : RPC_ERR_INVALID;
    if (len > RPC_MAX_PAYLOAD) return RPC_ERR_LIMIT;
    rpc_stream *st = &req->stream;
    stream_frame *frame = srpc_frame_new(RPC_STREAM_DATA, st->id, st->method,
                                         0, data, len);
    if (!frame) return RPC_ERR_NOMEM;
    int result = RPC_OK;
    int64_t deadline = srpc_clock_ms() + timeout_ms;
    pthread_mutex_lock(&st->lock);
    while (!st->failed && !st->local_finished &&
           (st->out_count >= RPC_STREAM_QUEUE_MESSAGES ||
            st->out_bytes + frame->len > RPC_STREAM_QUEUE_BYTES) &&
           result == RPC_OK)
        result = stream_wait_until(st, deadline);
    if (result == RPC_OK && (st->failed || st->local_finished))
        result = st->failed ? RPC_ERR_CLOSED : RPC_ERR_INVALID;
    if (result == RPC_OK) {
        stream_out_push(st, frame);
        stream_wake(st->server);
        frame = NULL;
    }
    pthread_mutex_unlock(&st->lock);
    srpc_frame_free(frame);
    return result;
}

int rpc_request_read(rpc_request *req, rpc_buf *message, int timeout_ms)
{
    if (!req || !req->has_stream || req->type != RPC_METHOD_CLIENT_STREAM ||
        !message || timeout_ms <= 0)
        return RPC_ERR_INVALID;
    rpc_stream *st = &req->stream;
    message->len = 0;
    int rc = RPC_OK;
    int64_t deadline = srpc_clock_ms() + timeout_ms;
    pthread_mutex_lock(&st->lock);
    while (!st->in_head && !st->remote_closed && !st->failed &&
           !st->local_finished && rc == RPC_OK)
        rc = stream_wait_until(st, deadline);
    if (st->failed) {
        rc = RPC_ERR_CLOSED;
    } else if (st->in_head) {
        stream_frame *frame = st->in_head;
        if (message->cap < frame->len) {
            void *p = realloc(message->data, frame->len);
            if (!p && frame->len) rc = RPC_ERR_NOMEM;
            else { message->data = p; message->cap = frame->len; }
        }
        if (rc == RPC_OK) {
            if (frame->len) memcpy(message->data, frame->bytes, frame->len);
            message->len = frame->len;
            st->in_head = frame->next;
            if (!st->in_head) st->in_tail = NULL;
            --st->in_count;
            st->in_bytes -= frame->len;
            srpc_frame_free(frame);
            stream_wake(st->server);
        }
    } else if (st->remote_closed) {
        rc = RPC_STREAM_EOF;
    } else if (st->local_finished) {
        rc = RPC_ERR_CLOSED;
    }
    pthread_mutex_unlock(&st->lock);
    return rc;
}

int srpc_request_stream_complete(rpc_request *req, uint32_t status,
                                 const void *response, size_t response_len)
{
    if (!req || !req->has_stream || (response_len && !response))
        return RPC_ERR_INVALID;
    if (response_len > RPC_MAX_PAYLOAD) return RPC_ERR_LIMIT;
    if (req->type == RPC_METHOD_SERVER_STREAM && response_len)
        return RPC_ERR_INVALID;
    if (req->type != RPC_METHOD_SERVER_STREAM &&
        req->type != RPC_METHOD_CLIENT_STREAM)
        return RPC_ERR_INVALID;

    rpc_stream *st = &req->stream;
    stream_frame *data_frame = NULL;
    if (req->type == RPC_METHOD_CLIENT_STREAM && response_len) {
        data_frame = srpc_frame_new(RPC_STREAM_DATA, st->id, st->method,
                                    0, response, response_len);
        if (!data_frame) return RPC_ERR_NOMEM;
    }
    stream_frame *finish_frame = srpc_frame_new(
        RPC_STREAM_FINISH, st->id, st->method, status, NULL, 0);
    if (!finish_frame) {
        srpc_frame_free(data_frame);
        return RPC_ERR_NOMEM;
    }
    bool expected = false;
    if (!atomic_compare_exchange_strong(&req->completed, &expected, true)) {
        srpc_frame_free(data_frame);
        srpc_frame_free(finish_frame);
        return RPC_ERR_INVALID;
    }
    pthread_mutex_lock(&st->lock);
    if (st->local_finished) {
        pthread_mutex_unlock(&st->lock);
        srpc_frame_free(data_frame);
        srpc_frame_free(finish_frame);
        return RPC_ERR_INVALID;
    }
    st->local_finished = true;
    if (!st->failed) {
        if (data_frame) {
            stream_out_push(st, data_frame);
            data_frame = NULL;
        }
        stream_out_push(st, finish_frame);
        finish_frame = NULL;
    }
    pthread_cond_broadcast(&st->cond);
    stream_wake(st->server);
    pthread_mutex_unlock(&st->lock);
    srpc_frame_free(data_frame);
    srpc_frame_free(finish_frame);
    return RPC_OK;
}

int srpc_stream_dispatch_open(rpc_conn *c)
{
    rpc_server *server = c->server;
    rpc_request *req = calloc(1, sizeof(*req));
    if (!req) return -1;
    req->server = server;
    req->conn = c;
    req->request_id = c->request_id;
    req->method = c->method;
    req->type = (rpc_method_type)srpc_get32(c->header + 16);
    atomic_init(&req->completed, false);

    rpc_stream *st = &req->stream;
    st->server = server;
    st->conn = c;
    st->owner = req;
    st->id = c->request_id;
    st->method = c->method;
    st->mode = req->type;
    st->remote_closed = req->type == RPC_METHOD_SERVER_STREAM;
    if (stream_init_server(st) < 0) {
        free(req);
        return -1;
    }
    req->has_stream = true;
    req->data = c->payload;
    req->size = c->payload_len;
    c->payload = NULL;
    c->payload_len = c->payload_read = c->header_read = 0;
    c->active = req;
    c->stream = st;
    ++server->pending_count;

    method_entry *entry = NULL;
    for (method_entry *e = server->methods; e; e = e->next)
        if (e->method == req->method) { entry = e; break; }
    bool matches = entry && entry->type == req->type;
    int result = matches ? entry->fn(req, entry->userdata) : RPC_HANDLER_DONE;

    pthread_mutex_lock(&st->lock);
    st->handler_returned = true;
    bool completed = st->local_finished;
    pthread_mutex_unlock(&st->lock);

    if (!completed && result != RPC_PENDING) {
        uint32_t status = !entry ? RPC_STATUS_NO_METHOD :
                          !matches ? RPC_STATUS_METHOD_TYPE_MISMATCH :
                          result == RPC_HANDLER_DONE ? RPC_STATUS_OK :
                              RPC_STATUS_HANDLER_ERROR;
        const void *response = status == RPC_STATUS_OK
                                   ? req->response.data : NULL;
        size_t response_len = status == RPC_STATUS_OK
                                  ? req->response.len : 0;
        if (rpc_request_complete(req, status, response, response_len) != RPC_OK) {
            atomic_store(&req->completed, true);
            pthread_mutex_lock(&st->lock);
            st->local_finished = true;
            pthread_cond_broadcast(&st->cond);
            pthread_mutex_unlock(&st->lock);
            return -1;
        }
    }
    return srpc_server_stream_refresh(c);
}

int srpc_stream_consume_frame(rpc_conn *c, uint16_t type)
{
    rpc_stream *st = c->stream;
    if (!st) return -1;
    if (type == RPC_STREAM_DATA) {
        stream_frame *frame = calloc(1, sizeof(*frame));
        if (!frame) return -1;
        frame->bytes = c->payload;
        frame->len = c->payload_len;
        c->payload = NULL;
        pthread_mutex_lock(&st->lock);
        if (st->failed || st->remote_closed ||
            st->in_count >= RPC_STREAM_QUEUE_MESSAGES) {
            pthread_mutex_unlock(&st->lock);
            srpc_frame_free(frame);
            return -1;
        }
        if (st->in_tail) st->in_tail->next = frame;
        else st->in_head = frame;
        st->in_tail = frame;
        ++st->in_count;
        st->in_bytes += frame->len;
        pthread_cond_signal(&st->cond);
        pthread_mutex_unlock(&st->lock);
    } else if (type == RPC_STREAM_HALF_CLOSE) {
        pthread_mutex_lock(&st->lock);
        if (st->remote_closed) {
            pthread_mutex_unlock(&st->lock);
            return -1;
        }
        st->remote_closed = true;
        pthread_cond_broadcast(&st->cond);
        pthread_mutex_unlock(&st->lock);
    } else return -1;
    c->payload_len = c->payload_read = c->header_read = 0;
    return srpc_server_stream_refresh(c);
}

int srpc_server_stream_refresh(rpc_conn *c)
{
    rpc_stream *st = c->stream;
    if (!st || c->closed) return 0;
    pthread_mutex_lock(&st->lock);
    bool finished = st->local_finished && st->handler_returned &&
                    st->out_count == 0;
    bool remote_closed = st->remote_closed;
    bool read_ready = st->in_count < RPC_STREAM_QUEUE_MESSAGES &&
                      st->in_bytes < RPC_STREAM_QUEUE_BYTES;
    if (!c->out && st->out_head) {
        stream_frame *frame = st->out_head;
        st->out_head = frame->next;
        if (!st->out_head) st->out_tail = NULL;
        c->out = frame->bytes;
        c->out_len = frame->len;
        c->out_written = 0;
        frame->bytes = NULL;
        srpc_frame_free(frame);
    }
    pthread_mutex_unlock(&st->lock);
    if (finished) {
        rpc_request *req = st->owner;
        c->stream = NULL;
        c->active = NULL;
        req->conn = NULL;
        srpc_free_request(req);
        --c->server->pending_count;
        if (!remote_closed) return -1;
        return srpc_conn_events(c, EPOLLIN);
    }
    uint32_t events = 0;
    if (!remote_closed && read_ready) events |= EPOLLIN;
    if (c->out) events |= EPOLLOUT;
    return srpc_conn_events(c, events);
}

int srpc_stream_conn_write(rpc_conn *c)
{
    rpc_stream *st = c->stream;
    size_t budget = 2u * RPC_MAX_PAYLOAD;
    for (;;) {
        if (!c->out) {
            if (srpc_server_stream_refresh(c) < 0) return -1;
            if (!c->stream || !c->out) return 0;
        }
        while (c->out_written < c->out_len) {
            ssize_t n = send(c->fd, c->out + c->out_written,
                             c->out_len - c->out_written, MSG_NOSIGNAL);
            if (n > 0) c->out_written += (size_t)n;
            else if (n < 0 && errno == EINTR) continue;
            else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
                return 0;
            else return -1;
        }
        size_t consumed = c->out_len;
        free(c->out);
        c->out = NULL;
        c->out_len = c->out_written = 0;
        pthread_mutex_lock(&st->lock);
        st->out_bytes -= consumed;
        --st->out_count;
        pthread_cond_broadcast(&st->cond);
        pthread_mutex_unlock(&st->lock);
        if (consumed >= budget) return srpc_server_stream_refresh(c);
        budget -= consumed;
    }
}
