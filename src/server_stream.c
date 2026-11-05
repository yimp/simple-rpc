#define _POSIX_C_SOURCE 200809L
#include "rpc_internal.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <sys/epoll.h>
#include <unistd.h>

/* Server-side streaming lifecycle, queues and backpressure. */

static void stream_wake(rpc_server *s)
{
    uint64_t one = 1;
    (void)write(s->wake_fd, &one, sizeof(one));
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

void srpc_stream_free(rpc_stream *st)
{
    if (!st) return;
    srpc_frame_list_free(st->in_head);
    srpc_frame_list_free(st->out_head);
    free(st->initial);
    if (st->server) {
        pthread_cond_destroy(&st->cond);
        pthread_mutex_destroy(&st->lock);
    }
    free(st);
}

void srpc_stream_fail(rpc_stream *st)
{
    pthread_mutex_lock(&st->lock);
    st->failed = true;
    pthread_cond_broadcast(&st->cond);
    pthread_mutex_unlock(&st->lock);
}

static int stream_wait_until(rpc_stream *st, int64_t deadline)
{
    struct timespec ts = {.tv_sec = deadline / 1000,
                          .tv_nsec = (deadline % 1000) * 1000000L};
    int rc = pthread_cond_timedwait(&st->cond, &st->lock, &ts);
    return rc == ETIMEDOUT ? RPC_ERR_TIMEOUT :
           rc == 0 || rc == EINTR ? RPC_OK : RPC_ERR_IO;
}

const void *rpc_stream_request_data(const rpc_stream *st)
{
    return st && st->server ? st->initial : NULL;
}

size_t rpc_stream_request_size(const rpc_stream *st)
{
    return st && st->server ? st->initial_len : 0;
}

static void stream_out_push(rpc_stream *st, stream_frame *f)
{
    if (st->out_tail) st->out_tail->next = f;
    else st->out_head = f;
    st->out_tail = f;
    ++st->out_count;
    st->out_bytes += f->len;
}

int rpc_stream_write(rpc_stream *st, const void *data, size_t len, int timeout_ms)
{
    if (!st || (len && !data) || len > RPC_MAX_PAYLOAD || timeout_ms <= 0)
        return len > RPC_MAX_PAYLOAD ? RPC_ERR_LIMIT : RPC_ERR_INVALID;
    if (st->client) return srpc_client_stream_write(st, data, len, timeout_ms);
    if (st->mode != RPC_STREAM_SERVER && st->mode != RPC_STREAM_CLIENT)
        return RPC_ERR_INVALID;

    /* One application writer per stream; the frame is copied before return. */
    stream_frame *f = srpc_frame_new(RPC_STREAM_DATA, st->id, st->method,
                                0, data, len);
    if (!f) return RPC_ERR_NOMEM;
    int result = RPC_OK;
    int64_t deadline = srpc_clock_ms() + timeout_ms;
    pthread_mutex_lock(&st->lock);
    if (st->mode == RPC_STREAM_CLIENT && st->response_messages)
        result = RPC_ERR_INVALID;
    while (result == RPC_OK && !st->failed && !st->local_finished &&
           (st->out_count >= RPC_STREAM_QUEUE_MESSAGES ||
            st->out_bytes + f->len > RPC_STREAM_QUEUE_BYTES)) {
        result = stream_wait_until(st, deadline);
    }
    if (result == RPC_OK && (st->failed || st->local_finished))
        result = st->failed ? RPC_ERR_CLOSED : RPC_ERR_INVALID;
    if (result == RPC_OK) {
        stream_out_push(st, f);
        if (st->mode == RPC_STREAM_CLIENT) ++st->response_messages;
        /* Notify while holding lock, preventing cleanup until notification. */
        stream_wake(st->server);
        f = NULL;
    }
    pthread_mutex_unlock(&st->lock);
    srpc_frame_free(f);
    return result;
}

int rpc_stream_finish(rpc_stream *st, uint32_t status)
{
    if (!st || !st->server) return RPC_ERR_INVALID;
    /* Even after a disconnect, finish() is necessary to complete ownership. */
    stream_frame *f = srpc_frame_new(RPC_STREAM_FINISH, st->id, st->method,
                                status, NULL, 0);
    if (!f) return RPC_ERR_NOMEM;
    pthread_mutex_lock(&st->lock);
    if (st->local_finished) {
        pthread_mutex_unlock(&st->lock);
        srpc_frame_free(f);
        return RPC_ERR_INVALID;
    }
    st->local_finished = true;
    if (!st->failed) {
        stream_out_push(st, f); /* Control frame bypasses data queue limit. */
        f = NULL;
    }
    pthread_cond_broadcast(&st->cond);
    stream_wake(st->server);
    pthread_mutex_unlock(&st->lock);
    srpc_frame_free(f);
    return RPC_OK;
}

int rpc_stream_read(rpc_stream *st, rpc_buf *message, int timeout_ms)
{
    if (!st || !message || timeout_ms <= 0) return RPC_ERR_INVALID;
    if (st->client) return srpc_client_stream_read(st, message, timeout_ms);
    if (st->mode != RPC_STREAM_CLIENT) return RPC_ERR_INVALID;
    message->len = 0;
    int rc = RPC_OK;
    int64_t deadline = srpc_clock_ms() + timeout_ms;
    pthread_mutex_lock(&st->lock);
    while (!st->in_head && !st->remote_closed && !st->failed &&
           !st->local_finished && rc == RPC_OK)
        rc = stream_wait_until(st, deadline);
    if (st->failed) rc = RPC_ERR_CLOSED;
    else if (st->in_head) {
        stream_frame *f = st->in_head;
        if (message->cap < f->len) {
            void *p = realloc(message->data, f->len);
            if (!p && f->len) rc = RPC_ERR_NOMEM;
            else { message->data = p; message->cap = f->len; }
        }
        if (rc == RPC_OK) {
            if (f->len) memcpy(message->data, f->bytes, f->len);
            message->len = f->len;
            st->in_head = f->next;
            if (!st->in_head) st->in_tail = NULL;
            --st->in_count;
            st->in_bytes -= f->len;
            srpc_frame_free(f);
            stream_wake(st->server); /* Resume EPOLLIN after queue frees up. */
        }
    } else if (st->remote_closed) rc = RPC_STREAM_EOF;
    else if (st->local_finished) rc = RPC_ERR_INVALID;
    pthread_mutex_unlock(&st->lock);
    return rc;
}

int srpc_stream_dispatch_open(rpc_conn *c)
{
    rpc_server *srv = c->server;
    rpc_stream *st = calloc(1, sizeof(*st));
    if (!st) return -1;
    st->server = srv;
    st->conn = c;
    st->id = c->request_id;
    st->method = c->method;
    st->mode = (rpc_stream_mode)srpc_get32(c->header + 16);
    st->initial = c->payload;
    st->initial_len = c->payload_len;
    st->remote_closed = st->mode == RPC_STREAM_SERVER;
    c->payload = NULL;
    c->payload_len = c->payload_read = c->header_read = 0;
    if (stream_init_server(st) < 0) { free(st->initial); free(st); return -1; }
    c->stream = st;
    ++srv->pending_count;
    method_entry *entry = NULL;
    for (method_entry *e = srv->methods; e; e = e->next)
        if (e->method == st->method) { entry = e; break; }
    int rc = RPC_DONE;
    if (entry && entry->stream_fn && entry->stream_mode == st->mode)
        rc = entry->stream_fn(st, entry->userdata);
    /* Workers may have written and finished while the handler was running. */
    pthread_mutex_lock(&st->lock);
    bool finished = st->local_finished;
    st->handler_returned = true;
    pthread_mutex_unlock(&st->lock);
    if (!finished && rc != RPC_PENDING) {
        uint32_t status = !entry ? RPC_STATUS_NO_METHOD :
                      !entry->fn ? RPC_STATUS_HANDLER_ERROR :
                          !entry->stream_fn || entry->stream_mode != st->mode ?
                          RPC_STATUS_HANDLER_ERROR :
                          rc == RPC_DONE ? RPC_STATUS_OK : RPC_STATUS_HANDLER_ERROR;
        if (rpc_stream_finish(st, status) != RPC_OK) {
            /* There is no worker to finish this call if completion-frame
             * allocation fails. Mark it done before closing the connection. */
            pthread_mutex_lock(&st->lock);
            st->local_finished = true;
            pthread_mutex_unlock(&st->lock);
            return -1;
        }
    }
    /* Do not dereference st after refresh; an early finish may be reaped. */
    return srpc_server_stream_refresh(c);
}

int srpc_stream_consume_frame(rpc_conn *c, uint16_t type)
{
    rpc_stream *st = c->stream;
    if (!st) return -1;
    if (type == RPC_STREAM_DATA) {
        stream_frame *f = calloc(1, sizeof(*f));
        if (!f) return -1;
        f->bytes = c->payload;
        f->len = c->payload_len;
        c->payload = NULL;
        pthread_mutex_lock(&st->lock);
        if (st->failed || st->remote_closed || st->in_count >= RPC_STREAM_QUEUE_MESSAGES) {
            pthread_mutex_unlock(&st->lock);
            srpc_frame_free(f);
            return -1;
        }
        if (st->in_tail) st->in_tail->next = f;
        else st->in_head = f;
        st->in_tail = f;
        ++st->in_count;
        st->in_bytes += f->len;
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
        stream_frame *f = st->out_head;
        st->out_head = f->next;
        if (!st->out_head) st->out_tail = NULL;
        c->out = f->bytes;
        c->out_len = f->len;
        c->out_written = 0;
        f->bytes = NULL;
        srpc_frame_free(f);
    }
    pthread_mutex_unlock(&st->lock);
    if (finished) {
        /* An early server-side error may leave unread request DATA frames. */
        c->stream = NULL;
        srpc_stream_free(st);
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
        /* Yield after a bounded amount of output to avoid starving other
         * connections under continuously generated stream data. */
        if (consumed >= budget) return srpc_server_stream_refresh(c);
        budget -= consumed;
        /* The last frame may have been FINISH; refresh can retire st. */
    }
}

