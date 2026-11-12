#define _POSIX_C_SOURCE 200809L
#include "rpc_internal.h"

#include <errno.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

/* Reactor-side connection ownership and transport IO. */

void srpc_close_conn(rpc_server *s, rpc_conn *c)
{
    if (c->closed) return;
    c->closed = 1;
    rpc_conn **p = &s->connections;
    while (*p && *p != c) p = &(*p)->next;
    if (*p) *p = c->next;
    if (s->epoll_fd >= 0) epoll_ctl(s->epoll_fd, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd);
    if (c->active) {
        c->active->conn = NULL;   /* Pending request stays alive until reply. */
        c->active = NULL;
    }
    if (c->stream) srpc_stream_fail(c->stream);
    free(c->payload);
    free(c->out);
    c->next = s->closed_connections;
    s->closed_connections = c;
}

void srpc_reap_closed(rpc_server *s)
{
    rpc_conn **pp = &s->closed_connections;
    while (*pp) {
        rpc_conn *c = *pp;
        if (c->stream) {
            rpc_stream *st = c->stream;
            pthread_mutex_lock(&st->lock);
            bool done = st->handler_returned && st->local_finished;
            pthread_mutex_unlock(&st->lock);
            if (!done) { pp = &c->next; continue; }
            c->stream = NULL;
            rpc_request *req = st->owner;
            if (c->active == req) c->active = NULL;
            srpc_free_request(req);
            --s->pending_count;
        }
        *pp = c->next;
        free(c);
    }
}

int srpc_conn_events(rpc_conn *c, uint32_t flags)
{
    struct epoll_event ev = {.events = flags | EPOLLRDHUP, .data.ptr = c};
    return epoll_ctl(c->server->epoll_fd, EPOLL_CTL_MOD, c->fd, &ev);
}

int srpc_send_response(rpc_conn *c, uint8_t *frame, size_t len)
{
    c->out = frame;
    c->out_len = len;
    c->out_written = 0;
    return srpc_conn_events(c, EPOLLOUT);
}

int srpc_conn_read(rpc_conn *c)
{
    for (;;) {
        if (c->header_read < RPC_HEADER_SIZE) {
            ssize_t n = recv(c->fd, c->header + c->header_read,
                             RPC_HEADER_SIZE - c->header_read, 0);
            if (n > 0) c->header_read += (size_t)n;
            else if (n == 0) return -1;
            else if (errno == EINTR) continue;
            else return errno == EAGAIN || errno == EWOULDBLOCK ? 0 : -1;
            if (c->header_read < RPC_HEADER_SIZE) continue;

            const uint8_t *h = c->header;
            uint16_t type = srpc_get16(h + 6);
            if (srpc_get32(h) != RPC_MAGIC || srpc_get16(h + 4) != RPC_VERSION ||
                srpc_get32(h + 8) == 0 || srpc_get32(h + 12) == 0 ||
                srpc_get32(h + 20) > RPC_MAX_PAYLOAD)
                return -1;
            c->request_id = srpc_get32(h + 8);
            c->method = srpc_get32(h + 12);
            c->payload_len = srpc_get32(h + 20);
            if (c->stream) {
                rpc_stream *st = c->stream;
                if (st->id != c->request_id || st->method != c->method ||
                    srpc_get32(h + 16) != 0 || st->mode != RPC_METHOD_CLIENT_STREAM ||
                    (type != RPC_STREAM_DATA && type != RPC_STREAM_HALF_CLOSE) ||
                    (type == RPC_STREAM_HALF_CLOSE && c->payload_len != 0))
                    return -1;
                pthread_mutex_lock(&st->lock);
                bool full = st->in_count >= RPC_STREAM_QUEUE_MESSAGES;
                bool remote_closed = st->remote_closed;
                pthread_mutex_unlock(&st->lock);
                if (full || remote_closed) return -1;
            } else if (type == RPC_REQUEST) {
                if (srpc_get32(h + 16) != 0) return -1;
            } else if (type == RPC_STREAM_OPEN) {
                uint32_t mode = srpc_get32(h + 16);
                if (mode != RPC_METHOD_SERVER_STREAM && mode != RPC_METHOD_CLIENT_STREAM)
                    return -1;
            } else return -1;
            if (c->payload_len) {
                c->payload = malloc(c->payload_len);
                if (!c->payload) return -1;
            } else {
                if (type == RPC_REQUEST) return srpc_dispatch(c);
                if (type == RPC_STREAM_OPEN) return srpc_stream_dispatch_open(c);
                return srpc_stream_consume_frame(c, type);
            }
        }
        ssize_t n = recv(c->fd, c->payload + c->payload_read,
                         c->payload_len - c->payload_read, 0);
        if (n > 0) c->payload_read += (size_t)n;
        else if (n == 0) return -1;
        else if (errno == EINTR) continue;
        else return errno == EAGAIN || errno == EWOULDBLOCK ? 0 : -1;
        if (c->payload_read == c->payload_len) {
            uint16_t type = srpc_get16(c->header + 6);
            if (type == RPC_REQUEST) return srpc_dispatch(c);
            if (type == RPC_STREAM_OPEN) return srpc_stream_dispatch_open(c);
            return srpc_stream_consume_frame(c, type);
        }
    }
}

int srpc_conn_write(rpc_conn *c)
{
    while (c->out_written < c->out_len) {
        ssize_t n = send(c->fd, c->out + c->out_written,
                         c->out_len - c->out_written, MSG_NOSIGNAL);
        if (n > 0) c->out_written += (size_t)n;
        else if (n < 0 && errno == EINTR) continue;
        else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return 0;
        else return -1;
    }
    free(c->out);
    c->out = NULL;
    c->out_len = c->out_written = 0;
    return srpc_conn_events(c, EPOLLIN);
}

