#define _POSIX_C_SOURCE 200809L
#include "rpc.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define RPC_MAGIC 0x53525043u /* "SRPC" */
#define RPC_VERSION 1u
#define RPC_REQUEST 1u
#define RPC_RESPONSE 2u
#define RPC_HEADER_SIZE 24u
#define RPC_MAX_EVENTS 64

static void put16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)v;
}

static void put32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

static uint16_t get16(const uint8_t *p)
{
    return (uint16_t)((uint16_t)p[0] << 8 | p[1]);
}

static uint32_t get32(const uint8_t *p)
{
    return (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 |
           (uint32_t)p[2] << 8 | p[3];
}

static void make_header(uint8_t *p, uint16_t type, uint32_t id,
                        uint32_t method, uint32_t status, uint32_t len)
{
    put32(p, RPC_MAGIC);
    put16(p + 4, RPC_VERSION);
    put16(p + 6, type);
    put32(p + 8, id);
    put32(p + 12, method);
    put32(p + 16, status);
    put32(p + 20, len);
}

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

static int nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    return flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0 ? -1 : 0;
}

typedef struct method_entry {
    uint32_t method;
    rpc_handler fn;
    rpc_stream_handler stream_fn;
    rpc_stream_mode stream_mode;
    void *userdata;
    struct method_entry *next;
} method_entry;

typedef struct rpc_conn {
    int fd;
    struct rpc_server *server;
    struct rpc_conn *next;
    int closed;
    uint8_t header[RPC_HEADER_SIZE];
    size_t header_read;
    uint8_t *payload;
    uint32_t payload_len;
    size_t payload_read;
    uint32_t request_id;
    uint32_t method;
    struct rpc_request *active;
    rpc_stream *stream;
    uint8_t *out;
    size_t out_len;
    size_t out_written;
} rpc_conn;

struct rpc_request {
    struct rpc_server *server;
    rpc_conn *conn;                   /* Reactor thread only; NULL if closed. */
    uint32_t request_id;
    uint32_t method;
    uint8_t *data;
    size_t size;
    rpc_buf response;                /* Handler thread (Reactor) only. */
    atomic_bool replied;             /* Prevent competing completions. */
    uint8_t *reply_frame;            /* Published through completion_lock. */
    size_t reply_len;
    struct rpc_request *completed_next;
};

struct rpc_server {
    int listen_fd;
    int epoll_fd;
    int wake_fd;
    uint16_t port;
    method_entry *methods;
    rpc_conn *connections;
    rpc_conn *closed_connections;     /* Deferred until epoll batch finishes. */
    pthread_mutex_t completion_lock;
    rpc_request *completed_head;
    rpc_request *completed_tail;
    size_t pending_count;            /* Reactor thread only. */
    atomic_bool stopping;
    atomic_bool running;
};

/* Each streaming frame is one complete application message. Reactor alone
 * accesses TCP sockets; workers coordinate through the stream mutex/cond. */
#define RPC_STREAM_OPEN 3u
#define RPC_STREAM_DATA 4u
#define RPC_STREAM_HALF_CLOSE 5u
#define RPC_STREAM_FINISH 6u
#define RPC_STREAM_QUEUE_MESSAGES 2u
#define RPC_STREAM_QUEUE_BYTES (2u * (RPC_MAX_PAYLOAD + RPC_HEADER_SIZE))

typedef struct stream_frame {
    uint8_t *bytes;                 /* header+payload for outbound; payload inbound */
    size_t len;
    struct stream_frame *next;
} stream_frame;

struct rpc_stream {
    rpc_server *server;             /* NULL for a client-side stream */
    rpc_client *client;             /* Non-NULL for a client-side stream */
    rpc_conn *conn;                 /* Reactor-owned, never accessed by workers */
    uint32_t id, method;
    rpc_stream_mode mode;
    uint8_t *initial;
    size_t initial_len;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    stream_frame *in_head, *in_tail;
    stream_frame *out_head, *out_tail;
    size_t in_count, in_bytes, out_count, out_bytes;
    bool remote_closed;            /* Request HALF_CLOSE observed, or server stream */
    bool local_finished;            /* Final status queued or peer gone */
    bool failed;                    /* Connection closed; wake blocked workers */
    bool handler_returned;          /* Prevent worker-finishes-first UAF */
    bool client_send_closed;
    bool client_final_received;
    unsigned client_data_count;
    uint32_t remote_status;
    unsigned response_messages;    /* Client streaming allows <= 1 response */
};

static int64_t clock_ms(void);
static void stream_free(rpc_stream *st);
static int server_stream_refresh(rpc_conn *c);
static int stream_conn_write(rpc_conn *c);
static int stream_dispatch_open(rpc_conn *c);
static int stream_consume_frame(rpc_conn *c, uint16_t type);
static int client_stream_write(rpc_stream *st, const void *data, size_t len, int timeout_ms);
static int client_stream_read(rpc_stream *st, rpc_buf *message, int timeout_ms);

static void stream_wake(rpc_server *s)
{
    uint64_t one = 1;
    (void)write(s->wake_fd, &one, sizeof(one));
}

static stream_frame *frame_new(uint16_t type, uint32_t id, uint32_t method,
                               uint32_t status, const void *data, size_t len)
{
    stream_frame *f = calloc(1, sizeof(*f));
    if (!f) return NULL;
    f->len = RPC_HEADER_SIZE + len;
    f->bytes = malloc(f->len);
    if (!f->bytes) { free(f); return NULL; }
    make_header(f->bytes, type, id, method, status, (uint32_t)len);
    if (len) memcpy(f->bytes + RPC_HEADER_SIZE, data, len);
    return f;
}

static void frame_free(stream_frame *f)
{
    if (!f) return;
    free(f->bytes);
    free(f);
}

static void frame_list_free(stream_frame *f)
{
    while (f) {
        stream_frame *next = f->next;
        frame_free(f);
        f = next;
    }
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

static void stream_free(rpc_stream *st)
{
    if (!st) return;
    frame_list_free(st->in_head);
    frame_list_free(st->out_head);
    free(st->initial);
    if (st->server) {
        pthread_cond_destroy(&st->cond);
        pthread_mutex_destroy(&st->lock);
    }
    free(st);
}

static void stream_fail(rpc_stream *st)
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

static uint8_t *encode_reply(uint32_t request_id, uint32_t method,
                             uint32_t status, const void *data, size_t len)
{
    uint8_t *frame = malloc(RPC_HEADER_SIZE + len);
    if (!frame) return NULL;
    make_header(frame, RPC_RESPONSE, request_id, method, status, (uint32_t)len);
    if (len) memcpy(frame + RPC_HEADER_SIZE, data, len);
    return frame;
}

int rpc_request_reply(rpc_request *req, uint32_t status,
                      const void *data, size_t size)
{
    if (!req || (size && !data)) return RPC_ERR_INVALID;
    if (size > RPC_MAX_PAYLOAD) return RPC_ERR_LIMIT;
    uint8_t *frame = encode_reply(req->request_id, req->method,
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

static void free_request(rpc_request *req)
{
    if (!req) return;
    free(req->data);
    rpc_buf_free(&req->response);
    free(req->reply_frame);
    free(req);
}

rpc_server *rpc_server_create(void)
{
    rpc_server *s = calloc(1, sizeof(*s));
    if (!s) return NULL;
    s->listen_fd = -1;
    s->epoll_fd = -1;
    s->wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (s->wake_fd < 0) {
        free(s);
        return NULL;
    }
    if (pthread_mutex_init(&s->completion_lock, NULL) != 0) {
        close(s->wake_fd);
        free(s);
        return NULL;
    }
    atomic_init(&s->stopping, false);
    atomic_init(&s->running, false);
    return s;
}

int rpc_server_register(rpc_server *s, uint32_t method,
                        rpc_handler fn, void *userdata)
{
    if (!s || !method || !fn || atomic_load(&s->running))
        return RPC_ERR_INVALID;
    for (method_entry *e = s->methods; e; e = e->next)
        if (e->method == method) return RPC_ERR_INVALID;
    method_entry *e = malloc(sizeof(*e));
    if (!e) return RPC_ERR_NOMEM;
    *e = (method_entry){.method = method, .fn = fn,
                        .userdata = userdata, .next = s->methods};
    s->methods = e;
    return RPC_OK;
}

int rpc_server_listen(rpc_server *s, const char *host, uint16_t port)
{
    if (!s || s->listen_fd >= 0 || atomic_load(&s->running))
        return RPC_ERR_INVALID;
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) return RPC_ERR_IO;
    int on = 1;
    struct sockaddr_in addr = {.sin_family = AF_INET, .sin_port = htons(port)};
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0 ||
        inet_pton(AF_INET, host ? host : "0.0.0.0", &addr.sin_addr) != 1 ||
        nonblocking(fd) < 0 ||
        bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0 ||
        listen(fd, SOMAXCONN) < 0) {
        close(fd);
        return RPC_ERR_IO;
    }
    socklen_t addrlen = sizeof(addr);
    if (getsockname(fd, (struct sockaddr *)&addr, &addrlen) < 0) {
        close(fd);
        return RPC_ERR_IO;
    }
    s->listen_fd = fd;
    s->port = ntohs(addr.sin_port);
    return RPC_OK;
}

uint16_t rpc_server_port(const rpc_server *s)
{
    return s ? s->port : 0;
}

static void close_conn(rpc_server *s, rpc_conn *c)
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
    if (c->stream) stream_fail(c->stream);
    free(c->payload);
    free(c->out);
    c->next = s->closed_connections;
    s->closed_connections = c;
}

static void reap_closed(rpc_server *s)
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
            stream_free(st);
            --s->pending_count;
        }
        *pp = c->next;
        free(c);
    }
}

static int conn_events(rpc_conn *c, uint32_t flags)
{
    struct epoll_event ev = {.events = flags | EPOLLRDHUP, .data.ptr = c};
    return epoll_ctl(c->server->epoll_fd, EPOLL_CTL_MOD, c->fd, &ev);
}

/* ---- Server-side streaming RPC ---------------------------------------- */

int rpc_server_register_stream(rpc_server *s, uint32_t method,
                               rpc_stream_mode mode, rpc_stream_handler fn,
                               void *userdata)
{
    if (!s || !method || !fn || atomic_load(&s->running) ||
        (mode != RPC_STREAM_SERVER && mode != RPC_STREAM_CLIENT))
        return RPC_ERR_INVALID;
    for (method_entry *e = s->methods; e; e = e->next)
        if (e->method == method) return RPC_ERR_INVALID;
    method_entry *e = calloc(1, sizeof(*e));
    if (!e) return RPC_ERR_NOMEM;
    e->method = method;
    e->stream_fn = fn;
    e->stream_mode = mode;
    e->userdata = userdata;
    e->next = s->methods;
    s->methods = e;
    return RPC_OK;
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
    if (st->client) return client_stream_write(st, data, len, timeout_ms);
    if (st->mode != RPC_STREAM_SERVER && st->mode != RPC_STREAM_CLIENT)
        return RPC_ERR_INVALID;

    /* One application writer per stream; the frame is copied before return. */
    stream_frame *f = frame_new(RPC_STREAM_DATA, st->id, st->method,
                                0, data, len);
    if (!f) return RPC_ERR_NOMEM;
    int result = RPC_OK;
    int64_t deadline = clock_ms() + timeout_ms;
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
    frame_free(f);
    return result;
}

int rpc_stream_finish(rpc_stream *st, uint32_t status)
{
    if (!st || !st->server) return RPC_ERR_INVALID;
    /* Even after a disconnect, finish() is necessary to complete ownership. */
    stream_frame *f = frame_new(RPC_STREAM_FINISH, st->id, st->method,
                                status, NULL, 0);
    if (!f) return RPC_ERR_NOMEM;
    pthread_mutex_lock(&st->lock);
    if (st->local_finished) {
        pthread_mutex_unlock(&st->lock);
        frame_free(f);
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
    frame_free(f);
    return RPC_OK;
}

int rpc_stream_read(rpc_stream *st, rpc_buf *message, int timeout_ms)
{
    if (!st || !message || timeout_ms <= 0) return RPC_ERR_INVALID;
    if (st->client) return client_stream_read(st, message, timeout_ms);
    if (st->mode != RPC_STREAM_CLIENT) return RPC_ERR_INVALID;
    message->len = 0;
    int rc = RPC_OK;
    int64_t deadline = clock_ms() + timeout_ms;
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
            frame_free(f);
            stream_wake(st->server); /* Resume EPOLLIN after queue frees up. */
        }
    } else if (st->remote_closed) rc = RPC_STREAM_EOF;
    else if (st->local_finished) rc = RPC_ERR_INVALID;
    pthread_mutex_unlock(&st->lock);
    return rc;
}

static int stream_dispatch_open(rpc_conn *c)
{
    rpc_server *srv = c->server;
    rpc_stream *st = calloc(1, sizeof(*st));
    if (!st) return -1;
    st->server = srv;
    st->conn = c;
    st->id = c->request_id;
    st->method = c->method;
    st->mode = (rpc_stream_mode)get32(c->header + 16);
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
    return server_stream_refresh(c);
}

/* The decoder owns c->payload. A DATA message hands it to the inbound queue. */
static int stream_consume_frame(rpc_conn *c, uint16_t type)
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
            frame_free(f);
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
    return server_stream_refresh(c);
}

/* Called only by the Reactor: moves queued frames into the socket writer. */
static int server_stream_refresh(rpc_conn *c)
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
        frame_free(f);
    }
    pthread_mutex_unlock(&st->lock);
    if (finished) {
        /* An early server-side error may leave unread request DATA frames. */
        c->stream = NULL;
        stream_free(st);
        --c->server->pending_count;
        if (!remote_closed) return -1;
        return conn_events(c, EPOLLIN);
    }
    uint32_t events = 0;
    if (!remote_closed && read_ready) events |= EPOLLIN;
    if (c->out) events |= EPOLLOUT;
    return conn_events(c, events);
}

static int stream_conn_write(rpc_conn *c)
{
    rpc_stream *st = c->stream;
    size_t budget = 2u * RPC_MAX_PAYLOAD;
    for (;;) {
        if (!c->out) {
            if (server_stream_refresh(c) < 0) return -1;
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
        if (consumed >= budget) return server_stream_refresh(c);
        budget -= consumed;
        /* The last frame may have been FINISH; refresh can retire st. */
    }
}

/* Transfers ownership of frame to the connection. Reactor thread only. */
static int send_response(rpc_conn *c, uint8_t *frame, size_t len)
{
    c->out = frame;
    c->out_len = len;
    c->out_written = 0;
    return conn_events(c, EPOLLOUT);
}

static int dispatch(rpc_conn *c)
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
        return conn_events(c, 0); /* No EPOLLIN until the reply is sent. */
    }
    uint32_t status = !entry ? RPC_STATUS_NO_METHOD :
                      result == RPC_DONE ? RPC_STATUS_OK : RPC_STATUS_HANDLER_ERROR;
    size_t size = status == RPC_STATUS_OK ? req->response.len : 0;
    uint8_t *frame = encode_reply(req->request_id, req->method, status,
                                  req->response.data, size);
    c->active = NULL;
    free_request(req);
    if (!frame) return -1;
    return send_response(c, frame, RPC_HEADER_SIZE + size);
}

static int conn_read(rpc_conn *c)
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
            uint16_t type = get16(h + 6);
            if (get32(h) != RPC_MAGIC || get16(h + 4) != RPC_VERSION ||
                get32(h + 8) == 0 || get32(h + 12) == 0 ||
                get32(h + 20) > RPC_MAX_PAYLOAD)
                return -1;
            c->request_id = get32(h + 8);
            c->method = get32(h + 12);
            c->payload_len = get32(h + 20);
            if (c->stream) {
                rpc_stream *st = c->stream;
                if (st->id != c->request_id || st->method != c->method ||
                    get32(h + 16) != 0 || st->mode != RPC_STREAM_CLIENT ||
                    (type != RPC_STREAM_DATA && type != RPC_STREAM_HALF_CLOSE) ||
                    (type == RPC_STREAM_HALF_CLOSE && c->payload_len != 0))
                    return -1;
                pthread_mutex_lock(&st->lock);
                bool full = st->in_count >= RPC_STREAM_QUEUE_MESSAGES;
                bool remote_closed = st->remote_closed;
                pthread_mutex_unlock(&st->lock);
                if (full || remote_closed) return -1;
            } else if (type == RPC_REQUEST) {
                if (get32(h + 16) != 0) return -1;
            } else if (type == RPC_STREAM_OPEN) {
                uint32_t mode = get32(h + 16);
                if (mode != RPC_STREAM_SERVER && mode != RPC_STREAM_CLIENT)
                    return -1;
            } else return -1;
            if (c->payload_len) {
                c->payload = malloc(c->payload_len);
                if (!c->payload) return -1;
            } else {
                if (type == RPC_REQUEST) return dispatch(c);
                if (type == RPC_STREAM_OPEN) return stream_dispatch_open(c);
                return stream_consume_frame(c, type);
            }
        }
        ssize_t n = recv(c->fd, c->payload + c->payload_read,
                         c->payload_len - c->payload_read, 0);
        if (n > 0) c->payload_read += (size_t)n;
        else if (n == 0) return -1;
        else if (errno == EINTR) continue;
        else return errno == EAGAIN || errno == EWOULDBLOCK ? 0 : -1;
        if (c->payload_read == c->payload_len) {
            uint16_t type = get16(c->header + 6);
            if (type == RPC_REQUEST) return dispatch(c);
            if (type == RPC_STREAM_OPEN) return stream_dispatch_open(c);
            return stream_consume_frame(c, type);
        }
    }
}

static int conn_write(rpc_conn *c)
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
    return conn_events(c, EPOLLIN);
}

/* Called on the Reactor thread; async reply() never accesses a connection. */
static void drain_completions(rpc_server *s, int deliver)
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
            if (send_response(c, frame, len) < 0) close_conn(s, c);
        } else if (c && c->active == req) {
            c->active = NULL;
            req->conn = NULL;
        }
        free_request(req);
        --s->pending_count;
    }
}

static int accept_clients(rpc_server *s)
{
    for (;;) {
        int fd = accept(s->listen_fd, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR) continue;
            return errno == EAGAIN || errno == EWOULDBLOCK ? 0 : -1;
        }
        if (nonblocking(fd) < 0 || fcntl(fd, F_SETFD, FD_CLOEXEC) < 0) {
            close(fd);
            return -1;
        }
        rpc_conn *c = calloc(1, sizeof(*c));
        if (!c) {
            close(fd);
            return -1;
        }
        c->fd = fd;
        c->server = s;
        struct epoll_event ev = {.events = EPOLLIN | EPOLLRDHUP, .data.ptr = c};
        if (epoll_ctl(s->epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
            close(fd);
            free(c);
            return -1;
        }
        c->next = s->connections;
        s->connections = c;
    }
}

static void read_wakeup(rpc_server *s)
{
    uint64_t value;
    while (read(s->wake_fd, &value, sizeof(value)) > 0) {}
}

int rpc_server_run(rpc_server *s)
{
    if (!s || s->listen_fd < 0 || atomic_exchange(&s->running, true))
        return RPC_ERR_INVALID;
    atomic_store(&s->stopping, false);
    int result = RPC_OK;
    s->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (s->epoll_fd < 0) result = RPC_ERR_IO;
    if (result == RPC_OK) {
        struct epoll_event ev = {.events = EPOLLIN, .data.ptr = NULL};
        if (epoll_ctl(s->epoll_fd, EPOLL_CTL_ADD, s->listen_fd, &ev) < 0)
            result = RPC_ERR_IO;
        ev.data.ptr = s;
        if (result == RPC_OK && epoll_ctl(s->epoll_fd, EPOLL_CTL_ADD,
                                         s->wake_fd, &ev) < 0)
            result = RPC_ERR_IO;
    }
    int shutting_down = 0;
    for (;;) {
        if ((result != RPC_OK || atomic_load(&s->stopping)) && !shutting_down) {
            shutting_down = 1;
            if (s->epoll_fd >= 0)
                epoll_ctl(s->epoll_fd, EPOLL_CTL_DEL, s->listen_fd, NULL);
            while (s->connections) close_conn(s, s->connections);
        }
        drain_completions(s, !shutting_down);
        for (rpc_conn *c = s->connections; c;) {
            rpc_conn *next = c->next;
            if (c->stream && server_stream_refresh(c) < 0) close_conn(s, c);
            c = next;
        }
        reap_closed(s);
        if (shutting_down && s->pending_count == 0) break;

        if (result != RPC_OK) {
            /* epoll failed; still wait for outstanding user tasks to reply. */
            struct pollfd pfd = {.fd = s->wake_fd, .events = POLLIN};
            int n = poll(&pfd, 1, -1);
            if (n > 0) read_wakeup(s);
            else if (n < 0 && errno != EINTR) break;
            continue;
        }
        struct epoll_event events[RPC_MAX_EVENTS];
        int count = epoll_wait(s->epoll_fd, events, RPC_MAX_EVENTS, -1);
        if (count < 0) {
            if (errno == EINTR) continue;
            result = RPC_ERR_IO;
            continue;
        }
        for (int i = 0; i < count; ++i) {
            if (atomic_load(&s->stopping)) break;
            if (events[i].data.ptr == NULL) {
                if (accept_clients(s) < 0) result = RPC_ERR_IO;
            } else if (events[i].data.ptr == s) {
                read_wakeup(s);
                drain_completions(s, 1);
            } else {
                rpc_conn *c = events[i].data.ptr;
                if (c->closed) continue; /* Stale event in current batch. */
                uint32_t flags = events[i].events;
                int rc = 0;
                if (flags & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) rc = -1;
                else if (c->stream) {
                    if (flags & EPOLLOUT) rc = stream_conn_write(c);
                    if (rc == 0 && c->stream && (flags & EPOLLIN))
                        rc = conn_read(c);
                } else if (c->out && (flags & EPOLLOUT)) rc = conn_write(c);
                else if (!c->out && !c->active && (flags & EPOLLIN))
                    rc = conn_read(c);
                if (rc < 0) close_conn(s, c);
            }
            if (result != RPC_OK) break;
        }
        reap_closed(s);
    }
    read_wakeup(s);
    drain_completions(s, 0);
    reap_closed(s);
    if (s->epoll_fd >= 0) close(s->epoll_fd);
    s->epoll_fd = -1;
    atomic_store(&s->running, false);
    return result;
}

void rpc_server_stop(rpc_server *s)
{
    if (!s) return;
    atomic_store(&s->stopping, true);
    uint64_t one = 1;
    (void)write(s->wake_fd, &one, sizeof(one));
}

void rpc_server_destroy(rpc_server *s)
{
    if (!s) return;
    while (s->connections) close_conn(s, s->connections);
    reap_closed(s);
    method_entry *e = s->methods;
    while (e) {
        method_entry *next = e->next;
        free(e);
        e = next;
    }
    if (s->epoll_fd >= 0) close(s->epoll_fd);
    if (s->listen_fd >= 0) close(s->listen_fd);
    close(s->wake_fd);
    pthread_mutex_destroy(&s->completion_lock);
    free(s);
}

struct rpc_client {
    int fd;
    uint32_t next_id;
    uint32_t last_status;
    bool busy;
};

static int64_t clock_ms(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) < 0) return -1;
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int wait_fd(int fd, short events, int64_t deadline)
{
    for (;;) {
        int64_t remain = deadline - clock_ms();
        if (remain <= 0) return RPC_ERR_TIMEOUT;
        struct pollfd pfd = {.fd = fd, .events = events};
        int rc = poll(&pfd, 1, remain > INT32_MAX ? INT32_MAX : (int)remain);
        if (rc > 0) return (pfd.revents & POLLNVAL) ? RPC_ERR_IO : RPC_OK;
        if (rc == 0) return RPC_ERR_TIMEOUT;
        if (errno != EINTR) return RPC_ERR_IO;
    }
}

rpc_client *rpc_client_connect(const char *host, uint16_t port, int timeout_ms)
{
    if (!host || !port || timeout_ms <= 0) return NULL;
    char service[6];
    snprintf(service, sizeof(service), "%u", port);
    struct addrinfo hints = {.ai_family = AF_INET, .ai_socktype = SOCK_STREAM};
    struct addrinfo *addresses = NULL;
    if (getaddrinfo(host, service, &hints, &addresses) != 0) return NULL;
    int fd = -1;
    int64_t deadline = clock_ms() + timeout_ms;
    for (struct addrinfo *p = addresses; p; p = p->ai_next) {
        int trial = socket(p->ai_family, SOCK_STREAM | SOCK_CLOEXEC, p->ai_protocol);
        if (trial < 0) continue;
        if (nonblocking(trial) == 0) {
            if (connect(trial, p->ai_addr, p->ai_addrlen) == 0) {
                fd = trial;
                break;
            }
            if (errno == EINPROGRESS &&
                wait_fd(trial, POLLOUT, deadline) == RPC_OK) {
                int err = 0;
                socklen_t len = sizeof(err);
                if (getsockopt(trial, SOL_SOCKET, SO_ERROR, &err, &len) == 0 &&
                    err == 0) {
                    fd = trial;
                    break;
                }
            }
        }
        close(trial);
    }
    freeaddrinfo(addresses);
    if (fd < 0) return NULL;
    rpc_client *client = calloc(1, sizeof(*client));
    if (!client) {
        close(fd);
        return NULL;
    }
    client->fd = fd;
    return client;
}

static int transfer(int fd, void *data, size_t len, int sending, int64_t deadline)
{
    uint8_t *p = data;
    while (len) {
        if (clock_ms() >= deadline) return RPC_ERR_TIMEOUT;
        ssize_t n = sending ? send(fd, p, len, MSG_NOSIGNAL) : recv(fd, p, len, 0);
        if (n > 0) {
            p += n;
            len -= (size_t)n;
            continue;
        }
        if (n == 0) return RPC_ERR_CLOSED;
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            int rc = wait_fd(fd, sending ? POLLOUT : POLLIN, deadline);
            if (rc != RPC_OK) return rc;
            continue;
        }
        return RPC_ERR_IO;
    }
    return RPC_OK;
}

/* ---- Synchronous client-side streaming ------------------------------- */

static int client_stream_break(rpc_stream *st, int rc)
{
    if (st->client->fd >= 0) {
        close(st->client->fd);
        st->client->fd = -1;
    }
    return rc;
}

int rpc_stream_open(rpc_client *c, uint32_t method, rpc_stream_mode mode,
                    const void *initial, size_t initial_len,
                    int timeout_ms, rpc_stream **out)
{
    if (!c || !out || !method || timeout_ms <= 0 || c->busy ||
        (initial_len && !initial) ||
        (mode != RPC_STREAM_SERVER && mode != RPC_STREAM_CLIENT))
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
    make_header(header, RPC_STREAM_OPEN, st->id, method, mode,
                (uint32_t)initial_len);
    int64_t deadline = clock_ms() + timeout_ms;
    int rc = transfer(c->fd, header, sizeof(header), 1, deadline);
    if (rc == RPC_OK && initial_len)
        rc = transfer(c->fd, (void *)initial, initial_len, 1, deadline);
    if (rc != RPC_OK) {
        client_stream_break(st, rc);
        free(st);
        return rc;
    }
    c->busy = true;
    *out = st;
    return RPC_OK;
}

static int client_stream_write(rpc_stream *st, const void *data,
                               size_t len, int timeout_ms)
{
    if (st->client->fd < 0) return RPC_ERR_CLOSED;
    if (st->mode != RPC_STREAM_CLIENT || st->client_send_closed ||
        st->client_final_received)
        return RPC_ERR_INVALID;
    uint8_t header[RPC_HEADER_SIZE];
    make_header(header, RPC_STREAM_DATA, st->id, st->method, 0, (uint32_t)len);
    int64_t deadline = clock_ms() + timeout_ms;
    int rc = transfer(st->client->fd, header, sizeof(header), 1, deadline);
    if (rc == RPC_OK && len)
        rc = transfer(st->client->fd, (void *)data, len, 1, deadline);
    return rc == RPC_OK ? RPC_OK : client_stream_break(st, rc);
}

int rpc_stream_finish_send(rpc_stream *st, int timeout_ms)
{
    if (!st || !st->client || st->mode != RPC_STREAM_CLIENT ||
        st->client_send_closed || timeout_ms <= 0)
        return RPC_ERR_INVALID;
    if (st->client->fd < 0) return RPC_ERR_CLOSED;
    uint8_t header[RPC_HEADER_SIZE];
    make_header(header, RPC_STREAM_HALF_CLOSE, st->id, st->method, 0, 0);
    int64_t deadline = clock_ms() + timeout_ms;
    int rc = transfer(st->client->fd, header, sizeof(header), 1, deadline);
    if (rc == RPC_OK) st->client_send_closed = true;
    else client_stream_break(st, rc);
    return rc;
}

static int client_stream_read(rpc_stream *st, rpc_buf *msg, int timeout_ms)
{
    rpc_client *c = st->client;
    msg->len = 0;
    if (c->fd < 0) return RPC_ERR_CLOSED;
    if (st->client_final_received) return st->remote_status == RPC_STATUS_OK ?
                                      RPC_STREAM_EOF : RPC_ERR_REMOTE;
    if (st->mode == RPC_STREAM_CLIENT && !st->client_send_closed)
        return RPC_ERR_INVALID;
    msg->len = 0;
    uint8_t header[RPC_HEADER_SIZE];
    int64_t deadline = clock_ms() + timeout_ms;
    int rc = transfer(c->fd, header, sizeof(header), 0, deadline);
    if (rc != RPC_OK) return client_stream_break(st, rc);
    uint16_t type = get16(header + 6);
    uint32_t len = get32(header + 20);
    if (get32(header) != RPC_MAGIC || get16(header + 4) != RPC_VERSION ||
        get32(header + 8) != st->id || get32(header + 12) != st->method ||
        len > RPC_MAX_PAYLOAD ||
        (type != RPC_STREAM_DATA && type != RPC_STREAM_FINISH) ||
        (type == RPC_STREAM_FINISH && len != 0) ||
        (type == RPC_STREAM_DATA && get32(header + 16) != 0) ||
        (type == RPC_STREAM_DATA && st->mode == RPC_STREAM_CLIENT &&
         st->client_data_count))
        return client_stream_break(st, RPC_ERR_PROTOCOL);
    if (type == RPC_STREAM_FINISH) {
        st->client_final_received = true;
        st->remote_status = get32(header + 16);
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
        rc = transfer(c->fd, msg->data, len, 0, deadline);
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

int rpc_call(rpc_client *c, uint32_t method, const void *request,
             size_t request_len, rpc_buf *response, int timeout_ms)
{
    if (!c || !response || !method || c->busy || timeout_ms <= 0 ||
        (request_len && !request)) return RPC_ERR_INVALID;
    if (request_len > RPC_MAX_PAYLOAD) return RPC_ERR_LIMIT;
    if (c->fd < 0) return RPC_ERR_CLOSED;
    response->len = 0;
    c->last_status = RPC_STATUS_OK;
    uint32_t id = ++c->next_id;
    if (id == 0) id = ++c->next_id;
    uint8_t header[RPC_HEADER_SIZE];
    make_header(header, RPC_REQUEST, id, method, 0, (uint32_t)request_len);
    int64_t deadline = clock_ms() + timeout_ms;
    int rc = transfer(c->fd, header, sizeof(header), 1, deadline);
    if (rc == RPC_OK && request_len)
        rc = transfer(c->fd, (void *)request, request_len, 1, deadline);
    if (rc == RPC_OK) rc = transfer(c->fd, header, sizeof(header), 0, deadline);
    if (rc != RPC_OK) goto broken;
    if (get32(header) != RPC_MAGIC || get16(header + 4) != RPC_VERSION ||
        get16(header + 6) != RPC_RESPONSE || get32(header + 8) != id ||
        get32(header + 12) != method || get32(header + 20) > RPC_MAX_PAYLOAD) {
        rc = RPC_ERR_PROTOCOL;
        goto broken;
    }
    uint32_t len = get32(header + 20);
    c->last_status = get32(header + 16);
    if (len) {
        /* Reserve memory without introducing an additional memcpy. */
        if (response->cap < len) {
            void *p = realloc(response->data, len);
            if (!p) {
                rc = RPC_ERR_NOMEM;
                goto broken;
            }
            response->data = p;
            response->cap = len;
        }
        rc = transfer(c->fd, response->data, len, 0, deadline);
        if (rc != RPC_OK) goto broken;
        response->len = len;
    }
    return c->last_status == RPC_STATUS_OK ? RPC_OK : RPC_ERR_REMOTE;

broken:
    close(c->fd);
    c->fd = -1;
    response->len = 0;
    return rc;
}

uint32_t rpc_client_last_status(const rpc_client *c)
{
    return c ? c->last_status : 0;
}

void rpc_client_close(rpc_client *c)
{
    if (!c) return;
    if (c->fd >= 0) close(c->fd);
    free(c);
}
