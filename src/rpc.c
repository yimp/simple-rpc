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
    free(c->payload);
    free(c->out);
    c->next = s->closed_connections;
    s->closed_connections = c;
}

static void reap_closed(rpc_server *s)
{
    while (s->closed_connections) {
        rpc_conn *c = s->closed_connections;
        s->closed_connections = c->next;
        free(c);
    }
}

static int conn_events(rpc_conn *c, uint32_t flags)
{
    struct epoll_event ev = {.events = flags | EPOLLRDHUP, .data.ptr = c};
    return epoll_ctl(c->server->epoll_fd, EPOLL_CTL_MOD, c->fd, &ev);
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
    int result = entry ? entry->fn(req, entry->userdata) : RPC_DONE;
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
            if (get32(h) != RPC_MAGIC || get16(h + 4) != RPC_VERSION ||
                get16(h + 6) != RPC_REQUEST || get32(h + 8) == 0 ||
                get32(h + 12) == 0 || get32(h + 16) != 0)
                return -1;
            c->request_id = get32(h + 8);
            c->method = get32(h + 12);
            c->payload_len = get32(h + 20);
            if (c->payload_len > RPC_MAX_PAYLOAD) return -1;
            if (c->payload_len) {
                c->payload = malloc(c->payload_len);
                if (!c->payload) return -1;
            } else {
                return dispatch(c);
            }
        }
        ssize_t n = recv(c->fd, c->payload + c->payload_read,
                         c->payload_len - c->payload_read, 0);
        if (n > 0) c->payload_read += (size_t)n;
        else if (n == 0) return -1;
        else if (errno == EINTR) continue;
        else return errno == EAGAIN || errno == EWOULDBLOCK ? 0 : -1;
        if (c->payload_read == c->payload_len) return dispatch(c);
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
                else if (c->out && (flags & EPOLLOUT)) rc = conn_write(c);
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

int rpc_call(rpc_client *c, uint32_t method, const void *request,
             size_t request_len, rpc_buf *response, int timeout_ms)
{
    if (!c || !response || !method || timeout_ms <= 0 ||
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
