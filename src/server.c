#define _POSIX_C_SOURCE 200809L
#include "rpc_internal.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

/* Server setup, method registry, Reactor and shutdown. */

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

int rpc_server_register(rpc_server *s, const rpc_method_desc *desc)
{
    if (!s || !desc || !desc->method || !desc->handler ||
        atomic_load(&s->running))
        return RPC_ERR_INVALID;
    if (desc->type != RPC_METHOD_UNARY &&
        desc->type != RPC_METHOD_SERVER_STREAM &&
        desc->type != RPC_METHOD_CLIENT_STREAM)
        return RPC_ERR_INVALID;
    for (method_entry *e = s->methods; e; e = e->next)
        if (e->method == desc->method) return RPC_ERR_INVALID;
    method_entry *e = malloc(sizeof(*e));
    if (!e) return RPC_ERR_NOMEM;
    *e = (method_entry){.method=desc->method, .type=desc->type,
                        .fn=desc->handler, .userdata=desc->userdata,
                        .next=s->methods};
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
        srpc_nonblocking(fd) < 0 ||
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

src/server.cstatic int accept_clients(rpc_server *s)
{
    for (;;) {
        int fd = accept(s->listen_fd, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR) continue;
            return errno == EAGAIN || errno == EWOULDBLOCK ? 0 : -1;
        }
        if (srpc_nonblocking(fd) < 0 || fcntl(fd, F_SETFD, FD_CLOEXEC) < 0) {
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
            while (s->connections) srpc_close_conn(s, s->connections);
        }
        srpc_drain_completions(s, !shutting_down);
        for (rpc_conn *c = s->connections; c;) {
            rpc_conn *next = c->next;
            if (c->stream && srpc_server_stream_refresh(c) < 0) srpc_close_conn(s, c);
            c = next;
        }
        srpc_reap_closed(s);
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
                srpc_drain_completions(s, 1);
            } else {
                rpc_conn *c = events[i].data.ptr;
                if (c->closed) continue; /* Stale event in current batch. */
                uint32_t flags = events[i].events;
                int rc = 0;
                if (flags & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) rc = -1;
                else if (c->stream) {
                    if (flags & EPOLLOUT) rc = srpc_stream_conn_write(c);
                    if (rc == 0 && c->stream && (flags & EPOLLIN))
                        rc = srpc_conn_read(c);
                } else if (c->out && (flags & EPOLLOUT)) rc = srpc_conn_write(c);
                else if (!c->out && !c->active && (flags & EPOLLIN))
                    rc = srpc_conn_read(c);
                if (rc < 0) srpc_close_conn(s, c);
            }
            if (result != RPC_OK) break;
        }
        srpc_reap_closed(s);
    }
    read_wakeup(s);
    srpc_drain_completions(s, 0);
    srpc_reap_closed(s);
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
    while (s->connections) srpc_close_conn(s, s->connections);
    srpc_reap_closed(s);
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

