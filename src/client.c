#define _POSIX_C_SOURCE 200809L
#include "rpc_internal.h"

#include <errno.h>
#include <netdb.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

/* Synchronous client connection and Unary calls. */

rpc_client *rpc_client_connect(const char *host, uint16_t port, int timeout_ms)
{
    if (!host || !port || timeout_ms <= 0) return NULL;
    char service[6];
    snprintf(service, sizeof(service), "%u", port);
    struct addrinfo hints = {.ai_family = AF_INET, .ai_socktype = SOCK_STREAM};
    struct addrinfo *addresses = NULL;
    if (getaddrinfo(host, service, &hints, &addresses) != 0) return NULL;
    int fd = -1;
    int64_t deadline = srpc_clock_ms() + timeout_ms;
    for (struct addrinfo *p = addresses; p; p = p->ai_next) {
        int trial = socket(p->ai_family, SOCK_STREAM | SOCK_CLOEXEC, p->ai_protocol);
        if (trial < 0) continue;
        if (srpc_nonblocking(trial) == 0) {
            if (connect(trial, p->ai_addr, p->ai_addrlen) == 0) {
                fd = trial;
                break;
            }
            if (errno == EINPROGRESS &&
                srpc_wait_fd(trial, POLLOUT, deadline) == RPC_OK) {
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
    srpc_make_header(header, RPC_REQUEST, id, method, 0, (uint32_t)request_len);
    int64_t deadline = srpc_clock_ms() + timeout_ms;
    int rc = srpc_transfer(c->fd, header, sizeof(header), 1, deadline);
    if (rc == RPC_OK && request_len)
        rc = srpc_transfer(c->fd, (void *)request, request_len, 1, deadline);
    if (rc == RPC_OK) rc = srpc_transfer(c->fd, header, sizeof(header), 0, deadline);
    if (rc != RPC_OK) goto broken;
    if (srpc_get32(header) != RPC_MAGIC || srpc_get16(header + 4) != RPC_VERSION ||
        srpc_get16(header + 6) != RPC_RESPONSE || srpc_get32(header + 8) != id ||
        srpc_get32(header + 12) != method || srpc_get32(header + 20) > RPC_MAX_PAYLOAD) {
        rc = RPC_ERR_PROTOCOL;
        goto broken;
    }
    uint32_t len = srpc_get32(header + 20);
    c->last_status = srpc_get32(header + 16);
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
        rc = srpc_transfer(c->fd, response->data, len, 0, deadline);
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

