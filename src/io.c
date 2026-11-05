#define _POSIX_C_SOURCE 200809L
#include "rpc_internal.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* Shared POSIX socket / deadline helpers. */

int srpc_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    return flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0 ? -1 : 0;
}

int64_t srpc_clock_ms(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) < 0) return -1;
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

int srpc_wait_fd(int fd, short events, int64_t deadline)
{
    for (;;) {
        int64_t remain = deadline - srpc_clock_ms();
        if (remain <= 0) return RPC_ERR_TIMEOUT;
        struct pollfd pfd = {.fd = fd, .events = events};
        int rc = poll(&pfd, 1, remain > INT32_MAX ? INT32_MAX : (int)remain);
        if (rc > 0) return (pfd.revents & POLLNVAL) ? RPC_ERR_IO : RPC_OK;
        if (rc == 0) return RPC_ERR_TIMEOUT;
        if (errno != EINTR) return RPC_ERR_IO;
    }
}

int srpc_transfer(int fd, void *data, size_t len, int sending, int64_t deadline)
{
    uint8_t *p = data;
    while (len) {
        if (srpc_clock_ms() >= deadline) return RPC_ERR_TIMEOUT;
        ssize_t n = sending ? send(fd, p, len, MSG_NOSIGNAL) : recv(fd, p, len, 0);
        if (n > 0) {
            p += n;
            len -= (size_t)n;
            continue;
        }
        if (n == 0) return RPC_ERR_CLOSED;
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            int rc = srpc_wait_fd(fd, sending ? POLLOUT : POLLIN, deadline);
            if (rc != RPC_OK) return rc;
            continue;
        }
        return RPC_ERR_IO;
    }
    return RPC_OK;
}

