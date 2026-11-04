#define _POSIX_C_SOURCE 200809L
#include "rpc.h"

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int echo(const rpc_buf *req, rpc_buf *resp, void *user)
{
    (void)user;
    return rpc_buf_append(resp, req->data, req->len);
}

static int fail_handler(const rpc_buf *req, rpc_buf *resp, void *user)
{
    (void)req;
    (void)resp;
    (void)user;
    return -1;
}

static int slow_handler(const rpc_buf *req, rpc_buf *resp, void *user)
{
    (void)req;
    (void)resp;
    (void)user;
    struct timespec delay = {.tv_sec = 0, .tv_nsec = 150000000};
    nanosleep(&delay, NULL);
    return 0;
}

static void *serve(void *p)
{
    int rc = rpc_server_run(p);
    assert(rc == RPC_OK);
    return NULL;
}

typedef struct {
    uint16_t port;
    int loops;
} client_work;

static void *concurrent_client(void *arg)
{
    client_work *work = arg;
    rpc_client *c = rpc_client_connect("127.0.0.1", work->port, 3000);
    assert(c);
    rpc_buf resp = {0};
    for (int i = 0; i < work->loops; ++i) {
        char message[40];
        int len = snprintf(message, sizeof(message), "request-%d", i);
        assert(rpc_call(c, 1, message, (size_t)len, &resp, 3000) == RPC_OK);
        assert(resp.len == (size_t)len);
        assert(memcmp(resp.data, message, resp.len) == 0);
    }
    rpc_buf_free(&resp);
    rpc_client_close(c);
    return NULL;
}

int main(void)
{
    rpc_buf scratch = {0};
    assert(rpc_buf_append(&scratch, NULL, 0) == RPC_OK);
    assert(rpc_buf_append(&scratch, NULL, 1) == RPC_ERR_INVALID);
    assert(rpc_buf_append(&scratch, "x", RPC_MAX_PAYLOAD + 1u) == RPC_ERR_LIMIT);
    rpc_buf_free(&scratch);

    rpc_server *s = rpc_server_create();
    assert(s);
    assert(rpc_server_register(s, 0, echo, NULL) == RPC_ERR_INVALID);
    assert(rpc_server_register(s, 1, echo, NULL) == RPC_OK);
    assert(rpc_server_register(s, 1, echo, NULL) == RPC_ERR_INVALID);
    assert(rpc_server_register(s, 2, fail_handler, NULL) == RPC_OK);
    assert(rpc_server_register(s, 3, slow_handler, NULL) == RPC_OK);
    assert(rpc_server_listen(s, "127.0.0.1", 0) == RPC_OK);
    uint16_t port = rpc_server_port(s);
    assert(port != 0);
    pthread_t server_thread;
    assert(pthread_create(&server_thread, NULL, serve, s) == 0);

    rpc_client *c = rpc_client_connect("127.0.0.1", port, 3000);
    assert(c);
    rpc_buf resp = {0};
    assert(rpc_call(c, 1, NULL, 0, &resp, 3000) == RPC_OK);
    assert(resp.len == 0);
    assert(rpc_call(c, 1, "abc", 3, &resp, 3000) == RPC_OK);
    assert(resp.len == 3 && memcmp(resp.data, "abc", 3) == 0);
    assert(rpc_call(c, 999, NULL, 0, &resp, 3000) == RPC_ERR_REMOTE);
    assert(rpc_client_last_status(c) == RPC_STATUS_NO_METHOD);
    assert(rpc_call(c, 2, NULL, 0, &resp, 3000) == RPC_ERR_REMOTE);
    assert(rpc_client_last_status(c) == RPC_STATUS_HANDLER_ERROR);
    assert(rpc_call(c, 1, "after-errors", 12, &resp, 3000) == RPC_OK);
    assert(resp.len == 12);

    char *large = malloc(RPC_MAX_PAYLOAD);
    assert(large);
    for (size_t i = 0; i < RPC_MAX_PAYLOAD; ++i)
        large[i] = (char)(i % 251);
    assert(rpc_call(c, 1, large, RPC_MAX_PAYLOAD, &resp, 3000) == RPC_OK);
    assert(resp.len == RPC_MAX_PAYLOAD);
    assert(memcmp(resp.data, large, RPC_MAX_PAYLOAD) == 0);
    assert(rpc_call(c, 1, large, RPC_MAX_PAYLOAD + 1u, &resp, 3000) == RPC_ERR_LIMIT);
    assert(rpc_call(c, 1, "still-open", 10, &resp, 3000) == RPC_OK);
    free(large);
    rpc_buf_free(&resp);
    rpc_client_close(c);

    enum { CLIENTS = 6 };
    pthread_t threads[CLIENTS];
    client_work work = {.port = port, .loops = 50};
    for (int i = 0; i < CLIENTS; ++i)
        assert(pthread_create(&threads[i], NULL, concurrent_client, &work) == 0);
    for (int i = 0; i < CLIENTS; ++i) pthread_join(threads[i], NULL);

    c = rpc_client_connect("127.0.0.1", port, 3000);
    assert(c);
    assert(rpc_call(c, 3, NULL, 0, &resp, 10) == RPC_ERR_TIMEOUT);
    assert(rpc_call(c, 1, NULL, 0, &resp, 3000) == RPC_ERR_CLOSED);
    rpc_buf_free(&resp);
    rpc_client_close(c);

    rpc_server_stop(s);
    pthread_join(server_thread, NULL);
    rpc_server_destroy(s);
    puts("all RPC integration tests passed");
    return 0;
}
