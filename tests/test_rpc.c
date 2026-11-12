#define _POSIX_C_SOURCE 200809L
#include "rpc.h"

#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct task {
    rpc_request *req;
    int delay_ms;
    struct task *next;
} task;

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    task *head;
    task *tail;
    int stopping;
    pthread_t threads[4];
    atomic_int submitted;
    atomic_int completed;
} worker_pool;

static void delay_ms(int ms)
{
    struct timespec delay = {.tv_sec = ms / 1000,
                             .tv_nsec = (ms % 1000) * 1000000L};
    while (nanosleep(&delay, &delay) != 0) {}
}

static int echo(rpc_request *req, void *user)
{
    (void)user;
    assert(rpc_request_method(req) == 1);
    return rpc_request_set_response(req, rpc_request_data(req),
                                    rpc_request_size(req)) == RPC_OK
               ? RPC_HANDLER_DONE : RPC_ERR_NOMEM;
}

static int fail_handler(rpc_request *req, void *user)
{
    (void)req;
    (void)user;
    return RPC_ERR_IO;
}

static int slow_handler(rpc_request *req, void *user)
{
    (void)req;
    (void)user;
    delay_ms(150);
    return RPC_HANDLER_DONE;
}

static void *worker(void *arg)
{
    worker_pool *p = arg;
    for (;;) {
        pthread_mutex_lock(&p->lock);
        while (!p->head && !p->stopping)
            pthread_cond_wait(&p->cond, &p->lock);
        if (!p->head && p->stopping) {
            pthread_mutex_unlock(&p->lock);
            return NULL;
        }
        task *t = p->head;
        p->head = t->next;
        if (!p->head) p->tail = NULL;
        pthread_mutex_unlock(&p->lock);

        delay_ms(t->delay_ms);
        /* The request and its bytes remain valid even after the Handler
         * returned or the client disconnected. No explicit retain needed. */
        const void *data = rpc_request_data(t->req);
        size_t len = rpc_request_size(t->req);
        assert(rpc_request_complete(t->req, RPC_STATUS_OK, data, len) == RPC_OK);
        /* t->req is invalid after the successful reply. */
        atomic_fetch_add(&p->completed, 1);
        free(t);
    }
}

static void pool_init(worker_pool *p)
{
    memset(p, 0, sizeof(*p));
    assert(pthread_mutex_init(&p->lock, NULL) == 0);
    assert(pthread_cond_init(&p->cond, NULL) == 0);
    atomic_init(&p->submitted, 0);
    atomic_init(&p->completed, 0);
    for (size_t i = 0; i < sizeof(p->threads) / sizeof(p->threads[0]); ++i)
        assert(pthread_create(&p->threads[i], NULL, worker, p) == 0);
}

static void pool_destroy(worker_pool *p)
{
    pthread_mutex_lock(&p->lock);
    p->stopping = 1;
    pthread_cond_broadcast(&p->cond);
    pthread_mutex_unlock(&p->lock);
    for (size_t i = 0; i < sizeof(p->threads) / sizeof(p->threads[0]); ++i)
        pthread_join(p->threads[i], NULL);
    assert(p->head == NULL);
    pthread_cond_destroy(&p->cond);
    pthread_mutex_destroy(&p->lock);
}

static int async_echo(rpc_request *req, void *userdata)
{
    worker_pool *p = userdata;
    task *t = calloc(1, sizeof(*t));
    if (!t) return RPC_ERR_NOMEM;
    t->req = req;
    t->delay_ms = rpc_request_method(req) == 5 ? 250 : 2;
    pthread_mutex_lock(&p->lock);
    if (p->tail) p->tail->next = t;
    else p->head = t;
    p->tail = t;
    atomic_fetch_add(&p->submitted, 1);
    pthread_cond_signal(&p->cond);
    pthread_mutex_unlock(&p->lock);
    return RPC_PENDING;
}

/* Demonstrate a real worker replying *before* the Handler returns PENDING.
 * The short join is only for this race test; normal handlers must not block. */
typedef struct {
    rpc_request *req;
    int rc;
} early_job;

static void *early_worker(void *arg)
{
    early_job *job = arg;
    job->rc = rpc_request_complete(job->req, RPC_STATUS_OK,
                                rpc_request_data(job->req),
                                rpc_request_size(job->req));
    return NULL;
}

static int early_reply(rpc_request *req, void *userdata)
{
    (void)userdata;
    early_job job = {.req = req, .rc = RPC_ERR_IO};
    pthread_t thread;
    if (pthread_create(&thread, NULL, early_worker, &job) != 0)
        return RPC_ERR_IO;
    pthread_join(thread, NULL);
    return job.rc == RPC_OK ? RPC_PENDING : RPC_ERR_IO;
}

static int inline_reply(rpc_request *req, void *userdata)
{
    (void)userdata;
    int rc = rpc_request_complete(req, RPC_STATUS_OK,
                               rpc_request_data(req), rpc_request_size(req));
    return rc == RPC_OK ? RPC_PENDING : rc;
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
    uint32_t method;
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
        assert(rpc_call(c, work->method, message, (size_t)len,
                        &resp, 3000) == RPC_OK);
        assert(resp.len == (size_t)len);
        assert(memcmp(resp.data, message, resp.len) == 0);
    }
    rpc_buf_free(&resp);
    rpc_client_close(c);
    return NULL;
}


static int register_method(rpc_server *server, uint32_t method,
                           rpc_method_type type, rpc_handler handler,
                           void *userdata)
{
    const rpc_method_desc desc = {
        .method = method, .type = type,
        .handler = handler, .userdata = userdata
    };
    return rpc_server_register(server, &desc);
}

int main(void)
{
    rpc_buf scratch = {0};
    assert(rpc_buf_append(&scratch, NULL, 0) == RPC_OK);
    assert(rpc_buf_append(&scratch, NULL, 1) == RPC_ERR_INVALID);
    assert(rpc_buf_append(&scratch, "x", RPC_MAX_PAYLOAD + 1u) == RPC_ERR_LIMIT);
    rpc_buf_free(&scratch);

    worker_pool pool;
    pool_init(&pool);
    rpc_server *s = rpc_server_create();
    assert(s);
    assert(register_method(s, 0, RPC_METHOD_UNARY, echo, NULL) == RPC_ERR_INVALID);
    assert(register_method(s, 1, RPC_METHOD_UNARY, echo, NULL) == RPC_OK);
    assert(register_method(s, 1, RPC_METHOD_UNARY, echo, NULL) == RPC_ERR_INVALID);
    assert(register_method(s, 2, RPC_METHOD_UNARY, fail_handler, NULL) == RPC_OK);
    assert(register_method(s, 3, RPC_METHOD_UNARY, slow_handler, NULL) == RPC_OK);
    assert(register_method(s, 4, RPC_METHOD_UNARY, async_echo, &pool) == RPC_OK);
    assert(register_method(s, 5, RPC_METHOD_UNARY, async_echo, &pool) == RPC_OK);
    assert(register_method(s, 6, RPC_METHOD_UNARY, early_reply, NULL) == RPC_OK);
    assert(register_method(s, 7, RPC_METHOD_UNARY, inline_reply, NULL) == RPC_OK);
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
    assert(rpc_call(c, 4, large, RPC_MAX_PAYLOAD, &resp, 3000) == RPC_OK);
    assert(resp.len == RPC_MAX_PAYLOAD);
    assert(memcmp(resp.data, large, RPC_MAX_PAYLOAD) == 0);
    assert(rpc_call(c, 1, large, RPC_MAX_PAYLOAD + 1u,
                    &resp, 3000) == RPC_ERR_LIMIT);
    assert(rpc_call(c, 1, "still-open", 10, &resp, 3000) == RPC_OK);
    free(large);

    assert(rpc_call(c, 4, "async", 5, &resp, 3000) == RPC_OK);
    assert(resp.len == 5 && memcmp(resp.data, "async", 5) == 0);
    assert(rpc_call(c, 6, "raced", 5, &resp, 3000) == RPC_OK);
    assert(resp.len == 5 && memcmp(resp.data, "raced", 5) == 0);
    assert(rpc_call(c, 7, "inline", 6, &resp, 3000) == RPC_OK);
    assert(resp.len == 6 && memcmp(resp.data, "inline", 6) == 0);
    /* Reuse the same TCP connection after async and sync calls. */
    assert(rpc_call(c, 1, "again", 5, &resp, 3000) == RPC_OK);
    assert(resp.len == 5);
    rpc_buf_free(&resp);
    rpc_client_close(c);

    enum { CLIENTS = 6 };
    pthread_t threads[CLIENTS];
    client_work work = {.port = port, .loops = 50, .method = 1};
    for (int i = 0; i < CLIENTS; ++i)
        assert(pthread_create(&threads[i], NULL, concurrent_client, &work) == 0);
    for (int i = 0; i < CLIENTS; ++i) pthread_join(threads[i], NULL);
    work = (client_work){.port = port, .loops = 25, .method = 4};
    for (int i = 0; i < CLIENTS; ++i)
        assert(pthread_create(&threads[i], NULL, concurrent_client, &work) == 0);
    for (int i = 0; i < CLIENTS; ++i) pthread_join(threads[i], NULL);

    /* A timeout and peer disconnect must NOT destroy an in-flight request. */
    c = rpc_client_connect("127.0.0.1", port, 3000);
    assert(c);
    assert(rpc_call(c, 3, NULL, 0, &resp, 10) == RPC_ERR_TIMEOUT);
    assert(rpc_call(c, 1, NULL, 0, &resp, 3000) == RPC_ERR_CLOSED);
    rpc_buf_free(&resp);
    rpc_client_close(c);

    delay_ms(250); /* Let the previous intentionally blocking handler exit. */
    c = rpc_client_connect("127.0.0.1", port, 3000);
    assert(c);
    int submitted = atomic_load(&pool.submitted);
    assert(rpc_call(c, 5, "abandoned", 9, &resp, 10) == RPC_ERR_TIMEOUT);
    rpc_client_close(c);
    /* The Reactor may still be finishing the earlier blocking test. */
    for (int i = 0; i < 1000 &&
         atomic_load(&pool.submitted) < submitted + 1; ++i)
        delay_ms(1);
    assert(atomic_load(&pool.submitted) == submitted + 1);

    /* stop() waits for the pending asynchronous reply, even after disconnect. */
    int completed = atomic_load(&pool.completed);
    rpc_server_stop(s);
    pthread_join(server_thread, NULL);
    assert(atomic_load(&pool.completed) >= completed + 1);
    pool_destroy(&pool);
    rpc_server_destroy(s);
    puts("all RPC integration tests passed");
    return 0;
}
