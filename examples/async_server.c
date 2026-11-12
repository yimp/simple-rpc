#define _POSIX_C_SOURCE 200809L
#include "rpc_server.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct job {
    rpc_request *req;
    struct job *next;
} job;

typedef struct {
    pthread_t thread;
    pthread_mutex_t lock;
    pthread_cond_t available;
    job *head;
    job *tail;
    int stop;
} executor;

static void *work(void *context)
{
    executor *pool = context;
    for (;;) {
        pthread_mutex_lock(&pool->lock);
        while (!pool->stop && !pool->head)
            pthread_cond_wait(&pool->available, &pool->lock);
        if (pool->stop && !pool->head) {
            pthread_mutex_unlock(&pool->lock);
            return NULL;
        }
        job *item = pool->head;
        pool->head = item->next;
        if (!pool->head) pool->tail = NULL;
        pthread_mutex_unlock(&pool->lock);

        struct timespec delay = {.tv_sec = 0, .tv_nsec = 20000000};
        nanosleep(&delay, NULL); /* Simulated blocking business operation. */
        int rc = rpc_request_complete(item->req, RPC_STATUS_OK,
                                   rpc_request_data(item->req),
                                   rpc_request_size(item->req));
        if (rc != RPC_OK) {
            /* A real application must handle transient allocation failure
             * and still complete the request; never silently abandon it. */
            fprintf(stderr, "failed to complete RPC: %s\n", rpc_strerror(rc));
            abort();
        }
        /* item->req is no longer valid. */
        free(item);
    }
}

static int async_echo(rpc_request *req, void *userdata)
{
    executor *pool = userdata;
    job *item = calloc(1, sizeof(*item));
    if (!item) return RPC_ERR_NOMEM;
    item->req = req;
    pthread_mutex_lock(&pool->lock);
    if (pool->tail) pool->tail->next = item;
    else pool->head = item;
    pool->tail = item;
    pthread_cond_signal(&pool->available);
    pthread_mutex_unlock(&pool->lock);
    return RPC_PENDING;
}

int main(void)
{
    executor pool = {0};
    if (pthread_mutex_init(&pool.lock, NULL) != 0 ||
        pthread_cond_init(&pool.available, NULL) != 0 ||
        pthread_create(&pool.thread, NULL, work, &pool) != 0) {
        fprintf(stderr, "failed to create executor\n");
        return 1;
    }

    rpc_server *server = rpc_server_create();
    const rpc_method_desc method = {
        .method = 1, .type = RPC_METHOD_UNARY,
        .handler = async_echo, .userdata = &pool
    };
    if (!server || rpc_server_register(server, &method) != RPC_OK ||
        rpc_server_listen(server, "127.0.0.1", 9001) != RPC_OK) {
        fprintf(stderr, "failed to initialize server\n");
        rpc_server_destroy(server);
        return 1;
    }
    printf("async server listening on 127.0.0.1:9001\n");
    fflush(stdout);
    int rc = rpc_server_run(server);
    /* rpc_server_run() does not return until all pending requests completed. */
    pthread_mutex_lock(&pool.lock);
    pool.stop = 1;
    pthread_cond_signal(&pool.available);
    pthread_mutex_unlock(&pool.lock);
    pthread_join(pool.thread, NULL);
    pthread_cond_destroy(&pool.available);
    pthread_mutex_destroy(&pool.lock);
    rpc_server_destroy(server);
    return rc == RPC_OK ? 0 : 1;
}
