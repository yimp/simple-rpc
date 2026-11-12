#define _POSIX_C_SOURCE 200809L
#include "rpc_server.h"
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define METHOD_DOWNLOAD 10u
#define METHOD_UPLOAD 11u
#define CHUNK 65536u
typedef struct job {
    rpc_request *request;
    unsigned method;
    struct job *next;
} job;
typedef struct {
    pthread_t worker;
    pthread_mutex_t lock;
    pthread_cond_t wake;
    job *head, *tail;
    int stopping;
} executor;
static void *run_jobs(void *userdata)
{
    executor *pool = userdata;
    char data[CHUNK];
    memset(data, 'A', sizeof(data));
    for (;;) {
        pthread_mutex_lock(&pool->lock);
        while (!pool->stopping && !pool->head)
            pthread_cond_wait(&pool->wake, &pool->lock);
        if (pool->stopping && !pool->head) {
            pthread_mutex_unlock(&pool->lock);
            return NULL;
        }
        job *task = pool->head;
        pool->head = task->next;
        if (!pool->head) pool->tail = NULL;
        pthread_mutex_unlock(&pool->lock);
        rpc_request *req = task->request;
        uint32_t status = RPC_STATUS_OK;
        uint64_t uploaded = 0;
        size_t response_len = 0;
        if (task->method == METHOD_DOWNLOAD) {
            uint64_t remaining = 0;
            if (rpc_request_size(req) != sizeof(remaining)) {
                status = RPC_STATUS_HANDLER_ERROR;
            } else {
                memcpy(&remaining, rpc_request_data(req), sizeof(remaining));
                while (remaining) {
                    size_t n = remaining > CHUNK ? CHUNK : (size_t)remaining;
                    if (rpc_request_write(req, data, n, 5000) != RPC_OK) {
                        status = RPC_STATUS_HANDLER_ERROR;
                        break;
                    }
                    remaining -= n;
                }
            }
        } else {
            rpc_buf msg = {0};
            int rc;
            while ((rc = rpc_request_read(req, &msg, 5000)) == RPC_OK)
                uploaded += msg.len;
            rpc_buf_free(&msg);
            if (rc != RPC_STREAM_EOF)
                status = RPC_STATUS_HANDLER_ERROR;
            else
                response_len = sizeof(uploaded);
        }
        if (rpc_request_complete(req, status,
                                 response_len ? &uploaded : NULL,
                                 response_len) != RPC_OK)
            abort();
        free(task);
    }
}
static int submit(rpc_request *req, void *userdata, unsigned method)
{
    executor *pool = userdata;
    job *task = calloc(1, sizeof(*task));
    if (!task) return RPC_ERR_NOMEM;
    task->request = req;
    task->method = method;
    pthread_mutex_lock(&pool->lock);
    if (pool->tail) pool->tail->next = task;
    else pool->head = task;
    pool->tail = task;
    pthread_cond_signal(&pool->wake);
    pthread_mutex_unlock(&pool->lock);
    return RPC_PENDING;
}
static int download_handler(rpc_request *req, void *userdata)
{
    return submit(req, userdata, METHOD_DOWNLOAD);
}
static int upload_handler(rpc_request *req, void *userdata)
{
    return submit(req, userdata, METHOD_UPLOAD);
}
int main(void)
{
    executor pool = {0};
    if (pthread_mutex_init(&pool.lock, NULL) != 0 ||
        pthread_cond_init(&pool.wake, NULL) != 0 ||
        pthread_create(&pool.worker, NULL, run_jobs, &pool) != 0)
        return 1;
    rpc_server *server = rpc_server_create();
    const rpc_method_desc download = {
        .method = METHOD_DOWNLOAD, .type = RPC_METHOD_SERVER_STREAM,
        .handler = download_handler, .userdata = &pool
    };
    const rpc_method_desc upload = {
        .method = METHOD_UPLOAD, .type = RPC_METHOD_CLIENT_STREAM,
        .handler = upload_handler, .userdata = &pool
    };
    if (!server || rpc_server_register(server, &download) != RPC_OK ||
        rpc_server_register(server, &upload) != RPC_OK ||
        rpc_server_listen(server, "127.0.0.1", 9002) != RPC_OK)
        return 1;
    puts("streaming server listening on 127.0.0.1:9002");
    fflush(stdout);
    int rc = rpc_server_run(server);
    pthread_mutex_lock(&pool.lock);
    pool.stopping = 1;
    pthread_cond_signal(&pool.wake);
    pthread_mutex_unlock(&pool.lock);
    pthread_join(pool.worker, NULL);
    rpc_server_destroy(server);
    pthread_cond_destroy(&pool.wake);
    pthread_mutex_destroy(&pool.lock);
    return rc == RPC_OK ? 0 : 1;
}
