#ifndef SIMPLE_RPC_INTERNAL_H
#define SIMPLE_RPC_INTERNAL_H
#include "rpc.h"
#include <pthread.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#define RPC_MAGIC 0x53525043u
#define RPC_VERSION 1u
#define RPC_REQUEST 1u
#define RPC_RESPONSE 2u
#define RPC_HEADER_SIZE 24u
#define RPC_MAX_EVENTS 64
typedef struct method_entry {
    uint32_t method;
    rpc_method_type type;
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
    rpc_stream *stream; /* Reactor alias of active->stream. */
    uint8_t *out;
    size_t out_len;
    size_t out_written;
} rpc_conn;
#define RPC_STREAM_OPEN 3u
#define RPC_STREAM_DATA 4u
#define RPC_STREAM_HALF_CLOSE 5u
#define RPC_STREAM_FINISH 6u
#define RPC_STREAM_QUEUE_MESSAGES 2u
#define RPC_STREAM_QUEUE_BYTES (2u * (RPC_MAX_PAYLOAD + RPC_HEADER_SIZE))
typedef struct stream_frame {
    uint8_t *bytes;
    size_t len;
    struct stream_frame *next;
} stream_frame;
/* Client handle or server-side stream state embedded in rpc_request. */
struct rpc_stream {
    rpc_server *server;
    rpc_client *client;
    rpc_conn *conn;
    rpc_request *owner;
    uint32_t id, method;
    rpc_method_type mode;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    stream_frame *in_head, *in_tail, *out_head, *out_tail;
    size_t in_count, in_bytes, out_count, out_bytes;
    bool remote_closed, local_finished, failed, handler_returned;
    bool client_send_closed, client_final_received;
    unsigned client_data_count;
    uint32_t remote_status;
};
struct rpc_request {
    rpc_server *server;
    rpc_conn *conn;
    uint32_t request_id, method;
    rpc_method_type type;
    uint8_t *data;
    size_t size;
    rpc_buf response;
    atomic_bool completed;
    uint8_t *reply_frame;
    size_t reply_len;
    struct rpc_request *completed_next;
    bool has_stream;
    rpc_stream stream; /* The Request owns all server-side stream state. */
};
struct rpc_server {
    int listen_fd, epoll_fd, wake_fd;
    uint16_t port;
    method_entry *methods;
    rpc_conn *connections, *closed_connections;
    pthread_mutex_t completion_lock;
    rpc_request *completed_head, *completed_tail;
    size_t pending_count;
    atomic_bool stopping, running;
};
struct rpc_client {
    int fd;
    uint32_t next_id, last_status;
    bool busy;
};
uint16_t srpc_get16(const uint8_t *p);
uint32_t srpc_get32(const uint8_t *p);
void srpc_make_header(uint8_t *p, uint16_t type, uint32_t id,
                      uint32_t method, uint32_t status, uint32_t len);
stream_frame *srpc_frame_new(uint16_t type, uint32_t id, uint32_t method,
                             uint32_t status, const void *data, size_t len);
void srpc_frame_free(stream_frame *f);
void srpc_frame_list_free(stream_frame *f);
uint8_t *srpc_encode_reply(uint32_t request_id, uint32_t method,
                           uint32_t status, const void *data, size_t len);
int srpc_nonblocking(int fd);
int64_t srpc_clock_ms(void);
int srpc_wait_fd(int fd, short events, int64_t deadline);
int srpc_transfer(int fd, void *data, size_t len, int sending, int64_t deadline);
void srpc_free_request(rpc_request *req);
int srpc_dispatch(rpc_conn *c);
void srpc_drain_completions(rpc_server *s, int deliver);
void srpc_close_conn(rpc_server *s, rpc_conn *c);
void srpc_reap_closed(rpc_server *s);
int srpc_conn_events(rpc_conn *c, uint32_t flags);
int srpc_send_response(rpc_conn *c, uint8_t *frame, size_t len);
int srpc_conn_read(rpc_conn *c);
int srpc_conn_write(rpc_conn *c);
void srpc_stream_cleanup(rpc_stream *st);
void srpc_stream_fail(rpc_stream *st);
int srpc_stream_dispatch_open(rpc_conn *c);
int srpc_stream_consume_frame(rpc_conn *c, uint16_t type);
int srpc_server_stream_refresh(rpc_conn *c);
int srpc_stream_conn_write(rpc_conn *c);
int srpc_request_stream_read(rpc_request *req, rpc_buf *message, int timeout_ms);
int srpc_request_stream_write(rpc_request *req, const void *data,
                              size_t len, int timeout_ms);
int srpc_request_stream_complete(rpc_request *req, uint32_t status,
                                 const void *response, size_t response_len);
int srpc_client_stream_write(rpc_stream *st, const void *data,
                             size_t len, int timeout_ms);
int srpc_client_stream_read(rpc_stream *st, rpc_buf *msg, int timeout_ms);
#endif
