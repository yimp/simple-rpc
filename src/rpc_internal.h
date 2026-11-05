#ifndef SIMPLE_RPC_INTERNAL_H
#define SIMPLE_RPC_INTERNAL_H

/* Private interfaces shared by the implementation. Applications must include
 * only <rpc.h>; structure layout and these helpers are not public API. */
#include "rpc.h"
#include <pthread.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#define RPC_MAGIC 0x53525043u /* "SRPC" */
#define RPC_VERSION 1u
#define RPC_REQUEST 1u
#define RPC_RESPONSE 2u
#define RPC_HEADER_SIZE 24u
#define RPC_MAX_EVENTS 64

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


struct rpc_client {
    int fd;
    uint32_t next_id;
    uint32_t last_status;
    bool busy;
};


/* Internal helpers are prefixed srpc_ to avoid collisions when the static
 * library is linked with an application's own unprefixed C symbols. */

/* protocol.c: wire codec and owned frame allocation. */
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

/* io.c: shared socket helpers, including deadline-bound blocking APIs. */
int srpc_nonblocking(int fd);
int64_t srpc_clock_ms(void);
int srpc_wait_fd(int fd, short events, int64_t deadline);
int srpc_transfer(int fd, void *data, size_t len, int sending, int64_t deadline);

/* request.c: Unary requests and the completion queue. */
void srpc_free_request(rpc_request *req);
int srpc_dispatch(rpc_conn *c);
void srpc_drain_completions(rpc_server *s, int deliver);

/* connection.c: Reactor-owned connections and socket IO. */
void srpc_close_conn(rpc_server *s, rpc_conn *c);
void srpc_reap_closed(rpc_server *s);
int srpc_conn_events(rpc_conn *c, uint32_t flags);
int srpc_send_response(rpc_conn *c, uint8_t *frame, size_t len);
int srpc_conn_read(rpc_conn *c);
int srpc_conn_write(rpc_conn *c);

/* server_stream.c: server-side streaming and backpressure. */
void srpc_stream_free(rpc_stream *st);
void srpc_stream_fail(rpc_stream *st);
int srpc_stream_dispatch_open(rpc_conn *c);
int srpc_stream_consume_frame(rpc_conn *c, uint16_t type);
int srpc_server_stream_refresh(rpc_conn *c);
int srpc_stream_conn_write(rpc_conn *c);

/* client_stream.c: stream operations on the synchronous client. */
int srpc_client_stream_write(rpc_stream *st, const void *data,
                               size_t len, int timeout_ms);
int srpc_client_stream_read(rpc_stream *st, rpc_buf *msg, int timeout_ms);

#endif /* SIMPLE_RPC_INTERNAL_H */
