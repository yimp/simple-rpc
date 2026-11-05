#define _POSIX_C_SOURCE 200809L
#include "rpc_internal.h"

#include <stdlib.h>
#include <string.h>

/* Wire format, frame allocation, and encoding. */

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

uint16_t srpc_get16(const uint8_t *p)
{
    return (uint16_t)((uint16_t)p[0] << 8 | p[1]);
}

uint32_t srpc_get32(const uint8_t *p)
{
    return (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 |
           (uint32_t)p[2] << 8 | p[3];
}

void srpc_make_header(uint8_t *p, uint16_t type, uint32_t id,
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

stream_frame *srpc_frame_new(uint16_t type, uint32_t id, uint32_t method,
                               uint32_t status, const void *data, size_t len)
{
    stream_frame *f = calloc(1, sizeof(*f));
    if (!f) return NULL;
    f->len = RPC_HEADER_SIZE + len;
    f->bytes = malloc(f->len);
    if (!f->bytes) { free(f); return NULL; }
    srpc_make_header(f->bytes, type, id, method, status, (uint32_t)len);
    if (len) memcpy(f->bytes + RPC_HEADER_SIZE, data, len);
    return f;
}

void srpc_frame_free(stream_frame *f)
{
    if (!f) return;
    free(f->bytes);
    free(f);
}

void srpc_frame_list_free(stream_frame *f)
{
    while (f) {
        stream_frame *next = f->next;
        srpc_frame_free(f);
        f = next;
    }
}

uint8_t *srpc_encode_reply(uint32_t request_id, uint32_t method,
                             uint32_t status, const void *data, size_t len)
{
    uint8_t *frame = malloc(RPC_HEADER_SIZE + len);
    if (!frame) return NULL;
    srpc_make_header(frame, RPC_RESPONSE, request_id, method, status, (uint32_t)len);
    if (len) memcpy(frame + RPC_HEADER_SIZE, data, len);
    return frame;
}

