#include "rpc.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define METHOD_DOWNLOAD 10u
#define METHOD_UPLOAD 11u

int main(void)
{
    rpc_client *c = rpc_client_connect("127.0.0.1", 9002, 5000);
    if (!c) { perror("connect"); return 1; }
    rpc_stream *s = NULL;
    uint64_t want = 3u * 1024u * 1024u;
    if (rpc_stream_open(c, METHOD_DOWNLOAD, RPC_STREAM_SERVER,
                        &want, sizeof(want), 5000, &s) != RPC_OK)
        return 1;
    rpc_buf msg = {0};
    uint64_t received = 0;
    int rc;
    while ((rc = rpc_stream_read(s, &msg, 5000)) == RPC_OK) {
        for (size_t i = 0; i < msg.len; ++i)
            if (((char *)msg.data)[i] != 'A') abort();
        received += msg.len;
    }
    if (rc != RPC_STREAM_EOF || received != want) return 1;
    rpc_buf_free(&msg);
    rpc_stream_destroy(s);
    printf("download: %llu bytes\n", (unsigned long long)received);

    if (rpc_stream_open(c, METHOD_UPLOAD, RPC_STREAM_CLIENT,
                        NULL, 0, 5000, &s) != RPC_OK)
        return 1;
    char chunk[65536];
    memset(chunk, 'B', sizeof(chunk));
    for (size_t i = 0; i < want / sizeof(chunk); ++i)
        if (rpc_stream_write(s, chunk, sizeof(chunk), 5000) != RPC_OK)
            return 1;
    if (rpc_stream_finish_send(s, 5000) != RPC_OK) return 1;
    if (rpc_stream_read(s, &msg, 5000) != RPC_OK ||
        msg.len != sizeof(uint64_t)) return 1;
    memcpy(&received, msg.data, sizeof(received));
    if (rpc_stream_read(s, &msg, 5000) != RPC_STREAM_EOF || received != want)
        return 1;
    printf("upload: %llu bytes\n", (unsigned long long)received);
    rpc_buf_free(&msg);
    rpc_stream_destroy(s);
    rpc_client_close(c);
    return 0;
}
