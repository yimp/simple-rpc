#include "rpc.h"
#include <stdio.h>

static int echo(const rpc_buf *req, rpc_buf *resp, void *user)
{
    (void)user;
    return rpc_buf_append(resp, req->data, req->len);
}

int main(void)
{
    rpc_server *server = rpc_server_create();
    if (!server || rpc_server_register(server, 1, echo, NULL) != RPC_OK ||
        rpc_server_listen(server, "127.0.0.1", 9000) != RPC_OK) {
        fprintf(stderr, "failed to initialize server\n");
        rpc_server_destroy(server);
        return 1;
    }
    printf("echo server listening on 127.0.0.1:%u\n", rpc_server_port(server));
    fflush(stdout);
    int rc = rpc_server_run(server);
    rpc_server_destroy(server);
    return rc == RPC_OK ? 0 : 1;
}
