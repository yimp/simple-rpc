#include "rpc.h"
#include <stdio.h>

static int echo(rpc_request *req, void *user)
{
    (void)user;
    return rpc_request_set_response(req, rpc_request_data(req),
                                    rpc_request_size(req)) == RPC_OK
               ? RPC_DONE : RPC_ERR_NOMEM;
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
