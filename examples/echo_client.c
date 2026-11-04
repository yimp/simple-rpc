#include "rpc.h"
#include <stdio.h>

int main(void)
{
    rpc_client *client = rpc_client_connect("127.0.0.1", 9000, 3000);
    if (!client) {
        fprintf(stderr, "connect failed\n");
        return 1;
    }
    rpc_buf response = {0};
    int rc = rpc_call(client, 1, "hello RPC", 9, &response, 3000);
    if (rc == RPC_OK)
        printf("response: %.*s\n", (int)response.len, (char *)response.data);
    else
        fprintf(stderr, "RPC failed: %s (remote status %u)\n",
                rpc_strerror(rc), rpc_client_last_status(client));
    rpc_buf_free(&response);
    rpc_client_close(client);
    return rc == RPC_OK ? 0 : 1;
}
