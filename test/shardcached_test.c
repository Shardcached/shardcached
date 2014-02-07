#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <signal.h>
#include <sys/wait.h>
#include <ut.h>
#include "http-client-c.h"

static pid_t start_node(char * const argv[])
{
    pid_t pid = fork();
    if (pid) {
        return pid;
    }
    execve("./shardcached", argv, NULL);
    return 0;
}

static pid_t stop_node(pid_t pid)
{
    kill(pid, 2);         
    return waitpid(pid, NULL, 0);
}

int main(int argc, char **argv)
{
    ut_init("shardcached_test");
    pid_t children[2];
    int i;
    for (i = 0; i < 2; i++) {
        char me[32];
        sprintf(me, "-mpeer%d", i+1);

        char listen[32];
        sprintf(listen, "-llocalhost:5432%d", i+1);

        char * const shcd_argv[] = {
            "shardcached", "-f", "-npeer1:localhost:4444,peer2:localhost:4445", me, listen, NULL
        };
        ut_testing("Start shardcached daemon %d", i+1);
        children[i] = start_node(shcd_argv);
        ut_validate_int(kill(children[i], 0), 0);
    }

    // give the daemons the time to start
    sleep(1);

    ut_testing("node1: HTTP PUT /test => TEST");
    struct http_response *response = http_put("http://127.0.0.1:54321/test", NULL, "TEST", 4);
    ut_validate_int(response->status_code_int, 200);
    http_response_free(response);

    ut_testing("node1: HTTP GET /test == TEST");
    response = http_get("http://127.0.0.1:54321/test", NULL);
    ut_validate_string(response->body, "TEST");
    http_response_free(response);

    ut_testing("node2: HTTP GET /test == TEST");
    response = http_get("http://127.0.0.1:54322/test", NULL);
    ut_validate_string(response->body, "TEST");
    http_response_free(response);

    ut_testing("node2: HTTP DELETE /test");
    response = http_delete("http://127.0.0.1:54322/test", NULL);
    ut_validate_int(response->status_code_int, 200);
    http_response_free(response);

    ut_testing("node1: HTTP GET /test == NOT FOUND");
    response = http_get("http://127.0.0.1:54321/test", NULL);
    ut_validate_int(response->status_code_int, 404);
    http_response_free(response);

    ut_testing("node2: HTTP GET /test == NOT FOUND");
    response = http_get("http://127.0.0.1:54322/test", NULL);
    ut_validate_int(response->status_code_int, 404);
    http_response_free(response);

    for (i = 0; i < 2; i++) {
        ut_testing("Stopping node %d to exit", i+1);
        ut_validate_int(stop_node(children[i]), children[i]);
    }

    ut_summary();

    exit(ut_failed);
}
