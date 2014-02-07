#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <signal.h>
#include <sys/wait.h>
#include <ut.h>
#include "http-client-c.h"

int main(int argc, char **argv)
{
    ut_init("shardcached_test");
    pid_t children[2];
    int i;
    for (i = 0; i < 2; i++) {
        ut_testing("Start shardcached daemon %d", i+1);
        pid_t pid = fork();
        if (pid) {
            children[i] = pid;
            ut_validate_int(kill(pid, 0), 0);
        } else {
            char me[32];
            sprintf(me, "-mpeer%d", i+1);

            char listen[32];
            sprintf(listen, "-llocalhost:5432%d", i+1);

            char * const shcd_argv[] = {
                "shardcached", "-f", "-npeer1:localhost:4444,peer2:localhost:4445", me, listen, NULL
            };
            execve("./shardcached", shcd_argv, NULL);
        }
    }

    // give the daemons the time to start
    sleep(1);

    ut_testing("HTTP PUT /test => TEST");
    struct http_response *response = http_put("http://127.0.0.1:54321/test", NULL, "TEST", 4);
    ut_validate_int(response->status_code_int, 200);
    http_response_free(response);


    ut_testing("HTTP GET /test == TEST");
    response = http_get("http://127.0.0.1:54321/test", NULL);
    ut_validate_string(response->body, "TEST");
    http_response_free(response);

    for (i = 0; i < 2; i++) {
        ut_testing("Waiting for node %d to exit", i+1);
        kill(children[i], 2);         
        pid_t pid = waitpid(children[i], NULL, 0);
        ut_validate_int(pid, children[i]);
    }

    ut_summary();
}
