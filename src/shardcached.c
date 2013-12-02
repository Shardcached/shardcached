#define _GNU_SOURCE
#include <getopt.h>
#include <string.h>

#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdint.h>
#include <signal.h>
#include <errno.h>
#include <sys/time.h>

#include "log.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <arpa/inet.h>

#include <mongoose.h>

#include <pthread.h>
#include <regex.h>

#include <fbuf.h>
#include <shardcache.h>

#include "storage.h"

#define SHARDCACHED_ADDRESS_DEFAULT "4321"
#define SHARDCACHED_LOGLEVEL_DEFAULT 0
#define SHARDCACHED_SECRET_DEFAULT "default"
#define SHARDCACHED_STORAGE_TYPE_DEFAULT "mem"
#define SHARDCACHED_STORAGE_OPTIONS_DEFAULT "initial_table_size=1024,max_table_size=1000000"
#define SHARDCACHED_STATS_INTERVAL_DEFAULT 60
#define SHARDCACHED_NUM_WORKERS_DEFAULT 50

#define SHARDCACHED_USERAGENT_SIZE_THRESHOLD 16
#define SHARDCACHED_MAX_SHARDS 1024

#define ADDR_REGEXP "^[a-z0-9_\\.\\-]+(:[0-9]+)?$"

static char *me = NULL;
static char *basepath = NULL;
static pthread_cond_t exit_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t exit_lock = PTHREAD_MUTEX_INITIALIZER;
static int should_exit = 0;

typedef struct {
    void *value;
    size_t size;
} shardcached_stored_item;

static void usage(char *progname, char *msg, ...)
{
    if (msg) {
        va_list arg;
        va_start(arg, msg);
        vprintf(msg, arg);
        printf("\n");
    }

    printf("Usage: %s [OPTION]...\n"
           "Possible options:\n"
           "    -d <plugins_path>     the path where to look for storage plugins (defaults to the CWD of the process)\n"
           "    -f                    run in foreground\n"
           "    -i <interval>         change the time interval (in seconds) after which stats are reported via syslog (defaults to '%d')\n"
           "    -l <ip_address:port>  ip address:port where to listen for incoming http connections\n"
           "    -b                    HTTP url basepath\n"
           "    -p <peers>            list of peers participating in the shardcache in the form : 'address:port,address2:port2'\n"
           "    -s                    shared secret used for message signing (defaults to : '%s')\n"
           "    -t <type>             storage type (available are : 'mem' and 'fs' (defaults to '%s')\n"
           "    -o <options>          storage options (defaults to '%s')\n"
           "    -v                    increate the log level (can be passed multiple times)\n"
           "    -w <num_workers>      number of shardcache worker threads (defaults to '%d')\n"
           "\n"
           "       Storage Types:\n"
           "         * mem       memory based storage\n"
           "            Options:\n"
           "              - initial_table_size     the initial size of the internal hashtable\n"
           "              - max_table_size         maximum limit the internal hashtable can be grown up to\n"
           "\n"
           "         * fs        filesystem based storage\n"
           "            Options:\n"
           "              - storage_path           the parh where to store the keys/values on the filesystem\n"
           "              - tmp_path               the path to a temporary directory to use while new data is being uploaded\n"
           , progname
           , SHARDCACHED_STATS_INTERVAL_DEFAULT
           , SHARDCACHED_SECRET_DEFAULT
           , SHARDCACHED_STORAGE_TYPE_DEFAULT
           , SHARDCACHED_STORAGE_OPTIONS_DEFAULT
           , SHARDCACHED_NUM_WORKERS_DEFAULT);

    exit(-2);
}

static void shardcached_stop(int sig)
{
    pthread_mutex_lock(&exit_lock);
    pthread_cond_signal(&exit_cond);
    pthread_mutex_unlock(&exit_lock);
    __sync_add_and_fetch(&should_exit, 1);
}

static void shardcached_do_nothing(int sig)
{
    DEBUG1("Signal %d received ... doing nothing\n", sig);
}

static int shardcached_request_handler(struct mg_connection *conn) {

    struct mg_request_info *request_info = mg_get_request_info(conn);
    shardcache_t *cache = request_info->user_data;
    char *key = (char *)request_info->uri;

    if (basepath) {
        if (strncmp(key, basepath, strlen(basepath)) != 0) {
            ERROR("Bad request uri : %s", request_info->uri);
            mg_printf(conn, "HTTP/1.0 404 Not Found\r\n\r\nNot Found");
            return 1;
        }
        key += strlen(basepath);
    }
    while (*key == '/')
        key++;
    if (*key == 0) {
        mg_printf(conn, "HTTP/1.0 404 Not Found\r\n\r\nNot Found");
        return 1;
    }

    if (strncasecmp(request_info->request_method, "GET", 3) == 0) {
        if (strcmp(key, "__stats__") == 0) {
            shardcache_stats_t stats;
            fbuf_t buf = FBUF_STATIC_INITIALIZER;

            shardcache_get_stats(cache, &stats);

            fbuf_printf(&buf,"Shardcache stats:  gets: %u\r\nsets: %u\r\ndels: %u\r\ncache misses: %u\r\nnot found: %u\r\n",
                                stats.ngets,
                                stats.nsets,
                                stats.ndels,
                                stats.ncache_misses,
                                stats.nnot_found);

            mg_printf(conn, "HTTP/1.0 200 OK\r\n"
                            "Content-Type: text/plain\r\n"
                            "Content-length: %d\r\n"
                            "Server: shardcached\r\n"
                            "Connection: Close\r\n\r\n%s", fbuf_used(&buf), fbuf_data(&buf));
            fbuf_destroy(&buf);
        }
        size_t vlen = 0;
        void *value = shardcache_get(cache, key, strlen(key), &vlen);
        if (value) {
            mg_printf(conn, "HTTP/1.0 200 OK\r\n"
                            "Content-Type: application/octet-stream\r\n"
                            "Content-length: %d\r\n"
                            "Server: shardcached\r\n"
                            "Connection: Close\r\n\r\n", (int)vlen);
            mg_write(conn, value, vlen);
            free(value);
        } else {
            mg_printf(conn, "HTTP/1.0 404 Not Found\r\n\r\nNot Found");
        }
    } else if (strncasecmp(request_info->request_method, "DELETE", 6) == 0) {
        int rc = shardcache_del(cache, key, strlen(key));
        mg_printf(conn, "HTTP/1.0 %s\r\n"
                        "Content-Length: 0\r\n\r\n",
                         rc == 0 ? "200 OK" : "500 ERR");
    } else if (strncasecmp(request_info->request_method, "PUT", 3) == 0) {
        int clen = 0;
        const char *clen_hdr = mg_get_header(conn, "Content-Length");
        if (clen_hdr) {
            clen = strtol(clen_hdr, NULL, 10); 
        }
        
        if (!clen) {
            mg_printf(conn, "HTTP/1.0 400 Bad Request\r\n\r\nNo Content-Length");
            return 1;
        }

        char *in = malloc(clen);
        int rb = 0;
        do {
            int n = mg_read(conn, in+rb, clen-rb);
            if (n == 0) {
                // connection closed by peer
                break;
            } else if (n < 0) {
                // error
                break;
            } else {
                rb += n;
            }
        } while (rb != clen);
        

        shardcache_set(cache, key, strlen(key), in, rb);

        mg_printf(conn, "HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n");
    }
    return 1;
}

void shardcached_end_request_handler(const struct mg_connection *conn, int reply_status_code) {
}

static void shardcached_run(shardcache_t *cache, uint32_t stats_interval)
{
    if (stats_interval) {
        shardcache_stats_t prevstats;
        memset(&prevstats, 0, sizeof(shardcache_stats_t));
        while (!__sync_fetch_and_add(&should_exit, 0)) {
            int rc = 0;
            struct timespec to_sleep = {
                .tv_sec = stats_interval,
                .tv_nsec = 0
            };
            struct timespec remainder = { 0, 0 };

            do {
                rc = nanosleep(&to_sleep, &remainder);
                if (__sync_fetch_and_add(&should_exit, 0))
                    break;
                memcpy(&to_sleep, &remainder, sizeof(struct timespec));
                memset(&remainder, 0, sizeof(struct timespec));
            } while (rc != 0);

            shardcache_stats_t stats;
            shardcache_get_stats(cache, &stats);
            NOTICE("Shardcache stats:  gets => %u, sets => %u, dels => %u, cache misses => %u, not found => %u\n",
                    stats.ngets - prevstats.ngets,
                    stats.nsets - prevstats.nsets,
                    stats.ndels - prevstats.ndels,
                    stats.ncache_misses - prevstats.ncache_misses,
                    stats.nnot_found - prevstats.nnot_found);
            memcpy(&prevstats, &stats, sizeof(shardcache_stats_t));
        }
    } else {
        // and keep working until we are told to exit
        pthread_mutex_lock(&exit_lock);
        pthread_cond_wait(&exit_cond, &exit_lock);
        pthread_mutex_unlock(&exit_lock);
    }
}

int main(int argc, char **argv)
{
    int option_index = 0;
    int foreground = 0;
    int loglevel = SHARDCACHED_LOGLEVEL_DEFAULT;
    char *listen_address = SHARDCACHED_ADDRESS_DEFAULT;
    char *peers = NULL;
    char *secret = SHARDCACHED_SECRET_DEFAULT;
    char *storage_type = SHARDCACHED_STORAGE_TYPE_DEFAULT;
    char options_string[MAX_OPTIONS_STRING_LEN];
    uint32_t stats_interval = SHARDCACHED_STATS_INTERVAL_DEFAULT;
    char *plugins_dir = "./";
    int num_workers = SHARDCACHED_NUM_WORKERS_DEFAULT;
    
    strcpy(options_string, SHARDCACHED_STORAGE_OPTIONS_DEFAULT);

    static struct option long_options[] = {
        {"base", 2, 0, 'b'},
        {"plugins_directory", 2, 0, 'd'},
        {"foreground", 0, 0, 'f'},
        {"stats_interval", 2, 0, 'i'},
        {"listen", 2, 0, 'l'},
        {"peers", 2, 0, 'p'},
        {"secret", 2, 0, 's'},
        {"type", 2, 0, 't'},
        {"options", 2, 0, 'o'},
        {"verbose", 0, 0, 'v'},
        {"workers", 2, 0, 'w'},
        {"help", 0, 0, 'h'},
        {0, 0, 0, 0}
    };

    char c;
    while ((c = getopt_long (argc, argv, "b:d:fhi:l:p:s:t:o:vw:?", long_options, &option_index))) {
        if (c == -1) {
            break;
        }
        switch (c) {
            case 'b':
                basepath = optarg;
                // skip leading '/'s
                while (*basepath == '/')
                    basepath++;
                break;
            case 'd':
                plugins_dir = optarg;
                break;
            case 'f':
                foreground = 1;
                break;
            case 'i':
                stats_interval = strtol(optarg, NULL, 10);
                break;
            case 'l':
                listen_address = optarg;
                break;
            case 'p':
                peers = strdup(optarg);
                break;
            case 's':
                secret = optarg;
                break;
            case 't':
                storage_type = optarg;
                break;
            case 'o':
                snprintf(options_string, sizeof(options_string), "%s", optarg);
                break;
            case 'v':
                loglevel++;
                break;
            case 'w':
                num_workers = strtol(optarg, NULL, 10);
                break;
            case 'h':
            case '?':
                usage(argv[0], NULL);
                break;
            default:
                break;
        }
    }
    me = argv[argc-1];

    regex_t addr_regexp;
    int rc = regcomp(&addr_regexp, ADDR_REGEXP, REG_EXTENDED|REG_ICASE);
    if (rc != 0) {
        char errbuf[1024];
        regerror(rc, &addr_regexp, errbuf, sizeof(errbuf));
        fprintf(stderr, "Can't compile regexp %s: %s\n", ADDR_REGEXP, errbuf);
        exit(-1);
    }

    if (!me || *me == '-') {
        usage(argv[0], "The local address is mandatory");
    }

    int matched = regexec(&addr_regexp, me, 0, NULL, 0);
    if (matched != 0) {
        usage(argv[0], "Bad address format: '%s'", me);
    }

    char *shard_names[SHARDCACHED_MAX_SHARDS];
    int cnt = 0;
    if (peers) {
        char *tok = strtok(peers, ",");
        while(tok) {
            matched = regexec(&addr_regexp, tok, 0, NULL, 0);
            if (matched != 0) {
                usage(argv[0], "Bad address format for peer: '%s'", tok);
            }
            shard_names[cnt] = tok;
            cnt++;
            tok = strtok(NULL, ",");
        } 
    }

    regfree(&addr_regexp);

    // go daemon if we have to
    if (!foreground) {
        int rc = daemon(0, 0);
        if (rc != 0) {
            fprintf(stderr, "Can't go daemon: %s\n", strerror(errno));
            exit(-1);
        }
    }

    signal(SIGINT, shardcached_stop);
    signal(SIGHUP, shardcached_stop);
    signal(SIGQUIT, shardcached_stop);
    signal(SIGPIPE, shardcached_do_nothing);

    log_init("shardcached", loglevel);

    shcd_storage_t *st = shcd_storage_init(storage_type, options_string, plugins_dir);
    if (!st) {
        ERROR("Can't initialize the storage subsystem");
        exit(-1);
    }

    DEBUG("Starting the shardcache engine with %d workers", num_workers);
    shardcache_t *cache = shardcache_create(me, shard_names, cnt, shcd_storage_get(st), secret, num_workers);
    if (!cache) {
        ERROR("Can't initialize the shardcache engine");
        exit(-1);
    }

    // initialize the mongoose callbacks descriptor
    struct mg_callbacks shardcached_callbacks = {
        .begin_request = shardcached_request_handler,
        .end_request = shardcached_end_request_handler,
    };

    if (strncmp(listen_address, "*:", 2) == 0)
        listen_address += 2;

    const char *mongoose_options[] = { "listening_ports", listen_address, NULL };
    // let's start mongoose
    struct mg_context *ctx = mg_start(&shardcached_callbacks, cache, mongoose_options);
    if (ctx) {
        shardcached_run(cache, stats_interval);
    } else {
        ERROR("Can't start the http subsystem");
    }

    NOTICE("exiting");

    if (ctx)
        mg_stop(ctx);

    shardcache_destroy(cache);

    shcd_storage_destroy(st);

    free(peers);

    exit(0);
}
