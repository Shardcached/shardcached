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
#include <hashtable.h>
#include <shardcache.h>

#include "storage.h"

#define SHARDCACHED_ADDRESS_DEFAULT "4321"
#define SHARDCACHED_LOGLEVEL_DEFAULT 0
#define SHARDCACHED_SECRET_DEFAULT "default"
#define SHARDCACHED_STORAGE_TYPE_DEFAULT "mem"
#define SHARDCACHED_STORAGE_OPTIONS_DEFAULT ""
#define SHARDCACHED_STATS_INTERVAL_DEFAULT 0
#define SHARDCACHED_NUM_WORKERS_DEFAULT 50
#define SHARDCACHED_PLUGINS_DIR_DEFAULT "./"
#define SHARDCACHED_ACCESS_LOG_DEFAULT "./shardcached_access.log"
#define SHARDCACHED_ERROR_LOG_DEFAULT "./shardcached_error.log"

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
           "    -a <access_log_file>  the path where to store the access_log file (detaults to '%s')\n"
           "    -e <error_log_file>   the path where to store the error_log file (defaults to '%s')\n"
           "    -d <plugins_path>     the path where to look for storage plugins (defaults to '%s')\n"
           "    -f                    run in foreground\n"
           "    -i <interval>         change the time interval (in seconds) used to report internal stats via syslog (defaults to '%d')\n"
           "    -l <ip_address:port>  ip_address:port where to listen for incoming http connections\n"
           "    -b                    HTTP url basepath\n"
           "    -p <peers>            list of peers participating in the shardcache in the form : 'address:port,address2:port2'\n"
           "    -s                    shared secret used for message signing (defaults to : '%s')\n"
           "    -t <type>             storage type (available are : 'mem' and 'fs' (defaults to '%s')\n"
           "    -o <options>          comma-separated list of storage options (defaults to '%s')\n"
           "    -v                    increase the log level (can be passed multiple times)\n"
           "    -w <num_workers>      number of shardcache worker threads (defaults to '%d')\n"
           "\n"
           "       Builtin storage types:\n"
           "         * mem            memory based storage\n"
           "            Options:\n"
           "              - initial_table_size=<size>    the initial number of slots in the internal hashtable\n"
           "              - max_table_size=<size>        the maximum number of slots that the internal hashtable can be grown up to\n"
           "\n"
           "         * fs             filesystem based storage\n"
           "            Options:\n"
           "              - storage_path=<path>          the parh where to store the keys/values on the filesystem\n"
           "              - tmp_path=<path>              the path to a temporary directory to use while new data is being uploaded\n"
           , progname
           , SHARDCACHED_ACCESS_LOG_DEFAULT
           , SHARDCACHED_ERROR_LOG_DEFAULT
           , SHARDCACHED_PLUGINS_DIR_DEFAULT
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

static int shcd_active_requests = 0;

static int shardcached_request_handler(struct mg_connection *conn)
{

    struct mg_request_info *request_info = mg_get_request_info(conn);
    shardcache_t *cache = request_info->user_data;
    char *key = (char *)request_info->uri;

    __sync_add_and_fetch(&shcd_active_requests, 1);

    if (basepath) {
        if (strncmp(key, basepath, strlen(basepath)) != 0) {
            ERROR("Bad request uri : %s", request_info->uri);
            mg_printf(conn, "HTTP/1.0 404 Not Found\r\n\r\nNot Found");
            __sync_sub_and_fetch(&shcd_active_requests, 1);
            return 1;
        }
        key += strlen(basepath);
    }
    while (*key == '/')
        key++;
    if (*key == 0) {
        mg_printf(conn, "HTTP/1.0 404 Not Found\r\n\r\nNot Found");
        __sync_sub_and_fetch(&shcd_active_requests, 1);
        return 1;
    }

    if (strncasecmp(request_info->request_method, "GET", 3) == 0) {
        if (strcmp(key, "__stats__") == 0) {
            int do_html = (!request_info->query_string ||
                           !strstr(request_info->query_string, "nohtml=1"));

            fbuf_t buf = FBUF_STATIC_INITIALIZER;


            if (do_html) {
                fbuf_printf(&buf,
                            "<html><body>"
                            "<table bgcolor='#000000' "
                            "cellspacing='1' "
                            "cellpadding='4'>"
                            "<tr bgcolor='#ffffff'>"
                            "<td><b>Counter</b></td>"
                            "<td><b>Value</b></td>"
                            "</tr>"
                            "<tr bgcolor='#ffffff'>"
                            "<td>active_http_requests</td>"
                            "<td>%d</td>",
                              __sync_fetch_and_add(&shcd_active_requests, 0));
            } else {
                fbuf_printf(&buf,
                            "active_http_requests;%d\r\n",
                            __sync_fetch_and_add(&shcd_active_requests, 0));
            }


            shardcache_counter_t *counters;
            int ncounters = shardcache_get_counters(cache, &counters);
            int i;

            for (i = 0; i < ncounters; i++) {
                if (do_html)
                    fbuf_printf(&buf,
                                "<tr bgcolor='#ffffff'><td>%s</td><td>%u</td>",
                                counters[i].name,
                                counters[i].value);
                else
                    fbuf_printf(&buf,
                                "%s;%u\r\n",
                                counters[i].name,
                                counters[i].value);
            }
            if (do_html)
                fbuf_printf(&buf, "</table></body></html>");
            free(counters);
                 
            mg_printf(conn, "HTTP/1.0 200 OK\r\n"
                            "Content-Type: text/%s\r\n"
                            "Content-length: %d\r\n"
                            "Server: shardcached\r\n"
                            "Connection: Close\r\n\r\n%s",
                            do_html ? "html" : "plain",
                            fbuf_used(&buf),
                            fbuf_data(&buf));
            fbuf_destroy(&buf);

        } else if (strcmp(key, "__index__") == 0) {
            shardcache_storage_index_t *index = shardcache_get_index(cache);
            fbuf_t buf = FBUF_STATIC_INITIALIZER;
            int i;
            int do_html = (!request_info->query_string ||
                           !strstr(request_info->query_string, "nohtml=1"));

            if (do_html) {
                fbuf_printf(&buf,
                            "<html><body>"
                            "<table bgcolor='#000000' "
                            "cellspacing='1' "
                            "cellpadding='4'>"
                            "<tr bgcolor='#ffffff'>"
                            "<td><b>Key</b></td>"
                            "<td><b>Value size</b></td>"
                            "</tr>");
            }
            for (i = 0; i < index->size; i++) {
                size_t klen = index->items[i].klen;
                char keystr[klen+1];
                memcpy(keystr, index->items[i].key, klen);
                keystr[klen] = 0;
                if (do_html)
                    fbuf_printf(&buf,
                                "<tr bgcolor='#ffffff'><td>%s</td>"
                                "<td>(%d)</td></tr>",
                                keystr,
                                index->items[i].vlen);
                else
                    fbuf_printf(&buf,
                                "%s;%d\r\n",
                                keystr,
                                index->items[i].vlen);
            }

            if (do_html)
                fbuf_printf(&buf, "</table></body></html>");

            mg_printf(conn, "HTTP/1.0 200 OK\r\n"
                            "Content-Type: text/%s\r\n"
                            "Content-length: %d\r\n"
                            "Server: shardcached\r\n"
                            "Connection: Close\r\n\r\n%s",
                            do_html ? "html" : "plain",
                            fbuf_used(&buf),
                            fbuf_data(&buf));

            fbuf_destroy(&buf);
            shardcache_free_index(index);
        } else {
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
            mg_printf(conn, "HTTP/1.0 400 Bad Request\r\n\r\n");
            __sync_sub_and_fetch(&shcd_active_requests, 1);
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
        free(in);

        mg_printf(conn, "HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n");
    }
    __sync_sub_and_fetch(&shcd_active_requests, 1);
    return 1;
}

static void shardcached_run(shardcache_t *cache, uint32_t stats_interval)
{
    if (stats_interval) {
        hashtable_t *prevcounters = ht_create(32, 256, free);
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

            shardcache_counter_t *counters;
            int ncounters = shardcache_get_counters(cache, &counters);
            int i;
            fbuf_t out = FBUF_STATIC_INITIALIZER;
            for (i = 0; i < ncounters; i++) {

                uint32_t *prev = ht_get(prevcounters,
                                        counters[i].name,
                                        strlen(counters[i].name),
                                        NULL);

                fbuf_printf(&out,
                            "%s: %u\n",
                            counters[i].name,
                            counters[i].value - (prev ? *prev : 0));

                if (prev) {
                    *prev = counters[i].value;
                } else {
                    uint32_t *prev_value = malloc(sizeof(uint32_t));
                    *prev_value = counters[i].value;
                    ht_set(prevcounters, counters[i].name,
                           strlen(counters[i].name), prev_value,
                           sizeof(uint32_t));
                }
            }
            NOTICE("Shardcache stats: %s\n", fbuf_data(&out));
            fbuf_destroy(&out);
            free(counters);
        }
        ht_destroy(prevcounters);
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
    char *plugins_dir = SHARDCACHED_PLUGINS_DIR_DEFAULT;
    int num_workers = SHARDCACHED_NUM_WORKERS_DEFAULT;
    char *access_log_file = SHARDCACHED_ACCESS_LOG_DEFAULT;
    char *error_log_file = SHARDCACHED_ERROR_LOG_DEFAULT;
    
    strcpy(options_string, SHARDCACHED_STORAGE_OPTIONS_DEFAULT);

    static struct option long_options[] = {
        {"access_log", 2, 0, 'a'},
        {"error_log", 2, 0, 'e'},
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
    while ((c = getopt_long (argc, argv, "a:b:d:fg:hi:l:p:s:t:o:vw:?",
                             long_options, &option_index)))
    {
        if (c == -1) {
            break;
        }
        switch (c) {
            case 'a':
                access_log_file = optarg;
                break;
            case 'b':
                basepath = optarg;
                // skip leading '/'s
                while (*basepath == '/')
                    basepath++;
                break;
            case 'd':
                plugins_dir = optarg;
                break;
            case 'e':
                error_log_file = optarg;
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
                usage(argv[0], "Unknown option : '-%c'", c);
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

    shcd_storage_t *st = shcd_storage_init(storage_type,
                                           options_string,
                                           plugins_dir);
    if (!st) {
        ERROR("Can't initialize the storage subsystem");
        exit(-1);
    }

    DEBUG("Starting the shardcache engine with %d workers", num_workers);
    shardcache_t *cache = shardcache_create(me, shard_names, cnt,
            shcd_storage_get(st), secret, num_workers);

    if (!cache) {
        ERROR("Can't initialize the shardcache engine");
        exit(-1);
    }

    // initialize the mongoose callbacks descriptor
    struct mg_callbacks shardcached_callbacks = {
        .begin_request = shardcached_request_handler
    };

    if (strncmp(listen_address, "*:", 2) == 0)
        listen_address += 2;

    const char *mongoose_options[] = { "listening_ports", listen_address,
                                       "access_log_file", access_log_file,
                                       "error_log_file", error_log_file,
                                        NULL };

    // let's start mongoose
    struct mg_context *ctx = mg_start(&shardcached_callbacks,
                                      cache,
                                      mongoose_options);
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
