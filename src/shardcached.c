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
#include <sys/stat.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <arpa/inet.h>

#include <sys/types.h>
#include <pwd.h>

#include <mongoose.h>

#include <pthread.h>
#include <regex.h>

#include <fbuf.h>
#include <hashtable.h>
#include <shardcache.h>

#include "storage.h"
#include "ini.h"
#include "acl.h"

#define SHARDCACHED_ADDRESS_DEFAULT "4321"
#define SHARDCACHED_LOGLEVEL_DEFAULT LOG_WARNING
#define SHARDCACHED_SECRET_DEFAULT ""
#define SHARDCACHED_STORAGE_TYPE_DEFAULT "mem"
#define SHARDCACHED_STORAGE_OPTIONS_DEFAULT ""
// default cache size : 512 MB
#define SHARDCACHED_CACHE_SIZE_DEFAULT 1<<29
#define SHARDCACHED_STATS_INTERVAL_DEFAULT 0
#define SHARDCACHED_NUM_WORKERS_DEFAULT 10
#define SHARDCACHED_NUM_HTTP_WORKERS_DEFAULT 10
#define SHARDCACHED_PLUGINS_DIR_DEFAULT "./"
#define SHARDCACHED_ACCESS_LOG_DEFAULT "./shardcached_access.log"
#define SHARDCACHED_ERROR_LOG_DEFAULT "./shardcached_error.log"

#define SHARDCACHED_USERAGENT_SIZE_THRESHOLD 16
#define SHARDCACHED_MAX_SHARDS 1024

#define ADDR_REGEXP "^([a-z0-9_\\.\\-]+|\\*)(:[0-9]+)?$"

#define HTTP_HEADERS_BASE "HTTP/1.0 200 OK\r\n" \
                          "Content-Type: %s\r\n" \
                          "Content-Length: %d\r\n" \
                          "Server: shardcached\r\n" \
                          "Connection: Close\r\n"
#define HTTP_HEADERS HTTP_HEADERS_BASE "\r\n"
#define HTTP_HEADERS_WITH_TIME HTTP_HEADERS_BASE "Last-Modified: %s\r\n\r\n"

static pthread_cond_t exit_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t exit_lock = PTHREAD_MUTEX_INITIALIZER;
static int should_exit = 0;
static shcd_acl_t *http_acl = NULL;
static hashtable_t *mime_types = NULL;

typedef struct {
    char me[256];
    char basepath[256];
    char baseadminpath[256];
    int foreground;
    int loglevel;
    char listen_address[256];
    shardcache_node_t *nodes;
    int  num_nodes;
    shardcache_node_t *migration_nodes;
    int  num_migration_nodes;
    char secret[1024];
    char storage_type[256];
    char storage_options[MAX_OPTIONS_STRING_LEN];
    uint32_t stats_interval;
    char plugins_dir[1024];
    int num_workers;
    int num_http_workers;
    char access_log_file[1024];
    char error_log_file[1024];
    size_t cache_size;
    int evict_on_delete;
    shcd_acl_action_t acl_default;
    int nohttp;
    int nostorage;
    char *username;
    int use_persistent_connections;
} shardcached_config_t;

static shardcached_config_t config = {
    .me = "",
    .basepath = "",
    .baseadminpath = "",
    .foreground = 0,
    .loglevel = SHARDCACHED_LOGLEVEL_DEFAULT,
    .listen_address = SHARDCACHED_ADDRESS_DEFAULT,
    .nodes = NULL,
    .num_nodes = 0,
    .migration_nodes = NULL,
    .num_migration_nodes = 0,
    .secret = SHARDCACHED_SECRET_DEFAULT,
    .storage_type = SHARDCACHED_STORAGE_TYPE_DEFAULT,
    .storage_options = SHARDCACHED_STORAGE_OPTIONS_DEFAULT,
    .stats_interval = SHARDCACHED_STATS_INTERVAL_DEFAULT,
    .plugins_dir = SHARDCACHED_PLUGINS_DIR_DEFAULT,
    .num_workers = SHARDCACHED_NUM_WORKERS_DEFAULT,
    .num_http_workers = SHARDCACHED_NUM_HTTP_WORKERS_DEFAULT,
    .access_log_file = SHARDCACHED_ACCESS_LOG_DEFAULT,
    .error_log_file = SHARDCACHED_ERROR_LOG_DEFAULT,
    .cache_size = SHARDCACHED_CACHE_SIZE_DEFAULT,
    .evict_on_delete = 1,
    .acl_default = SHCD_ACL_ACTION_ALLOW,
    .nohttp = 0,
    .nostorage = 0,
    .username = NULL,
    .use_persistent_connections = 1
};

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
           "    -a <access_log_file>  the path where to store the access_log file (defaults to '%s')\n"
           "    -e <error_log_file>   the path where to store the error_log file (defaults to '%s')\n"
           "    -c <config_file>      the config file to load\n"
           "    -d <plugins_path>     the path where to look for storage plugins (defaults to '%s')\n"
           "    -f                    run in foreground\n"
           "    -H                    disable the HTTP frontend\n"
           "    -i <interval>         change the time interval (in seconds) used to report internal stats via syslog (defaults to '%d')\n"
           "    -l <ip_address:port>  ip_address:port where to listen for incoming http connections\n"
           "    -b                    HTTP url basepath (optional, defaults to '')\n"
           "    -B                    HTTP url baseadminpath (optional, defaults to '')\n"
           "    -n <nodes>            list of nodes participating in the shardcache in the form : 'label:address:port,label2:address2:port2'\n"
           "    -N                    no storage subsystem, use only the internal libshardcache volatile storage\n"
           "    -m me                 the label of this node, to identify it among the ones participating in the shardcache\n"
           "    -s                    cache size in bytes (defaults to : '%d')\n"
           "    -S                    shared secret used for message signing (defaults to : '%s')\n"
           "    -t <type>             storage type (available are : 'mem' and 'fs' (defaults to '%s')\n"
           "    -o <options>          comma-separated list of storage options (defaults to '%s')\n"
           "    -u <username>         assume the identity of <username> (only when run as root)\n"
           "    -v                    increase the log level (can be passed multiple times)\n"
           "    -w <num_workers>      number of shardcache worker threads (defaults to '%d')\n"
           "    -x <nodes>            new list of nodes to migrate the shardcache to. The format to use is the same as for the '-n' option\n"
           "\n"
           "       Builtin storage types:\n"
           "         * mem            memory based storage\n"
           "            Options:\n"
           "              - initial_table_size=<size>    the initial number of slots in the internal hashtable\n"
           "              - max_table_size=<size>        the maximum number of slots that the internal hashtable can be grown up to\n"
           "\n"
           "         * fs             filesystem based storage\n"
           "            Options:\n"
           "              - storage_path=<path>          the path where to store the keys/values on the filesystem\n"
           "              - tmp_path=<path>              the path to a temporary directory to use while new data is being uploaded\n"
           , progname
           , SHARDCACHED_ACCESS_LOG_DEFAULT
           , SHARDCACHED_ERROR_LOG_DEFAULT
           , SHARDCACHED_PLUGINS_DIR_DEFAULT
           , SHARDCACHED_STATS_INTERVAL_DEFAULT
           , SHARDCACHED_CACHE_SIZE_DEFAULT
           , SHARDCACHED_SECRET_DEFAULT
           , SHARDCACHED_STORAGE_TYPE_DEFAULT
           , SHARDCACHED_STORAGE_OPTIONS_DEFAULT
           , SHARDCACHED_NUM_WORKERS_DEFAULT);

    exit(-2);
}

static void shardcached_stop(int sig)
{
    __sync_add_and_fetch(&should_exit, 1);
    pthread_mutex_lock(&exit_lock);
    pthread_cond_signal(&exit_cond);
    pthread_mutex_unlock(&exit_lock);
}

static void shardcached_do_nothing(int sig)
{
    SHC_DEBUG1("Signal %d received ... doing nothing\n", sig);
}

static int shcd_active_requests = 0;

static void shardcached_build_index_response(fbuf_t *buf, int do_html, shardcache_t *cache)
{
    int i;

    shardcache_storage_index_t *index = shardcache_get_index(cache);

    if (do_html) {
        fbuf_printf(buf,
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
        char keystr[klen * 5 + 1];
        char *t = keystr;
        char c;
        int p;
        for (p = 0 ; p < klen ; ++p) {
            c = ((char*)index->items[i].key)[p];
            if (c == '<')
                t = stpcpy(t, "&lt;");
            else if (c == '>')
                t = stpcpy(t, "&gt;");
            else if (c == '&')
                t = stpcpy(t, "&amp;");
            else if (c < ' ') {
                sprintf(t, "\\x%2x", (int)c);
                t += 4;
            }
            else
                *t++ = c;
        }
        *t = 0;
        if (do_html)
            fbuf_printf(buf,
                        "<tr bgcolor='#ffffff'><td>%s</td>"
                        "<td>(%d)</td></tr>",
                        keystr,
                        index->items[i].vlen);
        else
            fbuf_printf(buf,
                        "%s;%d\r\n",
                        keystr,
                        index->items[i].vlen);
    }

    if (do_html)
        fbuf_printf(buf, "</table></body></html>");

    shardcache_free_index(index);
}

static void shardcached_build_stats_response(fbuf_t *buf, int do_html, shardcache_t *cache)
{
    int i;
    int num_nodes = 0;
    shardcache_node_t *nodes = shardcache_get_nodes(cache, &num_nodes);
    if (do_html) {
        fbuf_printf(buf,
                    "<html><body>"
                    "<h1>%s</h1>"
                    "<table bgcolor='#000000' "
                    "cellspacing='1' "
                    "cellpadding='4'>"
                    "<tr bgcolor='#ffffff'>"
                    "<td><b>Counter</b></td>"
                    "<td><b>Value</b></td>"
                    "</tr>"
                    "<tr bgcolor='#ffffff'>"
                    "<td>active_http_requests</td>"
                    "<td>%d</td>"
                    "</tr>"
                    "<tr bgcolor='#ffffff'>"
                    "<td>num_nodes</td>"
                    "<td>%d</td>"
                    "</tr>",
                    config.me,
                    __sync_fetch_and_add(&shcd_active_requests, 0),
                    num_nodes);

        for (i = 0; i < num_nodes; i++) {
            fbuf_printf(buf,
                        "<tr bgcolor='#ffffff'>"
                        "<td>node::%s</td><td>%s</td>"
                        "</td></tr>",
                        nodes[i].label, nodes[i].address);
        }
    } else {
        fbuf_printf(buf,
                    "active_http_requests;%d\r\nnum_nodes;%d\r\n",
                    __sync_fetch_and_add(&shcd_active_requests, 0),
                    num_nodes);
        for (i = 0; i < num_nodes; i++) {
            fbuf_printf(buf, "node::%s;%s\r\n", nodes[i].label, nodes[i].address);
        }
    }

    if (nodes)
        free(nodes);

    shardcache_counter_t *counters;
    int ncounters = shardcache_get_counters(cache, &counters);

    for (i = 0; i < ncounters; i++) {
        if (do_html)
            fbuf_printf(buf,
                        "<tr bgcolor='#ffffff'><td>%s</td><td>%u</td></tr>",
                        counters[i].name,
                        counters[i].value);
        else
            fbuf_printf(buf,
                        "%s;%u\r\n",
                        counters[i].name,
                        counters[i].value);
    }
    if (do_html)
        fbuf_printf(buf, "</table></body></html>");
    free(counters);
}

static void shardcached_handle_admin_request(shardcache_t *cache, struct mg_connection *conn, char *key, int is_head)
{
    struct mg_request_info *request_info = mg_get_request_info(conn);

    if (http_acl) {
        shcd_acl_method_t method = SHCD_ACL_METHOD_GET;
        if (shcd_acl_eval(http_acl, method, key, request_info->remote_ip) != SHCD_ACL_ACTION_ALLOW) {
            mg_printf(conn, "HTTP/1.0 403 Forbidden\r\nContent-Length: 9\r\n\r\nForbidden");
            return;
        }
    }

    if (strcmp(key, "__stats__") == 0) {
        int do_html = (!request_info->query_string ||
                       !strstr(request_info->query_string, "nohtml=1"));

        fbuf_t buf = FBUF_STATIC_INITIALIZER;
        shardcached_build_stats_response(&buf, do_html, cache);

        mg_printf(conn, HTTP_HEADERS,
                        do_html ? "text/html" : "text/plain",
                        fbuf_used(&buf));

        if (!is_head)
            mg_printf(conn, "%s", fbuf_data(&buf));

        fbuf_destroy(&buf);

    } else if (strcmp(key, "__index__") == 0) {
        fbuf_t buf = FBUF_STATIC_INITIALIZER;
        int do_html = (!request_info->query_string ||
                       !strstr(request_info->query_string, "nohtml=1"));

        shardcached_build_index_response(&buf, do_html, cache);

        mg_printf(conn, HTTP_HEADERS,
                        do_html ? "text/html" : "text/plain",
                        fbuf_used(&buf));

        if (!is_head)
            mg_printf(conn, "%s", fbuf_data(&buf));

        fbuf_destroy(&buf);
    } else if (strcmp(key, "__health__") == 0) {
        int do_html = (!request_info->query_string ||
                       !strstr(request_info->query_string, "nohtml=1"));

        char *resp = do_html ? "<html><body>OK</body></html>" : "OK";

        mg_printf(conn, HTTP_HEADERS,
                        do_html ? "text/html" : "text/plain",
                        (int)strlen(resp));

        if (!is_head)
            mg_printf(conn, "%s", resp);
    } else {
        mg_printf(conn, "HTTP/1.0 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found");
    }
}

static void shardcached_handle_get_request(shardcache_t *cache, struct mg_connection *conn, char *key, int is_head)
{
    struct mg_request_info *request_info = mg_get_request_info(conn);

    if (http_acl) {
        shcd_acl_method_t method = SHCD_ACL_METHOD_GET;
        if (shcd_acl_eval(http_acl, method, key, request_info->remote_ip) != SHCD_ACL_ACTION_ALLOW) {
            mg_printf(conn, "HTTP/1.0 403 Forbidden\r\nContent-Length: 9\r\n\r\nForbidden");
            return;
        }
    }

    size_t vlen = 0;
    struct timeval ts;
    void *value = NULL;
    if (is_head) {
        vlen = shardcache_head(cache, key, strlen(key), NULL, 0, &ts);
    } else {
        value = shardcache_get(cache, key, strlen(key), &vlen, &ts);
    }

    if (vlen) {
        char *mtype = "application/octet-stream";
        if (mime_types) {
            char *p = key;
            while (*p && *p != '.')
                p++;
            if (*p && *(p+1)) {
                p++;
                char *mt = (char *)ht_get(mime_types, p, strlen(p), NULL);
                if (mt)
                    mtype = mt;
            }
        }
        char timestamp[256];
        struct tm gmts;
        strftime(timestamp, sizeof(timestamp), "%a, %d %b %Y %T %z", gmtime_r(&ts.tv_sec, &gmts));
        mg_printf(conn, HTTP_HEADERS_WITH_TIME, mtype, (int)vlen, timestamp);

        if (!is_head && value)
            mg_write(conn, value, vlen);

        if (value)
            free(value);
    } else {
        mg_printf(conn, "HTTP/1.0 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found");
    }
}

static void shardcached_handle_delete_request(shardcache_t *cache, struct mg_connection *conn, char *key)
{
    struct mg_request_info *request_info = mg_get_request_info(conn);
    if (http_acl) {
        shcd_acl_method_t method = SHCD_ACL_METHOD_DEL;
        if (shcd_acl_eval(http_acl, method, key, request_info->remote_ip) != SHCD_ACL_ACTION_ALLOW) {
            mg_printf(conn, "HTTP/1.0 403 Forbidden\r\nContent-Length: 9\r\n\r\nForbidden");
            return;
        }
    }

    int rc = shardcache_del(cache, key, strlen(key));
    mg_printf(conn, "HTTP/1.0 %s\r\n"
                    "Content-Length: 0\r\n\r\n",
                     rc == 0 ? "200 OK" : "500 ERR");

}

static void shardcached_handle_put_request(shardcache_t *cache, struct mg_connection *conn, char *key)
{
    struct mg_request_info *request_info = mg_get_request_info(conn);
    if (http_acl) {
        shcd_acl_method_t method = SHCD_ACL_METHOD_PUT;
        if (shcd_acl_eval(http_acl, method, key, request_info->remote_ip) != SHCD_ACL_ACTION_ALLOW) {
            mg_printf(conn, "HTTP/1.0 403 Forbidden\r\nContent-Length: 9\r\n\r\nForbidden");
            return;
        }
    }

    int clen = 0;
    const char *clen_hdr = mg_get_header(conn, "Content-Length");
    if (clen_hdr) {
        clen = strtol(clen_hdr, NULL, 10); 
    }
    
    if (!clen) {
        mg_printf(conn, "HTTP/1.0 400 Bad Request\r\nContent-Length: 0\r\n\r\n");
        return;
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

static int shardcached_request_handler(struct mg_connection *conn)
{
    struct mg_request_info *request_info = mg_get_request_info(conn);
    shardcache_t *cache = request_info->user_data;
    char *key = (char *)request_info->uri;
    int basepath_found = 0;

    int basepath_len = strlen(config.basepath);
    int baseadminpath_len = strlen(config.baseadminpath);
    int basepaths_differ = (basepath_len != baseadminpath_len || strcmp(config.basepath, config.baseadminpath) != 0);

    __sync_add_and_fetch(&shcd_active_requests, 1);

    while (*key == '/' && *key)
        key++;

    if (basepath_len) {
        if (strncmp(key, config.basepath, basepath_len) == 0) {
            key += basepath_len + 1;
            basepath_found = 1;
            while (*key == '/' && *key)
                key++;
        } else {
            if (!basepaths_differ) {
                SHC_ERROR("Bad request uri : %s", request_info->uri);
                mg_printf(conn, "HTTP/1.0 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found");
                __sync_sub_and_fetch(&shcd_active_requests, 1);
                return 1;
            }
        }
    }

    if (*key == 0) {
        mg_printf(conn, "HTTP/1.0 404 Not Found\r\nContent-Length 9\r\n\r\nNot Found");
        __sync_sub_and_fetch(&shcd_active_requests, 1);
        return 1;
    }

    if (baseadminpath_len && basepaths_differ) {
        if (!basepath_found && strncmp(key, config.baseadminpath, baseadminpath_len) == 0) {
            key += baseadminpath_len + 1;

            while (*key == '/' && *key)
                key++;
            if (*key == 0) {
                mg_printf(conn, "HTTP/1.0 404 Not Found\r\nContent-Length 9\r\n\r\nNot Found");
                __sync_sub_and_fetch(&shcd_active_requests, 1);
                return 1;
            }

            if (strncasecmp(request_info->request_method, "GET", 3) == 0)
                shardcached_handle_admin_request(cache, conn, key, 0);
            else
                mg_printf(conn, "HTTP/1.0 403 Forbidden\r\nContent-Length 9\r\n\r\nForbidden");
            __sync_sub_and_fetch(&shcd_active_requests, 1);
            return 1;
        }
    }

    // if baseadminpath is not defined or it's the same as basepath,
    // we need to check for the "special" admin keys and handle them differently
    // (in such cases the labels __stats__, __index__ and __health__ become reserved
    // and can't be used as keys from the http interface)
    if ((!baseadminpath_len || !basepaths_differ) &&
        (strcmp(key, "__stats__") == 0 || strcmp(key, "__index__") == 0 || strcmp(key, "__health__") == 0))
    {
        if (strncasecmp(request_info->request_method, "GET", 3) == 0)
            shardcached_handle_admin_request(cache, conn, key, 0);
        else
            mg_printf(conn, "HTTP/1.0 403 Forbidden\r\nContent-Length 9\r\n\r\nForbidden");
        __sync_sub_and_fetch(&shcd_active_requests, 1);
        return 1;

    }

    // handle the actual GET/PUT/DELETE request
    if (strncasecmp(request_info->request_method, "GET", 3) == 0)
        shardcached_handle_get_request(cache, conn, key, 0);
    else if (strncasecmp(request_info->request_method, "HEAD", 4) == 0)
        shardcached_handle_get_request(cache, conn, key, 1);
    else if (strncasecmp(request_info->request_method, "DELETE", 6) == 0)
        shardcached_handle_delete_request(cache, conn, key);
    else if (strncasecmp(request_info->request_method, "PUT", 3) == 0)
        shardcached_handle_put_request(cache, conn, key);
    else
        mg_printf(conn, "HTTP/1.0 405 Method Not Allowed\r\nContent-Length: 11\r\n\r\nNot Allowed");


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
            SHC_NOTICE("Shardcache stats: %s\n", fbuf_data(&out));
            fbuf_destroy(&out);
            free(counters);
        }
        ht_destroy(prevcounters);
    } else {
        while (!__sync_fetch_and_add(&should_exit, 0)) {
            // and keep waiting until we are told to exit
            pthread_mutex_lock(&exit_lock);
            pthread_cond_wait(&exit_cond, &exit_lock);
            pthread_mutex_unlock(&exit_lock);
        }
    }
}

static int check_address_string(char *str)
{
    regex_t addr_regexp;
    int rc = regcomp(&addr_regexp, ADDR_REGEXP, REG_EXTENDED|REG_ICASE);
    if (rc != 0) {
        char errbuf[1024];
        regerror(rc, &addr_regexp, errbuf, sizeof(errbuf));
        SHC_ERROR("Can't compile regexp %s: %s\n", ADDR_REGEXP, errbuf);
        return -1;
    }

    int matched = regexec(&addr_regexp, str, 0, NULL, 0);
    regfree(&addr_regexp);

    if (matched != 0) {
        return -1;
    }

    return 0;
}

int config_acl(char *pattern, char *aclstr)
{
    char *p = aclstr;
    shcd_acl_action_t action;

    if (!http_acl) {
        http_acl = shcd_acl_create(SHCD_ACL_ACTION_ALLOW);
    }
    char *action_string = strsep(&p, ":");
    if (!action_string) {
        SHC_ERROR("Invalid acl string %s", aclstr);
        return -1;
    }
    if (strcasecmp(action_string, "allow") == 0) {
        action = SHCD_ACL_ACTION_ALLOW;
    } else if (strcasecmp(action_string, "deny") == 0) {
        action = SHCD_ACL_ACTION_DENY;
    } else {
        SHC_ERROR("Invalid acl action %s (can be 'allow' or 'deny')", action_string);
        return -1;
    }

    char *method_string = strsep(&p, ":");
    if (!method_string) {
        SHC_ERROR("Invalid acl string %s (no method_string found)", aclstr);
        return -1;
    }

    shcd_acl_method_t method = SHCD_ACL_METHOD_ANY;
    if (strcasecmp(method_string, "*") == 0) {
        method = SHCD_ACL_METHOD_ANY;
    } else if (strcasecmp(method_string, "GET") == 0) {
        method = SHCD_ACL_METHOD_GET;
    } else if (strcasecmp(method_string, "PUT") == 0) {
        method = SHCD_ACL_METHOD_PUT;
    } else if (strcasecmp(method_string, "DELETE") == 0) {
        method = SHCD_ACL_METHOD_DEL;
    } else {
        SHC_ERROR("Invalid acl method %s (can be 'GET' or 'PUT' or 'DELETE')", method_string);
        return -1;
    }
    int ret = 0;
    struct in_addr ip;
    uint32_t mask = 0xffff;
    char *ipaddr_string = strsep(&p, "/");
    if (!ipaddr_string) {
        SHC_ERROR("Invalid acl, can't find the ip address");
        return -1;
    }
    char *maskstr = p;
    if (maskstr && *maskstr) {
        mask = -1 << strtol(maskstr, NULL, 10);
    }
    if (*ipaddr_string == '*' && strlen(ipaddr_string) == 1) {
        ip.s_addr = 0;
        mask = 0;
    } else {
        ret = inet_aton(ipaddr_string, &ip);
        if (ret != 1) {
            SHC_ERROR("Bad ip address format %s (%s)\n", ipaddr_string);
            return -1;
        }
    }
    return shcd_acl_add(http_acl, pattern, action, method, ntohl(ip.s_addr), mask);
}

static int config_listening_address(char *addr_string, shardcached_config_t *config)
{
    if (strncmp(addr_string, "*:", 2) == 0)
        addr_string += 2;

    if (!*addr_string)
        return 0;

    char *v = strdup(addr_string);
    char *f = v;
    char *addr = strsep(&v, ":");
    char *port = v;
    if (!port) {
        port = addr;
        addr = NULL;
    }
    if (addr && *addr) {
       struct hostent *h = gethostbyname(addr);
        if (!h) {
            fprintf(stderr, "Can't resolve address for hostname : %s\n", addr);
            free(f);
            return 0;
        }
        struct in_addr **addr_list = (struct in_addr **)h->h_addr_list;
        if (addr_list[0] != NULL)
            addr = inet_ntoa(*addr_list[0]);
        snprintf(config->listen_address,
            sizeof(config->listen_address), "%s:%s", addr, port);
    } else {
        snprintf(config->listen_address,
            sizeof(config->listen_address), "%s", port);
    }
    free(f);
    return 1;
}

int config_handler(void *user,
                   const char *section,
                   const char *name,
                   const char *value)
{
    shardcached_config_t *config = (shardcached_config_t *)user;

    if (strcmp(section, "nodes") == 0)
    {
        config->num_nodes++;
        config->nodes = realloc(config->nodes, config->num_nodes * sizeof(shardcache_node_t));
        shardcache_node_t *node = &config->nodes[config->num_nodes-1];
        snprintf(node->label, sizeof(node->label), "%s", name);
        snprintf(node->address, sizeof(node->address), "%s", value);
    }
    else if (strcmp(section, "acl") == 0)
    {
        if (config_acl((char *)name, (char *)value) != 0)
        {
            fprintf(stderr, "Errors configuring acl : %s = %s\n", name, value);
            return 0;
        }
    }
    else if (strcmp(section, "shardcached") == 0)
    {
        if (strcmp(name, "stats_interval") == 0) {
            config->stats_interval = strtol(value, NULL, 10);
        }
        else if (strcmp(name, "storage_type") == 0)
        {
            snprintf(config->storage_type, sizeof(config->storage_type),
                    "%s", value);
        }
        else if (strcmp(name, "storage_options") == 0)
        {
            snprintf(config->storage_options, sizeof(config->storage_options),
                    "%s", value);
        }
        else if (strcmp(name, "plugins_dir") == 0)
        {
            snprintf(config->plugins_dir, sizeof(config->plugins_dir),
                    "%s", value);
        }
        else if (strcmp(name, "loglevel") == 0)
        {
            config->loglevel = strtol(value, NULL, 10);
        }
        else if (strcmp(name, "daemon") == 0)
        {
            int b = strtol(value, NULL, 10);
            if (strcasecmp(value, "no") == 0 ||
                strcasecmp(value, "false") == 0 ||
                b == 0)
            {
                config->foreground = 1;
            } else if (strcasecmp(value, "yes") &&
                       strcasecmp(value, "true") &&
                       b != 1)
            {
                fprintf(stderr, "Invalid value %s for option %s\n", value, name);
                return 0;
            }
        }
        else if (strcmp(name, "me") == 0)
        {
            snprintf(config->me, sizeof(config->me),
                    "%s", value);
        }
        else if (strcmp(name, "nohttp") == 0)
        {
            int b = strtol(value, NULL, 10);
            if (strcasecmp(value, "yes") == 0 ||
                strcasecmp(value, "true") == 0 ||
                b == 1)
            {
                config->nohttp = 1;
            }
            else if (strcasecmp(value, "no") &&
                     strcasecmp(value, "false") &&
                     b != 1)
            {
                fprintf(stderr, "Invalid value %s for option %s\n", value, name);
                return 0;
            }

        }
        else if (strcmp(name, "user") == 0)
        {
            config->username = strdup(value);
        }
        else
        {
            fprintf(stderr, "Unknown option %s in section %s\n", name, section);
            return 0;
        }
    }
    else if (strcmp(section, "shardcache") == 0)
    {
        if (strcmp(name, "num_workers") == 0)
        {
            config->num_workers = strtol(value, NULL, 10);
        }
        else if (strcmp(name, "evict_on_delete") == 0)
        {
            if (strcasecmp(value, "no") == 0 ||
                strcasecmp(value, "false") == 0 ||
                strcasecmp(value, "0") == 0)
            {
                config->evict_on_delete = 0;
            }
            else if (strcasecmp(value, "yes") &&
                       strcasecmp(value, "true") &&
                       strcasecmp(value, "1"))
            {
                fprintf(stderr, "Invalid value %s for option %s\n", value, name);
                return 0;
            }
        }
        else if (strcmp(name, "use_persistent_connections") == 0)
        {
            if (strcasecmp(value, "no") == 0 ||
                strcasecmp(value, "false") == 0 ||
                strcasecmp(value, "0") == 0)
            {
                config->use_persistent_connections = 0;
            }
            else if (strcasecmp(value, "yes") &&
                       strcasecmp(value, "true") &&
                       strcasecmp(value, "1"))
            {
                fprintf(stderr, "Invalid value %s for option %s\n", value, name);
                return 0;
            }
        }
        else if (strcmp(name, "cache_size") == 0)
        {
            config->cache_size = strtol(value, NULL, 10);
        }
        else if (strcmp(name, "secret") == 0)
        {
            snprintf(config->secret, sizeof(config->secret),
                    "%s", value);
        }
        else
        {
            fprintf(stderr, "Unknown option %s in section %s\n", name, section);
            return 0;
        }
    }
    else if (strcmp(section, "http") == 0)
    {
        if (strcmp(name, "num_workers") == 0)
        {
            config->num_http_workers = strtol(value, NULL, 10);
        }
        else if (strcmp(name, "access_log") == 0)
        {
            snprintf(config->access_log_file, sizeof(config->access_log_file),
                    "%s", value);
        }
        else if (strcmp(name, "error_log") == 0)
        {
            snprintf(config->error_log_file, sizeof(config->error_log_file),
                    "%s", value);
        }
        else if (strcmp(name, "basepath") == 0)
        {
            snprintf(config->basepath, sizeof(config->basepath),
                    "%s", value);
        }
        else if (strcmp(name, "baseadminpath") == 0)
        {
            snprintf(config->baseadminpath, sizeof(config->baseadminpath),
                    "%s", value);
        }
        else if (strcmp(name, "listen") == 0)
        {
            if (!config_listening_address((char *)value, config)) {
                fprintf(stderr, "Can't use the listening address %s\n", value);
                return 0;
            }

        }
        else if (strcmp(name, "acl_default") == 0)
        {
            if (strcmp(value, "allow") == 0)
            {
                config->acl_default = SHCD_ACL_ACTION_ALLOW;
            }
            else if (strcmp(value, "deny") == 0)
            {
                config->acl_default = SHCD_ACL_ACTION_DENY;
            }
            else
            {
                fprintf(stderr, "Invalid value %s for option %s (can be only  'allow' or 'deny')\n",
                        value, name);
                return 0;
            }
        }
        else
        {
            fprintf(stderr, "Unknown option %s in section %s\n", name, section);
            return 0;
        }
    }
    else if (strcmp(section, "mime-types") == 0)
    {
        if (!mime_types)
        {
            mime_types = ht_create(128, 512, free);
        }
        ht_set(mime_types, (void *)name, strlen(name), (void *)strdup(value), strlen(value)+1);
    }
    else
    {
        fprintf(stderr, "Unknown section %s\n", section);
        return 0;
    }
    return 1;
}

static int parse_nodes_string(char *str, int migration)
{
    char *copy = strdup(str);
    char *s = copy;

    int *num_nodes = migration ? &config.num_migration_nodes : &config.num_nodes;
    shardcache_node_t **nodes = migration ? &config.migration_nodes : &config.nodes;
    while (s && *s) {
        char *tok = strsep(&s, ",");
        if(tok) {
            char *label = strsep(&tok, ":");
            char *addr = tok;
            if (!addr || check_address_string(addr) != 0) {
                SHC_ERROR("Bad address format for peer: '%s'", addr);
                free(copy);
                return -1;
            }
            (*num_nodes)++;
            *nodes = realloc(*nodes, *num_nodes * sizeof(shardcache_node_t));
            shardcache_node_t *node = &(*nodes)[(*num_nodes)-1];
            snprintf(node->label, sizeof(node->label), "%s", label);
            snprintf(node->address, sizeof(node->address), "%s", addr);
        } 
    }
    free(copy);
    return 0;
}

void parse_cmdline(int argc, char **argv)
{
    int option_index = 0;

    static struct option long_options[] = {
        {"cfgfile", 2, 0, 'c'},
        {"access_log", 2, 0, 'a'},
        {"error_log", 2, 0, 'e'},
        {"basepath", 2, 0, 'b'},
        {"baseadminpath", 2, 0, 'B'},
        {"plugins_directory", 2, 0, 'd'},
        {"foreground", 0, 0, 'f'},
        {"stats_interval", 2, 0, 'i'},
        {"listen", 2, 0, 'l'},
        {"me", 2, 0, 'm'},
        {"nodes", 2, 0, 'n'},
        {"size", 2, 0, 's'},
        {"secret", 2, 0, 'S'},
        {"type", 2, 0, 't'},
        {"options", 2, 0, 'o'},
        {"verbose", 0, 0, 'v'},
        {"workers", 2, 0, 'w'},
        {"migrate", 2, 0, 'x'},
        {"nohttp", 0, 0, 'H'},
        {"nostorage", 0, 0, 'N'},
        {"user", 2, 0, 'u'},
        {"help", 0, 0, 'h'},
        {0, 0, 0, 0}
    };

    char c;
    while ((c = getopt_long (argc, argv, "a:b:B:c:d:e:fg:hHi:l:m:n:Ns:S:t:o:u:vw:x:?",
                             long_options, &option_index)))
    {
        if (c == -1) {
            break;
        }
        switch (c) {
            case 'c':
                // ignore , we already checked for the cfgfile
                // option at program startup
                break;
            case 'a':
                snprintf(config.access_log_file,
                        sizeof(config.access_log_file), "%s", optarg);
                break;
            case 'b':
                // skip leading '/'s
                while (*optarg == '/')
                    optarg++;
                snprintf(config.basepath,
                        sizeof(config.basepath), "%s", optarg);
                break;
            case 'd':
                snprintf(config.plugins_dir,
                        sizeof(config.plugins_dir), "%s", optarg);
                break;
            case 'B':
                // skip leading '/'s
                while (*optarg == '/')
                    optarg++;
                snprintf(config.baseadminpath,
                        sizeof(config.baseadminpath), "%s", optarg);
                break;
            case 'e':
                snprintf(config.error_log_file,
                        sizeof(config.error_log_file), "%s", optarg);
                break;
            case 'f':
                config.foreground = 1;
                break;
            case 'H':
                config.nohttp = 1;
                break;
            case 'i':
                config.stats_interval = strtol(optarg, NULL, 10);
                break;
            case 'l':
                if (!config_listening_address(optarg, &config)) {
                    usage(argv[0], "Can't use the listening address : %s\n", optarg);
                }
                break;
            case 'm':
                snprintf(config.me,
                        sizeof(config.me), "%s", optarg);
                break;
            case 'n':
                // first reset the actual nodes configuration
                // (which might come from the cfg file)
                if (config.nodes) {
                    free(config.nodes);
                    config.nodes = NULL;
                }
                config.num_nodes = 0;
                if (parse_nodes_string(optarg, 0) != 0) {
                    usage(argv[0], "Bad format : '%s'", optarg);
                }
                break;
            case 'N':
                config.nostorage = 1;
                break;
            case 's':
                config.cache_size = strtol(optarg, NULL, 10);
                break;
            case 'S':
                snprintf(config.secret,
                        sizeof(config.secret), "%s", optarg);
                break;
            case 't':
                snprintf(config.storage_type,
                        sizeof(config.storage_type), "%s", optarg);
                break;
            case 'o':
                snprintf(config.storage_options,
                        sizeof(config.storage_options), "%s", optarg);
                break;
            case 'u':
                config.username = strdup(optarg);
                break;
            case 'v':
                config.loglevel++;
                break;
            case 'w':
                config.num_workers = strtol(optarg, NULL, 10);
                break;
            case 'x':
                // first reset the actual migration_nodes configuration
                // (which might come from the cfg file)
                if (config.migration_nodes) {
                    free(config.migration_nodes);
                    config.migration_nodes = NULL;
                }
                config.num_migration_nodes = 0;
                if (parse_nodes_string(optarg, 1) != 0) {
                    usage(argv[0], "Bad format : '%s'", optarg);
                }
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
}

int main(int argc, char **argv)
{
    int i;

    char *cfgfile = NULL;
    struct passwd *pw = NULL;

    for (i = 1; i < argc-1; i++) {
        if (strcmp(argv[i], "-c") == 0)
        {
            cfgfile = argv[i+1];
            break;
        } else if (strncmp(argv[i], "--cfgfile=", 10) == 0) {
            cfgfile = argv[i]+10;
            break;
        }
    }

    if (cfgfile) {
        int rc = ini_parse(cfgfile, config_handler, (void *)&config);
        if (rc != 0) {
            usage(argv[0], "Can't parse configuration file %s (line %d)\n",
                    cfgfile, rc);
        }
    }

    // options provided on cmdline override those defined in the config file
    parse_cmdline(argc, argv);

    if (!config.num_nodes || !config.nodes) {
        usage(argv[0], "Configuring 'nodes' is mandatory!");
    }

    if (!config.me[0]) {
        usage(argv[0], "Configuring 'me' is mandatory!");
    }

    // check if me matches one of the nodes
    int me_check = 0;
    for (i = 0; i < config.num_nodes; i++) {
        int is_me = (strcmp(config.me, config.nodes[i].label) == 0);
        if (is_me) {
            me_check = 1;
        } else if (config.nodes[i].address[0] == '*') {
            fprintf(stderr, "address wildcard '*' is allowed only for the local node (me)\n");
            exit(-1);
        }
    }

    if (!me_check) {
        // 'me' not found among peers, perhaps a migration will happen
        // and we are one the the new peers ?
        for (i = 0; i < config.num_migration_nodes; i++) {
            if (strcmp(config.me, config.migration_nodes[i].label) == 0) {
                me_check = 1;
                break;
            }
        }
        if (!me_check) // no it's really a misconfiguration
            usage(argv[0], "'me' MUST match the label of one of the configured nodes");
    }
 
    // go daemon if we have to
    if (!config.foreground) {
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

    shardcache_log_init("shardcached", config.loglevel);

    shcd_storage_t *st = NULL;
    if (!config.nostorage) {
        st = shcd_storage_init(config.storage_type,
                                               config.storage_options,
                                               config.plugins_dir);
        if (!st) {
            SHC_ERROR("Can't initialize the storage subsystem");
            exit(-1);
        }
    }

    SHC_DEBUG("Starting the shardcache engine with %d workers", config.num_workers);
    shardcache_t *cache = shardcache_create(config.me,
                                            config.nodes,
                                            config.num_nodes,
                                            st ? shcd_storage_get(st) : NULL,
                                            config.secret,
                                            config.num_workers,
                                            config.cache_size);

    if (!cache) {
        SHC_ERROR("Can't initialize the shardcache engine");
        exit(-1);
    }

    shardcache_evict_on_delete(cache, config.evict_on_delete);

    // initialize the mongoose callbacks descriptor
    struct mg_callbacks shardcached_callbacks = {
        .begin_request = shardcached_request_handler
    };

    char http_workers[6];
    snprintf(http_workers, sizeof(http_workers), "%d", config.num_http_workers);
    const char *mongoose_options[] = { "listening_ports", config.listen_address,
                                       "access_log_file", config.access_log_file,
                                       "error_log_file",  config.error_log_file,
                                       "num_threads",     http_workers,
                                        NULL };

    // let's start mongoose
    struct mg_context *ctx = NULL;

    if (config.nohttp) {
        SHC_NOTICE("HTTP subsystem has been administratively disabled");
    } else {
        ctx = mg_start(&shardcached_callbacks,
                       cache,
                       mongoose_options);
    }

    int rc = 0;
    /* lose root privileges if we have them */
    if (getuid() == 0 || geteuid() == 0) {
        if (config.username == 0 || *config.username == '\0') {
            fprintf(stderr, "can't run as root without the -u switch\n");
            rc = -99;
            goto __exit;
        }
        if ((pw = getpwnam(config.username)) == 0) {
            fprintf(stderr, "can't find the user %s to switch to\n",
                    config.username);
            rc = -99;
            goto __exit;
        }
        if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0) {
            fprintf(stderr, "failed to assume identity of user %s\n",
                    config.username);
            rc = -99;
            goto __exit;
        }
    }

    if (config.migration_nodes)
        shardcache_migration_begin(cache, config.migration_nodes, config.num_migration_nodes, 1);

    if (ctx || config.nohttp) {
        shardcached_run(cache, config.stats_interval);
    } else {
        SHC_ERROR("Can't start the http subsystem");
    }

__exit:
    SHC_NOTICE("exiting");

    if (ctx)
        mg_stop(ctx);

    if (http_acl)
        shcd_acl_destroy(http_acl);

    if (mime_types)
        ht_destroy(mime_types);

    shardcache_destroy(cache);

    if (st)
        shcd_storage_destroy(st);

    if (config.username)
        free(config.username);

    free(config.nodes);

    exit(rc);
}
