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
#include "ini.h"
#include "acl.h"

#define SHARDCACHED_ADDRESS_DEFAULT "4321"
#define SHARDCACHED_LOGLEVEL_DEFAULT 0
#define SHARDCACHED_SECRET_DEFAULT "default"
#define SHARDCACHED_STORAGE_TYPE_DEFAULT "mem"
#define SHARDCACHED_STORAGE_OPTIONS_DEFAULT ""
#define SHARDCACHED_STATS_INTERVAL_DEFAULT 0
#define SHARDCACHED_NUM_WORKERS_DEFAULT 10
#define SHARDCACHED_NUM_HTTP_WORKERS_DEFAULT 10
#define SHARDCACHED_PLUGINS_DIR_DEFAULT "./"
#define SHARDCACHED_ACCESS_LOG_DEFAULT "./shardcached_access.log"
#define SHARDCACHED_ERROR_LOG_DEFAULT "./shardcached_error.log"

#define SHARDCACHED_USERAGENT_SIZE_THRESHOLD 16
#define SHARDCACHED_MAX_SHARDS 1024

#define ADDR_REGEXP "^[a-z0-9_\\.\\-]+(:[0-9]+)?$"

static pthread_cond_t exit_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t exit_lock = PTHREAD_MUTEX_INITIALIZER;
static int should_exit = 0;
static shcd_acl_t *http_acl = NULL;
static hashtable_t *mime_types = NULL;

typedef struct {
    char me[256];
    char basepath[256];
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
} shardcached_config_t;

static shardcached_config_t config = {
    .me = "",
    .basepath = "",
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
    .cache_size = 1<<20, // 1 GB
    .evict_on_delete = 1,
    .acl_default = SHCD_ACL_ACTION_ALLOW,
    .nohttp = 0
};

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
           "    -n <nodes>            list of nodes participating in the shardcache in the form : 'label:address:port,label2:address2:port2'\n"
           "    -m me                 the label of this node, to identify it among the ones participating in the shardcache\n"
           "    -s                    cache size in bytes (defaults to : '%d')\n"
           "    -S                    shared secret used for message signing (defaults to : '%s')\n"
           "    -t <type>             storage type (available are : 'mem' and 'fs' (defaults to '%s')\n"
           "    -o <options>          comma-separated list of storage options (defaults to '%s')\n"
           "    -v                    increase the log level (can be passed multiple times)\n"
           "    -w <num_workers>      number of shardcache worker threads (defaults to '%d')\n"
           "    -x <nodes>            new list of nodes to migrate the shardcache to. The format to use is the same of the '-n' option\n"
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
           , 1<<20
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
    DEBUG1("Signal %d received ... doing nothing\n", sig);
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
        char keystr[klen+1];
        memcpy(keystr, index->items[i].key, klen);
        keystr[klen] = 0;
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
    if (do_html) {
        fbuf_printf(buf,
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
        fbuf_printf(buf,
                    "active_http_requests;%d\r\n",
                    __sync_fetch_and_add(&shcd_active_requests, 0));
    }


    shardcache_counter_t *counters;
    int ncounters = shardcache_get_counters(cache, &counters);

    int i;
    for (i = 0; i < ncounters; i++) {
        if (do_html)
            fbuf_printf(buf,
                        "<tr bgcolor='#ffffff'><td>%s</td><td>%u</td>",
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

static void shardcached_handle_get_request(shardcache_t *cache, struct mg_connection *conn, char *key)
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
        fbuf_t buf = FBUF_STATIC_INITIALIZER;
        int do_html = (!request_info->query_string ||
                       !strstr(request_info->query_string, "nohtml=1"));

        shardcached_build_index_response(&buf, do_html, cache);

        mg_printf(conn, "HTTP/1.0 200 OK\r\n"
                        "Content-Type: text/%s\r\n"
                        "Content-length: %d\r\n"
                        "Server: shardcached\r\n"
                        "Connection: Close\r\n\r\n%s",
                        do_html ? "html" : "plain",
                        fbuf_used(&buf),
                        fbuf_data(&buf));

        fbuf_destroy(&buf);
    } else {
        size_t vlen = 0;
        void *value = shardcache_get(cache, key, strlen(key), &vlen);
        if (value) {
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
            mg_printf(conn, "HTTP/1.0 200 OK\r\n"
                            "Content-Type: %s\r\n"
                            "Content-length: %d\r\n"
                            "Server: shardcached\r\n"
                            "Connection: Close\r\n\r\n", mtype, (int)vlen);
            mg_write(conn, value, vlen);
            free(value);
        } else {
            mg_printf(conn, "HTTP/1.0 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found");
        }
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

    __sync_add_and_fetch(&shcd_active_requests, 1);

    while (*key == '/' && *key)
        key++;
    if (config.basepath) {
        if (strncmp(key, config.basepath, strlen(config.basepath)) != 0) {
            ERROR("Bad request uri : %s", request_info->uri);
            mg_printf(conn, "HTTP/1.0 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found");
            __sync_sub_and_fetch(&shcd_active_requests, 1);
            return 1;
        }
        key += strlen(config.basepath);
    }
    while (*key == '/' && *key)
        key++;

    if (*key == 0) {
        mg_printf(conn, "HTTP/1.0 404 Not Found\r\nContent-Length 9\r\n\r\nNot Found");
        __sync_sub_and_fetch(&shcd_active_requests, 1);
        return 1;
    }

    // handle the actual GET/PUT/DELETE request
    if (strncasecmp(request_info->request_method, "GET", 3) == 0)
        shardcached_handle_get_request(cache, conn, key);
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
            NOTICE("Shardcache stats: %s\n", fbuf_data(&out));
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
        ERROR("Can't compile regexp %s: %s\n", ADDR_REGEXP, errbuf);
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
        ERROR("Invalid acl string %s", aclstr);
        return -1;
    }
    if (strcasecmp(action_string, "allow") == 0) {
        action = SHCD_ACL_ACTION_ALLOW;
    } else if (strcasecmp(action_string, "deny") == 0) {
        action = SHCD_ACL_ACTION_DENY;
    } else {
        ERROR("Invalid acl action %s (can be 'allow' or 'deny')", action_string);
        return -1;
    }

    char *method_string = strsep(&p, ":");
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
        ERROR("Invalid acl method %s (can be 'GET' or 'PUT' or 'DELETE')", method_string);
        return -1;
    }
    int ret = 0;
    struct in_addr ip;
    uint32_t mask = 0xffff;
    char *ipaddr_string = strsep(&p, "/");
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
            ERROR("Bad ip address format %s (%s)\n", ipaddr_string);
            return -1;
        }
    }
    return shcd_acl_add(http_acl, pattern, action, method, ntohl(ip.s_addr), mask);
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
            ERROR("Errors configuring acl : %s = %s", name, value);
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
                ERROR("Invalid value %s for option %s", value, name);
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
                ERROR("Invalid value %s for option %s", value, name);
                return 0;
            }

        }
        else
        {
            ERROR("Unknown option %s in section %s", name, section);
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
            int b = strtol(value, NULL, 10);
            if (strcasecmp(value, "yes") == 0 ||
                strcasecmp(value, "true") == 0 ||
                b == 1)
            {
                config->evict_on_delete = b;
            } else if (strcasecmp(value, "no") &&
                       strcasecmp(value, "false") &&
                       b != 0)
            {
                ERROR("Invalid value %s for option %s", value, name);
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
            ERROR("Unknown option %s in section %s", name, section);
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
        else if (strcmp(name, "listen") == 0)
        {
            if (strncmp(value, "*:", 2) == 0)
                value += 2;
            snprintf(config->listen_address, sizeof(config->listen_address),
                    "%s", value);
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
                ERROR("Invalid value %s for option %s (can be only  'allow' or 'deny')",
                        value, name);
                return 0;
            }
        }
        else
        {
            ERROR("Unknown option %s in section %s", name, section);
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
        ERROR("Unknown section %s", section);
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
            if (check_address_string(addr) != 0) {
                ERROR("Bad address format for peer: '%s'", addr);
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
        {"base", 2, 0, 'b'},
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
        {"help", 0, 0, 'h'},
        {0, 0, 0, 0}
    };

    char c;
    while ((c = getopt_long (argc, argv, "a:b:c:d:fg:hHi:l:m:n:s:S:t:o:vw:x:?",
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
                if (strncmp(optarg, "*:", 2) == 0)
                    optarg += 2;
                snprintf(config.listen_address,
                        sizeof(config.listen_address), "%s", optarg);
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
        if (strcmp(config.me, config.nodes[i].label) == 0) {
            me_check = 1;
            break;
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

    log_init("shardcached", config.loglevel);

    shcd_storage_t *st = shcd_storage_init(config.storage_type,
                                           config.storage_options,
                                           config.plugins_dir);
    if (!st) {
        ERROR("Can't initialize the storage subsystem");
        exit(-1);
    }

    DEBUG("Starting the shardcache engine with %d workers", config.num_workers);
    shardcache_t *cache = shardcache_create(config.me,
                                            config.nodes,
                                            config.num_nodes,
                                            shcd_storage_get(st),
                                            config.secret,
                                            config.num_workers,
                                            config.cache_size,
                                            config.evict_on_delete);

    if (!cache) {
        ERROR("Can't initialize the shardcache engine");
        exit(-1);
    }

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
        NOTICE("HTTP subsystem has been administratively disabled");
    } else {
        ctx = mg_start(&shardcached_callbacks,
                       cache,
                       mongoose_options);
    }

    if (config.migration_nodes)
        shardcache_migration_begin(cache, config.migration_nodes, config.num_migration_nodes, 1);

    if (ctx || config.nohttp) {
        shardcached_run(cache, config.stats_interval);
    } else {
        ERROR("Can't start the http subsystem");
    }

    NOTICE("exiting");

    if (ctx)
        mg_stop(ctx);

    if (http_acl)
        shcd_acl_destroy(http_acl);

    if (mime_types)
        ht_destroy(mime_types);

    shardcache_destroy(cache);

    shcd_storage_destroy(st);

    free(config.nodes);

    exit(0);
}
