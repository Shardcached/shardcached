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

#include "shcd_http.h"

#include <pthread.h>
#include <regex.h>

#include <fbuf.h>
#include <hashtable.h>
#include <shardcache.h>

#include "storage.h"
#include "ini.h"

#define SHARDCACHED_VERSION "1.0"

#ifndef BUILD_INFO
#define BUILD_INFO
#endif

#define STRINGIFY(x)   #x
#define X_STRINGIFY(x) STRINGIFY(x)

const char *SHARDCACHED_BUILD_INFO = X_STRINGIFY(BUILD_INFO);

#define SHARDCACHED_ADDRESS_DEFAULT "4321"
#define SHARDCACHED_LOGLEVEL_DEFAULT 0
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
#define SHARDCACHED_PIDFILE_DEFAULT NULL

#define SHARDCACHED_USERAGENT_SIZE_THRESHOLD 16
#define SHARDCACHED_MAX_SHARDS 1024

#define MAX(a, b) ( (a) > (b) ? (a) : (b) )

static int should_exit = 0;
static int should_reset = 0;
static shcd_acl_t *http_acl = NULL;
static hashtable_t *mime_types = NULL;
static shcd_storage_t *st = NULL;

typedef struct {
    char me[256];
    char basepath[256];
    char baseadminpath[256];
    int foreground;
    int loglevel;
    char listen_address[256];
    shardcache_node_t **nodes;
    int  num_nodes;
    shardcache_node_t **migration_nodes;
    int  num_migration_nodes;
    char secret[1024];
    char storage_type[256];
    char storage_options[MAX_OPTIONS_STRING_LEN];
    uint32_t stats_interval;
    char plugins_dir[1024];
    int num_workers;

    size_t cache_size;
    int nohttp;
    int nostorage;

    int num_http_workers;
    char access_log_file[1024];
    shcd_acl_action_t acl_default;

    char *username;
    char *pidfile;

    // libshardcache flags
    int use_persistent_connections;
    int evict_on_delete;
    int force_caching;
    int lazy_expiration;

    // libshardcache params
    int tcp_timeout;
    int expire_time;
    int iomux_run_timeout_low;
    int iomux_run_timeout_high;
    int pipelining_max;
    int arc_mode;

    // connection pool NOOP verification timeout
    time_t conn_expire_time;
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
    .cache_size = SHARDCACHED_CACHE_SIZE_DEFAULT,
    .acl_default = SHCD_ACL_ACTION_ALLOW,
    .nohttp = 0,
    .nostorage = 0,
    .username = NULL,
    .tcp_timeout = 0, // will use the default from libshardcache
    .evict_on_delete = 1,
    .use_persistent_connections = 1,
    .force_caching = 0,
    .lazy_expiration = 0,
    .expire_time = 0,
    .iomux_run_timeout_low = 0,
    .iomux_run_timeout_high = 0,
    .pipelining_max = SHARDCACHE_SERVING_LOOK_AHEAD_DEFAULT,
    .arc_mode = SHARDCACHE_ARC_MODE_STRICT,
    .pidfile = SHARDCACHED_PIDFILE_DEFAULT,
    .conn_expire_time = SHARDCACHE_CONNECTION_EXPIRE_DEFAULT
};

static void usage(char *progname, int rc, char *msg, ...)
{
    if (msg) {
        va_list arg;
        va_start(arg, msg);
        vprintf(msg, arg);
        printf("\n");
    }

    printf("Usage: %s [OPTION]...\n"
           "Version: %s (libshardcache: %s %s) %s\n"
           "Possible options:\n"
           "    -a <access_log_file>  the path where to store the access_log file (defaults to '%s')\n"
           "    -c <config_file>      the config file to load\n"
           "    -d <plugins_path>     the path where to look for storage plugins (defaults to '%s')\n"
           "    -f                    run in foreground\n"
           "    -F                    force caching\n"
           "    -H                    disable the HTTP frontend\n"
           "    -i <interval>         change the time interval (in seconds) used to report internal stats via syslog (defaults to '%d')\n"
           "    -l <ip_address:port>  ip_address:port where to listen for incoming http connections\n"
           "    -L                    enable lazy expiration\n"
           "    -M                    sets the arc_mode in libshardcache to 'loose' (defaults to : 'strict')\n" 
           "    -E <expire_time>      set the expiration time for cached items (defaults to: %d)\n"
           "    -e <conn_expire_time> the expiration time for a connection in the pool to trigger a NOOP (defaults to %d)\n"
           "    -r <mux_timeout_low>  set the low timeout passed to iomux_run() calls (in microsecs, defaults to: %d)\n"
           "    -R <mux_timeout_high> set the high timeout pssed to iomux_run() calls (in microsecs, defaults to: %d)\n"
           "    -b                    HTTP url basepath (optional, defaults to '')\n"
           "    -B                    HTTP url baseadminpath (optional, defaults to '')\n"
           "    -n <nodes>            list of nodes participating in the shardcache in the form : 'label:address:port,label2:address2:port2'\n"
           "    -N                    no storage subsystem, use only the internal libshardcache volatile storage\n"
           "    -m me                 the label of this node, to identify it among the ones participating in the shardcache\n"
           "    -P <pipelining_max>   the maximum amount of requests to handle ahead on the same connection while still serving a response (defaults to: %d)\n"
           "    -S                    shared secret used for message signing (defaults to : '%s')\n"
           "    -s                    cache size in bytes (defaults to : '%d')\n"
           "    -T <tcp_timeout>      tcp timeout (in milliseconds) used for connections opened by libshardcache (defaults to '%d')\n"
           "    -t <type>             storage type (available are : 'mem' and 'fs' (defaults to '%s')\n"
           "    -o <options>          comma-separated list of storage options (defaults to '%s')\n"
           "    -u <username>         assume the identity of <username> (only when run as root)\n"
           "    -v                    increase the log level (can be passed multiple times)\n"
           "    -V                    output the version number and exit\n"
           "    -w <num_workers>      number of shardcache worker threads (defaults to '%d')\n"
           "    -W <num_http_workers> number of http worker threads (defaults to '%d')\n"
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
           , SHARDCACHED_VERSION
           , LIBSHARDCACHE_VERSION
           , LIBSHARDCACHE_BUILD_INFO
           , SHARDCACHED_BUILD_INFO
           , SHARDCACHED_ACCESS_LOG_DEFAULT
           , SHARDCACHED_PLUGINS_DIR_DEFAULT
           , SHARDCACHED_STATS_INTERVAL_DEFAULT
           , SHARDCACHE_EXPIRE_TIME_DEFAULT
           , SHARDCACHE_CONNECTION_EXPIRE_DEFAULT
           , SHARDCACHE_IOMUX_RUN_TIMEOUT_LOW
           , SHARDCACHE_IOMUX_RUN_TIMEOUT_HIGH
           , SHARDCACHE_SERVING_LOOK_AHEAD_DEFAULT
           , SHARDCACHED_SECRET_DEFAULT
           , SHARDCACHED_CACHE_SIZE_DEFAULT
           , SHARDCACHE_TCP_TIMEOUT_DEFAULT
           , SHARDCACHED_STORAGE_TYPE_DEFAULT
           , SHARDCACHED_STORAGE_OPTIONS_DEFAULT
           , SHARDCACHED_NUM_WORKERS_DEFAULT
           , SHARDCACHED_NUM_HTTP_WORKERS_DEFAULT);

    exit(rc);
}

static void shardcached_stop(int sig)
{
    (void)__sync_add_and_fetch(&should_exit, 1);
}

static void shardcached_do_nothing(int sig)
{
    SHC_DEBUG1("Signal %d received ... doing nothing\n", sig);
}

static void shardcached_reset(int sig)
{
  // signal running thread, but don't set terminal flag, set reset instead
    if (st && __sync_bool_compare_and_swap(&should_reset, 0, 1)) {
        int rc = shcd_storage_reset(st);
        __sync_bool_compare_and_swap(&should_reset, 1, 0);
        if (rc != 0)
            SHC_ERROR("shardcached_reset failed with error %d", rc);
    }
}

static void shardcached_run(shardcache_t *cache, uint32_t stats_interval, shcd_storage_t *st)
{
    hashtable_t *prevcounters = ht_create(32, 256, free);
    while (!__sync_fetch_and_add(&should_exit, 0)) {
        int rc = 0;
        struct timespec to_sleep = {
            .tv_sec = MAX(stats_interval, 1),
            .tv_nsec = 0
        };
        struct timespec remainder = { 0, 0 };

        do {
            rc = nanosleep(&to_sleep, &remainder);
            if (__sync_fetch_and_add(&should_exit, 0))
                break;
            memcpy(&to_sleep, &remainder, sizeof(struct timespec));
            memset(&remainder, 0, sizeof(struct timespec));
        } while (rc != 0 && !__sync_fetch_and_add(&should_exit, 0));

        if (stats_interval) {
            shardcache_counter_t *counters;
            int ncounters = shardcache_get_counters(cache, &counters);
            int i;
            fbuf_t out = FBUF_STATIC_INITIALIZER;
            for (i = 0; i < ncounters; i++) {

                uint64_t *prev = ht_get(prevcounters,
                                        counters[i].name,
                                        strlen(counters[i].name),
                                        NULL);

                fbuf_printf(&out,
                            "%s: %llu\n",
                            counters[i].name,
                            counters[i].value - (prev ? *prev : 0));

                if (prev) {
                    *prev = counters[i].value;
                } else {
                    uint64_t *prev_value = malloc(sizeof(uint64_t));
                    *prev_value = counters[i].value;
                    ht_set(prevcounters, counters[i].name,
                           strlen(counters[i].name), prev_value,
                           sizeof(uint64_t));
                }
            }
            SHC_NOTICE("Shardcache stats: %s\n", fbuf_data(&out));
            fbuf_destroy(&out);
            free(counters);
        }
    }
    ht_destroy(prevcounters);
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
    return shcd_acl_add(http_acl, pattern, action, method, ip.s_addr, mask);
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

static inline int
parse_boolean_config_param(int *cfg_param,  const char *name, const char *value, int invert)
{
    int b = strtol(value, NULL, 10);
    if (strcasecmp(value, "yes") == 0 ||
        strcasecmp(value, "true") == 0 ||
        b == 1)
    {
        *cfg_param = invert ? 0 : 1;
    }
    else if (strcasecmp(value, "no") == 0 ||
             strcasecmp(value, "false") == 0 ||
             b == 0)
    {
        *cfg_param = invert ? 1 : 0;
    }
    else
    {
        fprintf(stderr, "Invalid value %s for option %s\n", value, name);
        return 0;
    }
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
        config->nodes = realloc(config->nodes, config->num_nodes * sizeof(shardcache_node_t *));
        config->nodes[config->num_nodes - 1] = shardcache_node_create((char *)name, (char **)&value, 1);
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
            if (!parse_boolean_config_param(&config->foreground, name, value, 1))
                return 0;
        }
        else if (strcmp(name, "me") == 0)
        {
            snprintf(config->me, sizeof(config->me),
                    "%s", value);
        }
        else if (strcmp(name, "nohttp") == 0)
        {
            if (!parse_boolean_config_param(&config->nohttp, name, value, 0))
                return 0;
        }
        else if (strcmp(name, "user") == 0)
        {
            config->username = strdup(value);
        }
        else if (strcmp(name, "pidfile") == 0)
        {
            config->pidfile = strdup(value);
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
        else if (strcmp(name, "lazy_expiration") == 0)
        {
            if (!parse_boolean_config_param(&config->lazy_expiration, name, value, 0))
                return 0;
        }
        else if (strcmp(name, "expire_time") == 0)
        {
            config->expire_time = strtol(value, NULL, 10);
        }
        else if (strcmp(name, "iomux_run_timeout_low") == 0)
        {
            config->iomux_run_timeout_low = strtol(value, NULL, 10);
        }
        else if (strcmp(name, "iomux_run_timeout_high") == 0)
        {
            config->iomux_run_timeout_high = strtol(value, NULL, 10);
        }
        else if (strcmp(name, "pipelining_max") == 0)
        {
            config->pipelining_max = strtol(value, NULL, 10);
        }
        else if (strcmp(name, "force_caching") == 0)
        {
            if (!parse_boolean_config_param(&config->force_caching, name, value, 0))
                return 0;
        }
        else if (strcmp(name, "evict_on_delete") == 0)
        {
            if (!parse_boolean_config_param(&config->evict_on_delete, name, value, 0))
                return 0;
        }
        else if (strcmp(name, "use_persistent_connections") == 0)
        {
            if (!parse_boolean_config_param(&config->evict_on_delete, name, value, 0))
                return 0;
        }
        else if (strcmp(name, "tcp_timeout") == 0)
        {
            config->tcp_timeout = strtol(value, NULL, 10);
        }
        else if (strcmp(name, "conn_expire_time") == 0)
        {
            config->conn_expire_time = strtol(value, NULL, 10);
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
        else if (strcmp(name, "arc_mode") == 0)
        {
            if (strcmp(value, "strict") == 0) {
                config->arc_mode = SHARDCACHE_ARC_MODE_STRICT;
            } else if (strcmp(value, "loose") == 0) {
                config->arc_mode = SHARDCACHE_ARC_MODE_LOOSE;
            } else {
                fprintf(stderr, "Invalid value %s for the option %s in section %s, "
                                "allowed values are : 'strict' or 'loose'\n",
                                value, name, section);
                return 0;
            }
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
        } else if (strcmp(name, "access_log") == 0)
        {
            snprintf(config->access_log_file, sizeof(config->access_log_file),
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

    //int *num_nodes = migration ? &config.num_migration_nodes : &config.num_nodes;
    int num_nodes = 0;
    shardcache_node_t **nodes = NULL;
    //migration ? &config.migration_nodes : &config.nodes;
    while (s && *s) {
        char *tok = strsep(&s, ",");
        if(tok) {
            shardcache_node_t *node = shardcache_node_create_from_string(tok);
            if (node) {
                num_nodes++;
                nodes = realloc(nodes, num_nodes * sizeof(shardcache_node_t *));
                nodes[num_nodes - 1] = node;
            } else {
                SHC_ERROR("Bad address format for peer: '%s'", tok);
                free(copy);
                if (nodes)
                    shardcache_free_nodes(nodes, num_nodes);
                return -1;
            }
        } 
    }
    free(copy);
    config.nodes = nodes;
    config.num_nodes = num_nodes;
    return 0;
}

void parse_cmdline(int argc, char **argv)
{
    int option_index = 0;

    static struct option long_options[] = {
        {"cfgfile", 2, 0, 'c'},
        {"access_log", 2, 0, 'a'},
        {"basepath", 2, 0, 'b'},
        {"baseadminpath", 2, 0, 'B'},
        {"plugins_directory", 2, 0, 'd'},
        {"conn_expire_time", 2, 0, 'e'},
        {"expire_time", 2, 0, 'E'},
        {"foreground", 0, 0, 'f'},
        {"stats_interval", 2, 0, 'i'},
        {"listen", 2, 0, 'l'},
        {"lazy_expiration", 0, 0, 'L'},
        {"me", 2, 0, 'm'},
        {"arc_mode_loose", 0, 0, 'M'},
        {"nodes", 2, 0, 'n'},
        {"pidfile", 2, 0, 'p'},
        {"mux_timeout_low", 2, 0, 'r'},
        {"mux_timeout_high", 2, 0, 'R'},
        {"size", 2, 0, 's'},
        {"secret", 2, 0, 'S'},
        {"type", 2, 0, 't'},
        {"tcp_timeout", 2, 0, 'T'},
        {"options", 2, 0, 'o'},
        {"verbose", 0, 0, 'v'},
        {"workers", 2, 0, 'w'},
        {"migrate", 2, 0, 'x'},
        {"nohttp", 0, 0, 'H'},
        {"nostorage", 0, 0, 'N'},
        {"user", 2, 0, 'u'},
        {"help", 0, 0, 'h'},
        {"version", 0, 0, 'V'},
        {0, 0, 0, 0}
    };

    char c;
    while ((c = getopt_long (argc, argv, "a:b:B:c:d:e:E:fFg:hHi:l:Lm:Mn:Np:P:r:R:s:S:t:T:o:u:vVw:x:?",
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
                config.conn_expire_time = strtol(optarg, NULL, 10);
                break;
            case 'E':
                config.expire_time = strtol(optarg, NULL, 10);
                break;
            case 'B':
                // skip leading '/'s
                while (*optarg == '/')
                    optarg++;
                snprintf(config.baseadminpath,
                        sizeof(config.baseadminpath), "%s", optarg);
                break;
            case 'f':
                config.foreground = 1;
                break;
            case 'F':
                config.force_caching = 1;
                break;
            case 'H':
                config.nohttp = 1;
                break;
            case 'i':
                config.stats_interval = strtol(optarg, NULL, 10);
                break;
            case 'l':
                if (!config_listening_address(optarg, &config)) {
                    usage(argv[0], -2, "Can't use the listening address : %s\n", optarg);
                }
                break;
            case 'L':
                config.lazy_expiration = 1;
                break;
            case 'm':
                snprintf(config.me,
                        sizeof(config.me), "%s", optarg);
                break;
            case 'M':
                config.arc_mode = SHARDCACHE_ARC_MODE_LOOSE;
                break;
            case 'n':
                // first reset the actual nodes configuration
                // (which might come from the cfg file)
                if (config.nodes) {
                    shardcache_free_nodes(config.nodes, config.num_nodes);
                    config.nodes = NULL;
                    config.num_nodes = 0;
                }
                config.num_nodes = 0;
                if (parse_nodes_string(optarg, 0) != 0) {
                    usage(argv[0], -2, "Bad format : '%s'", optarg);
                }
                break;
            case 'N':
                config.nostorage = 1;
                break;
            case 'P':
                config.pipelining_max = strtol(optarg, NULL, 10);
                break;
            case 'r':
                config.iomux_run_timeout_low = strtol(optarg, NULL, 10);
                break;
            case 'R':
                config.iomux_run_timeout_high = strtol(optarg, NULL, 10);
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
            case 'T':
                config.tcp_timeout = strtol(optarg, NULL, 10);
                break;
            case 'p':
                config.pidfile = strdup(optarg);
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
            case 'W':
                config.num_http_workers = strtol(optarg, NULL, 10);
                break;
            case 'x':
                // first reset the actual migration_nodes configuration
                // (which might come from the cfg file)
                if (config.migration_nodes) {
                    shardcache_free_nodes(config.migration_nodes, config.num_migration_nodes);
                    config.migration_nodes = NULL;
                    config.num_migration_nodes = 0;
                }
                if (parse_nodes_string(optarg, 1) != 0) {
                    usage(argv[0], -2, "Bad format : '%s'", optarg);
                }
                break;
            case 'h':
            case '?':
                usage(argv[0], 0, NULL);
                break;
            case 'V':
                printf("%s (libshardcache: %s %s) %s\n",
                       SHARDCACHED_VERSION,
                       LIBSHARDCACHE_VERSION,
                       LIBSHARDCACHE_BUILD_INFO,
                       SHARDCACHED_BUILD_INFO);
                exit(0);
            default:
                usage(argv[0], -3, "Unknown option : '-%c'", c);
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
            usage(argv[0], -2, "Can't parse configuration file %s (line %d)",
                    cfgfile, rc);
        }
    }

    // options provided on cmdline override those defined in the config file
    parse_cmdline(argc, argv);

    if (!config.num_nodes || !config.nodes)
        usage(argv[0], -2, "Configuring 'nodes' is mandatory!");

    if (!config.me[0])
        usage(argv[0], -2, "Configuring 'me' is mandatory!");

    if (config.pipelining_max < 0)
        usage(argv[0], -2, "pipelining_max MUST be a positive integer");

    if (config.iomux_run_timeout_low < 0)
        usage(argv[0], -2, "iomux_run_timeout_low MUST be 0 or a positive integer");

    if (config.iomux_run_timeout_high < 0)
        usage(argv[0], -2, "iomux_run_timeout_high MUST be 0 or a positive integer");

    shardcache_t *cache = NULL;
    shcd_http_t *http_server = NULL;

    int rc = 0;

    // go daemon if we have to
    if (!config.foreground) {
        rc = daemon(0, 0);
        if (rc != 0) {
            fprintf(stderr, "Can't go daemon: %s\n", strerror(errno));
            goto _exit;
        }
    }

    if (config.pidfile) {
        struct stat st;
        if (stat(config.pidfile, &st) == 0) {
            fprintf(stderr, "pidfile %s already exists\n", config.pidfile);
            free(config.pidfile);
            config.pidfile = NULL;
            rc = -1;
            goto _exit;
        }
        pid_t pid = getpid();
        FILE *pidfile = fopen(config.pidfile, "w");
        if (!pidfile) {
            fprintf(stderr, "Can't open pidfile %s: %s\n",
                    config.pidfile, strerror(errno));
            free(config.pidfile);
            config.pidfile = NULL;
            rc = -1;
            goto _exit;
        }
        char pidstr[32];
        snprintf(pidstr, 32, "%d", pid);
        if (!fwrite(pidstr, 1, strlen(pidstr), pidfile)) {
            fprintf(stderr, "Can't write to the pidfile %s : %s\n",
                    config.pidfile, strerror(errno));
            free(config.pidfile);
            config.pidfile = NULL;
            rc = -1;
            goto _exit;
        }
        fclose(pidfile);
    }
    signal(SIGINT, shardcached_stop);
    signal(SIGHUP, shardcached_reset);
    signal(SIGQUIT, shardcached_stop);
    signal(SIGPIPE, shardcached_do_nothing);

    shardcache_log_init("shardcached", LOG_INFO + config.loglevel);

    if (!config.nostorage) {
        st = shcd_storage_init(config.storage_type,
                config.storage_options,
                config.plugins_dir);
        if (!st) {
            SHC_ERROR("Can't initialize the storage subsystem");
            rc = -1;
            goto _exit;
        }
    }

    SHC_DEBUG("Starting the shardcache engine version %s with %d workers", LIBSHARDCACHE_VERSION, config.num_workers);
    cache = shardcache_create(config.me,
                              config.nodes,
                              config.num_nodes,
                              st ? shcd_storage_get(st) : NULL,
                              config.num_workers,
                              -1,
                              config.cache_size);

    if (!cache) {
        SHC_ERROR("Can't initialize the shardcache engine");
        rc = -1;
        goto _exit;
    }

    // set all the configurable libshardcache flags and options
    shardcache_evict_on_delete(cache, config.evict_on_delete);
    shardcache_use_persistent_connections(cache, config.use_persistent_connections);
    shardcache_force_caching(cache, config.force_caching);
    shardcache_lazy_expiration(cache, config.lazy_expiration);
    shardcache_expire_time(cache, config.expire_time);
    shardcache_iomux_run_timeout_low(cache, config.iomux_run_timeout_low);
    shardcache_iomux_run_timeout_high(cache, config.iomux_run_timeout_high);
    shardcache_serving_look_ahead(cache, config.pipelining_max);
    shardcache_arc_mode(cache, config.arc_mode);

    if (config.tcp_timeout > 0)
        shardcache_tcp_timeout(cache, config.tcp_timeout);

    if (config.conn_expire_time > 0)
        shardcache_conn_expire_time(cache, config.conn_expire_time);

    if (config.nohttp) {
        SHC_NOTICE("HTTP subsystem has been administratively disabled");
    } else {
        // initialize the http options
        const char *mongoose_options[] = { "listening_port", config.listen_address,
                                           "access_log_file", config.access_log_file,
                                            NULL };

        // and start the http subsystem
        http_server = shcd_http_create(cache,
                                       config.me,
                                       config.basepath,
                                       config.baseadminpath,
                                       http_acl,
                                       mime_types,
                                       (const char **)mongoose_options,
                                       config.num_http_workers);
        if (!http_server) {
            SHC_ERROR("Can't start the http subsystem!");
            rc = -1;
            goto _exit;
        }
    }

    /* lose root privileges if we have them */
    if (getuid() == 0 || geteuid() == 0) {
        if (config.username == 0 || *config.username == '\0') {
            fprintf(stderr, "can't run as root without the -u switch\n");
            rc = -99;
            goto _exit;
        }
        if ((pw = getpwnam(config.username)) == 0) {
            fprintf(stderr, "can't find the user %s to switch to\n",
                    config.username);
            rc = -99;
            goto _exit;
        }

        if (config.pidfile && (chown(config.pidfile, pw->pw_uid, pw->pw_gid) == -1)) {
            fprintf(stderr, "failed to chown pidfile %s\n",
                    config.pidfile);
            rc = -99;
            goto _exit;
        }

        if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0) {
            fprintf(stderr, "failed to assume identity of user %s\n",
                    config.username);
            rc = -99;
            goto _exit;
        }
    }

    if (config.migration_nodes)
        shardcache_migration_begin(cache, config.migration_nodes, config.num_migration_nodes, 1);

    if (http_server || config.nohttp) {
      shardcached_run(cache, config.stats_interval, st);
    } else {
        SHC_ERROR("Can't start the http subsystem");
    }

_exit:
    SHC_NOTICE("exiting");

    if (http_server)
        shcd_http_destroy(http_server);

    if (http_acl)
        shcd_acl_destroy(http_acl);

    if (mime_types)
        ht_destroy(mime_types);

    if (cache)
        shardcache_destroy(cache);

    if (st)
        shcd_storage_destroy(st);

    if (config.username)
        free(config.username);

    if (config.num_nodes)
        shardcache_free_nodes(config.nodes, config.num_nodes);

    if (config.pidfile) {
        unlink(config.pidfile);
        free(config.pidfile);
    }

    exit(rc);
}
