#define _GNU_SOURCE
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <riak.h>
#include <shardcache.h>
#include <pthread.h>
#include <errno.h>
#include <fbuf.h>
#include <queue.h>

#define ST_HOST_DEFAULT            "localhost"
#define ST_PORT_DEFAULT            "8087"
#define ST_NUM_CONNECTIONS_DEFAULT 5

int storage_version = SHARDCACHE_STORAGE_API_VERSION;

typedef struct {
    char *hostname;
    char *port;
    int num_connections;
    unsigned int connect_timeout;
    unsigned int read_timeout;
    queue_t *connections;
    riak_config *riak_cfg;
} storage_riak_t;

static void
storage_riak_set_defaults(storage_riak_t *st)
{
    if (!st->hostname)
        st->hostname = strdup(ST_HOST_DEFAULT);

    if (!st->port)
        st->port = strdup(ST_PORT_DEFAULT);

    if (!st->num_connections)
        st->num_connections = ST_NUM_CONNECTIONS_DEFAULT;

    if (!st->riak_cfg) {
        riak_error err = riak_config_new_default(&st->riak_cfg);
        if (err) {
            SHC_ERROR("Can't create a new riak config : %s", riak_strerror(err));
        }
    }


}

static void
storage_riak_config(storage_riak_t *st, const char **options)
{
    if (options) {
        while (*options) {
            char *key = (char *)*options++;
            if (!*key)
                break;
            char *value = NULL;
            if (*options) {
                value = (char *)*options++;
            } else {
                SHC_ERROR("Odd element in the options array");
                continue;
            }
            if (key && value) {
                if (strcmp(key, "hostname") == 0) {
                    st->hostname = strdup(value);
                } else if (strcmp(key, "port") == 0) {
                    st->port = strdup(value);
                } else if (strcmp(key, "num_connections") == 0) {
                    st->num_connections = strtol(value, NULL, 10);
                } else if (strcmp(key, "connect_timeout") == 0) {
                    st->connect_timeout = strtol(value, NULL, 10);
                } else if (strcmp(key, "read_timeout") == 0) {
                    st->read_timeout = strtol(value, NULL, 10);
                } else {
                    SHC_ERROR("Unknown option name %s", key);
                }
            }
        }
    }
    storage_riak_set_defaults(st);
}

static inline riak_connection *
st_init_connection(storage_riak_t *st)
{
    riak_connection *conn;
    SHC_DEBUG("Connecting to riak server %s:%s", st->hostname, st->port);
    riak_error err = riak_connection_new(st->riak_cfg, &conn, st->hostname, st->port, NULL);
    if (err)
        SHC_ERROR("Can't create a new riak connection : %s", riak_strerror(err));

    return conn;
}

static inline riak_connection *
st_get_connection(storage_riak_t *st)
{
    int rc = 0;
    int index = 0;
    int retries = 0;

    riak_connection *riak = queue_pop_left(st->connections);
    if (riak) {
        // we got a cached connection
        // let's check if it's still alive
        riak_error err = riak_ping(riak);
        if (err == ERIAK_OK)
            return riak;

        riak_connection_free(&riak);
        riak = NULL;
    }
   
    return st_init_connection(st);
}

static void
st_release_connection(storage_riak_t *st, riak_connection *riak)
{
    if (queue_count(st->connections) >= st->num_connections)
        riak_connection_free(&riak);
    else
        queue_push_right(st->connections, riak);
}

static int
st_fetch(void *key, size_t klen, void **value, size_t *vlen, void *priv)
{
    storage_riak_t *st = (storage_riak_t *)priv;

    char *keystr = calloc(1, klen+1);
    memcpy(keystr, key, klen);
    char *tofree = keystr;

    char *bucket = strsep(&keystr, ",");
    char *keybin = keystr;

    if (!bucket) {
        char k[klen+1];
        snprintf(k, sizeof(k), "%s", key);
        SHC_WARNING("Unsupported key : %s", k);
        free(tofree);
        return -1;
    }

    riak_connection *riak = st_get_connection(st);
    if (!riak) {
        free(tofree);
        return -1;
    }

    riak_get_options *get_options = riak_get_options_new(st->riak_cfg);
    if (get_options == NULL) {
        SHC_ERROR("Could not allocate a Riak Get Options");
        return 1;
    }
    riak_get_options_set_basic_quorum(get_options, RIAK_TRUE);
    riak_get_options_set_r(get_options, 2);
    riak_get_response *get_response = NULL;

    riak_binary *bucket_bin = riak_binary_copy_from_string(st->riak_cfg, bucket);
    riak_binary *key_bin = riak_binary_copy_from_string(st->riak_cfg, keybin);

    riak_error err = riak_get(riak, NULL, bucket_bin, key_bin, get_options, &get_response);
    if (err == ERIAK_OK) {
        if (riak_get_is_found(get_response)) {
            riak_object **objects = riak_get_get_content(get_response);
            riak_binary *bin = riak_object_get_value(objects[0]); // XXX - HC to access the first object
            riak_size_t size = riak_binary_len(bin);
            riak_uint8_t *data = riak_binary_data(bin);
            if (size && data) {
                if (vlen)
                    *vlen = size;
                if (value) {
                    *value = malloc(size);
                    memcpy(*value, data, size);
                }
                    
            }
        }
    } else {
        SHC_ERROR("Get Problems [%s]\n", riak_strerror(err));
        riak_connection_free(&riak);
        free(tofree);
        riak_binary_free(st->riak_cfg, &bucket_bin);
        riak_binary_free(st->riak_cfg, &key_bin);
        return -1;
    }

    if (get_response)
        riak_get_response_free(st->riak_cfg, &get_response);

    if (get_options)
        riak_get_options_free(st->riak_cfg, &get_options);

    st_release_connection(st, riak);

    free(tofree);
    riak_binary_free(st->riak_cfg, &bucket_bin);
    riak_binary_free(st->riak_cfg, &key_bin);

    return 0;
}

static void
storage_riak_destroy(storage_riak_t *st)
{
    int i;
    if (st->connections)
        queue_destroy(st->connections);

    if (st->hostname)
        free(st->hostname);

    if (st->port)
        free(st->port);

    if (st->riak_cfg)
        riak_config_free(&st->riak_cfg);

    free(st);
}

void
storage_destroy(void *priv)
{
    storage_riak_t *st = (storage_riak_t *)priv;
    if (st)
        storage_riak_destroy(st);

}

static void
riak_connection_free_wrapper(riak_connection *riak)
{
    riak_connection_free(&riak);
}

int
storage_init(shardcache_storage_t *storage, const char **options)
{
    storage_riak_t *st = calloc(1, sizeof(storage_riak_t));

    storage_riak_config(st, options);

    st->connections = queue_create();
    queue_set_free_value_callback(st->connections,
        (queue_free_value_callback_t)riak_connection_free_wrapper);

    int i;
    for (i = 0; i < st->num_connections; i++) {
        riak_connection *riak = st_init_connection(st);
        if (!riak) {
            storage_riak_destroy(st);
            return -1;
        }
        queue_push_right(st->connections, riak);
    }

    storage->fetch  = st_fetch;
    storage->shared = 1;
    storage->global = 1;
    storage->priv   = st;

    return 0;
}

