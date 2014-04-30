#include <stdlib.h>
#include <string.h>
#include <hiredis.h>
#include <shardcache.h>

#ifdef __MACH__
#include <libkern/OSAtomic.h>
#define SPIN_LOCK(__mutex)    OSSpinLockLock(__mutex)
#define SPIN_TRYLOCK(__mutex) OSSpinLockTry(__mutex)
#define SPIN_UNLOCK(__mutex)  OSSpinLockUnlock(__mutex)
#else
#define SPIN_LOCK(__mutex)    pthread_spin_lock(__mutex)
#define SPIN_TRYLOCK(__mutex) pthread_spin_trylock(__mutex)
#define SPIN_UNLOCK(__mutex)  pthread_spin_unlock(__mutex)
#endif

#define REDIS_PORT_DEFAULT 6379
#define REDIS_HOST_DEFAULT "localhost"
#define REDIS_NUM_CONNECTIONS_DEFAULT 5

typedef struct {
    redisContext *context;
#ifdef __MACH__
    OSSpinLock lock;
#else
    pthread_spinlock_t lock;
#endif
    int initialized;
} redis_connection_t;

typedef struct {
    char *host;
    int port;
    int num_connections;
    int connection_index;
    redis_connection_t *connections;
} storage_redis_t;

static void
parse_options(storage_redis_t *st, const char **options)
{
    while (options && *options) {
        char *key = (char *)*options++;
        char *value = NULL;

        if (!*key)
            break;

        if (*options) { 
            value = (char *)*options++;
        } else {
            fprintf(stderr, "Odd element in the options array\n");
            break;
        }

        if (key && value) {
            if (strcmp(key, "host") == 0) {
                st->host = strdup(value);
            } else if (strcmp(key, "port") == 0) {
                st->port = strtol(value, NULL, 10);
            } else if (strcmp(key, "num_connections") == 0) {
                st->num_connections = strtol(value, NULL, 10);
            } else {
                fprintf(stderr, "Unknown option name %s\n", key);
            }
        }
    }
}

static void
st_clear_connection(storage_redis_t *st, redis_connection_t *c)
{
    if (c->context)
        redisFree(c->context);
    c->context = NULL;
    if (c->initialized) {
#ifndef __MACH__
        pthread_spin_destroy(&c->lock);
#endif
        c->initialized = 0;
    }
}

static int
st_init_connection(storage_redis_t *st, redis_connection_t *c)
{
    c->context = redisConnect(st->host, st->port);
    if (c->context && c->context->err) {
        fprintf(stderr, "Redis error: %s\n", c->context->errstr);
        redisFree(c->context);
        c->context = NULL;
        return -1;
    }
    if (!c->initialized) {
#ifndef __MACH__
        pthread_spin_init(&c->lock, 0);
#endif
        c->initialized = 1;
    }
    return 0;
}

static redis_connection_t *
st_get_connection(storage_redis_t *st)
{
    int rc = 0;
    int index = 0;
    redis_connection_t *c = NULL;
    int retries = 0;
    do {
        index = __sync_fetch_and_add(&st->connection_index, 1)%st->num_connections;
        c = &st->connections[index];
        rc = SPIN_TRYLOCK(&c->lock); 
        if (retries++ == 100) {
            // ok .. it's too busy
            fprintf(stderr, "Can't acquire any connection lock\n");
            return NULL;
        }
    } while (rc != 0);

    if (!c->context) {
        if (st_init_connection(st, c) != 0) {
            SPIN_UNLOCK(&c->lock);
            return NULL;
        }
    } else {
        redisReply *resp = redisCommand(c->context, "PING");
        if (!resp || resp->type != REDIS_REPLY_STRING || strcmp(resp->str, "PONG") != 0) {
            // try refreshing the connection
            redisFree(c->context);
            if (st_init_connection(st, c) != 0) {
                SPIN_UNLOCK(&c->lock);
                c = NULL;
            }
        }
        if (resp)
            freeReplyObject(resp);
    }
    return c;
}

static int st_fetch(void *key, size_t klen, void **value, size_t *vlen, void *priv)
{
    storage_redis_t *st = (storage_redis_t *)priv;

    redis_connection_t *c = st_get_connection(st);
    if (!c) {
        return -1;
    }
    redisReply *resp = redisCommand(c->context, "GET %b", key, klen);
    SPIN_UNLOCK(&c->lock);
    if (!resp || resp->type != REDIS_REPLY_STRING) {
        // TODO - Error messages
        if (resp)
            freeReplyObject(resp);
        return -1;
    }
    if (value) {
        *value = malloc(resp->len);
        memcpy(*value, resp->str, resp->len);
    }
    if (vlen)
        *vlen = resp->len;

    freeReplyObject(resp);

    return 0;
}

static int st_store(void *key, size_t klen, void *value, size_t vlen, void *priv)
{
    storage_redis_t *st = (storage_redis_t *)priv;

    redis_connection_t *c = st_get_connection(st);
    if (!c) {
        return -1;
    }

    redisReply *resp = redisCommand(c->context, "SET %b %b", key, klen, value, vlen);
    SPIN_UNLOCK(&c->lock);
    if (!resp || resp->type != REDIS_REPLY_STRING || strcmp(resp->str, "OK") != 0) {
        // TODO - Error messages
        if (resp)
            freeReplyObject(resp);
        return -1;
    }
    freeReplyObject(resp);
    return 0;
}

static int st_remove(void *key, size_t klen, void *priv)
{
    storage_redis_t *st = (storage_redis_t *)priv;

    redis_connection_t *c = st_get_connection(st);
    if (!c) {
        return -1;
    }

    redisReply *resp = redisCommand(c->context, "DEL %b", key, klen);
    SPIN_UNLOCK(&c->lock);
    if (!resp || resp->type != REDIS_REPLY_INTEGER || resp->integer != 1) {
        // TODO - Error messages
        if (resp)
            freeReplyObject(resp);
        return -1;
    }
    freeReplyObject(resp);
    return 0;
}
 
static int st_exist(void *key, size_t klen, void *priv)
{
    storage_redis_t *st = (storage_redis_t *)priv;

    redis_connection_t *c = st_get_connection(st);
    if (!c) {
        return -1;
    }

    redisReply *resp = redisCommand(c->context, "DEL %b", key, klen);
    SPIN_UNLOCK(&c->lock);
    if (!resp || resp->type != REDIS_REPLY_INTEGER || resp->integer != 1) {
        // TODO - Error messages
        if (resp)
            freeReplyObject(resp);
        return -1;
    }
    freeReplyObject(resp);
    return 0;
}

static size_t st_count(void *priv)
{
    storage_redis_t *st = (storage_redis_t *)priv;

    redis_connection_t *c = st_get_connection(st);
    if (!c) {
        return 0;
    }

    redisReply *resp = redisCommand(c->context, "DBSIZE");
    SPIN_UNLOCK(&c->lock);
    if (!resp || resp->type != REDIS_REPLY_INTEGER) {
        // TODO - Error messages
        if (resp)
            freeReplyObject(resp);
        return 0;
    }
    size_t n = resp->integer;
    freeReplyObject(resp);
    return n;
}

static size_t st_index(shardcache_storage_index_item_t *index, size_t isize, void *priv)
{
    storage_redis_t *st = (storage_redis_t *)priv;

    redis_connection_t *c = st_get_connection(st);
    if (!c) {
        return 0;
    }

    redisReply *resp = redisCommand(c->context, "KEYS *");
    SPIN_UNLOCK(&c->lock);
    if (!resp || resp->type != REDIS_REPLY_ARRAY) {
        // TODO - Error messages
        if (resp)
            freeReplyObject(resp);
        return 0;
    }
    size_t n = resp->elements;
    if (n > isize)
        n = isize;
    int cnt = 0;
    int i;
    for (i = 0; i < n; i++) {
        redisReply *r = resp->element[i];
        if (r->type == REDIS_REPLY_STRING) { 
            shardcache_storage_index_item_t *item = &index[cnt++];
            item->key = malloc(r->len);
            memcpy(item->key, r->str, r->len);
            item->klen = r->len;
            item->vlen = 0; // length is not returned by the redis KEYS command
        } else {
            // TODO - Error Message
        }
    }
    freeReplyObject(resp);
    return cnt;
}

static void
storage_redis_destroy(storage_redis_t *st)
{
    int i;
    for (i = 0; i < st->num_connections; i++) {
        redis_connection_t *c = &st->connections[i];
    	st_clear_connection(st, c);
    }
    free(st->connections);
    if (st->host)
        free(st->host);
    free(st);
}

void
storage_destroy(shardcache_storage_t *storage)
{
    storage_redis_t *st = (storage_redis_t *)storage->priv;
    storage_redis_destroy(st);
    free(storage);
}

shardcache_storage_t *
storage_create(const char **options)
{
    storage_redis_t *st = calloc(1, sizeof(storage_redis_t));
 
    if (options)
        parse_options(st, options);

    if (!st->host)
        st->host = strdup(REDIS_HOST_DEFAULT);

    if (!st->port)
        st->port = REDIS_PORT_DEFAULT;

    if (!st->num_connections)
        st->num_connections = REDIS_NUM_CONNECTIONS_DEFAULT;

    st->connections = calloc(sizeof(redis_connection_t), st->num_connections);

    int i;
    for (i = 0; i < st->num_connections; i++) {
        redis_connection_t *c = &st->connections[i];
        if (st_init_connection(st, c) != 0) {
            storage_redis_destroy(st);
            return NULL;
        }
    }

    shardcache_storage_t *storage = calloc(1, sizeof(shardcache_storage_t));
    storage->fetch  = st_fetch;
    storage->store  = st_store;
    storage->remove = st_remove;
    storage->count  = st_count;
    storage->index  = st_index;
    storage->priv = st;

    return storage;
}


