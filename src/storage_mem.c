#include <stdlib.h>
#include <string.h>
#include <shardcache.h>
#include <hashtable.h>
#include "storage_mem.h"

typedef struct {
    void *value;
    size_t size;
} stored_item_t;

static void
free_item_cb(void *ptr)
{
    stored_item_t *item = (stored_item_t *)ptr;
    if (item->value)
        free(item->value);
    free(item);
}

static void *
copy_item_cb(void *ptr, size_t len, void *user)
{
    stored_item_t *item = (stored_item_t *)ptr;
    stored_item_t *copy = malloc(sizeof(stored_item_t));
    copy->value = malloc(item->size);
    memcpy(copy->value, item->value, item->size);
    copy->size = item->size;
    return copy;
}

static int
st_fetch(void *key, size_t len, void **value, size_t *vlen, void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    stored_item_t *item =  ht_get_deep_copy(storage,
                                            key,
                                            len,
                                            NULL,
                                            copy_item_cb,
                                            NULL);
    void *v = NULL;
    if (item) {
        v = item->value;
        if (vlen) 
            *vlen = item->size;
        free(item);
    }
    if (value)
        *value = v;

    return 0;
}

static int
st_store(void *key, size_t len, void *value, size_t vlen, void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    stored_item_t *new_item = malloc(sizeof(stored_item_t));
    new_item->value = malloc(vlen);
    memcpy(new_item->value, value, vlen);
    new_item->size = vlen;
    ht_set(storage, key, len, new_item, sizeof(stored_item_t));
    return 0;
}

static int
st_remove(void *key, size_t len, void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    ht_delete(storage, key, len, NULL, NULL);
    return 0;
}

static int
st_exist(void *key, size_t len, void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    return ht_exists(storage, key, len);
}

typedef struct {
    shardcache_storage_index_item_t *index;
    size_t size;
    size_t offset;
} st_pair_iterator_arg_t;

static size_t
st_count(void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    return ht_count(storage);
}

static int
st_pair_iterator(hashtable_t *table,
                 void *       key,
                 size_t       klen,
                 void *       value,
                 size_t       vlen,
                 void *       priv)
{
    st_pair_iterator_arg_t *arg = (st_pair_iterator_arg_t *)priv;
    if (arg->offset < arg->size) {
        shardcache_storage_index_item_t *index_item;
        
        index_item = &arg->index[arg->offset++];
        index_item->key = malloc(klen);
        memcpy(index_item->key, key, klen);
        index_item->klen = klen;
        stored_item_t *item = (stored_item_t *)value;
        index_item->vlen = item->size;
        return 1;
    }
    return 0;
}

static size_t
st_index(shardcache_storage_index_item_t *index, size_t isize, void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    st_pair_iterator_arg_t arg = { index, isize, 0 };
    ht_foreach_pair(storage, st_pair_iterator, &arg);
    return arg.offset;
}

int
storage_mem_init(shardcache_storage_t *st, char **options)
{
    st->fetch  = st_fetch;
    st->store  = st_store;
    st->remove = st_remove;
    st->exist  = st_exist;
    st->index  = st_index;
    st->count  = st_count;

    int size = 1024;
    int maxsize = 1 << 20;
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
                if (strcmp(key, "initial_table_size") == 0) {
                    size = strtol(value, NULL, 10);
                } else if (strcmp(key, "max_table_size") == 0) {
                    maxsize = strtol(value, NULL, 10);
                } else {
                    SHC_ERROR("Unknown option name %s", key);
                }
            }
        }
    }
    hashtable_t *storage = ht_create(size, maxsize, free_item_cb);
    st->priv = storage;
    
    return 0;
}

void
storage_mem_destroy(void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    ht_destroy(storage);
}
