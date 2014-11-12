#include <stdlib.h>
#include <string.h>
#include <shardcache.h>
#include <hashtable.h>
#include "storage_mem.h"

static int
st_fetch(void *key, size_t len, void **value, size_t *vlen, void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    size_t l = 0;
    void *v = ht_get_copy(storage, key, len, &l);

    if (l) 
        *vlen = l;
    if (v)
        *value = v;

    return 0;
}

static int
st_fetch_multi(void **keys, size_t *klens, int nkeys, void **values, size_t *vlens, void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    int i;

    for (i = 0; i < nkeys; i++)
        values[i] = ht_get_copy(storage, keys[i], klens[i], &vlens[i]);

    return 0;
}

static int
st_store(void *key, size_t len, void *value, size_t vlen, void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    ht_set_copy(storage, key, len, value, vlen, NULL, NULL);
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
        index_item->vlen = vlen;
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
    st->fetch_multi = st_fetch_multi;

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
    hashtable_t *storage = ht_create(size, maxsize, free);
    st->priv = storage;
    
    return 0;
}

void
storage_mem_destroy(void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    ht_destroy(storage);
}
