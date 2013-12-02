#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <hashtable.h>
#include <shardcache.h>

typedef struct {
    void *value;
    size_t size;
} stored_item_t;

static void free_item_cb(void *ptr) {
    stored_item_t *item = (stored_item_t *)ptr;
    if (item->value)
        free(item->value);
    free(item);
}

static void *copy_item_cb(void *ptr, size_t len) {
    stored_item_t *item = (stored_item_t *)ptr;
    stored_item_t *copy = malloc(sizeof(stored_item_t));
    copy->value = malloc(item->size);
    memcpy(copy->value, item->value, item->size);
    copy->size = item->size;
    return copy;
}

static void *st_fetch(void *key, size_t len, size_t *vlen, void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    stored_item_t *item =  ht_get_deep_copy(storage, key, len, NULL, copy_item_cb);
    void *v = NULL;
    if (item) {
        v = item->value;
        if (vlen) 
            *vlen = item->size;
    }
    return v;
}

static int st_store(void *key, size_t len, void *value, size_t vlen, void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    stored_item_t *new_item = malloc(sizeof(stored_item_t));
    new_item->value = malloc(vlen);
    memcpy(new_item->value, value, vlen);
    new_item->size = vlen;
    ht_set(storage, key, len, new_item, sizeof(stored_item_t), NULL, NULL);
    return 0;
}

static int st_remove(void *key, size_t len, void *priv) {
    hashtable_t *storage = (hashtable_t *)priv;
    ht_delete(storage, key, len, NULL, NULL);
    return 0;
}

// following the symbols exported by the plugin

shardcache_storage_t *storage_create(const char **options) {
    shardcache_storage_t *st = calloc(1, sizeof(shardcache_storage_t));
    st->fetch_item      = st_fetch;
    st->store_item      = st_store;
    st->remove_item     = st_remove;

    int size = 1024;
    int maxsize = 1 << 20;
    if (options) {
        while (*options) {
            char *key = (char *)*options++;
            char *value = NULL;
            if (*options) {
                value = (char *)*options++;
            } else {
                fprintf(stderr, "Odd element in the options array\n");
                continue;
            }
            if (key && value) {
                if (strcmp(key, "initial_table_size") == 0) {
                    size = strtol(value, NULL, 10);
                } else if (strcmp(key, "max_table_size") == 0) {
                    maxsize = strtol(value, NULL, 10);
                } else {
                    fprintf(stderr, "Unknown option name %s\n", key);
                }
            }
        }
    }
    hashtable_t *storage = ht_create(size, maxsize, free_item_cb);
    st->priv = storage; 
    return st;
}

void storage_destroy(shardcache_storage_t *st) {
    hashtable_t *storage = (hashtable_t *)st->priv;
    ht_destroy(storage);
    free(st);
}


