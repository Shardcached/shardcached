#include <stdlib.h>
#include "storage.h"
#include "shardcache.h"

#include "storage_mem.h"
#include "storage_fs.h"

typedef void (*shardcache_storage_destroyer_t)(void *);
typedef int (*shardcache_storage_resetter_t)(void *);
struct shcd_storage_s
{
    shardcache_storage_t *storage;
    shardcache_storage_destroyer_t destroyer;
    shardcache_storage_resetter_t resetter;
    void *handle;
    int internal;
};

shcd_storage_t *
shcd_storage_init(char *storage_type, char *options_string, char *plugins_dir)
{
    char *storage_options[MAX_STORAGE_OPTIONS];

    shcd_storage_t *st = calloc(1, sizeof(shcd_storage_t));

    int optidx = 0;
    char *p = options_string;
    char *str = p;
    while (*p != 0 && optidx < MAX_STORAGE_OPTIONS) {
        if (*p == '=' || *p == ',') {
            *p = 0;
            storage_options[optidx++] = str;
            str = p+1;
        }
        p++;
    }
    storage_options[optidx++] = str;
    storage_options[optidx] = NULL;

    // initialize the storage layer 
    int initialized = -1;
    if (strcmp(storage_type, "mem") == 0) {
        // TODO - validate options
        st->storage = calloc(1, sizeof(shardcache_storage_t));
        initialized = (storage_mem_init(st->storage, storage_options) == 0);
        st->destroyer = storage_mem_destroy;
        st->storage->version = SHARDCACHE_STORAGE_API_VERSION;
        st->internal = 1;
    } else if (strcmp(storage_type, "fs") == 0) {
        // TODO - validate options
        st->storage = calloc(1, sizeof(shardcache_storage_t));
        initialized = (storage_fs_init(st->storage, storage_options) == 0);
        st->destroyer = storage_fs_destroy;
        st->storage->version = SHARDCACHE_STORAGE_API_VERSION;
        st->internal = 1;
    } else {
        char libname[1024];
        snprintf(libname, sizeof(libname), "%s/%s.storage",
                 plugins_dir, storage_type);
        st->storage = shardcache_storage_load(libname, storage_options);
        initialized = (st->storage != NULL);
    }
    if (!initialized) {
        SHC_ERROR("Can't init the storage type: %s\n", storage_type);
        if (st->internal && st->storage) {
            free(st->storage);
        } else if (st->storage) {
            shardcache_storage_dispose(st->storage);
        }
        free(st);
        return NULL;
    }
    return st;
}

shardcache_storage_t *shcd_storage_get(shcd_storage_t *st) {
    return st->storage;
}

void shcd_storage_destroy(shcd_storage_t *st) {

    if (st->destroyer)
        st->destroyer(st->storage->priv);

    if (st->internal)
        free(st->storage);
    else
        shardcache_storage_dispose(st->storage);
    free(st);
}

int shcd_storage_reset(shcd_storage_t *st) {
    int ret = 0;
    SHC_DEBUG("resetting the storage module");
    if (!st->internal)
        ret = shardcache_storage_reset(st->storage, NULL);
    return ret;
}
