#include <stdlib.h>
#include "storage.h"
#include "shardcache.h"

#include "storage_mem.h"
#include "storage_fs.h"

#include <dlfcn.h>

typedef void (*shardcache_storage_destroyer_t)(void *);

struct shcd_storage_s
{
    shardcache_storage_t storage;
    shardcache_storage_destroyer_t destroyer;
    void *handle;
};

shcd_storage_t *
shcd_storage_init(char *storage_type, char *options_string, char *plugins_dir)
{
    const char *storage_options[MAX_STORAGE_OPTIONS];

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
    st->storage.version = SHARDCACHE_STORAGE_API_VERSION;
    if (strcmp(storage_type, "mem") == 0) {
        // TODO - validate options
        initialized = storage_mem_init(&st->storage, storage_options);
        st->destroyer = storage_mem_destroy;

    } else if (strcmp(storage_type, "fs") == 0) {
        // TODO - validate options
        initialized = storage_fs_init(&st->storage, storage_options);
        st->destroyer = storage_fs_destroy;
    } else {
        char libname[1024];
        snprintf(libname, sizeof(libname), "%s/%s.storage",
                 plugins_dir, storage_type);
        st->handle = dlopen(libname, RTLD_NOW);
        if (!st->handle) {
            SHC_ERROR("Unknown storage type: %s (%s)\n", storage_type, dlerror());
            free(st);
            return NULL;
        }
        char *error = NULL;
        int (*init)(shardcache_storage_t *st, const char **options);
        init = dlsym(st->handle, "storage_init");
        if (!init || ((error = dlerror()) != NULL))  {
            fprintf(stderr, "%s\n", error);
            dlclose(st->handle);
            free(st);
            return NULL;
        }

        void (*destroy)(void *);
        destroy = dlsym(st->handle, "storage_destroy");
        if (!destroy || ((error = dlerror()) != NULL))  {
            fprintf(stderr, "%s\n", error);
            dlclose(st->handle);
            free(st);
            return NULL;
        }

        initialized = (*init)(&st->storage, storage_options);
        st->destroyer = destroy;
    }
    if (initialized != 0) {
        SHC_ERROR("Can't init the storage type: %s\n", storage_type);
        free(st);
        return NULL;
    }
    return st;
}

shardcache_storage_t *shcd_storage_get(shcd_storage_t *st) {
    return &st->storage;
}

void shcd_storage_destroy(shcd_storage_t *st) {
    if (st->destroyer)
        st->destroyer(st->storage.priv);
    if (st->handle)
        dlclose(st->handle);
    free(st);
}
