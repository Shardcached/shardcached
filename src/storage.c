#include <stdlib.h>
#include "storage.h"
#include "shardcache.h"

#include "storage_mem.h"
#include "storage_fs.h"

#include <dlfcn.h>

typedef void (*shardcache_storage_destroyer_t)(shardcache_storage_t *);

struct shcd_storage_s
{
    shardcache_storage_t *storage;
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
    if (strcmp(storage_type, "mem") == 0) {
        // TODO - validate options
        st->storage = storage_mem_create(storage_options);
        st->destroyer = storage_mem_destroy;

    } else if (strcmp(storage_type, "fs") == 0) {
        // TODO - validate options
        st->storage = storage_fs_create(storage_options);
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
        shardcache_storage_t *(*create)(const char **options);
        create = dlsym(st->handle, "storage_create");
        if (!create || ((error = dlerror()) != NULL))  {
            fprintf(stderr, "%s\n", error);
            dlclose(st->handle);
            free(st);
            return NULL;
        }

        void (*destroy)(shardcache_storage_t *);
        destroy = dlsym(st->handle, "storage_destroy");
        if (!destroy || ((error = dlerror()) != NULL))  {
            fprintf(stderr, "%s\n", error);
            dlclose(st->handle);
            free(st);
            return NULL;
        }

        st->storage = (*create)(storage_options);
        st->destroyer = destroy;
    }
    if (!st->storage) {
        SHC_ERROR("Can't create storage type: %s\n", storage_type);
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
        st->destroyer(st->storage);
    if (st->handle)
        dlclose(st->handle);
    free(st);
}
