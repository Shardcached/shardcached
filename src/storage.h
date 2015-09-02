#ifndef SHCD_STORAGE_H
#define SHCD_STORAGE_H

#include <shardcache.h>

#define MAX_STORAGE_OPTIONS 256
#define MAX_OPTIONS_STRING_LEN 2048

// 
// Check <src_base>/storage_plugins/README
// for detaile about the Loadable Storage Modules API
//

typedef struct shcd_storage_s shcd_storage_t;

shcd_storage_t * shcd_storage_init(char *storage_type,
                                   char *options_string,
                                   char *plugins_dir);

void shcd_storage_destroy(shcd_storage_t *st);
int shcd_storage_reset(shcd_storage_t *st);

shardcache_storage_t *shcd_storage_get(shcd_storage_t *st);

#endif
