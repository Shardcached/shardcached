#ifndef __SHCD_HTTP_H__
#define __SHCD_HTTP_H__

#include <shardcache.h>
#include <hashtable.h>
#include "acl.h"

typedef struct __shcd_http_s shcd_http_t;

shcd_http_t *shcd_http_create(shardcache_t *cache,
                              const char *me,
                              const char *basepath,
                              const char *adminpath,
                              shcd_acl_t *acl,
                              hashtable_t *mime_types,
                              const char **options,
                              int num_workers);

void shcd_http_destroy(shcd_http_t *http);

#endif
