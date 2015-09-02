#ifndef SHCD_ACL_H
#define SHCD_ACL_H

#include <stdint.h>

typedef enum {
    SHCD_ACL_ACTION_ALLOW = 0,
    SHCD_ACL_ACTION_DENY = 1
} shcd_acl_action_t;

typedef enum {
    SHCD_ACL_METHOD_GET = 0x00,
    SHCD_ACL_METHOD_PUT = 0x01,
    SHCD_ACL_METHOD_DEL = 0x02,
    SHCD_ACL_METHOD_ANY = 0xff
} shcd_acl_method_t;

typedef struct _shcd_acl_item_s shcd_acl_item_t;

typedef struct _shcd_acl_s shcd_acl_t;

shcd_acl_t *shcd_acl_create(shcd_acl_action_t default_action);

void shcd_acl_destroy(shcd_acl_t *acl);

int shcd_acl_add(shcd_acl_t *acl,
                 char *pattern,
                 shcd_acl_action_t action,
                 shcd_acl_method_t method,
                 uint32_t ip,
                 uint32_t mask);

void shcd_acl_clear(shcd_acl_t *acl);

shcd_acl_action_t shcd_acl_eval(shcd_acl_t *acl,
                                shcd_acl_method_t method,
                                char *path,
                                uint32_t ipaddr);

#endif
