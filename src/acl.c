#include "acl.h"
#include <arpa/inet.h>
#include <linklist.h>
#include <regex.h>
#include <pthread.h>
#include <errno.h>
#include <shardcache.h>

struct __shcd_acl_item_s {
    regex_t exp;
    shcd_acl_action_t action;
    shcd_acl_method_t method;
    uint32_t ipaddr;
    uint32_t mask;
    pthread_mutex_t lock;
};

struct __shcd_acl_s {
    linked_list_t *list;
    shcd_acl_action_t default_action;
};

void shcd_acl_item_destroy(shcd_acl_item_t *item)
{
    regfree(&item->exp);
    free(item);
}

shcd_acl_t *shcd_acl_create(shcd_acl_action_t default_action)
{
    shcd_acl_t *acl = calloc(1, sizeof(shcd_acl_t));
    acl->list = list_create();
    list_set_free_value_callback(acl->list,  (free_value_callback_t)shcd_acl_item_destroy);
    acl->default_action = default_action;
    return acl;
}

int shcd_acl_add(shcd_acl_t *acl,
                 char *pattern,
                 shcd_acl_action_t action,
                 shcd_acl_method_t method,
                 uint32_t ipaddr,
                 uint32_t mask)
{
    shcd_acl_item_t *item = calloc(1, sizeof(shcd_acl_item_t));
    // XXX - note it's using REG_ICASE
    if (regcomp(&item->exp, pattern, REG_EXTENDED|REG_ICASE) != 0)
    {
        SHC_ERROR("Bad regex pattern: %s (%s)", pattern, strerror(errno));
        free(item);
        return -1;
    }
    item->action = action;
    item->method = method;
    item->ipaddr = ipaddr;
    item->mask = mask;
    pthread_mutex_init(&item->lock, NULL);
    list_push_value(acl->list, item);
    return 0;
}

shcd_acl_action_t shcd_acl_eval(shcd_acl_t *acl,
                                shcd_acl_method_t method,
                                char *path,
                                uint32_t ipaddr)
{
    shcd_acl_action_t res = acl->default_action;
    int i;

    for (i = 0; i < list_count(acl->list); i++) {
        shcd_acl_item_t *item = list_pick_value(acl->list, i);

        if (item->method != SHCD_ACL_METHOD_ANY && item->method != method)
            continue;

        if ((ipaddr & item->mask) != item->ipaddr) {
            continue;
        }

        pthread_mutex_lock(&item->lock);
        int matched = regexec(&item->exp, path, 0, NULL, 0);
        pthread_mutex_unlock(&item->lock);

        if (matched != 0)
            continue;

        res = item->action;

    }

    struct in_addr iaddr;
    iaddr.s_addr = ipaddr;
    SHC_DEBUG2("ACL result for action : %02x, key: %s, from %s == %s",
            method, path, inet_ntoa(iaddr),
            (res == SHCD_ACL_ACTION_ALLOW) ? "ALLOW" : "DENY");
    return res;
}

void shcd_acl_destroy(shcd_acl_t *acl)
{
    list_destroy(acl->list);
    free(acl);
}


