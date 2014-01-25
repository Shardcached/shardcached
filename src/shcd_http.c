#define _GNU_SOURCE
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <bsd_queue.h>
#include <queue.h>
#include <fbuf.h>
#include <errno.h>
#include "mongoose.h"

#include "shcd_http.h"

#define HTTP_HEADERS_BASE "HTTP/1.0 200 OK\r\n" \
                          "Content-Type: %s\r\n" \
                          "Content-Length: %d\r\n" \
                          "Server: shardcached\r\n" \
                          "Connection: Close\r\n"
#define HTTP_HEADERS HTTP_HEADERS_BASE "\r\n"
#define HTTP_HEADERS_WITH_TIME HTTP_HEADERS_BASE "Last-Modified: %s\r\n\r\n"

typedef struct __http_worker_s {
    TAILQ_ENTRY(__http_worker_s) next;
    pthread_t th;
    struct mg_server *server;
    const char *me;
    const char *basepath;
    const char *adminpath;
    shardcache_t *cache;
    shcd_acl_t *acl;
    hashtable_t *mime_types;
    int leave;
} http_worker_t;

struct __shcd_http_s {
    int num_workers;
    TAILQ_HEAD(, __http_worker_s) workers; 
};

typedef struct __http_job_s {

} http_job_t;

static int shcd_active_requests = 0;

static void
shardcached_build_index_response(fbuf_t *buf, int do_html, shardcache_t *cache)
{
    int i;

    shardcache_storage_index_t *index = shardcache_get_index(cache);

    if (do_html) {
        fbuf_printf(buf,
                    "<html><body>"
                    "<table bgcolor='#000000' "
                    "cellspacing='1' "
                    "cellpadding='4'>"
                    "<tr bgcolor='#ffffff'>"
                    "<td><b>Key</b></td>"
                    "<td><b>Value size</b></td>"
                    "</tr>");
    }
    for (i = 0; i < index->size; i++) {
        size_t klen = index->items[i].klen;
        char keystr[klen * 5 + 1];
        char *t = keystr;
        char c;
        int p;
        for (p = 0 ; p < klen ; ++p) {
            c = ((char*)index->items[i].key)[p];
            if (c == '<')
                t = stpcpy(t, "&lt;");
            else if (c == '>')
                t = stpcpy(t, "&gt;");
            else if (c == '&')
                t = stpcpy(t, "&amp;");
            else if (c < ' ') {
                sprintf(t, "\\x%2x", (int)c);
                t += 4;
            }
            else
                *t++ = c;
        }
        *t = 0;
        if (do_html)
            fbuf_printf(buf,
                        "<tr bgcolor='#ffffff'><td>%s</td>"
                        "<td>(%d)</td></tr>",
                        keystr,
                        index->items[i].vlen);
        else
            fbuf_printf(buf,
                        "%s;%d\r\n",
                        keystr,
                        index->items[i].vlen);
    }

    if (do_html)
        fbuf_printf(buf, "</table></body></html>");

    shardcache_free_index(index);
}

static void
shardcached_build_stats_response(fbuf_t *buf, int do_html, http_worker_t *wrk)
{
    int i;
    int num_nodes = 0;
    shardcache_node_t *nodes = shardcache_get_nodes(wrk->cache, &num_nodes);
    if (do_html) {
        fbuf_printf(buf,
                    "<html><body>"
                    "<h1>%s</h1>"
                    "<table bgcolor='#000000' "
                    "cellspacing='1' "
                    "cellpadding='4'>"
                    "<tr bgcolor='#ffffff'>"
                    "<td><b>Counter</b></td>"
                    "<td><b>Value</b></td>"
                    "</tr>"
                    "<tr bgcolor='#ffffff'>"
                    "<td>active_http_requests</td>"
                    "<td>%d</td>"
                    "</tr>"
                    "<tr bgcolor='#ffffff'>"
                    "<td>num_nodes</td>"
                    "<td>%d</td>"
                    "</tr>",
                    wrk->me,
                    __sync_fetch_and_add(&shcd_active_requests, 0),
                    num_nodes);

        for (i = 0; i < num_nodes; i++) {
            fbuf_printf(buf,
                        "<tr bgcolor='#ffffff'>"
                        "<td>node::%s</td><td>%s</td>"
                        "</td></tr>",
                        nodes[i].label, nodes[i].address);
        }
    } else {
        fbuf_printf(buf,
                    "active_http_requests;%d\r\nnum_nodes;%d\r\n",
                    __sync_fetch_and_add(&shcd_active_requests, 0),
                    num_nodes);
        for (i = 0; i < num_nodes; i++) {
            fbuf_printf(buf, "node::%s;%s\r\n", nodes[i].label, nodes[i].address);
        }
    }

    if (nodes)
        free(nodes);

    shardcache_counter_t *counters;
    int ncounters = shardcache_get_counters(wrk->cache, &counters);

    for (i = 0; i < ncounters; i++) {
        if (do_html)
            fbuf_printf(buf,
                        "<tr bgcolor='#ffffff'><td>%s</td><td>%u</td></tr>",
                        counters[i].name,
                        counters[i].value);
        else
            fbuf_printf(buf,
                        "%s;%u\r\n",
                        counters[i].name,
                        counters[i].value);
    }
    if (do_html)
        fbuf_printf(buf, "</table></body></html>");
    free(counters);
}

static void
shardcached_handle_admin_request(http_worker_t *wrk, struct mg_connection *conn, char *key, int is_head)
{
    if (wrk->acl) {
        shcd_acl_method_t method = SHCD_ACL_METHOD_GET;
        struct in_addr remote_addr;
        inet_aton(conn->remote_ip, &remote_addr);
        if (shcd_acl_eval(wrk->acl, method, key, remote_addr.s_addr) != SHCD_ACL_ACTION_ALLOW) {
            mg_printf(conn, "HTTP/1.0 403 Forbidden\r\nContent-Length: 9\r\n\r\nForbidden");
            return;
        }
    }

    if (strcmp(key, "__stats__") == 0) {
        int do_html = (!conn->query_string ||
                       !strstr(conn->query_string, "nohtml=1"));

        fbuf_t buf = FBUF_STATIC_INITIALIZER;
        shardcached_build_stats_response(&buf, do_html, wrk);

        mg_printf(conn, HTTP_HEADERS,
                        do_html ? "text/html" : "text/plain",
                        fbuf_used(&buf));

        if (!is_head)
            mg_printf(conn, "%s", fbuf_data(&buf));

        fbuf_destroy(&buf);

    } else if (strcmp(key, "__index__") == 0) {
        fbuf_t buf = FBUF_STATIC_INITIALIZER;
        int do_html = (!conn->query_string ||
                       !strstr(conn->query_string, "nohtml=1"));

        shardcached_build_index_response(&buf, do_html, wrk->cache);

        mg_printf(conn, HTTP_HEADERS,
                        do_html ? "text/html" : "text/plain",
                        fbuf_used(&buf));

        if (!is_head)
            mg_printf(conn, "%s", fbuf_data(&buf));

        fbuf_destroy(&buf);
    } else if (strcmp(key, "__health__") == 0) {
        int do_html = (!conn->query_string ||
                       !strstr(conn->query_string, "nohtml=1"));

        char *resp = do_html ? "<html><body>OK</body></html>" : "OK";

        mg_printf(conn, HTTP_HEADERS,
                        do_html ? "text/html" : "text/plain",
                        (int)strlen(resp));

        if (!is_head)
            mg_printf(conn, "%s", resp);
    } else {
        mg_printf(conn, "HTTP/1.0 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found");
    }
}

static void
shardcached_handle_get_request(http_worker_t *wrk, struct mg_connection *conn, char *key, int is_head)
{
    if (wrk->acl) {
        shcd_acl_method_t method = SHCD_ACL_METHOD_GET;
        struct in_addr remote_addr;
        inet_aton(conn->remote_ip, &remote_addr);
        if (shcd_acl_eval(wrk->acl, method, key, remote_addr.s_addr) != SHCD_ACL_ACTION_ALLOW) {
            mg_printf(conn, "HTTP/1.0 403 Forbidden\r\nContent-Length: 9\r\n\r\nForbidden");
            return;
        }
    }

    size_t vlen = 0;
    struct timeval ts = { 0, 0 };
    void *value = NULL;
    if (is_head) {
        vlen = shardcache_head(wrk->cache, key, strlen(key), NULL, 0, &ts);
    } else {
        value = shardcache_get(wrk->cache, key, strlen(key), &vlen, &ts);
    }

    if (vlen) {
        int i;
        for (i = 0; i < conn->num_headers; i++) {
            struct tm tm;
            const char *hdr_name = conn->http_headers[i].name;
            const char *hdr_value = conn->http_headers[i].value;
            if (strcasecmp(hdr_name, "If-Modified-Since") == 0) {
                if (strptime(hdr_value, "%a, %d %b %Y %T %z", &tm) != NULL) {
                    time_t time = mktime(&tm);
                    if (ts.tv_sec < time) {
                        mg_printf(conn, "HTTP/1.0 304 Not Modified\r\nContent-Length: 12\r\n\r\nNot Modified");
                        if (value)
                            free(value);
                        return;
                    }
                }
            } else if (strcasecmp(hdr_name, "If-Unmodified-Since") == 0) {
                if (strptime(hdr_value, "%a, %d %b %Y %T %z", &tm) != NULL) {
                    time_t time = mktime(&tm);
                    if (ts.tv_sec > time) {
                        mg_printf(conn, "HTTP/1.0 412 Precondition Failed\r\nContent-Length: 19\r\n\r\nPrecondition Failed");
                        if (value)
                            free(value);
                        return;
                    }

                }
            }
        }

        char *mtype = NULL;
        if (wrk->mime_types) {
            char *p = key;
            while (*p && *p != '.')
                p++;
            if (*p && *(p+1)) {
                p++;
                mtype = (char *)ht_get(wrk->mime_types, p, strlen(p), NULL);
                if (!mtype)
                    mtype = (char *)mg_get_mime_type(key, "application/octet-stream");
            }
        } else {
            mtype = (char *)mg_get_mime_type(key, "application/octet-stream");
        }

        char timestamp[256];
        struct tm gmts;
        strftime(timestamp, sizeof(timestamp), "%a, %d %b %Y %T %z", gmtime_r(&ts.tv_sec, &gmts));
        mg_printf(conn, HTTP_HEADERS_WITH_TIME, mtype, (int)vlen, timestamp);

        if (!is_head && value)
            mg_write(conn, value, vlen);

        if (value)
            free(value);
    } else {
        mg_printf(conn, "HTTP/1.0 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found");
    }
}

static void
shardcached_handle_delete_request(http_worker_t *wrk, struct mg_connection *conn, char *key)
{
    if (wrk->acl) {
        shcd_acl_method_t method = SHCD_ACL_METHOD_DEL;
        struct in_addr remote_addr;
        inet_aton(conn->remote_ip, &remote_addr);
        if (shcd_acl_eval(wrk->acl, method, key, remote_addr.s_addr) != SHCD_ACL_ACTION_ALLOW) {
            mg_printf(conn, "HTTP/1.0 403 Forbidden\r\nContent-Length: 9\r\n\r\nForbidden");
            return;
        }
    }

    int rc = shardcache_del(wrk->cache, key, strlen(key));
    mg_printf(conn, "HTTP/1.0 %s\r\n"
                    "Content-Length: 0\r\n\r\n",
                     rc == 0 ? "200 OK" : "500 ERR");

}

static void
shardcached_handle_put_request(http_worker_t *wrk, struct mg_connection *conn, char *key)
{
    if (wrk->acl) {
        shcd_acl_method_t method = SHCD_ACL_METHOD_PUT;
        struct in_addr remote_addr;
        inet_aton(conn->remote_ip, &remote_addr);
        if (shcd_acl_eval(wrk->acl, method, key, remote_addr.s_addr) != SHCD_ACL_ACTION_ALLOW) {
            mg_printf(conn, "HTTP/1.0 403 Forbidden\r\nContent-Length: 9\r\n\r\nForbidden");
            return;
        }
    }

    int clen = 0;
    const char *clen_hdr = mg_get_header(conn, "Content-Length");
    if (clen_hdr) {
        clen = strtol(clen_hdr, NULL, 10); 
    }
    
    if (!clen) {
        mg_printf(conn, "HTTP/1.0 400 Bad Request\r\nContent-Length: 0\r\n\r\n");
        return;
    }

    shardcache_set(wrk->cache, key, strlen(key), conn->content, conn->content_len);

    mg_printf(conn, "HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n");
}

static int
shardcached_request_handler(struct mg_connection *conn)
{
    http_worker_t *wrk = conn->server_param;
    
    char *key = (char *)conn->uri;
    int basepath_found = 0;

    int basepath_len = strlen(wrk->basepath);
    int baseadminpath_len = strlen(wrk->adminpath);
    int basepaths_differ = (basepath_len != baseadminpath_len || strcmp(wrk->basepath, wrk->adminpath) != 0);

    __sync_add_and_fetch(&shcd_active_requests, 1);

    while (*key == '/' && *key)
        key++;

    if (basepath_len) {
        if (strncmp(key, wrk->basepath, basepath_len) == 0) {
            key += basepath_len + 1;
            basepath_found = 1;
            while (*key == '/' && *key)
                key++;
        } else {
            if (!basepaths_differ) {
                SHC_ERROR("Bad request uri : %s", conn->uri);
                mg_printf(conn, "HTTP/1.0 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found");
                __sync_sub_and_fetch(&shcd_active_requests, 1);
                return 1;
            }
        }
    }

    if (*key == 0) {
        mg_printf(conn, "HTTP/1.0 404 Not Found\r\nContent-Length 9\r\n\r\nNot Found");
        __sync_sub_and_fetch(&shcd_active_requests, 1);
        return 1;
    }

    if (baseadminpath_len && basepaths_differ) {
        if (!basepath_found && strncmp(key, wrk->adminpath, baseadminpath_len) == 0) {
            key += baseadminpath_len + 1;

            while (*key == '/' && *key)
                key++;
            if (*key == 0) {
                mg_printf(conn, "HTTP/1.0 404 Not Found\r\nContent-Length 9\r\n\r\nNot Found");
                __sync_sub_and_fetch(&shcd_active_requests, 1);
                return 1;
            }

            if (strncasecmp(conn->request_method, "GET", 3) == 0)
                shardcached_handle_admin_request(wrk, conn, key, 0);
            else
                mg_printf(conn, "HTTP/1.0 403 Forbidden\r\nContent-Length 9\r\n\r\nForbidden");
            __sync_sub_and_fetch(&shcd_active_requests, 1);
            return 1;
        }
    }

    // if baseadminpath is not defined or it's the same as basepath,
    // we need to check for the "special" admin keys and handle them differently
    // (in such cases the labels __stats__, __index__ and __health__ become reserved
    // and can't be used as keys from the http interface)
    if ((!baseadminpath_len || !basepaths_differ) &&
        (strcmp(key, "__stats__") == 0 || strcmp(key, "__index__") == 0 || strcmp(key, "__health__") == 0))
    {
        if (strncasecmp(conn->request_method, "GET", 3) == 0)
            shardcached_handle_admin_request(wrk, conn, key, 0);
        else
            mg_printf(conn, "HTTP/1.0 403 Forbidden\r\nContent-Length 9\r\n\r\nForbidden");
        __sync_sub_and_fetch(&shcd_active_requests, 1);
        return 1;

    }

    // XXX
    // handle the actual GET/PUT/DELETE request
    if (strncasecmp(conn->request_method, "GET", 3) == 0)
        shardcached_handle_get_request(wrk, conn, key, 0);
    else if (strncasecmp(conn->request_method, "HEAD", 4) == 0)
        shardcached_handle_get_request(wrk, conn, key, 1);
    else if (strncasecmp(conn->request_method, "DELETE", 6) == 0)
        shardcached_handle_delete_request(wrk, conn, key);
    else if (strncasecmp(conn->request_method, "PUT", 3) == 0)
        shardcached_handle_put_request(wrk, conn, key);
    else
        mg_printf(conn, "HTTP/1.0 405 Method Not Allowed\r\nContent-Length: 11\r\n\r\nNot Allowed");


    __sync_sub_and_fetch(&shcd_active_requests, 1);
    return 1;
}


void *
shcd_http_run(void *priv)
{
    http_worker_t *wrk = (http_worker_t *)priv;
    while (!__sync_fetch_and_add(&wrk->leave, 0)) {
        mg_poll_server(wrk->server, 1000);
    }
    return NULL;
}

shcd_http_t *
shcd_http_create(shardcache_t *cache,
                 const char *me,
                 const char *basepath,
                 const char *adminpath,
                 shcd_acl_t *acl,
                 hashtable_t *mime_types,
                 const char **options,
                 int num_workers)
{
    int i, n;
    if (num_workers < 0)
        return NULL;

    shcd_http_t *http = calloc(1, sizeof(shcd_http_t));

    http->num_workers = num_workers;

    TAILQ_INIT(&http->workers);
    for (i = 0; i < num_workers; i++) {

        http_worker_t *wrk = calloc(1, sizeof(http_worker_t));

        wrk->server = mg_create_server(wrk);
        if (!wrk->server) {
            SHC_ERROR("Can't start mongoose server");
            shcd_http_destroy(http);
            return NULL;
        }

        wrk->cache = cache;
        wrk->me = me;
        wrk->basepath = basepath;
        wrk->adminpath = adminpath;
        wrk->acl = acl;
        wrk->mime_types = mime_types;

        for (n = 0; options[n]; n += 2) {
            const char *option = options[n];
            const char *value = options[n+1];
            if (!option || !value) {
                SHC_ERROR("Bad mongoose options");
                shcd_http_destroy(http);
                return NULL;

            }
            if (strcmp(option, "listening_port") == 0 && i > 0) {
                mg_set_listening_socket(wrk->server, mg_get_listening_socket(TAILQ_FIRST(&http->workers)->server));
            } else {
                const char *msg = mg_set_option(wrk->server, option, value);
                if (msg != NULL) {
                    SHC_ERROR("Failed to set mongoose option [%s=%s]: %s",
                               option, value, msg);
                    shcd_http_destroy(http);
                    return NULL;
                }
            }
        }

        mg_add_uri_handler(wrk->server, "/",  shardcached_request_handler);

        TAILQ_INSERT_TAIL(&http->workers, wrk, next);
        if (pthread_create(&wrk->th, NULL, shcd_http_run, wrk) != 0) {
            SHC_ERROR("Failed to start an http worker thread: %s",
                       strerror(errno));
            shcd_http_destroy(http);
            return NULL;
        }
    }
    return http;
};

void
shcd_http_destroy(shcd_http_t *http)
{
    http_worker_t *worker, *tmp;
    TAILQ_FOREACH_SAFE(worker, &http->workers, next, tmp) {
        TAILQ_REMOVE(&http->workers, worker, next);
        __sync_add_and_fetch(&worker->leave, 1);
        //pthread_cancel(worker->th);
        pthread_join(worker->th, NULL);
        mg_destroy_server(&worker->server);
        free(worker);
    }
    free(http);
}
