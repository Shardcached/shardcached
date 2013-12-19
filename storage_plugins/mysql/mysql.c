#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <mysql/mysql.h>
#include <shardcache.h>
#include <pthread.h>

#define ST_KEYFIELD_DEFAULT        "key"
#define ST_KEYBYTESFIELD_DEFAULT   "keybytes"
#define ST_KEYSIZEFIELD_DEFAULT    "keysize"
#define ST_VALUEFIELD_DEFAULT      "value"
#define ST_VALUESIZEFIELD_DEFAULT  "valuesize"
#define ST_DBNAME_DEFAULT          "shardcache"
#define ST_TABLE_DEFAULT           "storage"

typedef struct {
    char *dbname;
    char *dbhost;
    int  dbport;
    char *dbuser;
    char *dbpasswd;
    char *unix_socket;
    char *table;
    char *keyfield;
    char *keybytesfield;
    char *keysizefield;
    char *valuefield;
    char *valuesizefield;
    int  external_blobs;
    char *storage_path;
    MYSQL dbh;
    MYSQL_STMT *select_stmt;
    MYSQL_STMT *insert_stmt;
    MYSQL_STMT *delete_stmt;
    MYSQL_STMT *exist_stmt;
    MYSQL_STMT *count_stmt;
    MYSQL_STMT *index_stmt;
    pthread_mutex_t lock;
} storage_mysql_t;

static void
parse_options(storage_mysql_t *st, const char **options)
{
    while (*options) {
        char *key = (char *)*options++;
        char *value = NULL;
        if (*options) {
            value = (char *)*options++;
        } else {
            fprintf(stderr, "Odd element in the options array");
            continue;
        }
        if (key && value) {
            if (strcmp(key, "dbname") == 0) {
                st->dbname = strdup(value);
            } else if (strcmp(key, "dbhost") == 0) {
                st->dbhost = strdup(value);
            } else if (strcmp(key, "dbport") == 0) {
                st->dbport = strtol(value, NULL, 10);
            } else if (strcmp(key, "dbuser") == 0) {
                st->dbuser = strdup(value);
            } else if (strcmp(key, "dbpasswd") == 0) {
                st->dbpasswd = strdup(value);
            } else if (strcmp(key, "unix_socket") == 0) {
                st->unix_socket = strdup(value);
            } else if (strcmp(key, "table") == 0) {
                st->table = strdup(value);
            } else if (strcmp(key, "keyfield") == 0) {
                st->keyfield = strdup(value);
            } else if (strcmp(key, "keybytesfield") == 0) {
                st->keybytesfield = strdup(value);
            } else if (strcmp(key, "keysizefield") == 0) {
                st->keysizefield = strdup(value);
            } else if (strcmp(key, "valuefield") == 0) {
                st->valuefield = strdup(value);
            } else if (strcmp(key, "valuesizefield") == 0) {
                st->valuesizefield = strdup(value);
            } else if (strcmp(key, "external_blobs") == 0) {
                if (strcmp("value", "yes") == 0 ||
                    strcmp("value", "true") == 0 ||
                    strcmp("value", "1") == 0)
                {
                    st->external_blobs = 1;
                }
            } else if (strcmp(key, "storage_path") == 0) {
                st->storage_path = strdup(value);
            } else {
                fprintf(stderr, "Unknown option name %s\n", key);
            }
        }
    }
}

static void *st_fetch(void *key, size_t klen, size_t *vlen, void *priv)
{
    storage_mysql_t *st = (storage_mysql_t *)priv;

    char *keystr = malloc((klen*2)+ 1);
    char *p = (char *)key;
    char *o = (char *)keystr;
    int i;
    for (i = 0; i < klen; i++) {
        snprintf(o, 3, "%02x", p[i]);
        o += 2;
    }
    *o = 0;


    pthread_mutex_lock(&st->lock);

    MYSQL_BIND bnd = {
        .buffer_type = MYSQL_TYPE_STRING,
        .buffer = keystr,
        .buffer_length = strlen(keystr)
    };

    if (mysql_stmt_bind_param(st->select_stmt, &bnd) != 0) {
        // TODO - error messages
        pthread_mutex_unlock(&st->lock);
        return NULL;
    }

    if (mysql_stmt_execute(st->select_stmt) != 0) {
        // TODO - error messages
        fprintf(stderr, "Can't execute fetch statement : %s\n", mysql_stmt_error(st->select_stmt));
        pthread_mutex_unlock(&st->lock);
        return NULL;
    }

    size_t size = 256;
    void *data = malloc(size);
    my_bool error = 0;

    MYSQL_BIND obnd = {
        .buffer_type = MYSQL_TYPE_LONG_BLOB,
        .buffer = data,
        .buffer_length = size,
        .length = &size,
        .error = &error
    };

    mysql_stmt_bind_result(st->select_stmt, &obnd);

    mysql_stmt_fetch(st->select_stmt);

    if (error == 1) {
        data = realloc(data, size);
        error = 0;
        mysql_stmt_bind_result(st->select_stmt, &obnd);
        mysql_stmt_fetch(st->select_stmt);
    }

    if (vlen)
        *vlen = size;

    mysql_stmt_free_result(st->select_stmt);

    pthread_mutex_unlock(&st->lock);

    return data;
}

static int st_store(void *key, size_t klen, void *value, size_t vlen, void *priv)
{
    storage_mysql_t *st = (storage_mysql_t *)priv;
    char* errorMessage;

    char *keystr = malloc((klen*2)+ 1);
    char *p = (char *)key;
    char *o = (char *)keystr;
    int i;
    for (i = 0; i < klen; i++) {
        snprintf(o, 3, "%02x", p[i]);
        o += 2;
    }
    *o = 0;

    pthread_mutex_lock(&st->lock);

    MYSQL_BIND bnd[5] = {
        {
            .buffer_type = MYSQL_TYPE_STRING,
            .buffer = keystr,
            .buffer_length = strlen(keystr),
            .length = 0,
            .is_null = 0
        },
        {
            .buffer_type = MYSQL_TYPE_LONG,
            .buffer = &klen,
            .length = 0,
            .is_null = 0
        },
        {
            .buffer_type = MYSQL_TYPE_LONG_BLOB,
            .buffer = key,
            .buffer_length = klen,
            .length = 0,
            .is_null = 0
        },
        {
            .buffer_type = MYSQL_TYPE_LONG,
            .buffer = &vlen,
            .length = 0,
            .is_null = 0
        },
        {
            .buffer_type = MYSQL_TYPE_LONG_BLOB,
            .buffer = value,
            .buffer_length = vlen,
            .length = 0,
            .is_null = 0
        }
    };

    if (mysql_stmt_bind_param(st->insert_stmt, bnd) != 0) {
        // TODO - error messages
        pthread_mutex_unlock(&st->lock);
        fprintf(stderr, "Can't bind params to the insert statement: %s\n", mysql_stmt_error(st->insert_stmt));
        return -1;
    }

    if (mysql_stmt_execute(st->insert_stmt) != 0) {
        // TODO - error messages
        pthread_mutex_unlock(&st->lock);
        return -1;
    }

    mysql_stmt_free_result(st->insert_stmt);

    pthread_mutex_unlock(&st->lock);
    return 0;
}

static int st_remove(void *key, size_t klen, void *priv)
{

    storage_mysql_t *st = (storage_mysql_t *)priv;
    char* errorMessage;

    char *keystr = malloc((klen*2)+ 1);
    char *p = (char *)key;
    char *o = (char *)keystr;
    int i;
    for (i = 0; i < klen; i++) {
        snprintf(o, 3, "%02x", p[i]);
        o += 2;
    }
    *o = 0;

    pthread_mutex_lock(&st->lock);

    MYSQL_BIND bnd = {
        .buffer_type = MYSQL_TYPE_STRING,
        .buffer = keystr,
        .buffer_length = strlen(keystr)
    };

    if (mysql_stmt_bind_param(st->delete_stmt, &bnd) != 0) {
        // TODO - error messages
        pthread_mutex_unlock(&st->lock);
        return -1;
    }

    if (mysql_stmt_execute(st->delete_stmt) != 0) {
        // TODO - error messages
        pthread_mutex_unlock(&st->lock);
        return -1;
    }

    mysql_stmt_free_result(st->delete_stmt);

    pthread_mutex_unlock(&st->lock);
    return 0;
}

static int st_exist(void *key, size_t klen, void *priv) {
    storage_mysql_t *st = (storage_mysql_t *)priv;
    char* errorMessage;

    char *keystr = malloc((klen*2)+ 1);
    char *p = (char *)key;
    char *o = (char *)keystr;
    int i;
    for (i = 0; i < klen; i++) {
        snprintf(o, 3, "%02x", p[i]);
        o += 2;
    }
    *o = 0;

    pthread_mutex_lock(&st->lock);

    MYSQL_BIND bnd = {
        .buffer_type = MYSQL_TYPE_STRING,
        .buffer = keystr,
        .buffer_length = strlen(keystr)
    };

    if (mysql_stmt_bind_param(st->exist_stmt, &bnd) != 0) {
        // TODO - error messages
        pthread_mutex_unlock(&st->lock);
        return 0;
    }

    if (mysql_stmt_execute(st->exist_stmt) != 0) {
        // TODO - error messages
        pthread_mutex_unlock(&st->lock);
        return 0;
    }

    mysql_stmt_fetch(st->select_stmt);

    int count = 0;
    MYSQL_BIND obnd = {
        .buffer_type = MYSQL_TYPE_LONG,
        .buffer = &count
    };

    if (mysql_stmt_fetch_column(st->exist_stmt, &obnd, 0, 0) != 0) {
        // TODO - error messages
        mysql_stmt_free_result(st->exist_stmt);
        pthread_mutex_unlock(&st->lock);
        return 0;
    }

    mysql_stmt_free_result(st->exist_stmt);

    if (count > 1) {
        // TODO - error messages
    }

    pthread_mutex_unlock(&st->lock);
    return (count == 1);   
}

static size_t st_count(void *priv)
{
    storage_mysql_t *st = (storage_mysql_t *)priv;

    pthread_mutex_lock(&st->lock);

    if (mysql_stmt_execute(st->count_stmt) != 0) {
        // TODO - error messages
        pthread_mutex_unlock(&st->lock);
        return 0;
    }


    size_t count = 0;
    MYSQL_BIND obnd = {
        .buffer_type = MYSQL_TYPE_LONG,
        .buffer = &count
    };

    mysql_stmt_bind_result(st->count_stmt, &obnd);

    if (mysql_stmt_fetch(st->count_stmt) != 0) {
        // TODO - error messages
        mysql_stmt_free_result(st->count_stmt);
        pthread_mutex_unlock(&st->lock);
        return 0;
    }

    mysql_stmt_free_result(st->count_stmt);

    pthread_mutex_unlock(&st->lock);

    return count;
}

static size_t st_index(shardcache_storage_index_item_t *index, size_t isize, void *priv)
{
    storage_mysql_t *st = (storage_mysql_t *)priv;

    pthread_mutex_lock(&st->lock);

    if (mysql_stmt_execute(st->index_stmt) != 0) {
        // TODO - error messages
        pthread_mutex_unlock(&st->lock);
        return 0;
    }

    size_t klen, vlen = 0;
    my_bool error = 0;
    MYSQL_BIND obnd[2] = {
        {
            .buffer_type = MYSQL_TYPE_BLOB,
            .buffer_length = klen,
            .length = &klen,
            .error = &error
        },
        {
            .buffer_type = MYSQL_TYPE_LONG,
            .buffer_length = sizeof(vlen),
            .buffer = &vlen
        }
    };
    int rc = 0;
    size_t cnt = 0;
    while (cnt < isize) {
        klen = 256;
        void *key = malloc(klen);  
        obnd[0].buffer = key;
        
        rc = mysql_stmt_bind_result(st->index_stmt, obnd);
        rc = mysql_stmt_fetch(st->index_stmt);
        if (rc != 0) {
            free(key);
            break;
        }

        if (error == 1) {
            key = realloc(key, klen);
            error = 0;
            mysql_stmt_bind_result(st->select_stmt, obnd);
            mysql_stmt_fetch(st->select_stmt);
        }

        shardcache_storage_index_item_t *item = &index[cnt++];
        item->key = key;
        item->klen = klen;
        item->vlen = vlen;
    }

    if (rc != MYSQL_NO_DATA) {
        // TODO - Error messages
    }

    mysql_stmt_free_result(st->index_stmt);
    mysql_stmt_reset(st->index_stmt);

    pthread_mutex_unlock(&st->lock);

    return cnt;
}

void
storage_destroy(shardcache_storage_t *storage)
{
    storage_mysql_t *st = (storage_mysql_t *)storage->priv;
    mysql_stmt_free_result(st->select_stmt);
    mysql_stmt_free_result(st->insert_stmt);
    mysql_stmt_free_result(st->delete_stmt);
    mysql_stmt_free_result(st->exist_stmt);
    mysql_stmt_free_result(st->count_stmt);
    mysql_stmt_free_result(st->index_stmt);
    mysql_close(&st->dbh);
    free(st->dbhost);
    free(st->dbname);
    free(st->unix_socket);
    free(st->dbuser);
    free(st->dbpasswd);
    free(st->table);
    free(st->keyfield);
    free(st->keybytesfield);
    free(st->keysizefield);
    free(st->valuefield);
    free(st->valuesizefield);
    free(st);
    free(storage);
}

shardcache_storage_t *
storage_create(const char **options)
{
    storage_mysql_t *st = calloc(1, sizeof(storage_mysql_t));
 
    if (options)
        parse_options(st, options);

    if (!st->dbname)
        st->dbname = strdup(ST_DBNAME_DEFAULT);

    if (!st->table)
        st->table = strdup(ST_TABLE_DEFAULT);

    if (!st->keyfield)
        st->keyfield = strdup(ST_KEYFIELD_DEFAULT);

    if (!st->keybytesfield)
        st->keybytesfield = strdup(ST_KEYBYTESFIELD_DEFAULT);

    if (!st->keysizefield)
        st->keysizefield = strdup(ST_KEYSIZEFIELD_DEFAULT);

    if (!st->valuefield)
        st->valuefield = strdup(ST_VALUEFIELD_DEFAULT);

    if (!st->valuesizefield)
        st->valuesizefield = strdup(ST_VALUESIZEFIELD_DEFAULT);

    MYSQL *mysql = mysql_init(&st->dbh);

    if (!mysql_real_connect(mysql,
                            st->dbhost,
                            st->dbuser,
                            st->dbpasswd,
                            st->dbname,
                            st->dbport,
                            st->unix_socket,
                            0))
    {
        free(st->dbname);
        free(st->table);
        free(st->keyfield);
        free(st->keybytesfield);
        free(st->keysizefield);
        free(st->valuefield);
        free(st->valuesizefield);
        free(st);
        // TODO - print error
        return NULL;
    }

    char create_table_sql[2048];
    snprintf(create_table_sql, sizeof(create_table_sql),
            "CREATE TABLE IF NOT EXISTS `%s` (`%s` char(255) primary key, `%s` int, `%s` blob, `%s` int, `%s` longblob)",
            st->table, st->keyfield, st->keysizefield, st->keybytesfield, st->valuesizefield, st->valuefield);

    int rc = mysql_query(&st->dbh, create_table_sql);

    char sql[2048];
    snprintf(sql, sizeof(sql), "SELECT `%s` FROM `%s` WHERE `%s` = ?", st->valuefield, st->table, st->keyfield);
    st->select_stmt = mysql_stmt_init(mysql);
    rc = mysql_stmt_prepare(st->select_stmt, sql, strlen(sql));
    if (rc != 0) {
        // TODO - Errors
    }
  
    snprintf(sql, sizeof(sql), "REPLACE INTO `%s` VALUES(?, ?, ?, ?, ?)", st->table);
    st->insert_stmt = mysql_stmt_init(mysql);
    rc = mysql_stmt_prepare(st->insert_stmt, sql, strlen(sql));
    if (rc != 0) {
        // TODO - Errors
    }

    snprintf(sql, sizeof(sql), "DELETE FROM `%s` WHERE `%s` = ?", st->table, st->keyfield);
    st->delete_stmt = mysql_stmt_init(mysql);
    rc = mysql_stmt_prepare(st->delete_stmt, sql, strlen(sql));
    if (rc != 0) {
        // TODO - Errors
    }

    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM `%s` WHERE `%s` = ?", st->table, st->keyfield);
    st->exist_stmt = mysql_stmt_init(mysql);
    rc = mysql_stmt_prepare(st->exist_stmt, sql, strlen(sql));
    if (rc != 0) {
        // TODO - Errors
    }

    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM `%s`", st->table);
    st->count_stmt = mysql_stmt_init(mysql);
    rc = mysql_stmt_prepare(st->count_stmt, sql, strlen(sql));
    if (rc != 0) {
        // TODO - Errors
    }

    snprintf(sql, sizeof(sql), "SELECT `%s`, `%s`  FROM `%s`",
            st->keybytesfield, st->valuesizefield, st->table);
    st->index_stmt = mysql_stmt_init(mysql);
    rc = mysql_stmt_prepare(st->index_stmt, sql, strlen(sql)); 
    if (rc != 0) {
        // TODO - Errors
    }

    shardcache_storage_t *storage = calloc(1, sizeof(shardcache_storage_t));
    storage->fetch  = st_fetch;
    storage->store  = st_store;
    storage->remove = st_remove;
    storage->count  = st_count;
    storage->index  = st_index;
    storage->priv = st;

    pthread_mutex_init(&st->lock, NULL);
    return storage;
}


