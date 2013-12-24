#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <mysql/mysql.h>
#include <shardcache.h>
#include <pthread.h>

#ifdef __MACH__
#include <libkern/OSAtomic.h>
#define SPIN_LOCK(__mutex) OSSpinLockLock(__mutex)
#define SPIN_TRYLOCK(__mutex) OSSpinLockTry(__mutex)
#define SPIN_UNLOCK(__mutex) OSSpinLockUnlock(__mutex)
#else
#define SPIN_LOCK(__mutex) pthread_spin_lock(__mutex)
#define SPIN_TRYLOCK(__mutex) pthread_spin_trylock(__mutex)
#define SPIN_UNLOCK(__mutex) pthread_spin_unlock(__mutex)
#endif


#define ST_KEYFIELD_DEFAULT        "key"
#define ST_KEYBYTESFIELD_DEFAULT   "keybytes"
#define ST_VALUEFIELD_DEFAULT      "value"
#define ST_VALUESIZEFIELD_DEFAULT  "valuesize"
#define ST_DBNAME_DEFAULT          "shardcache"
#define ST_TABLE_DEFAULT           "storage"
#define ST_NUM_CONNECTIONS_DEFAULT 5

typedef struct {
    MYSQL      dbh;
    MYSQL_STMT *select_stmt;
    MYSQL_STMT *insert_stmt;
    MYSQL_STMT *delete_stmt;
    MYSQL_STMT *exist_stmt;
    MYSQL_STMT *count_stmt;
    MYSQL_STMT *index_stmt;
#ifdef __MACH__
    OSSpinLock lock;
#else
    pthread_spinlock_t lock;
#endif
    int initialized;
} db_connection_t;

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
    char *valuefield;
    char *valuesizefield;
    int  external_blobs;
    char *storage_path;
    int num_connections;
    int connection_index;
    db_connection_t *dbconnections;
    int table_checked;
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

static void st_clear_dbconnection(storage_mysql_t *st, db_connection_t *dbc)
{
    if (dbc->select_stmt) {
        mysql_stmt_close(dbc->select_stmt);
        dbc->select_stmt = NULL;
    }

    if (dbc->insert_stmt) {
        mysql_stmt_close(dbc->insert_stmt);
        dbc->insert_stmt = NULL;
    }

    if (dbc->delete_stmt) {
        mysql_stmt_close(dbc->delete_stmt);
        dbc->delete_stmt = NULL;
    }

    if (dbc->exist_stmt) {
        mysql_stmt_close(dbc->exist_stmt);
        dbc->exist_stmt = NULL;
    }

    if (dbc->count_stmt) {
        mysql_stmt_close(dbc->count_stmt);
        dbc->count_stmt = NULL;
    }

    if (dbc->index_stmt) {
        mysql_stmt_close(dbc->index_stmt);
        dbc->index_stmt = NULL;
    }

    if (dbc->initialized) {
        mysql_close(&dbc->dbh);
#ifndef __MACH__
        pthread_spin_destroy(&dbc->lock);
#endif
        dbc->initialized = 0;
    }
}

static int st_init_dbconnection(storage_mysql_t *st, db_connection_t *dbc)
{
    MYSQL *mysql = mysql_init(&dbc->dbh);
    if (!mysql) {
        fprintf(stderr, "Can't initialize the mysql handler\n");
        return -1;
    }
#ifndef __MACH__
    pthread_spin_init(&dbc->lock, 0);
#endif
    dbc->initialized = 1;

    my_bool b = 1;

    if (!mysql_real_connect(&dbc->dbh,
                            st->dbhost,
                            st->dbuser,
                            st->dbpasswd,
                            st->dbname,
                            st->dbport,
                            st->unix_socket,
                            0))
    {
        fprintf(stderr, "Can't connect to mysql database: %s\n",
                mysql_error(&dbc->dbh));
        return -1;
    }

    if (!st->table_checked) {
        char create_table_sql[2048];
        snprintf(create_table_sql, sizeof(create_table_sql),
                "CREATE TABLE IF NOT EXISTS `%s` (`%s` char(255) primary key, `%s` blob, `%s` int, `%s` longblob)",
                st->table, st->keyfield, st->keybytesfield, st->valuesizefield, st->valuefield);
        mysql_query(&dbc->dbh, create_table_sql);
        st->table_checked = 1;
    }

    char sql[2048];
    snprintf(sql, sizeof(sql), "SELECT `%s` FROM `%s` WHERE `%s` = ?", st->valuefield, st->table, st->keyfield);
    dbc->select_stmt = mysql_stmt_init(mysql);
    int rc = mysql_stmt_prepare(dbc->select_stmt, sql, strlen(sql));
    if (rc != 0) {
        fprintf(stderr, "Can't prepare the select_stmt '%s' : %s\n",
                sql, mysql_stmt_error(dbc->select_stmt));
        return -1;
    }
  
    snprintf(sql, sizeof(sql), "REPLACE INTO `%s` VALUES(?, ?, ?, ?)", st->table);
    dbc->insert_stmt = mysql_stmt_init(mysql);
    rc = mysql_stmt_prepare(dbc->insert_stmt, sql, strlen(sql));
    if (rc != 0) {
        fprintf(stderr, "Can't prepare the insert_stmt '%s' : %s\n",
                sql, mysql_stmt_error(dbc->insert_stmt));
        return -1;
    }

    snprintf(sql, sizeof(sql), "DELETE FROM `%s` WHERE `%s` = ?", st->table, st->keyfield);
    dbc->delete_stmt = mysql_stmt_init(mysql);
    rc = mysql_stmt_prepare(dbc->delete_stmt, sql, strlen(sql));
    if (rc != 0) {
        fprintf(stderr, "Can't prepare the delete_stmt '%s' : %s\n",
                sql, mysql_stmt_error(dbc->delete_stmt));
        return -1;
    }

    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM `%s` WHERE `%s` = ?", st->table, st->keyfield);
    dbc->exist_stmt = mysql_stmt_init(mysql);
    rc = mysql_stmt_prepare(dbc->exist_stmt, sql, strlen(sql));
    if (rc != 0) {
        fprintf(stderr, "Can't prepare the exist_stmt '%s' : %s\n",
                sql, mysql_stmt_error(dbc->exist_stmt));
        return -1;
    }

    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM `%s`", st->table);
    dbc->count_stmt = mysql_stmt_init(mysql);
    rc = mysql_stmt_prepare(dbc->count_stmt, sql, strlen(sql));
    if (rc != 0) {
        fprintf(stderr, "Can't prepare the count_stmt '%s' : %s\n",
                sql, mysql_stmt_error(dbc->count_stmt));
        return -1;
    }

    snprintf(sql, sizeof(sql), "SELECT `%s`, `%s`  FROM `%s`",
            st->keybytesfield, st->valuesizefield, st->table);
    dbc->index_stmt = mysql_stmt_init(mysql);
    rc = mysql_stmt_prepare(dbc->index_stmt, sql, strlen(sql)); 
    if (rc != 0) {
        fprintf(stderr, "Can't prepare the index_stmt '%s' : %s\n",
                sql, mysql_stmt_error(dbc->index_stmt));
        return -1;
    }

    return 0;
}

static db_connection_t *st_get_dbconnection(storage_mysql_t *st)
{
    int rc = 0;
    int index = 0;
    db_connection_t *dbc = NULL;
    int retries = 0;
    do {
        index = __sync_fetch_and_add(&st->connection_index, 1)%st->num_connections;
        dbc = &st->dbconnections[index];
        rc = SPIN_TRYLOCK(&dbc->lock); 
        if (retries++ == 100) {
            // ok .. it's too busy
            fprintf(stderr, "Can't acquire any connection lock\n");
            return NULL;
        }
    } while (rc != 0);

    // we acquired the lock for a connection
    // let's check if it's still alive
    if (mysql_ping(&dbc->dbh) != 0) {
        st_clear_dbconnection(st, dbc);
        if (st_init_dbconnection(st, dbc) != 0)
            return NULL;
    }
    // TODO - we might try again using a different connection, but if connect failed
    //        most likely the database is really unreachable
    return dbc; 
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


    db_connection_t *dbc = st_get_dbconnection(st);
    if (!dbc) {
        free(keystr);
        return NULL;
    }

    MYSQL_BIND bnd = {
        .buffer_type = MYSQL_TYPE_STRING,
        .buffer = keystr,
        .buffer_length = strlen(keystr)
    };

    if (mysql_stmt_bind_param(dbc->select_stmt, &bnd) != 0) {
        // TODO - error messages
        SPIN_UNLOCK(&dbc->lock);
        free(keystr);
        return NULL;
    }

    if (mysql_stmt_execute(dbc->select_stmt) != 0) {
        // TODO - error messages
        fprintf(stderr, "Can't execute fetch statement : %s\n", mysql_stmt_error(dbc->select_stmt));
        SPIN_UNLOCK(&dbc->lock);
        free(keystr);
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

    mysql_stmt_bind_result(dbc->select_stmt, &obnd);

    int rc = mysql_stmt_fetch(dbc->select_stmt);

    if (error == 1) {
        data = realloc(data, size);
        obnd.buffer = data;
        obnd.buffer_length = size;
        error = 0;
        mysql_stmt_bind_result(dbc->select_stmt, &obnd);
        mysql_stmt_fetch(dbc->select_stmt);
    }

    if (rc != 0 || obnd.is_null) {
        free(data);
        data = NULL;
    } else {
        if (vlen)
            *vlen = size;
    }

    mysql_stmt_free_result(dbc->select_stmt);
    mysql_stmt_reset(dbc->select_stmt);

    SPIN_UNLOCK(&dbc->lock);

    free(keystr);
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

    db_connection_t *dbc = st_get_dbconnection(st);
    if (!dbc) {
        free(keystr);
        return -1;
    }

    MYSQL_BIND bnd[5] = {
        {
            .buffer_type = MYSQL_TYPE_STRING,
            .buffer = keystr,
            .buffer_length = strlen(keystr),
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

    if (mysql_stmt_bind_param(dbc->insert_stmt, bnd) != 0) {
        // TODO - error messages
        SPIN_UNLOCK(&dbc->lock);
        fprintf(stderr, "Can't bind params to the insert statement: %s\n", mysql_stmt_error(dbc->insert_stmt));
        free(keystr);
        return -1;
    }

    if (mysql_stmt_execute(dbc->insert_stmt) != 0) {
        // TODO - error messages
        SPIN_UNLOCK(&dbc->lock);
        free(keystr);
        return -1;
    }

    mysql_stmt_free_result(dbc->insert_stmt);

    SPIN_UNLOCK(&dbc->lock);
    free(keystr);
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

    db_connection_t *dbc = st_get_dbconnection(st);
    if (!dbc) {
        free(keystr);
        return -1;
    }

    MYSQL_BIND bnd = {
        .buffer_type = MYSQL_TYPE_STRING,
        .buffer = keystr,
        .buffer_length = strlen(keystr)
    };

    if (mysql_stmt_bind_param(dbc->delete_stmt, &bnd) != 0) {
        // TODO - error messages
        SPIN_UNLOCK(&dbc->lock);
        free(keystr);
        return -1;
    }

    if (mysql_stmt_execute(dbc->delete_stmt) != 0) {
        // TODO - error messages
        SPIN_UNLOCK(&dbc->lock);
        free(keystr);
        return -1;
    }

    mysql_stmt_free_result(dbc->delete_stmt);

    SPIN_UNLOCK(&dbc->lock);
    free(keystr);
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

    db_connection_t *dbc = st_get_dbconnection(st);
    if (!dbc) {
        free(keystr);
        return 0;
    }

    MYSQL_BIND bnd = {
        .buffer_type = MYSQL_TYPE_STRING,
        .buffer = keystr,
        .buffer_length = strlen(keystr)
    };

    if (mysql_stmt_bind_param(dbc->exist_stmt, &bnd) != 0) {
        // TODO - error messages
        SPIN_UNLOCK(&dbc->lock);
        free(keystr);
        return 0;
    }

    if (mysql_stmt_execute(dbc->exist_stmt) != 0) {
        // TODO - error messages
        SPIN_UNLOCK(&dbc->lock);
        free(keystr);
        return 0;
    }

    mysql_stmt_fetch(dbc->select_stmt);

    int count = 0;
    MYSQL_BIND obnd = {
        .buffer_type = MYSQL_TYPE_LONG,
        .buffer = &count
    };

    if (mysql_stmt_fetch_column(dbc->exist_stmt, &obnd, 0, 0) != 0) {
        // TODO - error messages
        mysql_stmt_free_result(dbc->exist_stmt);
        SPIN_UNLOCK(&dbc->lock);
        free(keystr);
        return 0;
    }

    mysql_stmt_free_result(dbc->exist_stmt);

    if (count > 1) {
        // TODO - error messages
    }

    SPIN_UNLOCK(&dbc->lock);
    free(keystr);
    return (count == 1);   
}

static size_t st_count(void *priv)
{
    storage_mysql_t *st = (storage_mysql_t *)priv;

    db_connection_t *dbc = st_get_dbconnection(st);
    if (!dbc) {
        return 0;
    }

    if (mysql_stmt_execute(dbc->count_stmt) != 0) {
        // TODO - error messages
        SPIN_UNLOCK(&dbc->lock);
        return 0;
    }


    size_t count = 0;
    MYSQL_BIND obnd = {
        .buffer_type = MYSQL_TYPE_LONG,
        .buffer = &count
    };

    mysql_stmt_bind_result(dbc->count_stmt, &obnd);

    if (mysql_stmt_fetch(dbc->count_stmt) != 0) {
        // TODO - error messages
        mysql_stmt_free_result(dbc->count_stmt);
        SPIN_UNLOCK(&dbc->lock);
        return 0;
    }

    mysql_stmt_free_result(dbc->count_stmt);

    SPIN_UNLOCK(&dbc->lock);

    return count;
}

static size_t st_index(shardcache_storage_index_item_t *index, size_t isize, void *priv)
{
    storage_mysql_t *st = (storage_mysql_t *)priv;

    db_connection_t *dbc = st_get_dbconnection(st);
    if (!dbc) {
        return 0;
    }

    if (mysql_stmt_execute(dbc->index_stmt) != 0) {
        // TODO - error messages
        SPIN_UNLOCK(&dbc->lock);
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
        obnd[0].buffer_length = klen;
        
        rc = mysql_stmt_bind_result(dbc->index_stmt, obnd);
        rc = mysql_stmt_fetch(dbc->index_stmt);
        if (rc != 0) {
            free(key);
            break;
        }

        if (error == 1) {
            key = realloc(key, klen);
            obnd[0].buffer = key;
            obnd[0].buffer_length = klen;
            error = 0;
            mysql_stmt_bind_result(dbc->select_stmt, obnd);
            mysql_stmt_fetch(dbc->select_stmt);
        }

        shardcache_storage_index_item_t *item = &index[cnt++];
        item->key = key;
        item->klen = klen;
        item->vlen = vlen;
    }

    if (rc != MYSQL_NO_DATA) {
        // TODO - Error messages
    }

    mysql_stmt_free_result(dbc->index_stmt);
    mysql_stmt_reset(dbc->index_stmt);

    SPIN_UNLOCK(&dbc->lock);

    return cnt;
}

static void
storage_mysql_destroy(storage_mysql_t *st)
{
    int i;
    for (i = 0; i < st->num_connections; i++) {
        db_connection_t *dbc = &st->dbconnections[i];
    	st_clear_dbconnection(st, dbc);
    }
    free(st->dbconnections);
    mysql_server_end();

    free(st->dbhost);
    free(st->dbname);
    if (st->dbuser)
        free(st->dbuser);
    if (st->dbpasswd)
        free(st->dbpasswd);
    if (st->unix_socket)
        free(st->unix_socket);
    free(st->table);
    free(st->keyfield);
    free(st->keybytesfield);
    free(st->valuefield);
    free(st->valuesizefield);
    if (st->storage_path)
        free(st->storage_path);
    free(st);
}

void
storage_destroy(shardcache_storage_t *storage)
{
    storage_mysql_t *st = (storage_mysql_t *)storage->priv;
    storage_mysql_destroy(st);
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

    if (!st->valuefield)
        st->valuefield = strdup(ST_VALUEFIELD_DEFAULT);

    if (!st->valuesizefield)
        st->valuesizefield = strdup(ST_VALUESIZEFIELD_DEFAULT);

    if (!st->num_connections)
        st->num_connections = ST_NUM_CONNECTIONS_DEFAULT;


    st->dbconnections = calloc(sizeof(db_connection_t), st->num_connections);

    int i;
    for (i = 0; i < st->num_connections; i++) {
        db_connection_t *dbc = &st->dbconnections[i];
        if (st_init_dbconnection(st, dbc) != 0) {
            storage_mysql_destroy(st);
            return NULL;
        }
    }

    shardcache_storage_t *storage = calloc(1, sizeof(shardcache_storage_t));
    storage->fetch  = st_fetch;
    storage->store  = st_store;
    storage->remove = st_remove;
    storage->count  = st_count;
    storage->index  = st_index;
    storage->priv = st;

    return storage;
}


