#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
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
    char *dbfile;
    char *dbname;
    char *table;
    char *keyfield;
    char *keybytesfield;
    char *keysizefield;
    char *valuefield;
    char *valuesizefield;
    int  external_blobs;
    char *storage_path;
    sqlite3 *dbh;
    sqlite3_stmt *select_stmt;
    sqlite3_stmt *insert_stmt;
    sqlite3_stmt *delete_stmt;
    sqlite3_stmt *count_stmt;
    sqlite3_stmt *index_stmt;
    pthread_mutex_t lock;
} storage_sqlite_t;

static void
parse_options(storage_sqlite_t *st, const char **options)
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
            if (strcmp(key, "dbfile") == 0) {
                st->dbfile = strdup(value);
            } else if (strcmp(key, "dbname") == 0) {
                st->dbname = strdup(value);
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
    storage_sqlite_t *st = (storage_sqlite_t *)priv;

    char *keystr = malloc((klen*2)+ 1);
    char *p = (char *)key;
    char *o = (char *)keystr;
    int i;
    for (i = 0; i < klen; i++) {
        snprintf(o, 3, "%02x", p[i]);
        o += 2;
    }
    *o = 0;

    void *data = NULL;

    pthread_mutex_lock(&st->lock);
    int rc = sqlite3_reset(st->select_stmt);

    rc = sqlite3_bind_text(st->select_stmt, 1, keystr, strlen(keystr), SQLITE_STATIC);

    int cnt = 0;
    rc = sqlite3_step(st->select_stmt);
    while (rc == SQLITE_ROW) {
        if (cnt++ > 0) {
            fprintf(stderr, "Multiple rows found for key %s (%d)\n", keystr, rc);
            continue;
        }
        int bytes;
        const void *sqlite_data;
        bytes = sqlite3_column_int(st->select_stmt, 3);
        sqlite_data  = sqlite3_column_blob (st->select_stmt, 4);
        if (bytes && sqlite_data) {
            data = malloc(bytes);
            memcpy(data, sqlite_data, bytes);
            if (vlen)
                *vlen = bytes;
        }
        rc = sqlite3_step(st->select_stmt);
    }

    pthread_mutex_unlock(&st->lock);

    if (!data && vlen)
        *vlen = 0;

    return data;
}

static int st_store(void *key, size_t klen, void *value, size_t vlen, void *priv)
{
    storage_sqlite_t *st = (storage_sqlite_t *)priv;
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
    sqlite3_reset(st->insert_stmt);

    sqlite3_bind_text(st->insert_stmt, 1, keystr, strlen(keystr), SQLITE_STATIC);
    sqlite3_bind_int(st->insert_stmt, 2, klen);
    sqlite3_bind_blob(st->insert_stmt, 3, key, klen, SQLITE_STATIC);
    sqlite3_bind_int(st->insert_stmt, 4, vlen);
    sqlite3_bind_blob(st->insert_stmt, 5, value, vlen, SQLITE_STATIC);

    int rc = sqlite3_step(st->insert_stmt);
    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Insert Failed! %d\n", rc);
        pthread_mutex_unlock(&st->lock);
        return -1;
    }

    pthread_mutex_unlock(&st->lock);
    return 0;
}

static int st_remove(void *key, size_t klen, void *priv)
{

    storage_sqlite_t *st = (storage_sqlite_t *)priv;
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
    sqlite3_reset(st->delete_stmt);
    sqlite3_bind_text(st->delete_stmt, 1, keystr, strlen(keystr), SQLITE_STATIC);
    int rc = sqlite3_step(st->delete_stmt);

    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Delete Failed! %d\n", rc);
        pthread_mutex_unlock(&st->lock);
        return -1;
    }

    pthread_mutex_unlock(&st->lock);
    return 0;
}

static size_t st_count(void *priv)
{
    storage_sqlite_t *st = (storage_sqlite_t *)priv;

    size_t count;

    pthread_mutex_lock(&st->lock);

    int rc = sqlite3_reset(st->count_stmt);

    rc = sqlite3_step(st->count_stmt);
    if (rc == SQLITE_ROW) {
        count = sqlite3_column_int(st->count_stmt, 0);
    }
    pthread_mutex_unlock(&st->lock);

    return count;
}

static size_t st_index(shardcache_storage_index_item_t *index, size_t isize, void *priv)
{
    storage_sqlite_t *st = (storage_sqlite_t *)priv;

    pthread_mutex_lock(&st->lock);

    int rc = sqlite3_reset(st->index_stmt);

    size_t i = 0;
    rc = sqlite3_step(st->index_stmt);
    while (rc == SQLITE_ROW && i < isize) {
        int vlen;
        int klen;
        const void *key;

        key = sqlite3_column_blob(st->index_stmt, 0);
        klen = sqlite3_column_int(st->index_stmt, 1);
        vlen = sqlite3_column_int(st->index_stmt, 2);

        shardcache_storage_index_item_t *item = &index[i++];
        item->key = malloc(klen);
        memcpy(item->key, key, klen);
        item->klen = klen;
        item->vlen = vlen;

        rc = sqlite3_step(st->index_stmt);
    }
    pthread_mutex_unlock(&st->lock);

    return i;
}

void
storage_destroy(shardcache_storage_t *storage)
{
    storage_sqlite_t *st = (storage_sqlite_t *)storage->priv;
    sqlite3_finalize(st->select_stmt);
    sqlite3_finalize(st->insert_stmt);
    sqlite3_finalize(st->delete_stmt);
    sqlite3_finalize(st->count_stmt);
    sqlite3_finalize(st->index_stmt);
    sqlite3_close(st->dbh);
    free(st->dbname);
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
    storage_sqlite_t *st = calloc(1, sizeof(storage_sqlite_t));
 
    if (options)
        parse_options(st, options);

    if (!st->dbfile) {
        fprintf(stderr, "the dbfile option is mandatory!\n");
        free(st);
        return NULL;
    }

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

    int rc = sqlite3_open(st->dbfile, &st->dbh);
    if (rc != SQLITE_OK) {
        free(st->dbname);
        free(st->table);
        free(st->keyfield);
        free(st->keybytesfield);
        free(st->keysizefield);
        free(st->valuefield);
        free(st->valuesizefield);
        free(st);
        return NULL;
    }

    char create_table_sql[2048];
    snprintf(create_table_sql, sizeof(create_table_sql),
            "CREATE TABLE IF NOT EXISTS %s (%s TEXT PRIMARY KEY, %s INTEGER, %s BLOB, %s INTEGER, %s BLOB)",
            st->table, st->keyfield, st->keysizefield, st->keybytesfield, st->valuesizefield, st->valuefield);

    rc = sqlite3_exec(st->dbh, create_table_sql, NULL, NULL, NULL);

    const char *tail = NULL;
    char sql[2048];
    snprintf(sql, sizeof(sql), "SELECT * FROM %s WHERE %s = ?", st->table, st->keyfield);
    rc = sqlite3_prepare_v2(st->dbh, sql, 2048, &st->select_stmt, &tail); 
    if (rc != SQLITE_OK) {
        // TODO - Errors
    }
  
    snprintf(sql, sizeof(sql), "INSERT OR REPLACE INTO %s VALUES(?, ?, ?, ?, ?)", st->table);
    rc = sqlite3_prepare_v2(st->dbh, sql, 2048, &st->insert_stmt, &tail); 
    if (rc != SQLITE_OK) {
        // TODO - Errors
    }

    snprintf(sql, sizeof(sql), "DELETE FROM %s WHERE %s = ?", st->table, st->keyfield);
    rc = sqlite3_prepare_v2(st->dbh, sql, 2048, &st->delete_stmt, &tail); 
    if (rc != SQLITE_OK) {
        // TODO - Errors
    }

    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM %s", st->table);
    rc = sqlite3_prepare_v2(st->dbh, sql, 2048, &st->count_stmt, &tail); 
    if (rc != SQLITE_OK) {
        // TODO - Errors
    }

    snprintf(sql, sizeof(sql), "SELECT %s, %s, %s  FROM %s",
            st->keybytesfield, st->keysizefield, st->valuesizefield, st->table);
    rc = sqlite3_prepare_v2(st->dbh, sql, 2048, &st->index_stmt, &tail); 
    if (rc != SQLITE_OK) {
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


