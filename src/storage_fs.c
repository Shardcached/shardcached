#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fbuf.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <errno.h>
#include <hashtable.h>
#include <dirent.h>
#include <shardcache.h>
#include "storage_fs.h"

typedef struct {
    char *path;
    char *tmp;
    hashtable_t *index;
} storage_fs_t;

static char *
st_fs_filename(char *basepath, void *key, size_t klen, char **intermediate_path)
{
    int i;
    struct stat st; 

    if (!klen)
        return NULL;

    if (stat(basepath, &st) != 0) {
        if (mkdir(basepath, S_IRWXU) != 0 && errno != EEXIST) {
            fprintf(stderr, "Can't create directory %s: %s\n",
                    basepath, strerror(errno));
            return NULL;
        }
    }

    char fname[(klen*2)+1];
    char *p = &fname[0];
    for (i = 0; i < klen; i++) {
        snprintf(p, 3, "%02x", ((char *)key)[i]);
        p+=2;
    }

    char dname[5];
    if (klen >= 2) {
        snprintf(dname, 5, "%02x%02x", ((char *)key)[0], ((char *)key)[klen-1]);
    } else {
        snprintf(dname, 5, "%02x00", ((char *)key)[0]);
    }
    dname[4] = 0;

    int dirnamelen = strlen(basepath)+6;
    char dirname[dirnamelen];
    snprintf(dirname, dirnamelen, "%s/%s", basepath, dname);
    if (stat(dirname, &st) != 0) {
        if (mkdir(dirname, S_IRWXU) != 0 && errno != EEXIST) {
            fprintf(stderr, "Can't create directory %s: %s\n",
                    dirname, strerror(errno));
            return NULL;
        }
    }

    size_t fullpath_len = strlen(dirname) + strlen(fname) + 2;
    char *fullpath = malloc(fullpath_len);

    snprintf(fullpath, fullpath_len, "%s/%s", dirname, fname);

    if (intermediate_path) {
        *intermediate_path = strdup(dirname);
    }
    return fullpath;
}

static int
st_fetch(void *key, size_t klen, void **value, size_t *vlen, void *priv)
{
    storage_fs_t *storage = (storage_fs_t *)priv;
    char *fullpath = st_fs_filename(storage->path, key, klen, NULL);

    if (!fullpath)
        return -1;

    int fd = open(fullpath, O_RDONLY);
    if (fd >=0) {
        flock(fd, LOCK_SH);
        fbuf_t buf = FBUF_STATIC_INITIALIZER;
        int rb = fbuf_read(&buf, fd, 1024);
        while (rb != -1) {
            rb = fbuf_read(&buf, fd, 1024);
            if (rb == 0)
                break;
        }
        flock(fd, LOCK_UN);
        close(fd);
        if (fbuf_used(&buf)) {
            if (vlen)
                *vlen = fbuf_used(&buf);
            if (value)
                *value = fbuf_data(&buf);
            else
                fbuf_destroy(&buf);
            free(fullpath);
            return 0;
        }
    } else if (errno == ENOENT) {
        if (vlen)
            *vlen = 0;
        if (value)
            *value = NULL;
        free(fullpath);
        return 0;
    }

    free(fullpath);
    return -1;
}

static int
st_store(void *key, size_t klen, void *value, size_t vlen, int if_not_exists, void *priv)
{
    storage_fs_t *storage = (storage_fs_t *)priv;

    int ret = -1;
    long r = random();
    int i;

    char dname[9];
    char *p = &dname[0];
    for (i = 0; i < 4; i++) {
        snprintf(p, 3, "%02x", ((char *)&r)[i]);
        p += 2;
    }

    size_t tmpdir_len = strlen(storage->tmp)+9;
    char tmpdir[tmpdir_len];
    char *tmp_intermediate = NULL;
    char *intermediate_dir = NULL;

    snprintf(tmpdir, tmpdir_len, "%s/%s", storage->tmp, dname);

    char *fullpath = st_fs_filename(storage->path,
                                    key,
                                    klen,
                                    &intermediate_dir);

    if (if_not_exists) {
        struct stat st;
        if (stat(fullpath, &st) == 0) {
            free(fullpath);
            return 1;
        }
    }

    char *tmppath = st_fs_filename(tmpdir, key, klen, &tmp_intermediate);

    int fd = open(tmppath, O_WRONLY|O_TRUNC|O_CREAT, S_IRWXU|S_IRWXG|S_IRWXO);
    if (fd >=0) {
        int ofx = 0;
        while (ofx != vlen) {
            int wb = write(fd, value+ofx, vlen - ofx);
            if (wb > 0)
            { 
                ofx += wb;
            } else if (wb == 0 ||
                      (wb == -1 && errno != EINTR && errno != EAGAIN))
            {
                // TODO - Error messages
                break;
            }
        }
        if (ofx == vlen) {
            ret = rename(tmppath, fullpath);
            if (ret != 0)
                SHC_ERROR("Can't store data on file %s : %s\n",
                           fullpath, strerror(errno));

        }
        close(fd);
    }

    if (ret != 0)
        unlink(tmppath);

    free(fullpath);
    rmdir(tmp_intermediate);
    rmdir(tmpdir);
    free(tmppath);
    free(tmp_intermediate);
    if (intermediate_dir)
        free(intermediate_dir);
    size_t *sizep = malloc(sizeof(size_t));
    *sizep = vlen;
    ht_set(storage->index, key, klen, sizep, sizeof(size_t));
    return ret;
}

static int
st_remove(void *key, size_t klen, void *priv)
{
    storage_fs_t *storage = (storage_fs_t *)priv;
    char *intermediate_dir = NULL;
    char *fullpath = st_fs_filename(storage->path,
                                    key,
                                    klen,
                                    &intermediate_dir);
    int ret = unlink(fullpath); 
    rmdir(intermediate_dir);
    free(fullpath);
    free(intermediate_dir);
    ht_delete(storage->index, key, klen, NULL, NULL);
    return ret;
}

static int
st_exist(void *key, size_t klen, void *priv)
{
    storage_fs_t *storage = (storage_fs_t *)priv;
    char *intermediate_dir = NULL;
    char *fullpath = st_fs_filename(storage->path,
                                    key,
                                    klen,
                                    &intermediate_dir);


    struct stat st;
    int rc = stat(fullpath, &st);

    free(fullpath);
    free(intermediate_dir);
    return (rc == 0);
}


static size_t
st_count(void *priv)
{
    storage_fs_t *storage = (storage_fs_t *)priv;
    return ht_count(storage->index);
}

typedef struct {
    shardcache_storage_index_item_t *index;
    size_t size;
    size_t offset;
} st_pair_iterator_arg_t;

static int
st_pair_iterator(hashtable_t *table,
                 void *       key,
                 size_t       klen,
                 void *       value,
                 size_t       vlen,
                 void *       priv)
{
    st_pair_iterator_arg_t *arg = (st_pair_iterator_arg_t *)priv;
    if (arg->offset < arg->size) {
        shardcache_storage_index_item_t *index_item;

        index_item = &arg->index[arg->offset++];
        index_item->key = malloc(klen);
        memcpy(index_item->key, key, klen);
        size_t *size = (size_t *)value;
        index_item->klen = klen;
        index_item->vlen = *size;

        return 1;
    }
    return 0;
}

static size_t
st_index(shardcache_storage_index_item_t *index, size_t isize, void *priv)
{
    storage_fs_t *storage = (storage_fs_t *)priv;
    st_pair_iterator_arg_t arg = { index, isize, 0 };
    ht_foreach_pair(storage->index, st_pair_iterator, &arg);
    return arg.offset;
}


static void
storage_fs_walk_and_fill_index(char *path, hashtable_t *index)
{
    DIR *dirp = opendir(path);
    if (dirp) {
        struct dirent *dirent = readdir(dirp);
        while(dirent) {
            if (dirent->d_name[0] == '.') {
                dirent = readdir(dirp);
                continue;
            }

            size_t fpath_size = strlen(path) + dirent->d_reclen + 3;
            char *fpath = malloc(fpath_size);
            snprintf(fpath, fpath_size, "%s/%s", path, dirent->d_name);

            switch (dirent->d_type) {

                case DT_DIR:
                {
                    storage_fs_walk_and_fill_index(fpath, index);
                    break;
                }
                case DT_REG:
                {
                    size_t namelen = strlen(dirent->d_name);
                    size_t keylen = namelen/2;
                    char *keyname = malloc(keylen);
                    char *p = keyname;
                    int i;
                    for (i = 0; i < namelen; i+=2) {
                        uint8_t c;
                        sscanf(&dirent->d_name[i], "%02hhx", &c);
                        *p++ = c;
                    }
                    struct stat st; 
                    if (stat(fpath, &st) != 0) {
                        // TODO - Error messages
                    }
                    size_t *sizep = malloc(sizeof(size_t));
                    *sizep = st.st_size;
                    ht_set(index, keyname, keylen, sizep, sizeof(size_t));
                    free(keyname);
                    break;
                }
                default:
                    break;
            }
            free(fpath);
            dirent = readdir(dirp);
        }
        closedir(dirp);
    } else {
        fprintf(stderr, "Can't open dir %s : %s\n", path, strerror(errno));
    }
}

int
storage_fs_init(shardcache_storage_t *st, char **options)
{
    st->fetch  = st_fetch;
    st->store  = st_store;
    st->remove = st_remove;
    st->exist  = st_exist;
    st->index  = st_index;
    st->count  = st_count;

    storage_fs_t *storage = NULL;
    char *storage_path = NULL;
    char *tmp_path = NULL;
    if (options) {
        while (*options) {
            char *key = (char *)*options++;
            if (!*key)
                break;
            char *value = NULL;
            if (*options) {
                value = (char *)*options++;
            } else {
                SHC_ERROR("Odd element in the options array");
                continue;
            }
            if (key && value) {
                if (strcmp(key, "storage_path") == 0) {
                    storage_path = strdup(value);
                } else if (strcmp(key, "tmp_path") == 0) {
                    tmp_path = strdup(value);
                }else {
                    SHC_ERROR("Unknown option name %s", key);
                }
            }
        }
    }

    if (storage_path) {
        struct stat s;
        if (stat(storage_path, &s) != 0) {
            if (mkdir(storage_path, S_IRWXU) != 0) {
                SHC_ERROR("Can't create storage path %s: %s",
                        storage_path, strerror(errno));
                free(storage_path);
                if (tmp_path)
                    free(tmp_path);
                return -1;
            }
            SHC_NOTICE("Created storage path: %s", storage_path);
        }

        int check = access(storage_path, R_OK|W_OK);
        if (check != 0) {
            SHC_ERROR("Can't access the storage path %s : %s",
                    storage_path, strerror(errno));
            free(storage_path);
            if (tmp_path)
                free(tmp_path);
            return -1;
        }

        storage = calloc(1, sizeof(storage_fs_t));
        storage->path = storage_path;
        if (!tmp_path) {
            tmp_path = strdup("/tmp");
        }
        storage->tmp = tmp_path;
        if (storage->tmp) {
            check = access(storage->tmp, R_OK|W_OK);
            if (check != 0) {
                SHC_ERROR("Can't access the temporary path %s : %s",
                        storage->tmp, strerror(errno));
                free(storage);
                free(storage_path);
                free(tmp_path);
                return -1;
            }
        }
    } else {
        SHC_ERROR("No storage path defined");
        if (tmp_path)
            free(tmp_path);
        return -1;
    }
    storage->index = ht_create(1<<16, 0, free);
    storage_fs_walk_and_fill_index(storage->path, storage->index);

    st->priv = storage;
    return 0;
}

void
storage_fs_destroy(void *priv)
{
    storage_fs_t *storage = (storage_fs_t *)priv;
    free(storage->path);
    free(storage->tmp);
    ht_destroy(storage->index);
    free(storage);
}
