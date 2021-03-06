Loadable Storage Modules
------------------------

Storage plugins are expected to export only the two following symbols :

    - shardcache_storage_t *storage_create(const char **options)
    - void storage_destroy(shardcache_storage_t *storage)

`storage_create()` should return a pointer to a properly initialized
`shardcache_storage_t` structure.
The same pointer will be provided to `storage_destroy()` in order to release
all the resources used by the storage module.

The `**options` argument in `storage_create()` is expcted to be a NULL-terminated
array of strings containing key/value pairs.

For example:

    char **options = {
        "storage_path",         // key
        "/some/path",           // value
        "tmp_path",             // key
        "/some/temporary_path", // value
        NULL                    // terminator
    };

The storage module MUST implement the logic to check the validity of the
`**options` array in `storage_create()` and return a NULL pointer if it's invalid.

The `shardcache_storage_t` structure is so defined (check shardcache.h) :

    typedef struct __shardcache_storage_s {
        shardcache_fetch_item_callback_t       fetch;
        shardcache_store_item_callback_t       store;
        shardcache_remove_item_callback_t      remove;
        shardcache_exist_item_callback_t       exist;
        shardcache_get_index_callback_t        index;
        shardcache_count_items_callback_t      count;
        int                                    shared;
        void                                   *priv;
    } shardcache_storage_t;

All callbacks are optional, which means that read-only storage modules are
possible as well as write-only ones.

The `index` callback requires the `count` callback to be present as well,
since the former is used by the caller to determine the size of the index
before calling the `index` callback.

The `shared` member, if true, tells shardcache that the whole storage is
available to all the nodes using this same storage-type. This allows
the shardcache node to fallback querying the storage for a key it does not
own, when the responsible peer is not available.

The `priv` pointer will be provided to all callbacks and allows the storage
module to save its status/context variables which might be needed in the
callbacks implementation.

The storage module is expected to be thread-safe since access to callbacks
can happen from any shardcache worker thread and concurrent access is expected.
Storage modules should try allowing concurrent (and possibly parallel) access
to the storage if possible. If not feasible because of the nature of the
underlying storage a global lock or any other mean to serialize access needs
to be employed.

Check the documentation in shardcache.h for more details about the callbacks
to be exposed through the `shardcache_storage_t` structure.
