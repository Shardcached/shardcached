shardcached - A distributed cache and storage system
======

C implementation of a full-featured [shardcache](http://github.com/xant/libshardcache "libshardcache") daemon

shardcached implements an http frontend exposing all functionalities provided by [libshardcache](http://github.com/xant/libshardcache "libshardcache").

 * the internal counters and the storage index are exposed through the 'magic' keys (urls) : `__index__` and `__stats__`.

 * allows to define ACLs to control which IP addresses can access which keys (including the internal keys `__index__` and `__stats__`)

 * supports mime-types rules to use when serving back items via the http frontend

 * pluggable storage backend (sqlite, mysql and redis  storage plugins have been already implemented and provided as an example in the storage_plugins/ directory)

 * provides builtin storage modules for both volatile (mem-based) and persistent (filesystem-based) storage

 * supports migrations which can be initiated by new nodes at their startup



NOTE: Almost all options can be controlled/overridden via the cmdline,
      ACLs and mime-types rules are supported only via the configuration file.


===============================================================================================================================

```
Usage: ./shardcached [OPTION]...
Version: 0.16 (libshardcache: 0.21)
Possible options:
    -a <access_log_file>  the path where to store the access_log file (defaults to './shardcached_access.log')
    -c <config_file>      the config file to load
    -d <plugins_path>     the path where to look for storage plugins (defaults to './')
    -f                    run in foreground
    -F                    force caching
    -H                    disable the HTTP frontend
    -i <interval>         change the time interval (in seconds) used to report internal stats via syslog (defaults to '0')
    -l <ip_address:port>  ip_address:port where to listen for incoming http connections
    -L                    enable lazy expiration
    -E <expire_time>      set the expiration time for cached items (defaults to: 0)
    -r <mux_timeout_low>  set the low timeout passed to iomux_run() calls (in microsecs, defaults to: 100000)
    -R <mux_timeout_high> set the high timeout pssed to iomux_run() calls (in microsecs, defaults to: 500000)
    -b                    HTTP url basepath (optional, defaults to '')
    -B                    HTTP url baseadminpath (optional, defaults to '')
    -n <nodes>            list of nodes participating in the shardcache in the form : 'label:address:port,label2:address2:port2'
    -N                    no storage subsystem, use only the internal libshardcache volatile storage
    -m me                 the label of this node, to identify it among the ones participating in the shardcache
    -P <pipelining_max>   the maximum amount of requests to handle in parallel while still serving a response (defaults to: 64)
    -S                    shared secret used for message signing (defaults to : '')
    -s                    cache size in bytes (defaults to : '536870912')
    -T <tcp_timeout>      tcp timeout (in milliseconds) used for connections opened by libshardcache (defaults to '5000')
    -t <type>             storage type (available are : 'mem' and 'fs' (defaults to 'mem')
    -o <options>          comma-separated list of storage options (defaults to '')
    -u <username>         assume the identity of <username> (only when run as root)
    -v                    increase the log level (can be passed multiple times)
    -V                    output the version number and exit
    -w <num_workers>      number of shardcache worker threads (defaults to '10')
    -W <num_http_workers> number of http worker threads (defaults to '10')
    -x <nodes>            new list of nodes to migrate the shardcache to. The format to use is the same as for the '-n' option

       Builtin storage types:
         * mem            memory based storage
            Options:
              - initial_table_size=<size>    the initial number of slots in the internal hashtable
              - max_table_size=<size>        the maximum number of slots that the internal hashtable can be grown up to

         * fs             filesystem based storage
            Options:
              - storage_path=<path>          the path where to store the keys/values on the filesystem
              - tmp_path=<path>              the path to a temporary directory to use while new data is being uploaded
```

===============================================================================================================================

Example configuration file :
```
[shardcached]
stats_interval = 0                             ; The interval in seconds at which output stats to stdout and/or syslog
                                               ; if '0' no stats will be reported on stdout/systlog
                                               ; (optional, defaults to '0')

storage_type = fs                              ; The storage type (optional, defaults to 'mem')

storage_options = storage_path=/home/xant/shardcache_storage,tmp_path=/tmp
                                               ; storage options (possibly optional, depend on the storage implementation)


plugins_dir = ./                               ; The directory where to find plugins (optional, defaults to './')
loglevel = 2                                   ; The loglevel (optional, defaults to '0' == upto(LOG_ERR))
daemon = no                                    ; Run as daemon or in foreground (optional, defaults to 'yes')
nohttp = no                                    ; Disable the HTTP subsystem (optional, defaults to 'no')
me = peer1                                     ; Identifies this peer among the shardcache nodes
                                               ; which are defined in the [nodes] section
;user = username                               ; Assume the identity of 'username' (only when run as root)
pidfile = /var/run/shardcached.pid             ; File where to store the pid of the running instance (optional)

; the nodes taking part in the shardcache (required)
[nodes]
peer1 = my_address:4444
peer2 = some_peer:4445
peer3 = some_other_peer:4446

[shardcache]
num_workers = 50                               ; Number of shardcache workers (optional, defaults to '10')
evict_on_delete = yes                          ; Evict on delete (optional, defaults to 'yes')
use_persistent_connections = yes               ; Use persistent connections instead of creating a new connection
force_caching = no                             ; Always cache remote items instead of applying a 10% chance (optional, defaults to 'no')
                                               ; for each command sent to peers
tcp_timeout = 0                                ; Set the tcp timeout for all the outgoing connections
                                               ; (optional, a 0 value will make libshardcache use the compile-time default)
                                               ; (if set to 0 or omitted the libshardcache default timeout will be used)
lazy_expiration = no                           ; Enable lazy expiration (optional, defaults to 'no')
expire_time = 0                                ; Sets the global expiration time for cached items (optional, defaults to 0, items will never expire
                                               ; and will be removed from the cache only if explicitly/naturally evicted)
iomux_run_timeout_low = 0                      ; Sets the low timeout (in microsecs) which will be passed to iomux_run() calls
                                               ; by both the serving workers and the async reader
                                               ; (optional, a 0 value will make libshardcache use the compile-time default)
iomux_run_timeout_high = 0                     ; Sets the high timeout (in microsecs) which will be passed to iomux_run() calls
                                               ; by both the listener and the expirer
                                               ; (optional, a 0 value will make libshardcache use the compile-time default)
pipelining_max = 64                            ; maximum number of requests to process ahead when pipelining
                                               ; (if omitted, the libshardcache compiled-in default will be used)
secret = default                               ; Shared secret used for message signing (optional, defaults to 'default') 

[http]
listen = *:4321                                ; HTTP address:port where to listen for incoming connections (optional)
num_workers = 50                               ; Number of http worker threads (optional, defaults to '10')
access_log = ./shardcached_access.log          ; Path to the acces_log file (optional)
basepath = shardcache                          ; Base http path (optional) 
baseadminpath = admin                          ; Base http path for administrative pages (optional, will be the same as basepath if not defined) 
acl_default = allow                            ; Default behavior for paths not matching any of those defined 
                                               ; in the acl section. Possible values are : 'allow' , 'deny'
                                               ; (optional, defaults to 'allow')

[acl]
__(stats|index)__  = deny:*:*
.*                 = deny:PUT:*
.*                 = deny:DELETE:*
.*                 = allow:*:192.168.1.123/32
.*                 = allow:*:127.0.0.1/32


[mime-types]
pdf      = application/pdf
ps       = application/postscript
xml      = application/xml
;js       = application/javascript
json     = application/json
gif      = image/gif
jpeg     = image/jpeg
jpg      = image/jpeg
png      = image/png
tiff     = image/tiff
html     = text/html
txt      = text/plain
csv      = text/csv
css      = text/css
mpg      = video/mpeg
mp4      = video/mp4

```
