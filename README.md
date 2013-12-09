shardcached
======

C implementation of a full-featured shardcache daemon

shardcached implements an http frontend exposing all functionalities provided by libshardcache.

 * the internal counters and the storage index are exposed through the 'magic' keys (urls) :'__index__' and '__stats__'.

 * allows to define ACLs to control which ip addresses can access which keys (including the internal keys __index__ and __stats__)

 * supports mime-types rules to use when serving back items via the http frontend

 * pluggable storage backend (and sqlite storage plugin has been already implemented and provided as example

 * provides builtin storage modules for both volatile (mem-based) and persistent (filesystem-based) storage

 * supports migrations which can be initiated by new nodes at their startup



NOTE: Almost all options can be controlled/overridden via the cmdline,
      ACLs and mime-types rules are supported only via the configuration file


===============================================================================================================================

Usage: shardcached [OPTION]...
    Possible options:
    -a <access_log_file>  the path where to store the access_log file (detaults to './shardcached_access.log')
    -e <error_log_file>   the path where to store the error_log file (defaults to './shardcached_error.log')
    -d <plugins_path>     the path where to look for storage plugins (defaults to './')
    -f                    run in foreground
    -i <interval>         change the time interval (in seconds) used to report internal stats via syslog (defaults to '0')
    -l <ip_address:port>  ip_address:port where to listen for incoming http connections
    -b                    HTTP url basepath
    -p <peers>            list of peers participating in the shardcache in the form : 'address:port,address2:port2'
    -s                    shared secret used for message signing (defaults to : 'default')
    -t <type>             storage type (available are : 'mem' and 'fs' (defaults to 'mem')
    -o <options>          comma-separated list of storage options (defaults to '')
    -v                    increase the log level (can be passed multiple times)
    -w <num_workers>      number of shardcache worker threads (defaults to '50')

    Builtin storage types:
        * mem            memory based storage
          Options:
            - initial_table_size=<size>    the initial number of slots in the internal hashtable
            - max_table_size=<size>        the maximum number of slots that the internal hashtable can be grown up to

        * fs             filesystem based storage
          Options:
            - storage_path=<path>          the parh where to store the keys/values on the filesystem
            - tmp_path=<path>              the path to a temporary directory to use while new data is being uploaded



===============================================================================================================================

Example configuration file :

[shardcached]
stats_interval = 0                             ; The interval in seconds at which output stats to stdout and/or syslog
storage_type = fs
storage_options = storage_path=/home/xant/shardcache_storage,tmp_path=/tmp
plugins_dir = ./
loglevel = 2
daemon = 0
me = peer1

; the nodes taking part in the shardcache (required)
[nodes]
peer1 = localhost:4444
peer2 = some_peer:4445
peer3 = some_other_peer:4446

[shardcache]
num_workers = 50
evict_on_delete = 1                            ; Evict on delete (optional, defaults to 'yes')
secret = default                               ; Shared secret used for message signing (optional, defaults to 'default') 

[http]
listen = *:4321                                ; HTTP address:port where to listen for incoming connections (optional)
num_workers = 50                               ; Number of http worker threads (optional)
access_log = ./shardcached_access.log          ; Path to the acces_log file (optional)
error_log = ./shardcached_error.log            ; Path to the error_log file (optional)
basepath = shardcache                          ; Base http path (optional) 
acl_default = allow                            ; Default behavior of for paths not mentioned in the acl section
                                               ; possible values are : 'allow' , 'deny'
                                               ; (optional, defaults to 'allow')

[acl]
__(stats|index)__  = deny:*:*
.*                 = deny:PUT:*
.*                 = deny:DELETE:*
.*                 = allow:*:192.168.0.0/24
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


