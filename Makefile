UNAME := $(shell uname)

DEPS += deps/.libs/libshardcache.a \
        deps/.libs/libiomux.a \
        deps/.libs/libhl.a \
        deps/.libs/libchash.a \
        deps/.libs/libsiphash.a

LDFLAGS += -L. -ldl

ifeq ($(UNAME), Linux)
LDFLAGS += -pthread
else
LDFLAGS +=
CFLAGS += -Wno-deprecated-declarations
endif

#CC = gcc
TARGETS = $(patsubst %.c, %.o, $(wildcard src/*.c))

all: $(DEPS) objects shardcached

.PHONY: build_deps
build_deps:
	@make -eC deps all

$(LIBSHARDCACHE_DIR)/libshardcache.a:
	make -C $(LIBSHARDCACHE_DIR) static

shardcached: objects
	$(CC) src/*.o $(LDFLAGS) $(DEPS) -o shardcached

.PHONY: dynamic
dynamic: objects
	$(CC) src/*.o $(LDFLAGS) -o shardcached -lshardcache -lhl

$(DEPS): build_deps

objects: CFLAGS += -fPIC -Ideps/.incs -Isrc -Ideps/.incs -Wall -Werror -Wno-parentheses -Wno-pointer-sign -O3 -g
objects: $(TARGETS)

clean:
	rm -f src/*.o
	rm -f shardcached
	make -C deps clean
