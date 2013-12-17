UNAME := $(shell uname)

DEPS += deps/.libs/libshardcache.a \
        deps/.libs/libiomux.a \
        deps/.libs/libhl.a \
        deps/.libs/libchash.a \
        deps/.libs/libsiphash.a \
        deps/.libs/libjemalloc.a

LDFLAGS += -L. -ldl

ifeq ($(UNAME), Linux)
LDFLAGS += -pthread
else
LDFLAGS +=
CFLAGS += -Wno-deprecated-declarations
endif

#CC = gcc
TARGETS = $(patsubst %.c, %.o, $(wildcard src/*.c))

all: objects shardcached

.PHONY: build_deps
build_deps:
	@make -eC deps all

update_deps:
	@make -C deps update

purge_deps:
	@make -C deps purge

$(LIBSHARDCACHE_DIR)/libshardcache.a:
	make -C $(LIBSHARDCACHE_DIR) static

shardcached: objects
	gcc src/*.o $(LDFLAGS) $(DEPS) -o shardcached

$(DEPS): build_deps

objects: CFLAGS += -fPIC -Ideps/.incs -Isrc -Ideps/.incs -Wall -Werror -Wno-parentheses -Wno-pointer-sign -O3
objects: $(DEPS) $(TARGETS)

clean:
	rm -f src/*.o
	rm -f shardcached
	make -C deps clean
