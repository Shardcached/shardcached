UNAME := $(shell uname)

LDFLAGS += deps/.libs/libshardcache.a \
	   deps/.libs/libiomux.a \
	   deps/.libs/libhl.a \
	   deps/.libs/libchash.a \
	   deps/.libs/libsiphash.a \
	   -L. -ldl

ifeq ($(UNAME), Linux)
LDFLAGS += -pthread
else
LDFLAGS +=
CFLAGS += -Wno-deprecated-declarations
endif

#CC = gcc
TARGETS = $(patsubst %.c, %.o, $(wildcard src/*.c))

all: build_deps objects shardcached

build_deps:
	@make -C deps all

update_deps:
	@make -C deps update

purge_deps:
	@make -C deps purge

$(LIBSHARDCACHE_DIR)/libshardcache.a:
	make -C $(LIBSHARDCACHE_DIR) static

shardcached: objects
	gcc src/*.o $(LDFLAGS) -o shardcached

objects: CFLAGS += -fPIC -Ideps/.incs -Isrc -Wall -Werror -Wno-parentheses -Wno-pointer-sign -O3
objects: $(TARGETS)

clean:
	rm -f src/*.o
	rm -f shardcached
	make -C deps clean
