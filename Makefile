UNAME := $(shell uname)

DEPS += deps/.libs/libshardcache.a \
        deps/.libs/libiomux.a \
        deps/.libs/libhl.a \
        deps/.libs/libchash.a \
        deps/.libs/libsiphash.a

LDFLAGS += -L.

ifeq ($(UNAME), Linux)
LDFLAGS += -pthread
else
LDFLAGS +=
CFLAGS += -Wno-deprecated-declarations
endif

MONGOOSE_OPTIONS="-DMONGOOSE_NO_CGI -DMONGOOSE_NO_DAV -DMONGOOSE_NO_SOCKETPAIR -DMONGOOSE_NO_DIRECTORY_LISTING"

#CC = gcc
TARGETS = $(patsubst %.c, %.o, $(wildcard src/*.c))

all: $(DEPS) objects shardcached

.PHONY: tsan
tsan:
	@export CC=gcc-4.8; \
	export LDFLAGS="-pie -ltsan"; \
	export CFLAGS="-fsanitize=thread -g -fPIC -pie"; \
	make all


.PHONY: build_deps
build_deps:
	@make -eC deps all

$(LIBSHARDCACHE_DIR)/libshardcache.a:
	make -C $(LIBSHARDCACHE_DIR) static

shardcached: $(DEPS) objects
	$(CC) src/*.o $(LDFLAGS) $(DEPS) -o shardcached -ldl

.PHONY: dynamic
dynamic: objects
	$(CC) src/*.o $(LDFLAGS) -o shardcached -lshardcache -lhl

$(DEPS): build_deps

.PHONY: objects
objects: CFLAGS += -fPIC $(MONGOOSE_OPTIONS) -Ideps/.incs -Isrc -Ideps/.incs -Wall -Werror -Wno-parentheses -Wno-pointer-sign -O3 -g
objects: $(TARGETS)

clean:
	rm -f src/*.o
	rm -f shardcached
	rm -f test/*_test
	make -C deps clean

TESTS = $(patsubst %.c, %, $(wildcard test/*.c))
TEST_EXEC_ORDER =  shardcached_test

.PHONY: libut
libut:
	@if [ ! -f support/libut/Makefile ]; then git submodule init; git submodule update; fi; make -C support/libut

.PHONY: tests
tests: CFLAGS += -Isrc -Isupport/libut/src -Wno-parentheses -Wno-pointer-sign  -Wno-pointer-to-int-cast -DTHREAD_SAFE -O3 -g
tests: shardcached libut
	@for i in $(TESTS); do\
	  echo "$(CC) $(CFLAGS) $$i.c -o $$i $(LDFLAGS) -lm";\
	  $(CC) $(CFLAGS) $$i.c -o $$i support/libut/libut.a $(LDFLAGS) -lm;\
	done;\
	for i in $(TEST_EXEC_ORDER); do echo; test/$$i; echo; done

.PHONY: test
test: tests


