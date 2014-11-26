export DEPS_INSTALL_DIR=$(shell pwd)

export SHARDCACHE_INSTALL_LIBDIR := $(DEPS_INSTALL_DIR)/.libs
export SHARDCACHE_INSTALL_INCDIR := $(DEPS_INSTALL_DIR)/.incs

all:
	@mkdir -p .libs; \
	mkdir -p .incs; \
	export LIBDIR="$(LIBDIR)"; \
	export INCDIR="$(INCDIR)"; \
	export INSTALL_INCLUDE_PATH="$(SHARDCACHE_INSTALL_INCDIR)"; \
	export INSTALL_LIBRARY_PATH="$(SHARDCACHE_INSTALL_LIBDIR)"; \
	export CFLAGS="$(CFLAGS)"; \
	make -eC hiredis; \
	if [ $$? -ne 0 ] ; then exit $$?; fi; \
	make -eC hiredis install; \
	cd protobuf-cpp ;\
	if [ ! -f Makefile ]; then \
	./autogen.sh ;\
	./configure --libdir=$(SHARDCACHE_INSTALL_LIBDIR) --includedir=$(SHARDCACHE_INSTALL_INCDIR) --bindir=$(SHARDCACHE_INSTALL_LIBDIR);\
	fi; \
	cd - ;\
	make -eC protobuf-cpp ;
	if [ $$? -ne 0 ] ; then exit $$?; fi; \
	make -eC protobuf-cpp install; \
	cd protobuf-c ;\
	if [ ! -f Makefile ]; then \
	./autogen.sh ;\
	./configure --libdir=$(SHARDCACHE_INSTALL_LIBDIR) --includedir=$(SHARDCACHE_INSTALL_INCDIR) --bindir=$(SHARDCACHE_INSTALL_LIBDIR);\
	fi; \
	cd - ;\
	make -eC protobuf-c ;
	if [ $$? -ne 0 ] ; then exit $$?; fi; \
	make -eC protobuf-c install; \
	cd riak-c-client ;\
	if [ ! -f Makefile ]; then \
	./autogen.sh ;\
	export PROTOBUFC_LIBS=$(SHARDCACHE_INSTALL_LIBDIR)/libprotobuf-c.a ;\
	export PROTOBUFC_CFLAGS=-I$(SHARDCACHE_INSTALL_INCDIR)/protobuf-c ;\
	export PROTOBUF_LIBS=$(SHARDCACHE_INSTALL_LIBDIR)/libprotobuf.a ;\
	export PROTOBUF_CFLAGS=-I$(SHARDCACHE_INSTALL_INCDIR)/google/protobuf ;\
	./configure --libdir=$(SHARDCACHE_INSTALL_LIBDIR) --includedir=$(SHARDCACHE_INSTALL_INCDIR) --bindir=$(SHARDCACHE_INSTALL_LIBDIR) --with-protoc-c=$(SHARDCACHE_INSTALL_LIBDIR);\
	fi; \
	cd - ;\
	make -eC riak-c-client ;
	if [ $$? -ne 0 ] ; then exit $$?; fi; \
	make -eC riak-c-client install; \


clean:
	@make -eC hiredis clean
	@make -eC protobuf-c clean
	@make -eC protobuf-cpp clean
	@rm -rf $(DEPS_INSTALL_DIR)/.incs
	@rm -rf $(DEPS_INSTALL_DIR)/.libs

