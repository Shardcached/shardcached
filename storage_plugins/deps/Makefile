export DEPS_INSTALL_DIR=$(shell pwd)

export SHARDCACHE_INSTALL_LIBDIR := $(DEPS_INSTALL_DIR)/.libs
export SHARDCACHE_INSTALL_INCDIR := $(DEPS_INSTALL_DIR)/.incs

all: _prepare_env riak_deps

.PHONY: _prepare_env
_prepare_env:
	@mkdir -p .libs; \
	mkdir -p .incs; \
	export LIBDIR="$(LIBDIR)"; \
	export INCDIR="$(INCDIR)"; \
	export INSTALL_INCLUDE_PATH="$(SHARDCACHE_INSTALL_INCDIR)"; \
	export INSTALL_LIBRARY_PATH="$(SHARDCACHE_INSTALL_LIBDIR)"; \
	export CFLAGS="$(CFLAGS) -I$(SHARDCACHE_INSTALL_INCDIR)"; \
	export CPPFLAGS="$(CPPFLAGS) -I$(SHARDCACHE_INSTALL_INCDIR)"; \

.PHONY: cunit_dep
cunit_dep:
	@cd CUnit ;\
	if [ ! -f Makefile ]; then \
	libtoolize --force ;\
	aclocal ;\
	autoheader ;\
	automake --force-missing --add-missing ;\
	autoconf ;\
	./configure --datarootdir=$(SHARDCACHE_INSTALL_LIBDIR) --libdir=$(SHARDCACHE_INSTALL_LIBDIR) --includedir=$(SHARDCACHE_INSTALL_INCDIR) --bindir=$(SHARDCACHE_INSTALL_LIBDIR) --prefix=$(SHARDCACHE_INSTALL_LIBDIR);\
	fi; \
	cd - ;\
	make -eC CUnit ;
	if [ $$? -ne 0 ] ; then exit $$?; fi; \
	make -eC CUnit install; \
	
.PHONY: protobuf_dep
protobuf_dep:
	@cd protobuf-cpp ;\
	if [ ! -f Makefile ]; then \
	./autogen.sh ;\
	./configure --libdir=$(SHARDCACHE_INSTALL_LIBDIR) --includedir=$(SHARDCACHE_INSTALL_INCDIR) --bindir=$(SHARDCACHE_INSTALL_LIBDIR) --enable-shared=no --with-pic;\
	fi; \
	cd - ;\
	make -eC protobuf-cpp ;
	if [ $$? -ne 0 ] ; then exit $$?; fi; \
	make -eC protobuf-cpp install;

.PHONY: protobuf_c_dep
protobuf_c_dep:
	@cd protobuf-c ;\
	if [ ! -f Makefile ]; then \
	./autogen.sh ;\
	export PATH=$$PATH:$(SHARDCACHE_INSTALL_LIBDIR) ;\
	export PROTOC=$(SHARDCACHE_INSTALL_LIBDIR)/protoc ;\
	export protobuf_LIBS="-L$(SHARDCACHE_INSTALL_LIBDIR) $(SHARDCACHE_INSTALL_LIBDIR)/libprotobuf.a $(SHARDCACHE_INSTALL_LIBDIR)/libprotoc.a";\
	export protobuf_CFLAGS=-I$(SHARDCACHE_INSTALL_INCDIR) ;\
	./configure --libdir=$(SHARDCACHE_INSTALL_LIBDIR) --includedir=$(SHARDCACHE_INSTALL_INCDIR) --bindir=$(SHARDCACHE_INSTALL_LIBDIR) --enable-shared=no --with-pic;\
	fi; \
	cd - ;\
	make -eC protobuf-c ;
	if [ $$? -ne 0 ] ; then exit $$?; fi; \
	make -eC protobuf-c install; \

.PHONY: riak_c_client_dep
riak_c_client_dep:
	@export PROTOBUF_LIBS=$(SHARDCACHE_INSTALL_LIBDIR)/libprotobuf.a ;\
	export PROTOBUF_CFLAGS=-I$(SHARDCACHE_INSTALL_INCDIR) ;\
	export protobuf_LIBS="-L$(SHARDCACHE_INSTALL_LIBDIR) $(SHARDCACHE_INSTALL_LIBDIR)/libprotobuf.a $(SHARDCACHE_INSTALL_LIBDIR)/libprotoc.a";\
	export protobuf_CFLAGS=-I$(SHARDCACHE_INSTALL_INCDIR) ;\
	export PROTOC=$(SHARDCACHE_INSTALL_LIBDIR)/protoc ;\
	export PROTOBUFC_LIBS=$(SHARDCACHE_INSTALL_LIBDIR)/libprotobuf-c.a ;\
	export PROTOBUFC_CFLAGS=-I$(SHARDCACHE_INSTALL_INCDIR) ;\
	export PROTOBUF_LIBS=$(SHARDCACHE_INSTALL_LIBDIR)/libprotobuf.a ;\
	export PROTOBUF_CFLAGS=-I$(SHARDCACHE_INSTALL_INCDIR) ;\
	export CUNIT_LIBS=$(SHARDCACHE_INSTALL_LIBDIR)/libcunit.a ;\
	export CUNIT_CFLAGS=-I$(SHARDCACHE_INSTALL_INCDIR) ;\
	export CFLAGS="$(CFLAGS) -I$(SHARDCACHE_INSTALL_INCDIR)"; \
	export CPPFLAGS="$(CPPFLAGS) -I$(SHARDCACHE_INSTALL_INCDIR)"; \
	export PATH=$$PATH:$(SHARDCACHE_INSTALL_LIBDIR) ;\
	cd riak-c-client ;\
	./autogen.sh ;\
	./configure --libdir=$(SHARDCACHE_INSTALL_LIBDIR) --includedir=$(SHARDCACHE_INSTALL_INCDIR) --bindir=$(SHARDCACHE_INSTALL_LIBDIR) --enable-shared=no --with-protoc-c=$(SHARDCACHE_INSTALL_LIBDIR)/protoc-c --with-pic --with-gnu-ld;\
	cd - ;\
	make -eC riak-c-client ; \
	if [ $$? -ne 0 ] ; then exit $$?; fi; \
	make -eC riak-c-client install;

.PHONY: riak_deps
riak_deps: _prepare_env cunit_dep protobuf_dep protobuf_c_dep riak_c_client_dep

.PHONY: redis_deps
redis_deps:
	@make -eC hiredis; \
	if [ $$? -ne 0 ] ; then exit $$?; fi; \
	make -eC hiredis install;

clean:
	@make -eC hiredis clean
	@make -eC protobuf-c clean
	@make -eC protobuf-cpp clean
	@rm -rf $(DEPS_INSTALL_DIR)/.incs
	@rm -rf $(DEPS_INSTALL_DIR)/.libs

