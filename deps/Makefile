export DEPS_INSTALL_DIR=$(shell pwd)

export SHARDCACHE_INSTALL_LIBDIR := $(DEPS_INSTALL_DIR)/.libs
export SHARDCACHE_INSTALL_INCDIR := $(DEPS_INSTALL_DIR)/.incs

all:
	@mkdir -p .libs; \
	mkdir -p .incs; \
	export LIBDIR="$(LIBDIR)"; \
	export INCDIR="$(INCDIR)"; \
	export DEPS_INSTALL_DIR=$(DEPS_INSTALL_DIR); \
	export CFLAGS="$(CFLAGS)"; \
	if [ ! -f libshardcache/Makefile ]; then \
	    git submodule init;\
	    git submodule update;\
	fi;\
	make -eC libshardcache; \
	if [ $$? -ne 0 ] ; then exit $$?; fi; \
	make -eC libshardcache install; \
	cp libshardcache/deps/.incs/* $(DEPS_INSTALL_DIR)/.incs/; \
	cp libshardcache/deps/.libs/* $(DEPS_INSTALL_DIR)/.libs/

clean:
	@make -C libshardcache clean;
	@rm -rf $(DEPS_INSTALL_DIR)/.incs
	@rm -rf $(DEPS_INSTALL_DIR)/.libs

