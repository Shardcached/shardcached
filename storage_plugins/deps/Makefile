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
	make -eC hiredis install;

clean:
	@rm -rf $(DEPS_INSTALL_DIR)/.incs
	@rm -rf $(DEPS_INSTALL_DIR)/.libs

