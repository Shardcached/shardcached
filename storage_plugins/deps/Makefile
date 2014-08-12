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
	cmake lua; \
	make -eC lua; \
	cd luasql; \
	$(CC) -c $(CFLAGS) src/luasql.c src/ls_mysql.c src/ls_sqlite3.c; \
	cd -; \
	ar -r .libs/liblua.a lua/CMakeFiles/liblua.dir/src/*.o  luasql/luasql.o luasql/ls_mysql.o luasql/ls_sqlite3.o; \
	ranlib .libs/liblua.a; \
	cp lua/src/lua.h .incs/; \
	cp lua/src/lua.hpp .incs/; \
	cp lua/luaconf.h .incs/; \
	cp lua/src/lualib.h .incs/; \
	cp lua/src/lauxlib.h .incs/; \
	if [ $$? -ne 0 ] ; then exit $$?; fi; \
	make -eC hiredis install;

clean:
	@rm -rf $(DEPS_INSTALL_DIR)/.incs
	@rm -rf $(DEPS_INSTALL_DIR)/.libs
	@rm luasql/*.o
	@make -eC lua clean

