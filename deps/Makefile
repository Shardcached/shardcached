DEPS= https://github.com/xant/libshardcache

export DEPS_INSTALL_DIR=$(shell pwd)

export SHARDCACHE_INSTALL_LIBDIR := $(DEPS_INSTALL_DIR)/.libs
export SHARDCACHE_INSTALL_INCDIR := $(DEPS_INSTALL_DIR)/.incs

all:
	@mkdir -p .libs; \
	mkdir -p .incs; \
	for i in $(DEPS); do \
	    name=`echo $$i | sed 's,.*/,,;s/.git$$//' `; \
	    if [ ! -d "$$name" ]; then \
		git clone $$i; \
	    fi; \
	    export LIBDIR="$(LIBDIR)"; \
	    export INCDIR="$(INCDIR)"; \
	    export DEPS_INSTALL_DIR=$(DEPS_INSTALL_DIR); \
	    export CFLAGS="$(CFLAGS)"; \
	    make -eC $$name; \
	    if [ $$? -ne 0 ] ; then exit $$?; fi; \
	    make -eC $$name install; \
	done; \
        cp libshardcache/deps/.incs/* $(DEPS_INSTALL_DIR)/.incs/; \
        cp libshardcache/deps/.libs/* $(DEPS_INSTALL_DIR)/.libs/

update:
	@for i in $(DEPS); do \
	    name=`echo $$i | sed 's,.*/,,;s/.git$$//' `; \
	    if [ -d "$$name" ]; then \
	        cd $$name;\
		git pull;\
		cd -;\
	    fi;\
	done; \
	make -eC libshardcache update_deps

purge: clean
	@for i in $(DEPS); do \
	    name=`echo $$i | sed 's,.*/,,;s/.git$$//' `; \
	    if [ -d "$$name" ]; then \
		rm -rf $$name; \
	    fi;\
	done

clean:
	@for i in $(DEPS); do \
	    name=`echo $$i | sed 's,.*/,,;s/.git$$//' `; \
	    if [ -d "$$name" ]; then \
		make -C $$name clean; \
	    fi; \
	done;
	@rm -rf $(DEPS_INSTALL_DIR)/.incs
	@rm -rf $(DEPS_INSTALL_DIR)/.libs

