DEPS= https://github.com/xant/libshardcache

export DEPS_INSTALL_DIR=$(shell pwd)

export LIBDIR := $(DEPS_INSTALL_DIR)/.libs
export INCDIR := $(DEPS_INSTALL_DIR)/.incs

all:
	@mkdir -p .libs; \
	mkdir -p .incs; \
	for i in $(DEPS); do \
	    name=`echo $$i | cut -d/ -f5 `; \
	    if [ ! -d $$name ]; then \
		git clone $$i; \
	    fi; \
	    export LIBDIR=$(LIBDIR); \
	    export INCDIR=$(INCDIR); \
	    export DEPS_INSTALL_DIR=$(DEPS_INSTALL_DIR); \
	    export CFLAGS="$(CFLAGS) -DHAVE_UINT64_T"; \
	    make -eC $$name; \
	    make -eC $$name install; \
	done

update:
	@for i in $(DEPS); do \
	    name=`echo $$i | cut -d/ -f5 `; \
	    if [ -d $$name ]; then \
	        cd $$name;\
		git pull;\
		cd -;\
	    fi;\
	done

purge: clean
	@for i in $(DEPS); do \
	    name=`echo $$i | cut -d/ -f5 `; \
	    if [ -d $$name ]; then \
		rm -rf $$name; \
	    fi;\
	done

clean:
	@for i in $(DEPS); do \
	    name=`echo $$i | cut -d/ -f5 `; \
	    if [ -d $$name ]; then \
		make -C $$name clean; \
	    fi; \
	done;
	@rm -rf $(LIBDIR);
	@rm -rf $(INCDIR);

