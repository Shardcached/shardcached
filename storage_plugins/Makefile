PLUGINS=sqlite mysql redis

all: build_deps $(PLUGINS)

dynamic:
	@for i in $(PLUGINS); do make -C $$i dynamic; done

.PHONY: $(PLUGINS)
$(PLUGINS):
	make -C $@ all

clean: purge_deps
	@for i in $(PLUGINS); do make -C $$i clean; done

.PHONY: build_deps
build_deps:
	@make -eC deps all

.PHONY: update_deps
update_deps:
	@make -C deps update

.PHONY: purge_deps
purge_deps:
	@make -C deps purge

