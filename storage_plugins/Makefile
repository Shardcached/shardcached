PLUGINS=sqlite mysql redis

all: build_deps $(PLUGINS)

dynamic:
	@for i in $(PLUGINS); do make -C $$i dynamic; done

.PHONY: $(PLUGINS)
$(PLUGINS):
	make -C $@ all

.PHONY: clean
clean:
	-@for i in $(PLUGINS); do make -C $$i clean; done

.PHONY: build_deps
build_deps:
	@make -eC deps all
