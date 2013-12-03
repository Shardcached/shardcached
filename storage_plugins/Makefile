PLUGINS=sqlite

all: $(PLUGINS)

.PHONY: $(PLUGINS)
$(PLUGINS):
	make -C $@ all

clean:
	@for i in $(PLUGINS); do make -C $$i clean; done
