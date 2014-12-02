TARGET=riak
EXT=storage

all: CFLAGS += -I../../deps/.incs -I../deps/.incs -g
all: LDFLAGS += -L../../deps/.libs ../deps/.libs/libriak_c_client-0.5.a ../deps/.libs/libprotobuf-c.a ../deps/.libs/libprotobuf.a 
all: $(TARGET)

dynamic: LDFLAGS += -lriak_c_client
dynamic: $(TARGET)

$(TARGET): $(TARGET).c
	$(CC) $(CFLAGS) -fPIC -shared $(TARGET).c $(LDFLAGS) -lshardcache -lhl -o $(TARGET).$(EXT)

clean:
	@rm $(TARGET).$(EXT)
