TARGET=redis
EXT=storage

all: CFLAGS += -I../../deps/.incs -I../deps/.incs
all: LDFLAGS += -L../../deps/.libs ../deps/.libs/libhiredis.a
all: $(TARGET)

dynamic: LDFLAGS += -lhiredis
dynamic: $(TARGET)

$(TARGET): $(TARGET).c
	$(CC) $(CFLAGS) -fPIC -shared $(TARGET).c $(LDFLAGS) -o $(TARGET).$(EXT) 

clean:
	@rm $(TARGET).$(EXT)
