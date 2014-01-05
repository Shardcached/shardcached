TARGET=sqlite
EXT=storage

all: CFLAGS += -I../../deps/.incs
all: LDFLAGS += -L../../deps/.libs
all: $(TARGET)

dynamic: $(TARGET)

$(TARGET): $(TARGET).c
	$(CC) $(CFLAGS) $(LDFLAGS) -fPIC -shared $(TARGET).c -o $(TARGET).$(EXT) -lsqlite3

clean:
	@rm $(TARGET).$(EXT)
