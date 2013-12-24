TARGET=sqlite
EXT=storage

all: $(TARGET)

$(TARGET): $(TARGET).c
	$(CC) $(CFLAGS) $(LDFLAGS) -I../../deps/.incs -L../../deps/.libs -fPIC -shared $(TARGET).c -o $(TARGET).$(EXT) -lsqlite3

clean:
	@rm $(TARGET).$(EXT)
