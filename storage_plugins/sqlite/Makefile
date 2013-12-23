TARGET=sqlite.storage

all: $(TARGET)

$(TARGET): sqlite.c
	$(CC) $(CFLAGS) $(LDFLAGS) -I../../deps/.incs -L../../deps/.libs -fPIC -shared sqlite.c -o $(TARGET) -lsqlite3

clean:
	@rm $(TARGET)
