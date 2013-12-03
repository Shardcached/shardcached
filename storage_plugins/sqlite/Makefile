all: sqlite.so

sqlite.so:
	$(CC) $(CFLAGS) $(LDFLAGS) -I../../deps/.incs -L../../deps/.libs -fPIC -shared sqlite.c -o sqlite.storage -lsqlite3

clean:
	@rm sqlite.storage

