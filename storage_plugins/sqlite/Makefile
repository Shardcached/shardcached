all: sqlite.so

sqlite.so:
	$(CC) $(CFLAGS) $(LDFLAGS) -I../../deps/.incs -L../../deps/.libs -shared sqlite.c -o sqlite.storage -lsqlite3

clean:
	@rm sqlite.storage

