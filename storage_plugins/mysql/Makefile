TARGET=mysql
EXT=storage

all: $(TARGET)

$(TARGET): $(TARGET).c
	$(CC) $(CFLAGS) $(LDFLAGS) -I../../deps/.incs -L../../deps/.libs -fPIC -shared $(TARGET).c -o $(TARGET).$(EXT) -lmysqlclient

clean:
	@rm $(TARGET).$(EXT)

