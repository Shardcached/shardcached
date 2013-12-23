TARGET=mysql.storage

all: $(TARGET)

$(TARGET): mysql.c
	$(CC) $(CFLAGS) $(LDFLAGS) -I../../deps/.incs -L../../deps/.libs -fPIC -shared mysql.c -o $(TARGET) -lmysqlclient -O3

clean:
	@rm $(TARGET)

