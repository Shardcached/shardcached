TARGET=mysql.storage

all: $(TARGET)

$(TARGET):
	$(CC) $(CFLAGS) $(LDFLAGS) -I../../deps/.incs -L../../deps/.libs -fPIC -shared mysql.c -o $(TARGET) -lmysqlclient -g

clean:
	@rm $(TARGET)

