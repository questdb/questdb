CC = gcc
CFLAGS = -Wall -Wextra -O2

TARGET = set_munmap_priority

all: $(TARGET)

$(TARGET): set_munmap_priority.c
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -f $(TARGET)

install: $(TARGET)
	install -m 755 $(TARGET) /usr/local/bin/

.PHONY: all clean install
