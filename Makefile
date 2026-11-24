CC = gcc
CFLAGS = -Wall -Wextra -pthread

all: server worker client

server: src/server.c
	$(CC) $(CFLAGS) src/server.c -o server

worker: src/worker.c
	$(CC) $(CFLAGS) src/worker.c -o worker

client: src/client.c
	$(CC) $(CFLAGS) src/client.c -o client

clean:
	rm -f server worker client

.PHONY: all clean
