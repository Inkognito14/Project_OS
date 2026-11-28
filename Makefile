CC      = gcc
CFLAGS  = -Wall -Wextra -O2 -pthread

all: server

server: src/server.c src/scheduler.c src/scheduler.h
	$(CC) $(CFLAGS) -o server src/server.c src/scheduler.c

# When your team adds src/worker.c and src/client.c, add them to `all:`
# worker: src/worker.c
# 	$(CC) $(CFLAGS) -o worker src/worker.c
# client: src/client.c
# 	$(CC) $(CFLAGS) -o client src/client.c

clean:
	rm -f server worker client

.PHONY: all clean
