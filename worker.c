#include <studio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "net.h"

#define MAX_K 64	// upper limit for K, the size of the fixed array
#define OUT_LIMIT (256 * 1024)	// we keep the first 256 kb of a job's output

// The reasons why we killed the job because waitpid() would just say 'killed by signal 9' but nothing more specific than that
#define KILL_NONE 0	// We did not kill it, it killed itself or was killed by someone else
#define KILL_TIMEOUT 1	// Killed because time was up
#define KILL_CANCEL 2	// Killed because the server sent 'cancel'

// All the data about the running job
struct job {
    int active;		// 1 = this slot holds a job, o = this slot is free
    char id[32];	// the job id from the server
    pid_t pid;		// pid of the job and it's process group
    int out_fd;		// our end of the job's output pipe (read end) -1 means closed
    long long deadline;	// time at which the job must be killed
    int why;		// why did we kill the job (KILL_NONE, KILL_TIMEOUT, KILL_CANCEL
    int exited;		// 1 = waitpit() already collected this job's child
    int status;		// status integer that waitpid() gave us
    char *out;		// buffer holding the captured ouptut
    size_t out_len;	// How many bytes of the buffer are filled in
}

struct job jobs[MAX_K];	// slots for the jobs
int K = 1; 		// how many jobs may run at once
int sock;
int sig_pipe[2];

int main(int argc, char *argv[]){
    // So that fork doesn't print twice
    setvbuf(stdout, NULL, _IONBF, 0);
    
    // Checking the passed arguments
    if (argc < 3 || argc > 4) {
        fprintf(stderr, "usage: %s <server-host> <server-port> [K]\n", argv[0]);
        return 1;
    }
    if (argc == 4) K = atoi(argv[3]);
    if (K < 1 || K > MAX_K) {
        fprintf(stderr, "worker: K must be between 1 and %d\n", MAX_K);
        return 1;
    }
    
    return 0;
}
