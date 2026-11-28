#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <sys/wait.h>
#include "net.h"
#include <errno.h>
#include <poll.h>

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
};

struct job jobs[MAX_K];	// slots for the jobs
int K = 1; 		// how many jobs may run at once, default 1
int sock;
int sig_pipe[2];

volatile sig_atomic_t got_term = 0;
// --------------------- Helper Functions ----------------------------

// Current time in ms
long long now_ms(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (long long)t.tv_sec * 1000 + t.tv_nsec / 1000000;
}

// Signal Handler
void set_handler(int sig, void (*handler)(int)) {
    struct sigaction sa;
    sa.sa_handler = handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    if (sigaction(sig, &sa, NULL) == -1) {
        perror("sigaction");
        exit(1);
    }
}

// Since write() may send fewer bytes then you asked for this function keeps writing untill everything is gone
int write_all(int fd, const char *buf, size_t len) {
    while (len > 0) {
        ssize_t n = write(fd, buf, len);	// n is how many bytes were really written
        if (n < 0) return -1;			// catches the error, e.g. the server is gone
        buf += n;				// move forward, past what was sent
        len -= n;				// to send only what is left
    }
    return 0;
}

void on_signal(int sig) {
    int saved = errno;	// saving because write() can change errno
    
    if (sig == SIGTERM) got_term = 1;
    write(sig_pipe[1], "x", 1);	// Writing to sig_pipe to wake up
    errno = saved;
}

// Sends a message to the server stating how many jobs the worker can run at once
void send_hello(void) {
    char buf[64];
    snprintf(buf, sizeof buf, "HELLO %d\n", K);
    write_all(sock, buf, strlen(buf));
}

// Kill the job and everything it started
void kill_job(struct job *j, int why){
    // Specifying the reason it was killed 
    if (j->why == KILL_NONE)
    	j->why = why;
    kill(-j->pid, SIGKILL);	// Kill the whole process group
}

// Cancel the job with a specified ID
void cancel_job(const char *id){
    for (int i = 0; i < K; i++){
    	if (jobs[i].active && strcmp(jobs[i].id, id) == 0){
    	    kill_job(&jobs[i], KILL_CANCEL);
    	}
    }
}

// Handle a command from the server
void handle_line(char *line){
    char id[32];
    int timeout, off = 0;
    
    if (sscanf(line, "RUN %31s %d %n", id, &timeout, &off) == 2)
        // Run job here: run_job(id, line + off, timeout);
    else if (sscanf(line, "CANCEL %31s", id) == 1)
        cancel_job(id);
}

// Read what is currently in a job's pipe, keeping only the first OUT_LIMIT bytes, but keep reading after that just throwing the rest away in order to free up the pipe
void read_output(struct job *j) {
    char buf[4096];
    ssize_t n;
    while((n = read(j->out_fd, buf, sizeof buf)) > 0){
    	size_t room = OUT_LIMIT - j->out_len;	// How much room left in the buffer
    	size_t take = n;	// How many bytes we want to take
    	if(take > room)	// Take only as much as you can fit
    	    take = room;
    	if(j->out != NULL && take > 0){
    	    memcpy(j->out + j->out_len, buf, take);	// Apends what's in the buffer to the end of j->out
    	    j->out_len += take;
    	}
    }
}

int main(int argc, char *argv[]){
    // So that fork doesn't print twice
    setvbuf(stdout, NULL, _IONBF, 0);
    
    // Checking the passed arguments
    if (argc < 3 || argc > 4) {
        fprintf(stderr, "usage: %s <server-host> <server-port> [K]\n", argv[0]);
        return 1;
    }
    if (argc == 4) K = atoi(argv[3]);	// if needed specify the num of concurent jobs
    if (K < 1 || K > MAX_K) {
        fprintf(stderr, "worker: K must be between 1 and %d\n", MAX_K);
        return 1;
    }
    // setting the pipe of all jobs to "no pipe"
    for (int i = 0; i < MAX_K; i++) jobs[i].out_fd = -1;
    
    // The self pipe that is made non-blocking at both ends so that the signal handler can never block and our emptying loop can never block
    pipe(sig_pipe);
    fcntl(sig_pipe[0], F_SETFL, O_NONBLOCK);
    fcntl(sig_pipe[1], F_SETFL, O_NONBLOCK);
    // Setting up signal handlers
    set_handler(SIGCHLD, on_signal);
    set_handler(SIGTERM, on_signal);
    set_handler(SIGPIPE, SIG_IGN);
    
    sock = client_connect(argv[1], atoi(argv[2]));
    
    // Changing the property of a socket so that the worker can exit if the server can not be reached within a second and write() does not wait forever
    struct timeval tv = {1, 0};
    setsockpot(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
    
    // The conncetion confirmation
    printf("worker: connected to %s:%s, K=%d\n", argv[1], argv[2], K);
    send_hello();
    
    // ---------- THE MAIN LOOP -----------------
    while (1){
    	// Creating a list of things that poll() should watch. poll() monitors multiple file descriptors to see if I/O is possible on any of them without blocking. It is rebuilt every round because jobs come and go. It is MAX_K + 2 because first 2 entries are for sock and sig_pipe. Each poll has 'fd' - file desctiptor it monitors, 'events' - where you specify the events you care about and 'revents' - events that actually happened
    	struct pollfd pfds[MAX_K + 2];
    	
    	int slot_of[MAX_K + 2];	// keeps track of which job entry k belongs to (-1 means not a job)
    	memset(pfds, 0, sizeof pfds);	// sets every byte of the array to 0, so revents starts clean
    	
    	// first entry of pfds, connection to the server
    	pfds[0].fd = sock;
    	pfds[0].events = POLLIN; // POLLIN wakes when there is something to read
    	slot_of[0] = -1;
    	// second entry for the signal pipe
    	pfds[1].fd = sig_pipe[0];
    	pfds[1].events = POLLIN;
        slot_of[1] = -1;
        int np = 2;	// keeping track of how many entries we filled in so far
        
        long long nearest = -1;	// The closest deadline of all running jobs, -1 = none
        // Loop to set up pollfd for all active jobs
        for(int i = 0; i < K; i++){
       	    if(jobs[i].active){
       	    	if(jobs[i].out_fd = 0){
       	    	    pfds[np].fd = jobs[i].out_fd;
       	    	    pfds[np].events = POLLIN;
       	    	    slot_of[np] = i;
       	    	    np = np + 1;
       	    	}
       	    	// Updating the nearest deadline, except for the jobs we already killed
       	    	if(jobs[i].why == KILL_NONE && (nearest < 0 || jobs[i].deadline < nearest)){
       	    	    nearest = jobs[i].deadline;
       	    	}
       	    }
        }
        
        // Sleep until something happens, but wakeup for the closest deadline
        int wait_ms = -1;	// -1 = sleep for as long as it takes, no deadline
        if (nearest >= 0) {
            long long d = nearest - now_ms();
            if (d < 0) d = 0;
            if (d > 1000000) d = 1000000;
            wait_ms = (int)d;
        }
        
        // poll() returns when an fd is ready, a signal comes, or time is up
        poll(pfds, np, wait_ms);
        // ----------------------------------------------------------------------------------->
    
    	// Reacting to whatever woke us up
    	// if the signal poked the self-pipe then empty it, otherwise poll() would wake up forever
    	if (pfds[1].revents & POLLIN){
    	    char tmp[64];
    	    while (read(sig_pipe[0], tmp, sizeof tmp) > 0){}
    	}
    	// looping through job pipes to clean them up
    	for (int k=2; k < np; k++){
    	    if(pfds[k].revents & (POLLIN | POLLHUP)){
    	    	struct job *j = &jobs[slot_of[k]];
    	    	if (j->active && j->out_fd >= 0)
            	    read_output(j);	// This will close the pipe at end-of-file
    	    }
    	}
    
   	 // The server said something
	if (pfds[0].revents & (POLLIN | POLLHUP | POLLERR)){
	    char line[8192];
	    if (read_line(sock, line, sizeof line) == 0){	// 0 = the server hung up
 	   	 // Shut everything down ---------------------------------------------------
	    }
	    handle_line(line);
    	}
    }
    
    return 0;
}
