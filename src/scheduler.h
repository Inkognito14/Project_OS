#ifndef SCHEDULER_H
#define SCHEDULER_H

#include <pthread.h>
#include <stddef.h>
#include <time.h>

#define MAX_JOBS        4096
#define MAX_ID_LEN      32
#define MAX_CMD_LEN     2048
#define MAX_STATE_LEN   64

/* Default team prefix (change to your team number, e.g., "T12") */
#define DEFAULT_TEAM_ID "T1"

/* Aging interval in seconds for Priority-with-Aging policy:
 * every AGING_INTERVAL_SEC seconds spent in QUEUED adds +1 effective priority. */
#define AGING_INTERVAL_SEC 5

typedef enum {
    POLICY_FIFO = 0,
    POLICY_PRIORITY_AGING = 1
} SchedPolicy;

typedef enum {
    JOB_QUEUED = 0,
    JOB_RUNNING,
    JOB_DONE,
    JOB_KILLED_TIMEOUT,
    JOB_KILLED_SIGNAL,
    JOB_CANCELLED
} JobState;

typedef struct {
    int        seq;                    /* Monotonically increasing sequence number */
    char       id[MAX_ID_LEN];         /* e.g. "T1-1" */
    char       cmd[MAX_CMD_LEN];       /* Shell command string */
    int        timeout;                /* Timeout in seconds (>= 1) */
    int        priority;               /* Base priority 0..9 (9 = highest, 0 = default) */
    JobState   state;                  /* Current job state */
    int        exit_code;              /* Exit code (for DONE) or signal (for KILLED SIGNAL) */
    int        worker_id;              /* Assigned worker ID when RUNNING, else -1 */
    time_t     enqueue_time;           /* Timestamp when job entered QUEUED */
    int        has_output;             /* 1 if output file was written */
} Job;

typedef struct {
    Job             jobs[MAX_JOBS];
    int             count;             /* Number of jobs currently in table */
    int             next_seq;          /* Next sequence number to issue (never reused) */
    SchedPolicy     policy;
    char            team_id[16];
    pthread_mutex_t lock;
    pthread_cond_t  cond;              /* Signaled when a new job is queued or server shuts down */
    int             shutdown;          /* Set to 1 on SIGTERM to wake waiting threads */
} Scheduler;

/* Lifecycle */
int  scheduler_init(Scheduler *s, SchedPolicy policy, const char *team_id);
void scheduler_destroy(Scheduler *s);
void scheduler_signal_shutdown(Scheduler *s);
const char *scheduler_policy_name(SchedPolicy policy);

/* Core synchronized queue & job table operations */
int  scheduler_submit(Scheduler *s, const char *cmd, int timeout, int priority,
                      char *out_id, size_t out_id_size);

/* Single state-change entry point (Details 1 §7 & Details 2 §2-3).
 * All state transitions go through this function while holding s->lock. */
int  scheduler_set_state(Scheduler *s, const char *id, JobState new_state,
                         int code_or_sig, int worker_id);

/* Pick next QUEUED job according to active policy (FIFO or Priority-with-Aging),
 * transition it to RUNNING, and copy its snapshot to out_job.
 * Returns 1 if a job was dispatched, 0 if none available. */
int  scheduler_dispatch_next(Scheduler *s, int worker_id, Job *out_job,
                             int *out_eff_priority, long *out_wait_sec);

/* Query & control */
int  scheduler_format_status(Scheduler *s, const char *id, char *buf, size_t size);
int  scheduler_cancel(Scheduler *s, const char *id, int *out_was_running,
                      int *out_worker_id, int *out_already_finished);
int  scheduler_is_finished(Scheduler *s, const char *id, int *out_found);
void scheduler_requeue_worker_jobs(Scheduler *s, int worker_id);

/* Journal serialization helpers (Details 1 §7 & Details 2 §3) */
void scheduler_format_submit_line(const Job *job, char *buf, size_t size);
void scheduler_format_state_line(const char *id, JobState state, int code_or_sig,
                                 char *buf, size_t size);
int  scheduler_apply_journal_line(Scheduler *s, const char *line);
void scheduler_requeue_all_running(Scheduler *s);

#endif /* SCHEDULER_H */
