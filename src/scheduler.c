#include "scheduler.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static Job *find_job_locked(Scheduler *s, const char *id) {
    for (int i = 0; i < s->count; i++) {
        if (strcmp(s->jobs[i].id, id) == 0) {
            return &s->jobs[i];
        }
    }
    return NULL;
}

static int is_terminal_state(JobState st) {
    return st == JOB_DONE || st == JOB_KILLED_TIMEOUT ||
           st == JOB_KILLED_SIGNAL || st == JOB_CANCELLED;
}

const char *scheduler_policy_name(SchedPolicy policy) {
    switch (policy) {
        case POLICY_FIFO:           return "FIFO";
        case POLICY_PRIORITY_AGING: return "PRIORITY_AGING";
        default:                   return "UNKNOWN";
    }
}

int scheduler_init(Scheduler *s, SchedPolicy policy, const char *team_id) {
    memset(s, 0, sizeof(*s));
    s->count = 0;
    s->next_seq = 1;
    s->policy = policy;
    s->shutdown = 0;

    if (team_id && team_id[0] != '\0') {
        snprintf(s->team_id, sizeof(s->team_id), "%s", team_id);
    } else {
        snprintf(s->team_id, sizeof(s->team_id), "%s", DEFAULT_TEAM_ID);
    }

    if (pthread_mutex_init(&s->lock, NULL) != 0) {
        return -1;
    }
    if (pthread_cond_init(&s->cond, NULL) != 0) {
        pthread_mutex_destroy(&s->lock);
        return -1;
    }
    return 0;
}

void scheduler_destroy(Scheduler *s) {
    pthread_cond_destroy(&s->cond);
    pthread_mutex_destroy(&s->lock);
}

void scheduler_signal_shutdown(Scheduler *s) {
    pthread_mutex_lock(&s->lock);
    s->shutdown = 1;
    pthread_cond_broadcast(&s->cond);
    pthread_mutex_unlock(&s->lock);
}

void scheduler_format_submit_line(const Job *job, char *buf, size_t size) {
    snprintf(buf, size, "SUBMIT %s %d %d %s",
             job->id, job->timeout, job->priority, job->cmd);
}

void scheduler_format_state_line(const char *id, JobState state, int code_or_sig,
                                 char *buf, size_t size) {
    switch (state) {
        case JOB_QUEUED:
            snprintf(buf, size, "STATE %s QUEUED", id);
            break;
        case JOB_RUNNING:
            snprintf(buf, size, "STATE %s RUNNING", id);
            break;
        case JOB_DONE:
            snprintf(buf, size, "STATE %s DONE %d", id, code_or_sig);
            break;
        case JOB_KILLED_TIMEOUT:
            snprintf(buf, size, "STATE %s KILLED TIMEOUT", id);
            break;
        case JOB_KILLED_SIGNAL:
            snprintf(buf, size, "STATE %s KILLED SIGNAL %d", id, code_or_sig);
            break;
        case JOB_CANCELLED:
            snprintf(buf, size, "STATE %s CANCELLED", id);
            break;
    }
}

/* Internal single-point state mutator (caller holds s->lock).
 * Part 2 journal hook: call journal_append(jfd, line) right here BEFORE mutating job->state. */
static int set_state_locked(Scheduler *s, Job *job, JobState new_state,
                            int code_or_sig, int worker_id) {
    (void)s;
    if (!job) {
        return -1;
    }
    job->state = new_state;
    if (new_state == JOB_DONE || new_state == JOB_KILLED_SIGNAL) {
        job->exit_code = code_or_sig;
    }
    if (new_state == JOB_RUNNING) {
        job->worker_id = worker_id;
    } else if (new_state == JOB_QUEUED) {
        job->worker_id = -1;
        job->enqueue_time = time(NULL);
    } else {
        job->worker_id = -1;
    }
    return 0;
}

int scheduler_set_state(Scheduler *s, const char *id, JobState new_state,
                        int code_or_sig, int worker_id) {
    pthread_mutex_lock(&s->lock);
    Job *job = find_job_locked(s, id);
    int rc = set_state_locked(s, job, new_state, code_or_sig, worker_id);
    if (rc == 0 && new_state == JOB_QUEUED) {
        pthread_cond_broadcast(&s->cond);
    }
    pthread_mutex_unlock(&s->lock);
    return rc;
}

int scheduler_submit(Scheduler *s, const char *cmd, int timeout, int priority,
                     char *out_id, size_t out_id_size) {
    if (!cmd || cmd[0] == '\0' || timeout < 1) {
        return -1;
    }
    if (priority < 0) priority = 0;
    if (priority > 9) priority = 9;

    pthread_mutex_lock(&s->lock);
    if (s->count >= MAX_JOBS) {
        pthread_mutex_unlock(&s->lock);
        return -1;
    }

    Job *job = &s->jobs[s->count];
    memset(job, 0, sizeof(*job));
    job->seq = s->next_seq++;
    snprintf(job->id, sizeof(job->id), "%s-%d", s->team_id, job->seq);
    snprintf(job->cmd, sizeof(job->cmd), "%s", cmd);
    job->timeout = timeout;
    job->priority = priority;
    job->worker_id = -1;
    job->enqueue_time = time(NULL);

    /* Part 2 journal hook: journal_append(jfd, submit_line) before committing */
    set_state_locked(s, job, JOB_QUEUED, 0, -1);
    s->count++;

    if (out_id && out_id_size > 0) {
        snprintf(out_id, out_id_size, "%s", job->id);
    }

    pthread_cond_broadcast(&s->cond);
    pthread_mutex_unlock(&s->lock);
    return 0;
}

/* Compute effective priority for Priority-with-Aging policy:
 *   eff_priority = base_priority + (wait_seconds / AGING_INTERVAL_SEC)
 * This guarantees that low-priority jobs gradually climb in priority and never starve. */
static int compute_effective_priority(const Job *job, time_t now, long *out_wait_sec) {
    long wait_sec = (now > job->enqueue_time) ? (long)(now - job->enqueue_time) : 0;
    if (out_wait_sec) {
        *out_wait_sec = wait_sec;
    }
    int aging_boost = (int)(wait_sec / AGING_INTERVAL_SEC);
    return job->priority + aging_boost;
}

int scheduler_dispatch_next(Scheduler *s, int worker_id, Job *out_job,
                            int *out_eff_priority, long *out_wait_sec) {
    pthread_mutex_lock(&s->lock);

    int best_idx = -1;
    int best_eff = -1;
    long best_wait = 0;
    time_t now = time(NULL);

    for (int i = 0; i < s->count; i++) {
        Job *j = &s->jobs[i];
        if (j->state != JOB_QUEUED) {
            continue;
        }

        if (s->policy == POLICY_FIFO) {
            /* Baseline FIFO: oldest job (lowest seq) first; priority is ignored */
            if (best_idx == -1 || j->seq < s->jobs[best_idx].seq) {
                best_idx = i;
                best_eff = j->priority;
                best_wait = (now > j->enqueue_time) ? (long)(now - j->enqueue_time) : 0;
            }
        } else {
            /* Priority with Aging: highest effective priority first;
             * ties broken by oldest arrival (lowest seq). */
            long wait_sec = 0;
            int eff = compute_effective_priority(j, now, &wait_sec);
            if (best_idx == -1 ||
                eff > best_eff ||
                (eff == best_eff && j->seq < s->jobs[best_idx].seq)) {
                best_idx = i;
                best_eff = eff;
                best_wait = wait_sec;
            }
        }
    }

    if (best_idx == -1) {
        pthread_mutex_unlock(&s->lock);
        return 0;
    }

    Job *chosen = &s->jobs[best_idx];
    set_state_locked(s, chosen, JOB_RUNNING, 0, worker_id);

    if (out_job) {
        *out_job = *chosen;
    }
    if (out_eff_priority) {
        *out_eff_priority = best_eff;
    }
    if (out_wait_sec) {
        *out_wait_sec = best_wait;
    }

    pthread_mutex_unlock(&s->lock);
    return 1;
}

int scheduler_format_status(Scheduler *s, const char *id, char *buf, size_t size) {
    pthread_mutex_lock(&s->lock);
    Job *job = find_job_locked(s, id);
    if (!job) {
        snprintf(buf, size, "%s UNKNOWN", id);
        pthread_mutex_unlock(&s->lock);
        return 0; /* not found */
    }

    switch (job->state) {
        case JOB_QUEUED:
            snprintf(buf, size, "%s QUEUED", job->id);
            break;
        case JOB_RUNNING:
            snprintf(buf, size, "%s RUNNING", job->id);
            break;
        case JOB_DONE:
            snprintf(buf, size, "%s DONE %d", job->id, job->exit_code);
            break;
        case JOB_KILLED_TIMEOUT:
            snprintf(buf, size, "%s KILLED TIMEOUT", job->id);
            break;
        case JOB_KILLED_SIGNAL:
            snprintf(buf, size, "%s KILLED SIGNAL %d", job->id, job->exit_code);
            break;
        case JOB_CANCELLED:
            snprintf(buf, size, "%s CANCELLED", job->id);
            break;
    }
    pthread_mutex_unlock(&s->lock);
    return 1; /* found */
}

int scheduler_cancel(Scheduler *s, const char *id, int *out_was_running,
                     int *out_worker_id, int *out_already_finished) {
    if (out_was_running)      *out_was_running = 0;
    if (out_worker_id)        *out_worker_id = -1;
    if (out_already_finished) *out_already_finished = 0;

    pthread_mutex_lock(&s->lock);
    Job *job = find_job_locked(s, id);
    if (!job) {
        pthread_mutex_unlock(&s->lock);
        return 0; /* UNKNOWN */
    }

    if (is_terminal_state(job->state)) {
        if (out_already_finished) *out_already_finished = 1;
        pthread_mutex_unlock(&s->lock);
        return 1;
    }

    if (job->state == JOB_RUNNING) {
        if (out_was_running) *out_was_running = 1;
        if (out_worker_id)   *out_worker_id = job->worker_id;
    }

    set_state_locked(s, job, JOB_CANCELLED, 0, -1);
    pthread_mutex_unlock(&s->lock);
    return 1;
}

int scheduler_is_finished(Scheduler *s, const char *id, int *out_found) {
    pthread_mutex_lock(&s->lock);
    Job *job = find_job_locked(s, id);
    if (!job) {
        if (out_found) *out_found = 0;
        pthread_mutex_unlock(&s->lock);
        return 0;
    }
    if (out_found) *out_found = 1;
    int fin = is_terminal_state(job->state);
    pthread_mutex_unlock(&s->lock);
    return fin;
}

void scheduler_requeue_worker_jobs(Scheduler *s, int worker_id) {
    pthread_mutex_lock(&s->lock);
    int requeued = 0;
    for (int i = 0; i < s->count; i++) {
        if (s->jobs[i].state == JOB_RUNNING && s->jobs[i].worker_id == worker_id) {
            set_state_locked(s, &s->jobs[i], JOB_QUEUED, 0, -1);
            requeued = 1;
        }
    }
    if (requeued) {
        pthread_cond_broadcast(&s->cond);
    }
    pthread_mutex_unlock(&s->lock);
}

int scheduler_apply_journal_line(Scheduler *s, const char *line) {
    if (!line || line[0] == '\0') return -1;

    pthread_mutex_lock(&s->lock);
    if (strncmp(line, "SUBMIT ", 7) == 0) {
        char id[MAX_ID_LEN];
        int timeout = 0, priority = 0, consumed = 0;
        if (sscanf(line + 7, "%31s %d %d %n", id, &timeout, &priority, &consumed) >= 3 &&
            consumed > 0 && s->count < MAX_JOBS) {
            const char *cmd = line + 7 + consumed;
            Job *job = &s->jobs[s->count++];
            memset(job, 0, sizeof(*job));
            snprintf(job->id, sizeof(job->id), "%s", id);
            snprintf(job->cmd, sizeof(job->cmd), "%s", cmd);
            job->timeout = timeout;
            job->priority = priority;
            job->state = JOB_QUEUED;
            job->worker_id = -1;
            job->enqueue_time = time(NULL);

            const char *dash = strrchr(id, '-');
            if (dash) {
                int seq = atoi(dash + 1);
                job->seq = seq;
                if (seq >= s->next_seq) {
                    s->next_seq = seq + 1;
                }
            } else {
                job->seq = s->next_seq++;
            }
            pthread_mutex_unlock(&s->lock);
            return 0;
        }
    } else if (strncmp(line, "STATE ", 6) == 0) {
        char id[MAX_ID_LEN], st1[32], st2[32];
        int code = 0;
        int n = sscanf(line + 6, "%31s %31s %31s %d", id, st1, st2, &code);
        Job *job = (n >= 2) ? find_job_locked(s, id) : NULL;
        if (job) {
            if (strcmp(st1, "QUEUED") == 0) {
                job->state = JOB_QUEUED;
            } else if (strcmp(st1, "RUNNING") == 0) {
                job->state = JOB_RUNNING;
            } else if (strcmp(st1, "DONE") == 0 && n >= 3) {
                job->state = JOB_DONE;
                job->exit_code = atoi(st2);
            } else if (strcmp(st1, "CANCELLED") == 0) {
                job->state = JOB_CANCELLED;
            } else if (strcmp(st1, "KILLED") == 0 && n >= 3) {
                if (strcmp(st2, "TIMEOUT") == 0) {
                    job->state = JOB_KILLED_TIMEOUT;
                } else if (strcmp(st2, "SIGNAL") == 0 && n >= 4) {
                    job->state = JOB_KILLED_SIGNAL;
                    job->exit_code = code;
                }
            }
            pthread_mutex_unlock(&s->lock);
            return 0;
        }
    }
    pthread_mutex_unlock(&s->lock);
    return -1;
}

void scheduler_requeue_all_running(Scheduler *s) {
    pthread_mutex_lock(&s->lock);
    for (int i = 0; i < s->count; i++) {
        if (s->jobs[i].state == JOB_RUNNING) {
            s->jobs[i].state = JOB_QUEUED;
            s->jobs[i].worker_id = -1;
            s->jobs[i].enqueue_time = time(NULL);
        }
    }
    pthread_mutex_unlock(&s->lock);
}
