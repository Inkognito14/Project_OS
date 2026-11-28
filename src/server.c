#define _POSIX_C_SOURCE 200809L

#include "scheduler.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#define MAX_WORKERS    64
#define LINE_BUF_SIZE  4096
#define LISTEN_BACKLOG 64
#define POLL_STEP_MS   200

typedef struct {
    int             id;
    int             fd;
    int             active;
    int             max_slots;
    int             free_slots;
    pthread_mutex_t write_lock;
} WorkerConn;

typedef struct {
    int  fd;
    char peer_ip[INET_ADDRSTRLEN];
    int  peer_port;
} ClientArg;

static volatile sig_atomic_t g_running = 1;
static Scheduler             g_sched;
static WorkerConn            g_workers[MAX_WORKERS];
static pthread_mutex_t       g_workers_lock = PTHREAD_MUTEX_INITIALIZER;
static int                   g_next_worker_id = 1;

static void handle_sigterm(int sig) {
    (void)sig;
    g_running = 0;
}

/* Write all bytes reliably despite partial writes or EINTR. */
static int write_all(int fd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = write(fd, p + sent, len - sent);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        sent += (size_t)n;
    }
    return 0;
}

/* Read exact number of bytes reliably despite partial reads or EINTR. */
static int read_exact(int fd, void *buf, size_t len) {
    char *p = (char *)buf;
    size_t got = 0;
    while (got < len) {
        struct pollfd pfd = { .fd = fd, .events = POLLIN };
        int pr = poll(&pfd, 1, POLL_STEP_MS);
        if (pr < 0) {
            if (errno == EINTR) {
                if (!g_running) return -1;
                continue;
            }
            return -1;
        }
        if (pr == 0) {
            if (!g_running) return -1;
            continue;
        }
        ssize_t n = read(fd, p + got, len - got);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1; /* EOF */
        got += (size_t)n;
    }
    return 0;
}

/* Read a single '\n'-terminated line in a loop (one write != one read).
 * Uses poll() so idle clients never block SIGTERM shutdown or other clients.
 * Returns line length (>= 0) on success, -1 on EOF/error/shutdown. */
static ssize_t read_line(int fd, char *buf, size_t max_len) {
    size_t pos = 0;
    while (pos + 1 < max_len) {
        struct pollfd pfd = { .fd = fd, .events = POLLIN };
        int pr = poll(&pfd, 1, POLL_STEP_MS);
        if (pr < 0) {
            if (errno == EINTR) {
                if (!g_running) return -1;
                continue;
            }
            return -1;
        }
        if (pr == 0) {
            if (!g_running) return -1;
            continue;
        }
        char c;
        ssize_t n = read(fd, &c, 1);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) {
            if (pos == 0) return -1; /* Clean EOF */
            break;
        }
        if (c == '\n') {
            break;
        }
        if (c != '\r') {
            buf[pos++] = c;
        }
    }
    buf[pos] = '\0';
    return (ssize_t)pos;
}

static int send_line(int fd, const char *line) {
    size_t len = strlen(line);
    if (write_all(fd, line, len) < 0) return -1;
    if (write_all(fd, "\n", 1) < 0) return -1;
    return 0;
}

static int send_worker_line(WorkerConn *w, const char *line) {
    pthread_mutex_lock(&w->write_lock);
    int rc = send_line(w->fd, line);
    pthread_mutex_unlock(&w->write_lock);
    return rc;
}

/* Save job output to out/<id>.out (creates out/ directory if needed). */
static void save_job_output(const char *id, const char *data, size_t len) {
    mkdir("out", 0755);
    char path[256];
    snprintf(path, sizeof(path), "out/%s.out", id);
    FILE *fp = fopen(path, "wb");
    if (!fp) return;
    if (len > 0) {
        fwrite(data, 1, len, fp);
    }
    fclose(fp);
}

/* Try to match QUEUED jobs to any worker with free slots. */
static void dispatch_available_jobs(void) {
    for (;;) {
        WorkerConn *target = NULL;

        pthread_mutex_lock(&g_workers_lock);
        for (int i = 0; i < MAX_WORKERS; i++) {
            if (g_workers[i].active && g_workers[i].free_slots > 0) {
                target = &g_workers[i];
                target->free_slots--; /* Reserve slot before dispatching */
                break;
            }
        }
        pthread_mutex_unlock(&g_workers_lock);

        if (!target) {
            return;
        }

        Job job;
        int eff_prio = 0;
        long wait_sec = 0;
        if (!scheduler_dispatch_next(&g_sched, target->id, &job, &eff_prio, &wait_sec)) {
            /* No QUEUED jobs left: release reserved slot */
            pthread_mutex_lock(&g_workers_lock);
            if (target->active) {
                target->free_slots++;
            }
            pthread_mutex_unlock(&g_workers_lock);
            return;
        }

        char msg[LINE_BUF_SIZE];
        snprintf(msg, sizeof(msg), "RUN %s %d %s", job.id, job.timeout, job.cmd);
        if (send_worker_line(target, msg) < 0) {
            /* Worker write failed: requeue job and restore slot */
            scheduler_set_state(&g_sched, job.id, JOB_QUEUED, 0, -1);
            pthread_mutex_lock(&g_workers_lock);
            if (target->active) {
                target->free_slots++;
            }
            pthread_mutex_unlock(&g_workers_lock);
            return;
        }

        printf("server: job sent %s to worker %d (policy=%s, base_prio=%d, wait=%lds, eff_prio=%d)\n",
               job.id, target->id, scheduler_policy_name(g_sched.policy),
               job.priority, wait_sec, eff_prio);
    }
}

/* Register a worker slot in g_workers[]. */
static WorkerConn *register_worker(int fd, int k_slots) {
    if (k_slots < 1) k_slots = 1;
    pthread_mutex_lock(&g_workers_lock);
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (!g_workers[i].active) {
            g_workers[i].id = g_next_worker_id++;
            g_workers[i].fd = fd;
            g_workers[i].active = 1;
            g_workers[i].max_slots = k_slots;
            g_workers[i].free_slots = k_slots;
            pthread_mutex_init(&g_workers[i].write_lock, NULL);
            pthread_mutex_unlock(&g_workers_lock);
            return &g_workers[i];
        }
    }
    pthread_mutex_unlock(&g_workers_lock);
    return NULL;
}

static void unregister_worker(WorkerConn *w) {
    int wid = w->id;
    pthread_mutex_lock(&g_workers_lock);
    w->active = 0;
    w->fd = -1;
    w->free_slots = 0;
    pthread_mutex_destroy(&w->write_lock);
    pthread_mutex_unlock(&g_workers_lock);

    scheduler_requeue_worker_jobs(&g_sched, wid);
    dispatch_available_jobs();
}

static void notify_worker_cancel(int worker_id, const char *job_id) {
    char msg[128];
    snprintf(msg, sizeof(msg), "CANCEL %s", job_id);

    pthread_mutex_lock(&g_workers_lock);
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (g_workers[i].active && g_workers[i].id == worker_id) {
            send_worker_line(&g_workers[i], msg);
            break;
        }
    }
    pthread_mutex_unlock(&g_workers_lock);
}

/* Persistent loop for a connected worker. */
static void handle_worker_session(int fd, int k_slots, const char *ip, int port) {
    WorkerConn *w = register_worker(fd, k_slots);
    if (!w) {
        send_line(fd, "ERROR too many workers");
        return;
    }

    printf("server: worker %d connected (%s:%d, K=%d)\n", w->id, ip, port, k_slots);
    send_worker_line(w, "OK");
    dispatch_available_jobs();

    char line[LINE_BUF_SIZE];
    while (g_running) {
        ssize_t n = read_line(fd, line, sizeof(line));
        if (n < 0) {
            break;
        }
        if (n == 0) {
            continue;
        }

        if (strncmp(line, "REPORT ", 7) == 0) {
            char id[MAX_ID_LEN];
            char status_word[32];
            char detail_word[32];
            int code_or_sig = 0;
            size_t out_len = 0;
            JobState final_state = JOB_DONE;
            int parsed = 0;

            if (sscanf(line + 7, "%31s %31s %31s %zu",
                       id, status_word, detail_word, &out_len) == 4 &&
                strcmp(status_word, "DONE") == 0) {
                final_state = JOB_DONE;
                code_or_sig = atoi(detail_word);
                parsed = 1;
            } else if (sscanf(line + 7, "%31s %31s %31s %zu",
                              id, status_word, detail_word, &out_len) == 4 &&
                       strcmp(status_word, "KILLED") == 0 &&
                       strcmp(detail_word, "TIMEOUT") == 0) {
                final_state = JOB_KILLED_TIMEOUT;
                parsed = 1;
            } else if (sscanf(line + 7, "%31s %31s %31s %d %zu",
                              id, status_word, detail_word, &code_or_sig, &out_len) == 5 &&
                       strcmp(status_word, "KILLED") == 0 &&
                       strcmp(detail_word, "SIGNAL") == 0) {
                final_state = JOB_KILLED_SIGNAL;
                parsed = 1;
            } else if (sscanf(line + 7, "%31s %31s %zu",
                              id, status_word, &out_len) == 3 &&
                       strcmp(status_word, "CANCELLED") == 0) {
                final_state = JOB_CANCELLED;
                parsed = 1;
            }

            if (parsed) {
                char *out_buf = NULL;
                if (out_len > 0) {
                    out_buf = (char *)malloc(out_len);
                    if (!out_buf || read_exact(fd, out_buf, out_len) < 0) {
                        free(out_buf);
                        break;
                    }
                }
                save_job_output(id, out_buf ? out_buf : "", out_len);
                free(out_buf);

                scheduler_set_state(&g_sched, id, final_state, code_or_sig, -1);

                char status_str[128];
                scheduler_format_status(&g_sched, id, status_str, sizeof(status_str));
                printf("server: job finished %s\n", status_str);

                pthread_mutex_lock(&g_workers_lock);
                if (w->active && w->free_slots < w->max_slots) {
                    w->free_slots++;
                }
                pthread_mutex_unlock(&g_workers_lock);

                dispatch_available_jobs();
            }
        }
    }

    printf("server: worker %d disconnected\n", w->id);
    unregister_worker(w);
}

/* Handle SUBMIT <timeout> [priority] <command> */
static void handle_submit_cmd(int fd, const char *args) {
    int timeout = 0;
    int priority = 0;
    int consumed = 0;
    const char *cmd = NULL;

    /* Try: SUBMIT <timeout> <priority> <cmd> */
    char token2[64];
    int c1 = 0, c2 = 0;
    if (sscanf(args, "%d %n", &timeout, &c1) != 1 || c1 <= 0 || timeout < 1) {
        send_line(fd, "ERROR invalid submit arguments");
        return;
    }

    const char *rest = args + c1;
    if (sscanf(rest, "%63s %n", token2, &c2) == 1 && c2 > 0) {
        char *endptr = NULL;
        long maybe_prio = strtol(token2, &endptr, 10);
        if (endptr && *endptr == '\0' && maybe_prio >= 0 && maybe_prio <= 9 &&
            rest[c2] != '\0') {
            priority = (int)maybe_prio;
            consumed = c1 + c2;
        } else {
            priority = 0;
            consumed = c1;
        }
    } else {
        priority = 0;
        consumed = c1;
    }

    cmd = args + consumed;
    while (*cmd == ' ') cmd++;
    if (*cmd == '\0') {
        send_line(fd, "ERROR empty command");
        return;
    }

    char id[MAX_ID_LEN];
    if (scheduler_submit(&g_sched, cmd, timeout, priority, id, sizeof(id)) != 0) {
        send_line(fd, "ERROR job queue full");
        return;
    }

    printf("server: job accepted %s (timeout=%d, priority=%d, cmd=\"%s\")\n",
           id, timeout, priority, cmd);

    send_line(fd, id);
    dispatch_available_jobs();
}

static void handle_status_cmd(int fd, const char *args) {
    char id[MAX_ID_LEN];
    if (sscanf(args, "%31s", id) != 1) {
        send_line(fd, "ERROR missing job id");
        return;
    }
    char resp[128];
    scheduler_format_status(&g_sched, id, resp, sizeof(resp));
    send_line(fd, resp);
}

static void handle_cancel_cmd(int fd, const char *args) {
    char id[MAX_ID_LEN];
    if (sscanf(args, "%31s", id) != 1) {
        send_line(fd, "ERROR missing job id");
        return;
    }

    int was_running = 0, worker_id = -1, already_finished = 0;
    int found = scheduler_cancel(&g_sched, id, &was_running, &worker_id, &already_finished);

    char resp[128];
    if (!found) {
        snprintf(resp, sizeof(resp), "%s UNKNOWN", id);
        send_line(fd, resp);
        return;
    }
    if (already_finished) {
        snprintf(resp, sizeof(resp), "%s ALREADY FINISHED", id);
        send_line(fd, resp);
        return;
    }

    if (was_running && worker_id > 0) {
        notify_worker_cancel(worker_id, id);
    }

    printf("server: job cancelled %s\n", id);
    snprintf(resp, sizeof(resp), "%s CANCELLED", id);
    send_line(fd, resp);
}

static void handle_fetch_cmd(int fd, const char *args) {
    char id[MAX_ID_LEN];
    if (sscanf(args, "%31s", id) != 1) {
        send_line(fd, "ERROR missing job id");
        return;
    }

    int found = 0;
    int finished = scheduler_is_finished(&g_sched, id, &found);
    char resp[128];

    if (!found) {
        snprintf(resp, sizeof(resp), "%s UNKNOWN", id);
        send_line(fd, resp);
        return;
    }
    if (!finished) {
        snprintf(resp, sizeof(resp), "%s NOT FINISHED", id);
        send_line(fd, resp);
        return;
    }

    char path[256];
    snprintf(path, sizeof(path), "out/%s.out", id);
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        send_line(fd, "OK 0");
        return;
    }

    fseek(fp, 0, SEEK_END);
    long fsize = ftell(fp);
    if (fsize < 0) fsize = 0;
    fseek(fp, 0, SEEK_SET);

    char header[64];
    snprintf(header, sizeof(header), "OK %ld", fsize);
    if (send_line(fd, header) == 0 && fsize > 0) {
        char *buf = (char *)malloc((size_t)fsize);
        if (buf) {
            size_t rd = fread(buf, 1, (size_t)fsize, fp);
            write_all(fd, buf, rd);
            free(buf);
        }
    }
    fclose(fp);
}

/* Thread entry point for each accepted TCP connection.
 * Detached immediately so resources are reclaimed with zero leaks. */
static void *connection_thread(void *arg) {
    ClientArg *ca = (ClientArg *)arg;
    int fd = ca->fd;
    char ip[INET_ADDRSTRLEN];
    int port = ca->peer_port;
    snprintf(ip, sizeof(ip), "%s", ca->peer_ip);
    free(ca);

    printf("server: client connected (%s:%d)\n", ip, port);

    char line[LINE_BUF_SIZE];
    ssize_t n = read_line(fd, line, sizeof(line));
    if (n > 0) {
        if (strncmp(line, "WORKER", 6) == 0 && (line[6] == '\0' || line[6] == ' ')) {
            int k_slots = 1;
            if (line[6] == ' ') {
                k_slots = atoi(line + 7);
            }
            handle_worker_session(fd, k_slots, ip, port);
        } else if (strncmp(line, "SUBMIT ", 7) == 0) {
            handle_submit_cmd(fd, line + 7);
        } else if (strncmp(line, "STATUS ", 7) == 0) {
            handle_status_cmd(fd, line + 7);
        } else if (strncmp(line, "CANCEL ", 7) == 0) {
            handle_cancel_cmd(fd, line + 7);
        } else if (strncmp(line, "FETCH ", 6) == 0) {
            handle_fetch_cmd(fd, line + 6);
        } else {
            send_line(fd, "ERROR unknown command");
        }
    }

    close(fd);
    return NULL;
}

static int create_listen_socket(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    int opt = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt(SO_REUSEADDR)");
        close(fd);
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY); /* Listen on all interfaces */
    addr.sin_port = htons((uint16_t)port);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }

    if (listen(fd, LISTEN_BACKLOG) < 0) {
        perror("listen");
        close(fd);
        return -1;
    }
    return fd;
}

int main(int argc, char *argv[]) {
    /* Flush every log line immediately (even when stdout is redirected) */
    setvbuf(stdout, NULL, _IOLBF, 0);

    if (argc < 2 || argc > 3) {
        fprintf(stderr, "Usage: %s <port> [fifo|priority]\n", argv[0]);
        return 1;
    }

    char *endptr = NULL;
    long port_val = strtol(argv[1], &endptr, 10);
    if (!endptr || *endptr != '\0' || port_val < 1 || port_val > 65535) {
        fprintf(stderr, "server: invalid port '%s'\n", argv[1]);
        return 1;
    }
    int port = (int)port_val;

    /* Default to Priority-with-Aging; allow switching to FIFO via argv[2] or SCHED_POLICY env */
    SchedPolicy policy = POLICY_PRIORITY_AGING;
    const char *policy_str = (argc == 3) ? argv[2] : getenv("SCHED_POLICY");
    if (policy_str) {
        if (strcmp(policy_str, "fifo") == 0 || strcmp(policy_str, "FIFO") == 0) {
            policy = POLICY_FIFO;
        } else if (strcmp(policy_str, "priority") == 0 || strcmp(policy_str, "PRIORITY") == 0) {
            policy = POLICY_PRIORITY_AGING;
        } else {
            fprintf(stderr, "server: unknown policy '%s' (use fifo or priority)\n", policy_str);
            return 1;
        }
    }

    const char *team_id = getenv("TEAM_ID");
    if (scheduler_init(&g_sched, policy, team_id) != 0) {
        fprintf(stderr, "server: failed to initialize scheduler\n");
        return 1;
    }

    /* Ignore SIGPIPE so broken client/worker sockets never kill the server */
    signal(SIGPIPE, SIG_IGN);

    /* Clean shutdown on SIGTERM / SIGINT (without SA_RESTART so poll/accept returns promptly) */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_sigterm;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);

    int listen_fd = create_listen_socket(port);
    if (listen_fd < 0) {
        scheduler_destroy(&g_sched);
        return 1;
    }

    /* Required startup line (Details 1 §5.2) */
    printf("server: listening on port %d\n", port);

    while (g_running) {
        struct pollfd pfd = { .fd = listen_fd, .events = POLLIN };
        int pr = poll(&pfd, 1, POLL_STEP_MS);
        if (pr < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }
        if (pr == 0) {
            continue;
        }

        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }

        ClientArg *ca = (ClientArg *)malloc(sizeof(ClientArg));
        if (!ca) {
            close(client_fd);
            continue;
        }
        ca->fd = client_fd;
        inet_ntop(AF_INET, &client_addr.sin_addr, ca->peer_ip, sizeof(ca->peer_ip));
        ca->peer_port = ntohs(client_addr.sin_port);

        pthread_t tid;
        if (pthread_create(&tid, NULL, connection_thread, ca) != 0) {
            perror("pthread_create");
            close(client_fd);
            free(ca);
            continue;
        }
        pthread_detach(tid);
    }

    /* Clean shutdown sequence (Details 1 §4.4: close listening socket, exit within 2s) */
    printf("server: shutting down\n");
    close(listen_fd);
    scheduler_signal_shutdown(&g_sched);

    /* Give detached threads a brief moment to notice g_running == 0 and close their FDs */
    struct timespec ts = { .tv_sec = 0, .tv_nsec = 250000000L }; /* 250ms */
    nanosleep(&ts, NULL);

    scheduler_destroy(&g_sched);
    return 0;
}
