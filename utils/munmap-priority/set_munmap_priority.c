#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sched.h>
#include <unistd.h>
#include <errno.h>

#define THREAD_NAME "async-munmap"

static const char* policy_name(int policy) {
    switch (policy) {
        case SCHED_OTHER: return "SCHED_OTHER";
        case SCHED_FIFO:  return "SCHED_FIFO";
        case SCHED_RR:    return "SCHED_RR";
        case SCHED_BATCH: return "SCHED_BATCH";
        case SCHED_IDLE:  return "SCHED_IDLE";
        default:          return "UNKNOWN";
    }
}

static void print_thread_priority(pid_t tid, const char* label) {
    struct sched_param param;
    int policy = sched_getscheduler(tid);

    if (policy < 0) {
        fprintf(stderr, "Failed to get scheduler for tid %d: %s\n", tid, strerror(errno));
        return;
    }

    if (sched_getparam(tid, &param) < 0) {
        fprintf(stderr, "Failed to get sched params for tid %d: %s\n", tid, strerror(errno));
        return;
    }

    printf("%s: tid=%d, policy=%s, priority=%d\n",
           label, tid, policy_name(policy), param.sched_priority);
}

static pid_t find_munmap_thread(pid_t pid) {
    char task_path[128];
    DIR *dir;
    struct dirent *entry;
    pid_t found_tid = 0;

    snprintf(task_path, sizeof(task_path), "/proc/%d/task", pid);
    dir = opendir(task_path);
    if (!dir) {
        fprintf(stderr, "Failed to open %s: %s\n", task_path, strerror(errno));
        return 0;
    }

    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] == '.') continue;

        char comm_path[512];
        char comm[64];
        FILE *f;

        snprintf(comm_path, sizeof(comm_path), "/proc/%d/task/%s/comm", pid, entry->d_name);
        f = fopen(comm_path, "r");
        if (!f) continue;

        if (fgets(comm, sizeof(comm), f)) {
            // Remove trailing newline
            comm[strcspn(comm, "\n")] = 0;

            if (strstr(comm, THREAD_NAME) != NULL) {
                found_tid = atoi(entry->d_name);
                printf("Found thread: '%s' (tid=%d)\n", comm, found_tid);
                fclose(f);
                break;
            }
        }
        fclose(f);
    }

    closedir(dir);
    return found_tid;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <questdb_pid>\n", argv[0]);
        fprintf(stderr, "  Sets the async-munmap thread to SCHED_FIFO max priority.\n");
        fprintf(stderr, "  Must be run as root.\n");
        return 1;
    }

    if (geteuid() != 0) {
        fprintf(stderr, "Warning: Not running as root. Setting SCHED_FIFO may fail.\n");
    }

    pid_t pid = atoi(argv[1]);
    if (pid <= 0) {
        fprintf(stderr, "Invalid PID: %s\n", argv[1]);
        return 1;
    }

    printf("Searching for '%s' thread in process %d...\n", THREAD_NAME, pid);

    pid_t tid = find_munmap_thread(pid);
    if (tid == 0) {
        fprintf(stderr, "Could not find '%s' thread in process %d\n", THREAD_NAME, pid);
        fprintf(stderr, "Make sure QuestDB is running with cairo.file.async.munmap.enabled=true\n");
        return 1;
    }

    // Print current priority
    print_thread_priority(tid, "BEFORE");

    // Set to max SCHED_FIFO priority
    int max_priority = sched_get_priority_max(SCHED_FIFO);
    printf("\nSetting SCHED_FIFO with priority %d...\n", max_priority);

    struct sched_param param = {
        .sched_priority = max_priority
    };

    if (sched_setscheduler(tid, SCHED_FIFO, &param) < 0) {
        fprintf(stderr, "Failed to set scheduler: %s\n", strerror(errno));
        if (errno == EPERM) {
            fprintf(stderr, "Permission denied. Run as root.\n");
        }
        return 1;
    }

    // Print new priority
    print_thread_priority(tid, "AFTER ");

    printf("\nSuccess! Thread %d is now running with real-time SCHED_FIFO priority.\n", tid);
    return 0;
}
