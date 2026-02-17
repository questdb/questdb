/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

#define _GNU_SOURCE

#include <unistd.h>
#include <sys/errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include "../share/os.h"
#include "jemalloc-cmake/include/jemalloc/jemalloc.h"

#ifdef __APPLE__

#include <mach/mach_init.h>
#include <mach/task.h>

#endif

JNIEXPORT jint JNICALL Java_io_questdb_std_Os_getPid
        (JNIEnv *e, jclass cp) {
    return getpid();
}

#ifdef __APPLE__

JNIEXPORT jlong JNICALL Java_io_questdb_std_Os_getRss
        (JNIEnv *e, jclass cl) {
    struct mach_task_basic_info info;
    mach_msg_type_number_t infoCount = MACH_TASK_BASIC_INFO_COUNT;
    kern_return_t status = task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t) &info, &infoCount);
    if (status != KERN_SUCCESS) {
        return (jlong) 0L;
    }
    return (jlong) info.resident_size;
}

#else

JNIEXPORT jlong JNICALL Java_io_questdb_std_Os_getRss
        (JNIEnv *e, jclass cl) {
    FILE* fd = fopen("/proc/self/statm", "r");
    if (fd == NULL) {
        return 0L;
    }
    long rss = 0L;
    int res = fscanf(fd, "%*s%ld", &rss);
    fclose(fd);

    if (res == 1) {
        return rss * sysconf(_SC_PAGESIZE);
    } else {
        return 0L;
    }
}

#endif

JNIEXPORT jlong JNICALL Java_io_questdb_std_Os_currentTimeMicros
        (JNIEnv *e, jclass cl) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000 + tv.tv_usec;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Os_currentTimeNanos
        (JNIEnv *e, jclass cl) {

    struct timespec timespec;
    clock_gettime(CLOCK_REALTIME, &timespec);
    return timespec.tv_sec * 1000000000LL + timespec.tv_nsec;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Os_errno
        (JNIEnv *e, jclass cl) {
    return errno;
}

#if defined(__linux__)

JNIEXPORT jint JNICALL Java_io_questdb_std_Os_getEnvironmentType
        (JNIEnv *e, jclass cl) {
    FILE* fd = fopen("/proc/version", "r");
    if (fd == NULL) {
        return 0;
    }
    char version[64];
    int res = fscanf(fd, "%*s %*s %63s", version);
    fclose(fd);

    if (res == 1) {
        if (strstr(version, "aws") != NULL || strstr(version, "amzn") != NULL) {
            return 1;
        }
        if (strstr(version, "azure") != NULL) {
            return 2;
        }
        if (strstr(version, "cloud") != NULL) {
            return 3;
        }
        if (strstr(version, "WSL2") != NULL) {
            return 4;
        }
    }
    return 0;
}

#else

JNIEXPORT jint JNICALL Java_io_questdb_std_Os_getEnvironmentType
        (JNIEnv *e, jclass cl) {
    return 0; // no-op
}

#endif

typedef struct {
    int fdRead;
    int fdWrite;
    pid_t pid;
} fork_exec_t;

fork_exec_t *forkExec(char *argv[]) {

    int childIn[2];
    int childOut[2];

    if (pipe(childIn) == -1) {
        return NULL;
    }

    if (pipe(childOut) == -1) {
        close(childIn[0]);
        close(childIn[1]);
        return NULL;
    }

    pid_t pid = fork();

    if (pid < 0) {
        close(childIn[0]);
        close(childIn[1]);
        close(childOut[0]);
        close(childOut[1]);
        return NULL;
    }

    if (pid == 0) {
        dup2(childIn[0], STDIN_FILENO);
        dup2(childOut[1], STDOUT_FILENO);

        close(childIn[0]);
        close(childIn[1]);
        close(childOut[0]);
        close(childOut[1]);
        execv(argv[0], argv);
        _exit(0);
    } else {
        fork_exec_t *p = malloc(sizeof(fork_exec_t));
        p->pid = pid;
        p->fdWrite = childIn[1];
        p->fdRead = childOut[0];
        close(childIn[0]);
        close(childOut[1]);
        return p;
    }

}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Os_forkExec
        (JNIEnv *e, jclass cl, jlong argv) {
    return (jlong) forkExec((char **) argv);
}
