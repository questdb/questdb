/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

#define _GNU_SOURCE

#include <unistd.h>
#include <sys/errno.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include "../share/os.h"

JNIEXPORT jint JNICALL Java_com_questdb_std_Os_getPid
        (JNIEnv *e, jclass cp) {
    return getpid();
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Os_currentTimeMicros
        (JNIEnv *e, jclass cl) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000 + tv.tv_usec;
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Os_currentTimeNanos
        (JNIEnv *e, jclass cl) {

    struct timespec timespec;
    clock_gettime(CLOCK_REALTIME, &timespec);
    return timespec.tv_sec * 100000000L + timespec.tv_nsec;
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Os_errno
        (JNIEnv *e, jclass cl) {
    return errno;
}

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

JNIEXPORT jlong JNICALL Java_com_questdb_std_Os_forkExec
        (JNIEnv *e, jclass cl, jlong argv) {
    return (jlong) forkExec((char **) argv);
}
