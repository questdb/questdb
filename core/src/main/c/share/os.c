/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

#include <unistd.h>
#include <sys/errno.h>
#include <stdlib.h>
#include "../share/os.h"

JNIEXPORT jint JNICALL Java_com_questdb_misc_Os_getPid
        (JNIEnv *e, jclass cp) {
    return getpid();
}

JNIEXPORT jint JNICALL Java_com_questdb_misc_Os_errno
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

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Os_forkExec
        (JNIEnv *e, jclass cl, jlong argv) {
    return (jlong) forkExec((char **) argv);
}
