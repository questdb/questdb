/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
 *  See the License for the specific languageext install ms-vscode.cpptools governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

#include <jni.h>
#include <errno.h>
#include <stdint.h>
#include <stdbool.h>
#include <poll.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/inotify.h>
#include <string.h>
#include <unistd.h>
#include <libgen.h>

struct dir_watcher
{
    // fd is the inotify api file descriptor
    uintptr_t fd;
    // wd is the watch descriptor for poll calls
    int *wd;
    /* buf is used to hold inotify events for processing. We align the buffer
       used for reading from the inotify file descriptor to the inotify_event struct
       as per the inotify man page
    */
    char buf[4096]
        __attribute__((aligned(__alignof__(struct inotify_event))));
};

/* Creates a dir_watcher that uses inotify to watch a single directory

   Args:
   dp (dir path) is the memory address of string that holds the path of the directory to be watched

   Returns:
   the address of the initialized dir_watcher struct. If value is 0, then an error has occurred

*/
static uintptr_t setup(const char *dp)
{
    struct dir_watcher *dw;

    /* Create the file descriptor for accessing the inotify API. */
    const int fd = inotify_init();
    if (fd == -1)
    {
        return 0;
    }

    /* Allocate memory for watch descriptor. Return 0 if
       there is a malloc error. */
    int *wd = malloc(sizeof(int));
    if (wd == NULL)
    {
        return 0;
    }

    /* Add directory watch using inotify */
    *wd = inotify_add_watch(
        fd,
        dp,
        IN_MODIFY | IN_CLOSE_WRITE | IN_CREATE);
    if (wd == NULL)
    {
        return 0;
    }

    /* Set up dir_watcher struct */
    dw = malloc(sizeof(struct dir_watcher));
    if (dw == NULL)
    {
        return 0;
    }
    dw->fd = fd;
    dw->wd = wd;

    return (uintptr_t)dw;
}

JNIEXPORT jlong JNICALL Java_io_questdb_InotifyDirWatcher_setup(JNIEnv *e, jclass cl, jlong lpszName)
{
    return setup((const char *)lpszName);
}

static int waitForChange(uintptr_t wp)
{
    char buf[4096]
        __attribute__((aligned(__alignof__(struct inotify_event))));
    ssize_t len;
    struct dir_watcher *fw = (struct dir_watcher *)wp;

    len = read(fw->fd, buf, sizeof(buf));
    if (len > 0)
    {
        return 0;
    }
    return len;
}

JNIEXPORT int JNICALL Java_io_questdb_InotifyDirWatcher_waitForChange(JNIEnv *e, jclass cl, jlong address)
{
    return waitForChange(address);
}

static void teardown(uintptr_t wp)
{
    struct dir_watcher *fw = (struct dir_watcher *)wp;

    // Clean up the inotify watch descriptor first
    free(fw->wd);
    // Then clean up the filewatcher struct itself
    free(fw);
}

JNIEXPORT void JNICALL Java_io_questdb_InotifyDirWatcher_teardown(JNIEnv *e, jclass cl, jlong address)
{
    teardown(address);
}

int main(int argc, char *argv[])
{
    intptr_t fw_ptr = setup("/home/steven/tmp/qdbdev/conf");
    int a;
    int b = 0;
    for (;;)
    {
        a = waitForChange(fw_ptr);
        b += 1;
        printf("change detected: %d iteration %d\n", a, b);
    }

    teardown(fw_ptr);
}