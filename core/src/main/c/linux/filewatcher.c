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

struct file_watcher
{
    // fd is the inotify api file descriptor
    uintptr_t fd;
    // wd is the watch descriptor for poll calls
    int *wd;
    // filename is the name of the watched file
    char *filename;
};

/* Creates a file_watcher that uses inotify to watch a single file for changes

   Args:
   fp (filepath) is the memory address of string that holds the path of the file to be watched

   Returns:
   the address of the initialized file_watcher struct. If value is 0, then an error has occurred

*/
static uintptr_t setup(const char *fp)
{
    struct file_watcher *fw;

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

    /* Create a copy of the path so we can run dirname/basename on it. We use
       inotify to watch the entire directory (and its contents) for changes
       but only surface changes made to the watched file */
    char pathCpy[strlen(fp)];
    strcpy(pathCpy, fp);

    /* Add directory watch using inotify */
    *wd = inotify_add_watch(
        fd,
        dirname(pathCpy),
        IN_MODIFY | IN_CLOSE_WRITE | IN_CREATE | IN_MOVED_TO);
    if (wd == NULL)
    {
        return 0;
    }

    /* Re-copy the path so we can extract the filename */
    strcpy(pathCpy, fp);
    char *filename = malloc(sizeof(char) * strlen(pathCpy) + 1);

    /* Set up file_watcher struct */
    fw = malloc(sizeof(struct file_watcher));
    if (fw == NULL)
    {
        return 0;
    }
    fw->fd = fd;
    fw->wd = wd;
    fw->filename = filename;

    return (uintptr_t)fw;
}

JNIEXPORT jlong JNICALL Java_io_questdb_InotifyFileWatcher_setup(JNIEnv *e, jclass cl, jlong lpszName)
{
    return setup((const char *)lpszName);
}

static int waitForChange(uintptr_t wp)
{
    char buf[4096]
        __attribute__((aligned(__alignof__(struct inotify_event))));
    const struct inotify_event *event;
    ssize_t len;
    struct file_watcher *fw = (struct file_watcher *)wp;

    for (;;)
    {
        /* Read some events from the buffer. This call is blocking */
        len = read(fw->fd, buf, sizeof(buf));
        if (len == -1 && errno != EAGAIN)
        {
            return -1;
        }

        for (char *ptr = buf; ptr < buf + len;
             ptr += sizeof(struct inotify_event) + event->len)
        {
            event = (const struct inotify_event *)ptr;

            if (event->len && !(event->mask & IN_ISDIR))
            {
                if (strcmp(event->name, fw->filename))
                {
                    return 0;
                }
            }
        }
    }
}

JNIEXPORT int JNICALL Java_io_questdb_InotifyFileWatcher_waitForChange(JNIEnv *e, jclass cl, jlong address)
{
    return waitForChange(address);
}

static void teardown(uintptr_t wp)
{
    struct file_watcher *fw = (struct file_watcher *)wp;

    // Clean up the inotify watch descriptor first
    free(fw->wd);
    // Then clean up the filewatcher struct itself
    free(fw);
}

JNIEXPORT void JNICALL Java_io_questdb_InotifyFileWatcher_teardown(JNIEnv *e, jclass cl, jlong address)
{
    teardown(address);
}

int main(int argc, char *argv[])
{
    intptr_t fw_ptr = setup("/home/steven/tmp/qdbdev/conf/server.conf");
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