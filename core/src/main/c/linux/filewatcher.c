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
#include <asm-generic/errno-base.h>
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

struct file_watcher {
    // fd is the inotify api file descriptor
    uintptr_t fd;
    // wd is the watch descriptor for poll calls
    int* wd;
    /* buf is used to hold inotify events for processing. We align the buffer 
       used for reading from the inotify file descriptor to the inotify_event struct 
       as per the inotify man page
    */
    char buf[4096]   
        __attribute__ ((aligned(__alignof__(struct inotify_event))));
    
    char *name;
};


/* Creates a file_watcher that uses inotify to watch a single file
   
   Args:
   fp (file path) is the memory address of string that holds the filepath to be watched

   Returns:
   the address of the initialized file_watcher struct. If value is 0, then an error has occurred

*/
static uintptr_t setup(const char *filepath) {
    struct file_watcher *fw;
    
    
    /* make copy of filepath for dirname/basename usage, otherwise they segfault*/
    char filepath_copy[strlen(filepath)+1];
    strcpy(filepath_copy, filepath);


    /* Create the file descriptor for accessing the inotify API. */
    const int fd = inotify_init1(IN_NONBLOCK);
    if (fd == -1) {
        return 0;
    }

    
    /* Allocate memory for watch descriptor. Return 0 if
       there is a malloc error. */
    int *wd = calloc(1, sizeof(int));
    if (wd == NULL) {
        return 0;
    }

    /* Mark file's directory for events. We will filter them down
       to only changes related to the specific file later. Return 0
       if inotify_add_watch fails. */
    char *d = dirname(filepath_copy);
    *wd = inotify_add_watch(
        fd, 
        d,
        IN_MODIFY | IN_CLOSE_WRITE | IN_CREATE
    );
    if (wd == NULL) {
        return 0;
    }
    
    /* Set up file_watcher struct */
    fw = calloc(1, sizeof(struct file_watcher));
    fw->fd = fd;
    fw->wd = wd;
    fw->name = basename(filepath_copy);
    
    return (uintptr_t)fw;

}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Filewatcher_setup
    (JNIEnv *e, jclass cl, jlong lpszName) {
        return setup((const char *)lpszName);
    }

static void teardown(uintptr_t wp) {
    struct file_watcher *fw = (struct file_watcher *)wp;

    // Clean up the inotify watch descriptor first
    free(fw->wd);
    // Then clean up the filewatcher struct itself
    free(fw);
}

JNIEXPORT void JNICALL Java_io_questdb_std_Filewatcher_teardown
    (JNIEnv *e, jclass cl, jlong address) {
        teardown(address);
}

static int changed(uintptr_t wp) {
    int poll_num;
    struct pollfd fds[1];

    struct file_watcher *fw = (struct file_watcher *)wp;
    fds[0].fd = fw->fd;
    fds[0].events = POLLIN;

    poll_num = poll(fds, 1, 1);
    if (poll_num == -1) {
        if (errno == EINTR)
                return -1;
        }

    if (poll_num > 0) {
        if (fds[0].revents & POLLIN) {
            const struct inotify_event *event;
            ssize_t len;


            /* Loop while events can be read from inotify file descriptor. */
            for (;;) {
                /* Read some events. */

                len = read(fw->fd, fw->buf, sizeof(fw->buf));
                if (len == -1 && errno != EAGAIN) {
                    return -1;
                }

                /* If the nonblocking read() found no events to read, then
                it returns -1 with errno set to EAGAIN. In that case, we exit loop */
                if (len <= 0)
                    return 0;

                /* Loop over all events in the buffer */
                for (char *ptr = fw->buf; ptr < fw->buf + len;
                        ptr += sizeof(struct inotify_event) + event->len) {
                            event = (const struct inotify_event *) ptr; 

                            if (event->len && strcmp(event->name, fw->name)) {
                                return 1;
                            }
                        }
            }
        }
    }

    return 0;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_std_Filewatcher_changed
    (JNIEnv *e, jclass cl, jlong address) {
        return changed(address);
}


int
main(int argc, char* argv[]) {
    intptr_t fw_ptr = setup("/home/steven/tmp/qdbdev/conf/server.conf");
    int a; 
    for(;;) {
        a = changed(fw_ptr);
        printf("%d\n", a);
        sleep(1);
    }
   
    teardown(fw_ptr);
}