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
    uintptr_t fd;
    uintptr_t wd;
    char *name;
};


/* Creates a file_watcher that uses inotify to watch a single file
   
   Args:
   fp (file path) is the memory address of string that holds the filepath to be watched

   Returns:
   the address of the initialized file_watcher struct. If value is 0, then an error has occurred

*/
static uintptr_t filewatcher_setup(char *filepath) {
    int fd;
    int *wd;
    struct file_watcher *fw;
    
    
    /* make copy of filepath for dirname/basename usage, otherwise they segfault*/
    char *filepath_copy[strlen(filepath)];
    strcpy(filepath_copy, filepath);



    /* Create the file descriptor for accessing the inotify API. */
    fd = inotify_init1(IN_NONBLOCK);
    if (fd == -1) {
        return 0;
    }

    
    /* Allocate memory for watch descriptors. Return 0 if
       there is a calloc error. */
    wd = calloc(1, sizeof(int));
    if (wd == NULL) {
        return 0;
    }

    /* Mark file's directory for events. We will filter them down
       to only changes related to the specific file later. Return 0
       if inotify_add_watch fails. */
    char *d = dirname(filepath_copy);
    wd[0] = inotify_add_watch(
        fd, 
        d,
        IN_ALL_EVENTS
    );
    if (wd[0] == -1) {
        return 0;
    }
    
    /* Set up file_watcher struct */
    fw = malloc(sizeof(struct file_watcher));
    fw->fd = fd;
    fw->wd = (uintptr_t)wd[0];
    fw->name = basename(filepath_copy);
    
    return (uintptr_t)fw;

}

static void filewatcher_teardown(uintptr_t wp) {
    struct file_watcher *fw = (struct file_watcher *)wp;
    free(fw);
}

static int filewatcher_changed(uintptr_t wp) {
    int poll_num;
    struct pollfd fds[1];

    struct file_watcher *fw = (struct file_watcher *)wp;
    fds[0].fd = fw->fd;
    fds[0].events = POLLIN;

    poll_num = poll(fds, 1, -1);
    if (poll_num == -1) {
        if (errno == EINTR)
                return -1;
        }

    if (poll_num > 0) {
        if (fds[0].revents & POLLIN) {
            /* From inotify man page: align the buffer used for reading from the
            inotify file descriptor to the inotify_event struct*/
            char buf[4096]   
                __attribute__ ((aligned(__alignof__(struct inotify_event))));
            const struct inotify_event *event;
            ssize_t len;


            /* Loop while events can be read from inotify file descriptor. */
            for (;;) {
                /* Read some events. */

                len = read(fw->fd, buf, sizeof(buf));
                if (len == -1 && errno != EAGAIN) {
                    return -1;
                }

                /* If the nonblocking read() found no events to read, then
                it returns -1 with errno set to EAGAIN. In that case, we exit loop */
                if (len <= 0)
                    return 0;

                /* Loop over all events in the buffer */
                for (char *ptr = buf; ptr < buf + len;
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


int
main(int argc, char* argv[]) {
    intptr_t fw_ptr = filewatcher_setup("/home/steven/tmp/qdbdev/conf/server.conf");
    int a; 
    for(;;) {
        a = filewatcher_changed(fw_ptr);
        printf("%d\n", a);
        sleep(4);
    }
   
    filewatcher_teardown(fw_ptr);
}