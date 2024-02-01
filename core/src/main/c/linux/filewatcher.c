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
    bool changed;
    uintptr_t wd;
    char *name;
};


/* Creates a file_watcher that uses inotify to watch a single file
   
   Args:
   fp (file path) is the memory address of string that holds the filepath to be watched

   Returns:
   the address of the initialized file_watcher struct. If value is 0, then an error has occurred

*/
static uintptr_t filewatcher_setup(char filepath[]) {
    int fd;
    int *wd;
    struct file_watcher *fw;

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
    wd[0] = inotify_add_watch(
        fd, 
        dirname(filepath),
        IN_ALL_EVENTS
    );
    if (wd[0] == -1) {
        return 0;
    }
    
    /* Set up file_watcher struct */
    fw = malloc(sizeof(struct file_watcher));
    fw->changed = false;
    fw->fd = fd;
    fw->wd = (uintptr_t)wd[0];
    fw->name = basename(filepath);
    
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

    poll_num = poll(fds, 1, -1);
    if (poll_num == -1) {
        if (errno == EINTR)
                return 0;
        }

    if (poll_num > 0) {
        if (fds[0].revents & POLLIN) {
            handle_events(wp->fd, wp->wd);
        }
    }
}

/* Read all available inotify events from the file descriptor 'fd'.
    wd is the table of watch descriptors for the directories in argv.
    Entry 0 of wd and argv is unused. */
static void
handle_events(int fd, int *wd) {
    /* From inotify man page: align the buffer used for reading from the
    inotify file descriptor to the inotify_event struct*/
    char buf[4096]   
        __attribute__ ((aligned(__alignof__(struct inotify_event))));
    const struct inotify_event *event;
    ssize_t len;

    /* Loop while events can be read from inotify file descriptor. */
    for (;;) {
        /* Read some events. */

        len = read(fd, buf, sizeof(buf));
        if (len == -1 && errno != EAGAIN) {
            perror("read");
            exit(EXIT_FAILURE);
        }

        /* If the nonblocking read() found no events to read, then
        it returns -1 with errno set to EAGAIN. In that case, we exit loop */

        if (len <= 0)
            break;

        /* Loop over all events in the buffer */
        for (char *ptr = buf; ptr < buf + len;
                ptr += sizeof(struct inotify_event) + event->len) {
                    event = (const struct inotify_event *) ptr; 

                    /* Print event type. */
                    if (event->mask & IN_ACCESS) {
                        printf("IN_ACCESS: ");
                    }
                    if (event->mask & IN_MODIFY) {
                        printf("IN_MODIFY: ");
                    }
                    if (event->mask & IN_ATTRIB) {
                        printf("IN_ATTRIB: ");
                    }
                    if (event->mask & IN_CLOSE_WRITE) {
                        printf("IN_CLOSE_WRITE: ");
                    }
                    if (event->mask & IN_CLOSE_NOWRITE) {
                        printf("IN_CLOSE_NOWRITE: ");
                    }
                    if (event->mask & IN_OPEN) {
                        printf("IN_OPEN: ");
                    }
                    if (event->mask & IN_MOVED_FROM) {
                        printf("IN_MOVED_FROM: ");
                    }
                    if (event->mask & IN_MOVED_TO) {
                        printf("IN_MOVED_TO: ");
                    }
                    if (event->mask & IN_CREATE) {
                        printf("IN_CREATE: ");
                    }
                    if (event->mask & IN_DELETE) {
                        printf("IN_DELETE: ");
                    }
                    if (event->mask & IN_DELETE_SELF) {
                        printf("IN_DELETE_SELF: ");
                    }
                    if (event->mask & IN_MOVE_SELF) {
                        printf("IN_MOVE_SELF: ");
                    }

                    if (event->len) {
                        printf("%s", event->name);
                    }

                    printf("\n");

                }
    
    }
}

int
main(int argc, char* argv[]) {

    char buf;
    int fd, i, poll_num;
    int *wd;
    nfds_t nfds;
    struct pollfd fds[2];

    const char dirPath[] = "/home/steven/tmp/qdbdev/conf";


    /* Create the file descriptor for accessing the inotify API. */
    fd = inotify_init1(IN_NONBLOCK);
    if (fd == -1) {
        perror("inotify_init1");
        exit(EXIT_FAILURE);
    }

    /* Allocate memory for watch descriptors. */
    wd = calloc(1, sizeof(int));
    if (wd == NULL) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    /* Mark file for events */
    wd[0] = inotify_add_watch(
        fd, 
        dirPath, 
        IN_ALL_EVENTS
    );

    if (wd[0] == -1) {
        fprintf(stderr, "Cannot watch %s: %s\n",
        dirPath, strerror(errno));
    }

    /* Prepare for polling */
    
    nfds = 2;

    fds[0].fd = STDIN_FILENO; /* Console input */
    fds[0].events = POLLIN;

    fds[1].fd = fd; /* Inotify input */
    fds[1].events = POLLIN;

    /* Wait for events and/or terminal input */

    printf("Listening for events in %s\n", dirPath);

    while(1) {
        poll_num = poll(fds, nfds, -1);
        if (poll_num == -1) {
            if (errno == EINTR)
                continue;
            perror("poll");
            exit(EXIT_FAILURE);
        }

        if (poll_num > 0) {
            if (fds[0].revents & POLLIN) {
                /* Console input is available. Empty stdin and quit.*/
                while (read(STDIN_FILENO, &buf, 1) > 0 && buf != '\n')
                    continue;
                break;
            }

            if (fds[1].revents & POLLIN) {
                /* Inotify events are available */
                handle_events(fd, wd);
            }
        }

    }


    free(wd);
    exit(EXIT_SUCCESS);
}