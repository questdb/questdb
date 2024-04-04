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

#include <CoreServices/CoreServices.h>
#include <libgen.h>

struct file_watcher
{
    FSEventStreamRef stream;
    char *name;
};

void myCallback(
    ConstFSEventStreamRef streamRef,
    void *clientCallBackInfo,
    size_t numEvents,
    void *eventPaths,
    const FSEventStreamEventFlags eventFlags[],
    const FSEventStreamEventId eventIds[])
{
    int i;
    char **paths = eventPaths;

    printf("Callback called\n");
    for (i = 0; i < numEvents; i++)
    {
        int count;
        /* flags are unsigned long, IDs are uint64_t */
        printf("Change %llu in %s, flags %lu\n", eventIds[i], paths[i], eventFlags[i]);
    }
}

static uintptr_t setup(const char *filepath)
{
    struct file_watcher *fw;

    /* make copy of filepath for dirname/basename usage, otherwise they segfault */
    char filepath_copy[strlen(filepath) + 1];
    strcpy(filepath_copy, filepath);

    /* Create the FSEventStream */
    CFStringRef *d = dirname(filepath_copy);
    CFArrayRef pathsToWatch = CFArrayCreate(NULL, (const void **)&d, 1, NULL);
    void *callbackInfo = NULL; // could put stream-specific data here
    FSEventStreamRef stream;
    CFAbsoluteTime latency = 3.0; // latency in seconds

    /* Create the stream, passing in a callback */
    stream = FSEventStreamCreate(NULL,
                                 &myCallback,
                                 callbackInfo,
                                 pathsToWatch,
                                 kFSEventStreamEventIdSinceNow, /* Or a previous event ID */
                                 latency,
                                 kFSEventStreamCreateFlagNone /* Flags explained in reference */
    );

    fw->stream = stream;
    fw->name = basename(filepath_copy);

    return (uintptr_t)fw;
}

int main(int argc, char *argv[])
{
    /* Define variables and create a CFArray object containing
       CFString objects containing paths to watch.
    */
    CFStringRef mypath = CFSTR("/home/steven/tmp/qdbdev/conf/server.conf");
    CFArrayRef pathsToWatch = CFArrayCreate(NULL, (const void **)&mypath, 1, NULL);
    void *callbackInfo = NULL; // could put stream-specific data here
    FSEventStreamRef stream;
    CFAbsoluteTime latency = 3.0; // latency in seconds

    /* Create the stream, passing in a callback */
    stream = FSEventStreamCreate(NULL,
                                 &myCallback,
                                 callbackInfo,
                                 pathsToWatch,
                                 kFSEventStreamEventIdSinceNow, /* Or a previous event ID */
                                 latency,
                                 kFSEventStreamCreateFlagNone /* Flags explained in reference */
    );

    /* Set up a run loop to schedule FSEventStream. This snippet schedules the stream
       on the current thread's run loop (not yet running)
    */
    FSEventStreamScheduleWithRunLoop(stream, CFRunLoopGetCurrent(), kCFRunLoopDefaultMode);

    /* Start the loop */
    printf("%d\n", FSEventStreamStart(stream));

    CFRunLoopRun();
}
