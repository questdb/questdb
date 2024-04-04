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
#include <sys/stat.h>

struct file_watcher_context
{
    char *filePathPtr;
    time_t lastChangedSec;
    long lastChangedNsec;
};

void myCallback(
    ConstFSEventStreamRef streamRef,
    void *context,
    size_t numEvents,
    void *eventPaths,
    const FSEventStreamEventFlags eventFlags[],
    const FSEventStreamEventId eventIds[])
{
    int i;
    char **paths = eventPaths;
    printf("Callback called\n");

    struct file_watcher_context *ctx = (struct file_watcher_context *)context;
    char *filePath = (char *)ctx->filePathPtr;
    printf("filePath %s\n", filePath);
    for (i = 0; i < numEvents; i++)
    {
        int count;
        /* flags are unsigned long, IDs are uint64_t */
        printf("Change %llu in %s, flags %lu\n", eventIds[i], paths[i], eventFlags[i]);

        struct stat confStat;
        printf("stat %d\n", stat(filePath, &confStat));
        printf("%ld\n", confStat.st_mtimespec.tv_sec);

        if (confStat.st_mtimespec.tv_sec > ctx->lastChangedSec)
        {
            printf("changed!\n");
            ctx->lastChangedSec = confStat.st_mtimespec.tv_sec;
            ctx->lastChangedNsec = confStat.st_mtimespec.tv_nsec;
        }

        if (confStat.st_mtimespec.tv_sec == ctx->lastChangedSec &&
            confStat.st_mtimespec.tv_nsec > ctx->lastChangedNsec)
        {
            printf("changed!\n");
            ctx->lastChangedSec = confStat.st_mtimespec.tv_sec;
            ctx->lastChangedNsec = confStat.st_mtimespec.tv_nsec;
        }
    }
}

int main(int argc, char *argv[])
{
    /* Define variables and create a CFArray object containing
       CFString objects containing paths to watch.
    */
    char *confPath = "/Users/steven/tmp/qdbdev/conf/server.conf";
    CFStringRef confDir = CFStringCreateWithCString(NULL, dirname(confPath), kCFStringEncodingUTF8);
    CFArrayRef pathsToWatch = CFArrayCreate(NULL, (const void **)&confDir, 1, NULL);

    /* Init context */
    struct file_watcher_context *ctx = malloc(sizeof(struct file_watcher_context));

    /* Set the filename of the watched file in the context */
    char *confPathPtr = malloc(sizeof(char) * strlen(confPath) + 1);
    strcpy(confPathPtr, confPath);
    ctx->filePathPtr = confPathPtr;
    printf("ctx->filePathPtr %p\n", ctx->filePathPtr);

    /* Set the last modified time of the watched file in the context */
    struct stat confStat;
    printf("stat %s %d\n", confPath, stat(confPath, &confStat));
    ctx->lastChangedSec = confStat.st_mtimespec.tv_sec;
    ctx->lastChangedNsec = confStat.st_mtimespec.tv_nsec;

    struct FSEventStreamContext context;
    context.info = ctx;

    FSEventStreamRef stream;
    CFAbsoluteTime latency = 3.0; // latency in seconds

    /* Create the stream, passing in a callback */
    stream = FSEventStreamCreate(NULL,
                                 &myCallback,
                                 &context,
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
    printf("FSEventStreamStart %d\n", FSEventStreamStart(stream));

    CFRunLoopRun();
}
