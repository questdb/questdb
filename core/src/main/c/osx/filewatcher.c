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
#include <pthread.h>

struct file_watcher_context
{
    char *filePathPtr;
    time_t lastChangedSec;
    long lastChangedNsec;
    FSEventStreamRef stream;
    CFRunLoopRef runLoop;
    bool changed;
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
            ctx->changed = true;
            ctx->lastChangedSec = confStat.st_mtimespec.tv_sec;
            ctx->lastChangedNsec = confStat.st_mtimespec.tv_nsec;
            return;
        }

        if (confStat.st_mtimespec.tv_sec == ctx->lastChangedSec &&
            confStat.st_mtimespec.tv_nsec > ctx->lastChangedNsec)
        {
            ctx->changed = true;
            ctx->lastChangedSec = confStat.st_mtimespec.tv_sec;
            ctx->lastChangedNsec = confStat.st_mtimespec.tv_nsec;
            return;
        }
    }
}

void *runLoop(void *vargp)
{
    /* Set up a run loop to schedule FSEventStream. This snippet schedules the stream
       on the current thread's run loop (not yet running)
    */
    FSEventStreamRef stream = (FSEventStreamRef)vargp;
    FSEventStreamScheduleWithRunLoop(stream, CFRunLoopGetCurrent(), kCFRunLoopDefaultMode);
    /* Start the loop */
    printf("FSEventStreamStart %d\n", FSEventStreamStart(stream));
    CFRunLoopRun();
    return NULL;
}

static uintptr_t setup(const char *filepath)
{
    /* Init context */
    FSEventStreamContext *context = malloc(sizeof(struct FSEventStreamContext));
    struct file_watcher_context *ctx = malloc(sizeof(struct file_watcher_context));
    char *filepathPtr = malloc(sizeof(char) * strlen(filepath) + 1);

    /* Set the filename of the watched file in the context */
    ctx->filePathPtr = filepathPtr;
    strcpy(filepathPtr, filepath);
    printf("filepath %p\n", filepath);
    printf("filepathPtr %p\n", filepathPtr);
    printf("ctx->filePathPtr %p %s\n", ctx->filePathPtr, ctx->filePathPtr);

    /* Add the file's directory to the watch array */
    CFStringRef confDir = CFStringCreateWithCString(NULL, dirname(filepathPtr), kCFStringEncodingUTF8);
    CFArrayRef pathsToWatch = CFArrayCreate(NULL, (const void **)&confDir, 1, NULL);

    /* Set the last modified time of the watched file in the context */
    struct stat fileStat;
    printf("stat %s %d\n", filepath, stat(filepath, &fileStat));
    ctx->lastChangedSec = fileStat.st_mtimespec.tv_sec;
    ctx->lastChangedNsec = fileStat.st_mtimespec.tv_nsec;

    context->info = ctx;

    FSEventStreamRef stream;
    CFAbsoluteTime latency = 3.0; // latency in seconds

    /* Create the stream, passing in a callback */
    stream = FSEventStreamCreate(NULL,
                                 &myCallback,
                                 context,
                                 pathsToWatch,
                                 kFSEventStreamEventIdSinceNow, /* Or a previous event ID */
                                 latency,
                                 kFSEventStreamCreateFlagNone /* Flags explained in reference */
    );

    ctx->stream = stream;

    pthread_t tid;
    pthread_create(&tid, NULL, runLoop, stream);

    return (uintptr_t)ctx;
}

static int changed(uintptr_t ctxPtr)
{
    struct file_watcher_context *ctx = (struct file_watcher_context *)ctxPtr;
    if (ctx->changed)
    {
        ctx->changed = false;
        return 1;
    }
    return 0;
}

static void teardown(uintptr_t ctxPtr)
{
    struct file_watcher_context *ctx = (struct file_watcher_context *)ctxPtr;

    FSEventStreamStop(ctx->stream);
    CFRunLoopStop(CFRunLoopGetCurrent());

    free(ctx->filePathPtr);
    free(ctx);
}

int main(int argc, char *argv[])
{
    struct file_watcher_context *ctx = (struct file_watcher_context *)setup("/Users/steven/tmp/qdbdev/conf/server.conf");
    uintptr_t *ctxPtr = (uintptr_t)ctx;
    int i;
    // Run for 60 seconds then cleanup
    for (i = 0; i < 60; i++)
    {
        sleep(1);
        if (changed(ctxPtr))
        {
            printf("changed!\n");
        }
    }

    teardown(ctxPtr);
}
