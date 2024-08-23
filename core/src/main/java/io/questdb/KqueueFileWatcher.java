/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb;

import io.questdb.cairo.CairoException;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.filewatch.FileWatcher;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;

public class KqueueFileWatcher extends FileWatcher {
    private final int bufferSize;
    private final long dirFd;
    private final long eventList;
    private final long evtDir;
    private final long evtFile;
    private final long evtPipe;
    private final long fileFd;
    private final long kq;
    private final long readEndFd;
    private final long writeEndFd;

    public KqueueFileWatcher(Utf8Sequence filePath, FileEventCallback callback) {
        super(callback);

        try (Path p = new Path()) {

            p.of(filePath);
            this.fileFd = Files.openRO(p.$());
            if (this.fileFd < 0) {
                throw CairoException.critical(Os.errno()).put("could not open file [path=").put(p).put(']');
            }

            this.dirFd = Files.openRO(p.parent().$());
            if (this.dirFd < 0) {
                int errno = Os.errno();
                Files.close(this.fileFd);
                throw CairoException.critical(errno).put("could not open directory [path=").put(p).put(']');
            }
        }

        try {
            this.evtFile = KqueueAccessor.evtAlloc(
                    this.fileFd,
                    KqueueAccessor.EVFILT_VNODE,
                    KqueueAccessor.EV_ADD | KqueueAccessor.EV_CLEAR,
                    KqueueAccessor.NOTE_DELETE | KqueueAccessor.NOTE_WRITE |
                            KqueueAccessor.NOTE_ATTRIB | KqueueAccessor.NOTE_EXTEND |
                            KqueueAccessor.NOTE_LINK | KqueueAccessor.NOTE_RENAME |
                            KqueueAccessor.NOTE_REVOKE,
                    0
            );

            if (this.evtFile == 0) {
                throw CairoException.critical(Os.errno()).put("could not allocate kevent for file");
            }

            this.evtDir = KqueueAccessor.evtAlloc(
                    this.dirFd,
                    KqueueAccessor.EVFILT_VNODE,
                    KqueueAccessor.EV_ADD | KqueueAccessor.EV_CLEAR,
                    KqueueAccessor.NOTE_DELETE | KqueueAccessor.NOTE_WRITE |
                            KqueueAccessor.NOTE_ATTRIB | KqueueAccessor.NOTE_EXTEND |
                            KqueueAccessor.NOTE_LINK | KqueueAccessor.NOTE_RENAME |
                            KqueueAccessor.NOTE_REVOKE,
                    0
            );

            if (this.evtDir == 0) {
                throw CairoException.critical(Os.errno()).put("could not allocate kevent for directory");
            }

            long fds = KqueueAccessor.pipe();
            if (fds < 0) {
                throw CairoException.critical(Os.errno()).put("could not create pipe");
            }

            this.readEndFd = Files.createUniqueFd((int) (fds >>> 32));
            this.writeEndFd = Files.createUniqueFd((int) fds);

            this.evtPipe = KqueueAccessor.evtAlloc(
                    this.readEndFd,
                    KqueueAccessor.EVFILT_READ,
                    KqueueAccessor.EV_ADD | KqueueAccessor.EV_CLEAR,
                    0,
                    0
            );

            kq = Files.createUniqueFd(KqueueAccessor.kqueue());
            if (kq < 0) {
                throw CairoException.critical(Os.errno()).put("could create kqueue");
            }
            this.bufferSize = KqueueAccessor.SIZEOF_KEVENT;
            this.eventList = Unsafe.calloc(bufferSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);

            // Register events with queue
            int fileRes = KqueueAccessor.keventRegister(
                    kq,
                    evtFile,
                    1
            );
            if (fileRes < 0) {
                throw CairoException.critical(Os.errno()).put("could register file events in kqueue");
            }

            int dirRes = KqueueAccessor.keventRegister(
                    kq,
                    evtDir,
                    1
            );
            if (dirRes < 0) {
                throw CairoException.critical(Os.errno()).put("could register directory events in kqueue");
            }

            int pipeRes = KqueueAccessor.keventRegister(
                    kq,
                    evtPipe,
                    1
            );
            if (pipeRes < 0) {
                throw CairoException.critical(Os.errno()).put("could register pipe events in kqueue");
            }

        } catch (RuntimeException e) {
            cleanUp();
            throw e;
        }
    }

    private void cleanUp() {
        if (kq > 0) {
            Files.close(kq);
        }

        if (fileFd > 0) {
            Files.close(fileFd);
        }

        if (dirFd > 0) {
            Files.close(dirFd);
        }

        if (readEndFd > 0) {
            Files.close(readEndFd);
        }

        if (writeEndFd > 0) {
            Files.close(writeEndFd);
        }

        if (this.eventList > 0) {
            Unsafe.free(this.eventList, bufferSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
        }

        if (this.evtFile > 0) {
            KqueueAccessor.evtFree(this.evtFile);
        }

        if (this.evtDir > 0) {
            KqueueAccessor.evtFree(this.evtDir);
        }

        if (this.evtPipe > 0) {
            KqueueAccessor.evtFree(this.evtPipe);
        }
    }

    @Override
    protected void _close() {
        cleanUp();
    }

    @Override
    protected void releaseWait() {
        KqueueAccessor.writePipe(writeEndFd);
    }

    @Override
    protected void waitForChange() {
        // Blocks until there is a change in the watched dir
        int res = KqueueAccessor.keventGetBlocking(
                kq,
                eventList,
                1
        );
        if (res < 0) {
            throw CairoException.critical(Os.errno()).put("kevent error");
        }
        // For now, we don't filter events down to the file,
        // we trigger on every change in the directory
        callback.onFileEvent();
    }
}
