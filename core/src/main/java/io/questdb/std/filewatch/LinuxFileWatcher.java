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

package io.questdb.std.filewatch;

import io.questdb.FileEventCallback;
import io.questdb.cairo.CairoException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.Epoll;
import io.questdb.network.EpollAccessor;
import io.questdb.network.EpollFacade;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

import java.nio.file.Paths;

public final class LinuxFileWatcher extends FileWatcher {

    private static final Log LOG = LogFactory.getLog(LinuxFileWatcher.class);
    private final LinuxAccessorFacade accessorFacade;
    private final long buf;
    private final int bufSize = LinuxAccessor.getSizeofEvent() + 4096;
    private final Path dirPath = new Path();
    private final Epoll epoll;
    private final DirectUtf8Sink fileName = new DirectUtf8Sink(0);
    private final long inotifyFd;
    private final long readEndFd;
    private final int wd;
    private final long writeEndFd;

    public LinuxFileWatcher(
            LinuxAccessorFacade accessorFacade,
            EpollFacade epollFacade,
            Utf8Sequence filePath,
            FileEventCallback callback
    ) {
        super(callback);
        this.accessorFacade = accessorFacade;
        this.epoll = new Epoll(epollFacade, 2);
        try {
            int inotifyFd2 = accessorFacade.inotifyInit();
            if (inotifyFd2 < 0) {
                throw CairoException.critical(Os.errno()).put("inotify_init error");
            }
            this.inotifyFd = Files.createUniqueFd(inotifyFd2);

            this.dirPath.of(filePath).parent();
            this.fileName.put(Paths.get(filePath.toString()).getFileName().toString());

            this.wd = accessorFacade.inotifyAddWatch(
                    this.inotifyFd,
                    this.dirPath.$().ptr(),
                    LinuxAccessor.IN_CREATE | LinuxAccessor.IN_MODIFY |
                            LinuxAccessor.IN_MOVED_TO | LinuxAccessor.IN_CLOSE_WRITE
            );

            if (this.wd < 0) {
                throw CairoException.critical(Os.errno()).put("inotify_add_watch exited");
            }

            if (epoll.control(inotifyFd, 0, EpollAccessor.EPOLL_CTL_ADD, EpollAccessor.EPOLLIN) < 0) {
                throw CairoException.critical(Os.errno()).put("epoll_ctl error");
            }

            this.buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
            if (this.buf < 0) {
                throw CairoException.critical(Os.errno()).put("malloc error");
            }

            long fds = accessorFacade.pipe();
            if (fds < 0) {
                throw CairoException.critical(Os.errno()).put("create a pipe error");
            }

            this.readEndFd = Files.createUniqueFd((int) (fds >>> 32));
            this.writeEndFd = Files.createUniqueFd((int) fds);

            if (epoll.control(readEndFd, 0, EpollAccessor.EPOLL_CTL_ADD, EpollAccessor.EPOLLIN) < 0) {
                throw CairoException.critical(Os.errno()).put("epoll_ctl error");
            }

        } catch (RuntimeException e) {
            cleanUp();
            throw e;
        }
    }

    @Override
    public void waitForChange() {
        // Thread is parked here until epoll is triggered
        if (epoll.poll(-1) < 0) {
            throw CairoException.critical(Os.errno()).put("epoll_wait error");
        }

        if (isClosed()) {
            return;
        }

        // Read the inotify_event into the buffer
        int res = accessorFacade.readEvent(inotifyFd, buf, bufSize);
        if (res < 0) {
            throw CairoException.critical(Os.errno()).put("read error");
        }

        // iterate over buffer and check all files that have been modified
        int i = 0;
        do {
            int len = Unsafe.getUnsafe().getInt(buf + i + LinuxAccessor.getEventFilenameSizeOffset());
            i += LinuxAccessor.getEventFilenameOffset();
            // In the below equality statement, we use fileName.size() instead of the event len because inotify_event
            // structs may include padding at the end of the event data, which we don't want to use for the filename
            // equality check.
            // Because of this, we will match on anything with a "server.conf" prefix. It's a bit hacky, but it works...
            if (Utf8s.equals(fileName, buf + i, fileName.size())) {
                callback.onFileEvent();
                break;
            }
            i += len;
        }
        while (i < res);

        // Rearm the epoll
        if (epoll.control(inotifyFd, 0, EpollAccessor.EPOLL_CTL_MOD, EpollAccessor.EPOLLIN) < 0) {
            throw CairoException.critical(Os.errno()).put("epoll_ctl error");
        }
    }

    private void cleanUp() {
        Misc.free(this.dirPath);
        Misc.free(this.fileName);

        if (this.inotifyFd > 0 && accessorFacade.inotifyRmWatch(this.inotifyFd, this.wd) < 0) {
            System.out.println(this.inotifyFd);
            // todo: handle error, but continue execution
        }

        epoll.close();

        if (this.inotifyFd > 0) {
            Files.close(this.inotifyFd);
        }

        if (this.readEndFd > 0) {
            Files.close(this.readEndFd);
        }

        if (this.writeEndFd > 0) {
            Files.close(this.writeEndFd);
        }

        if (this.buf > 0) {
            Unsafe.free(this.buf, this.bufSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
        }
    }

    @Override
    protected void _close() {
        cleanUp();
        LOG.info().$("inotify filewatcher closed").$();
    }

    @Override
    protected void releaseWait() {
        accessorFacade.writePipe(writeEndFd);
    }
}