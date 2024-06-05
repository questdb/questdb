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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.Epoll;
import io.questdb.network.EpollAccessor;
import io.questdb.network.EpollFacadeImpl;
import io.questdb.std.*;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

import java.nio.file.Paths;

public final class InotifyFileWatcher extends FileWatcher {

    private static final Log LOG = LogFactory.getLog(InotifyFileWatcher.class);
    private final long buf;
    private final int bufSize = InotifyAccessor.getSizeofEvent() + 4096;
    private final Path dirPath = new Path();
    private final Epoll epoll = new Epoll(
            new EpollFacadeImpl(),
            2
    );
    private final int fd;
    private final DirectUtf8Sink fileName = new DirectUtf8Sink(0);
    private final int readEndFd;
    private final int wd;
    private final int writeEndFd;

    public InotifyFileWatcher(Utf8Sequence filePath, FileEventCallback callback) {
        super(callback);

        this.fd = InotifyAccessor.inotifyInit();
        if (this.fd < 0) {
            throw CairoException.critical(Os.errno()).put("inotify_init error");
        }
        Files.bumpFileCount(this.fd);

        try {
            this.dirPath.of(filePath).parent().$();
            this.fileName.put(Paths.get(filePath.toString()).getFileName().toString());

            this.wd = InotifyAccessor.inotifyAddWatch(
                    this.fd,
                    this.dirPath.ptr(),
                    InotifyAccessor.IN_CREATE | InotifyAccessor.IN_MODIFY |
                            InotifyAccessor.IN_MOVED_TO | InotifyAccessor.IN_CLOSE_WRITE
            );

            if (this.wd < 0) {
                throw CairoException.critical(Os.errno()).put("inotify_add_watch exited");
            }

            if (epoll.control(fd, 0, EpollAccessor.EPOLL_CTL_ADD, EpollAccessor.EPOLLIN) < 0) {
                throw CairoException.critical(Os.errno()).put("epoll_ctl error");
            }

            this.buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
            if (this.buf < 0) {
                throw CairoException.critical(Os.errno()).put("malloc error");
            }

            long fds = InotifyAccessor.pipe();
            if (fds < 0) {
                throw CairoException.critical(Os.errno()).put("create a pipe error");
            }

            this.readEndFd = (int) (fds >>> 32);
            this.writeEndFd = (int) fds;
            Files.bumpFileCount(this.readEndFd);
            Files.bumpFileCount(this.writeEndFd);

            if (epoll.control(readEndFd, 0, EpollAccessor.EPOLL_CTL_ADD, EpollAccessor.EPOLLIN) < 0) {
                throw CairoException.critical(Os.errno()).put("epoll_ctl error");
            }

        } catch (RuntimeException e) {
            cleanUp();
            throw e;
        }
    }

    @Override
    public void close() {

        if (closed.compareAndSet(false, true)) {
            // Write to pipe to close
            if (InotifyAccessor.writePipe(writeEndFd) < 0) {
                // todo: handle error, but continue execution
            }

            // WaitGroup is located in the super class destructor that waits
            // for the watcher thread to close. We wait for this before cleaning
            // up any additional resources.
            super.close();

            cleanUp();

            LOG.info().$("inotify filewatcher closed").$();
        }
    }

    @Override
    public void waitForChange() {
        // Thread is parked here until epoll is triggered
        if (epoll.poll(-1) < 0) {
            throw CairoException.critical(Os.errno()).put("epoll_wait error");
        }

        if (closed.get()) {
            return;
        }

        // Read the inotify_event into the buffer
        int res = InotifyAccessor.readEvent(fd, buf, bufSize);
        if (res < 0) {
            throw CairoException.critical(Os.errno()).put("read error");
        }

        // iterate over buffer and check all files that have been modified
        int i = 0;
        do {
            int len = Unsafe.getUnsafe().getInt(buf + i + InotifyAccessor.getEventFilenameSizeOffset());
            i += InotifyAccessor.getEventFilenameOffset();
            // In the below equality statement, we use fileName.size() instead of the event len because inotify_event
            // structs may include padding at the end of the event data, which we don't want to use for the filename
            // equality check.
            // Because of this, we will match on anything with a "server.conf" prefix. It's a bit hacky, but it works...
            if (Utf8s.equals(fileName, buf + i, fileName.size())) {
                runnable.run();
                break;
            }
            i += len;
        }
        while (i < res);

        // Rearm the epoll
        if (epoll.control(fd, 0, EpollAccessor.EPOLL_CTL_MOD, EpollAccessor.EPOLLIN) < 0) {
            throw CairoException.critical(Os.errno()).put("epoll_wait error");
        }

    }

    private void cleanUp() {
        Misc.free(this.dirPath);
        Misc.free(this.fileName);

        if (InotifyAccessor.inotifyRmWatch(this.fd, this.wd) < 0) {
            System.out.println(this.fd);
            // todo: handle error, but continue execution
        }

        epoll.close();

        if (this.fd > 0) {
            Files.close(this.fd);
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
}

