/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cairo;

import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import java.util.concurrent.atomic.AtomicLong;

public class IDGeneratorFactory {

    public static IDGenerator newIDGenerator(CairoConfiguration configuration, String uniqueIdFileName, int step) {
        if (configuration.getCommitMode() == CommitMode.NOSYNC) {
            return new NoSyncIDGenerator(configuration, uniqueIdFileName);
        } else {
            return new SyncIDGenerator(configuration, uniqueIdFileName, step);
        }
    }

    public static abstract class AbstractIDGenerator implements IDGenerator {
        protected final CairoConfiguration configuration;
        protected final String uniqueIdFileName;
        protected final long uniqueIdMemSize;
        protected long uniqueIdFd = -1;
        // the current ID is consumed by other threads, and we need to make sure
        // memory address is visible to them
        protected volatile long uniqueIdMem = 0;

        public AbstractIDGenerator(CairoConfiguration configuration, String uniqueIdFileName) {
            this.configuration = configuration;
            this.uniqueIdFileName = uniqueIdFileName;
            this.uniqueIdMemSize = Files.PAGE_SIZE;
        }

        @Override
        public void close() {
            final FilesFacade ff = configuration.getFilesFacade();
            if (uniqueIdMem != 0) {
                ff.munmap(uniqueIdMem, uniqueIdMemSize, MemoryTag.MMAP_DEFAULT);
                uniqueIdMem = 0;
            }
            if (ff.close(uniqueIdFd)) {
                uniqueIdFd = -1;
            }
        }

        @Override
        public long getCurrentId() {
            assert uniqueIdMem != 0;
            return Unsafe.getUnsafe().getLong(uniqueIdMem);
        }

        @Override
        public void open(Path path) {
            close();
            if (path == null) {
                path = Path.getThreadLocal(configuration.getDbRoot());
            }
            final int rootLen = path.size();
            try {
                path.concat(uniqueIdFileName);
                final FilesFacade ff = configuration.getFilesFacade();
                uniqueIdFd = TableUtils.openFileRWOrFail(ff, path.$(), configuration.getWriterFileOpenOpts());
                uniqueIdMem = TableUtils.mapRW(ff, uniqueIdFd, uniqueIdMemSize, MemoryTag.MMAP_DEFAULT);
            } catch (Throwable th) {
                close();
                throw th;
            } finally {
                path.trimTo(rootLen);
            }
        }

        // This is not thread safe way to reset the id back to 0
        // It is useful for testing only
        @Override
        public void reset() {
            Unsafe.getUnsafe().putLong(uniqueIdMem, 0);
        }
    }

    static class NoSyncIDGenerator extends AbstractIDGenerator {
        public NoSyncIDGenerator(CairoConfiguration configuration, String uniqueIdFileName) {
            super(configuration, uniqueIdFileName);
        }

        @Override
        public long getNextId() {
            long next;
            long x = getCurrentId();
            do {
                next = x;
                x = Os.compareAndSwap(uniqueIdMem, next, next + 1);
            } while (next != x);
            return next + 1;
        }
    }

    static class SyncIDGenerator extends AbstractIDGenerator {
        private final int step;
        private AtomicLong base;
        private long end;

        public SyncIDGenerator(CairoConfiguration configuration, String uniqueIdFileName, int step) {
            super(configuration, uniqueIdFileName);
            this.step = step;
        }

        @Override
        public long getCurrentId() {
            return base.get();
        }

        @Override
        public synchronized long getNextId() {
            while (true) {
                long next = base.incrementAndGet();
                if (next <= end) {
                    // hotPath
                    return next;
                }

                synchronized (this) {
                    // Check if new batch allocation still needed
                    if (base.get() > end) {
                        // Allocate new batch
                        long newEnd = end + step;
                        Unsafe.getUnsafe().putLong(uniqueIdMem, newEnd);
                        configuration.getFilesFacade().msync(uniqueIdMem, uniqueIdMemSize, configuration.getCommitMode() == CommitMode.ASYNC);
                        base.set(newEnd - step);
                        end = newEnd;
                    }
                }
            }
        }

        @Override
        public void open(Path path) {
            super.open(path);
            end = super.getCurrentId();
            base = new AtomicLong(end);
        }
    }
}
