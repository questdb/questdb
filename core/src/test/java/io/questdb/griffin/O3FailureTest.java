/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.*;
import io.questdb.mp.Job;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class O3FailureTest extends AbstractO3Test {

    private final static AtomicInteger counter = new AtomicInteger(0);

    private static final FilesFacade ffAllocateFailure = new FilesFacadeImpl() {
        private boolean failNextAlloc = false;

        @Override
        public long length(long fd) {
            if (counter.decrementAndGet() == 0) {
                failNextAlloc = true;
                return 0;
            }
            return super.length(fd);
        }

        @Override
        public boolean allocate(long fd, long size) {
            if (failNextAlloc) {
                failNextAlloc = false;
                return false;
            }
            return super.allocate(fd, size);
        }
    };

    private static final FilesFacade ffIndexAllocateFailure = new FilesFacadeImpl() {

        long theFd = 0;
        boolean failNextAlloc = false;

        @Override
        public long length(long fd) {
            if (fd == theFd) {
                failNextAlloc = true;
                return 0;
            }
            return super.length(fd);
        }

        @Override
        public long openRW(LPSZ name) {
            long fd = super.openRW(name);
            if (Chars.endsWith(name, "1970-01-06" + Files.SEPARATOR + "sym.v") && counter.decrementAndGet() == 0) {
                theFd = fd;
            }
            return fd;
        }

        @Override
        public boolean allocate(long fd, long size) {
            if (fd == theFd && failNextAlloc) {
                // don't forget to set this to 0 so that next attempt doesn't fail
                theFd = 0;
                failNextAlloc = false;
                return false;
            }
            return super.allocate(fd, size);
        }
    };

    private static final FilesFacade ffOpenIndexFailure = new FilesFacadeImpl() {
        @Override
        public long openRW(LPSZ name) {
            if (Chars.endsWith(name, "1970-01-02" + Files.SEPARATOR + "sym.v") && counter.decrementAndGet() == 0) {
                return -1;
            }
            return super.openRW(name);
        }
    };

    private static final FilesFacade ffOpenFailure = new FilesFacadeImpl() {
        @Override
        public long openRW(LPSZ name) {
            if (Chars.endsWith(name, "1970-01-06" + Files.SEPARATOR + "ts.d") && counter.decrementAndGet() == 0) {
                return -1;
            }
            return super.openRW(name);
        }
    };

    private static final FilesFacade ffMkDirFailure = new FilesFacadeImpl() {
        @Override
        public int mkdirs(LPSZ path, int mode) {
            if (Chars.contains(path, "1970-01-06.14") && counter.decrementAndGet() == 0) {
                return -1;
            }
            return super.mkdirs(path, mode);
        }
    };

    private static final FilesFacade ffWriteTop = new FilesFacadeImpl() {
        long theFd;

        @Override
        public long openRW(LPSZ name) {
            long fd = super.openRW(name);
            if (Chars.endsWith(name, "1970-01-06" + Files.SEPARATOR + "v.top") && counter.decrementAndGet() == 0) {
                theFd = fd;
            }
            return fd;
        }

        @Override
        public long write(long fd, long address, long len, long offset) {
            if (fd == theFd) {
                theFd = 0;
                return 5;
            }
            return super.write(fd, address, len, offset);
        }
    };

    private static final FilesFacade ffWriteAndRemoveTop = new FilesFacadeImpl() {
        long theFd;

        @Override
        public long openRW(LPSZ name) {
            long fd = super.openRW(name);
            if (Chars.endsWith(name, "1970-01-06" + Files.SEPARATOR + "v.top") && counter.decrementAndGet() == 0) {
                theFd = fd;
            }
            return fd;
        }

        @Override
        public boolean remove(LPSZ name) {
            if (Chars.endsWith(name, "1970-01-06" + Files.SEPARATOR + "v.top")) {
                return false;
            }
            return super.remove(name);
        }

        @Override
        public long write(long fd, long address, long len, long offset) {
            if (fd == theFd) {
                theFd = 0;
                return 5;
            }
            return super.write(fd, address, len, offset);
        }
    };

    private static final FilesFacade ffWriteTop19700107 = new FilesFacadeImpl() {
        long theFd;

        @Override
        public long openRW(LPSZ name) {
            long fd = super.openRW(name);
            if (Chars.endsWith(name, "1970-01-07.15" + Files.SEPARATOR + "v.top") && counter.decrementAndGet() == 0) {
                theFd = fd;
            }
            return fd;
        }

        @Override
        public long write(long fd, long address, long len, long offset) {
            if (fd == theFd) {
                theFd = 0;
                return 5;
            }
            return super.write(fd, address, len, offset);
        }
    };

    private static final FilesFacade ffMapRW = new FilesFacadeImpl() {

        private long theFd = 0;

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            if (theFd == fd) {
                theFd = 0;
                return -1;
            }
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long openRW(LPSZ name) {
            long fd = super.openRW(name);
            if (Chars.endsWith(name, "1970-01-06.14" + Files.SEPARATOR + "i.d") && counter.decrementAndGet() == 0) {
                theFd = fd;
            }
            return fd;
        }
    };

    private static final FilesFacade ffOpenRW = new FilesFacadeImpl() {
        @Override
        public long openRW(LPSZ name) {
            if (Chars.endsWith(name, "1970-01-06.14" + Files.SEPARATOR + "i.d") && counter.decrementAndGet() == 0) {
                return -1;
            }
            return super.openRW(name);
        }
    };

    private static final FilesFacade ffFailToAllocateIndex = new FilesFacadeImpl() {
        long theFd;
        boolean failNextAlloc = false;

        @Override
        public boolean close(long fd) {
            if (fd == theFd) {
                theFd = -1;
            }
            return super.close(fd);
        }

        @Override
        public long length(long fd) {
            if (fd == theFd && counter.decrementAndGet() == 0) {
                failNextAlloc = true;
                return 0L;
            }
            return super.length(fd);
        }

        @Override
        public long openRW(LPSZ name) {
            long fd = super.openRW(name);
            if (Chars.endsWith(name, "x" + Files.SEPARATOR + "1970-01-07" + Files.SEPARATOR + "m.i")) {
                theFd = fd;
            }
            return fd;
        }

        @Override
        public boolean allocate(long fd, long size) {
            if (fd == theFd && failNextAlloc) {
                failNextAlloc = false;
                return false;
            }
            return super.allocate(fd, size);
        }
    };

    @Test
    public void testColumnTopLastDataOOODataFailRetryCantWriteTop() throws Exception {
        counter.set(1);
        executeWithoutPool(O3FailureTest::testColumnTopLastDataOOODataFailRetry0, ffWriteTop19700107);
    }

    @Test
    public void testColumnTopLastDataOOODataFailRetryCantWriteTopContended() throws Exception {
        counter.set(1);
        executeWithPool(0, O3FailureTest::testColumnTopLastDataOOODataFailRetry0, ffWriteTop19700107);
    }

    @Test
    public void testColumnTopLastDataOOODataFailRetryMapRo() throws Exception {
        counter.set(1);
        executeWithoutPool(O3FailureTest::testColumnTopLastDataOOODataFailRetry0, new FilesFacadeImpl() {

            long theFd = 0;

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (fd == theFd && flags == Files.MAP_RO) {
                    theFd = 0;
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name) {
                long fd = super.openRW(name);
                if (Chars.endsWith(name, "1970-01-07" + Files.SEPARATOR + "v11.d") && counter.decrementAndGet() == 0) {
                    theFd = fd;
                }
                return fd;
            }
        });
    }

    @Test
    public void testColumnTopLastDataOOODataFailRetryMapRoContended() throws Exception {
        counter.set(1);
        executeWithPool(0, O3FailureTest::testColumnTopLastDataOOODataFailRetry0, new FilesFacadeImpl() {

            long theFd = 0;

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (fd == theFd && flags == Files.MAP_RO) {
                    theFd = 0;
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name) {
                long fd = super.openRW(name);
                if (Chars.endsWith(name, "1970-01-07" + Files.SEPARATOR + "v11.d") && counter.decrementAndGet() == 0) {
                    theFd = fd;
                }
                return fd;
            }
        });
    }

    @Test
    public void testColumnTopMidAppend() throws Exception {
        counter.set(3);
        executeWithoutPool(O3FailureTest::testColumnTopMidAppendColumnFailRetry0, new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "1970-01-07" + Files.SEPARATOR + "v12.d") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testColumnTopMidAppendBlank() throws Exception {
        counter.set(1);
        executeWithoutPool(O3FailureTest::testColumnTopMidAppendBlankColumnFailRetry0, ffWriteTop);
    }

    @Test
    public void testColumnTopFailToWriteFollowedByFailToRemove() throws Exception {
        counter.set(1);
        executeWithoutPool(O3FailureTest::testColumnTopMidAppendBlankColumnFailRetry0, ffWriteAndRemoveTop);
    }

    @Test
    public void testColumnTopMidAppendBlankContended() throws Exception {
        counter.set(1);
        executeWithPool(0, O3FailureTest::testColumnTopMidAppendBlankColumnFailRetry0, ffWriteTop);
    }

    @Test
    public void testColumnTopMidAppendContended() throws Exception {
        counter.set(3);
        executeWithPool(0, O3FailureTest::testColumnTopMidAppendColumnFailRetry0, new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "1970-01-07" + Files.SEPARATOR + "v12.d") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testColumnTopMidDataMergeDataFailRetryReadTop() throws Exception {
        counter.set(2);
        executeWithoutPool(O3FailureTest::testColumnTopMidDataMergeDataFailRetry0, new FilesFacadeImpl() {
            long theFd;

            @Override
            public long openRW(LPSZ name) {
                long fd = super.openRW(name);
                if (Chars.endsWith(name, "1970-01-07" + Files.SEPARATOR + "v2.top") && counter.decrementAndGet() == 0) {
                    theFd = fd;
                }
                return fd;
            }

            @Override
            public long read(long fd, long address, long len, long offset) {
                if (fd == theFd) {
                    theFd = 0;
                    return 5;
                }
                return super.read(fd, address, len, offset);
            }
        });
    }

    @Test
    public void testColumnTopMidDataMergeDataFailRetryReadTopContended() throws Exception {
        counter.set(2);
        executeWithPool(0, O3FailureTest::testColumnTopMidDataMergeDataFailRetry0, new FilesFacadeImpl() {
            long theFd;

            @Override
            public long openRW(LPSZ name) {
                long fd = super.openRW(name);
                if (Chars.endsWith(name, "1970-01-07" + Files.SEPARATOR + "v2.top") && counter.decrementAndGet() == 0) {
                    theFd = fd;
                }
                return fd;
            }

            @Override
            public long read(long fd, long address, long len, long offset) {
                if (fd == theFd) {
                    theFd = 0;
                    return 5;
                }
                return super.read(fd, address, len, offset);
            }
        });
    }

    @Test
    public void testColumnTopMidMergeBlankFailRetryMapRW() throws Exception {
        counter.set(1);
        executeWithoutPool(O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, ffMapRW);
    }

    @Test
    public void testColumnTopMidMergeBlankFailRetryMapRWContended() throws Exception {
        counter.set(1);
        executeWithPool(0, O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, ffMapRW);
    }

    @Test
    public void testColumnTopMidMergeBlankFailRetryMergeFixMapRW() throws Exception {
        counter.set(1);
        executeWithoutPool(O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, new FilesFacadeImpl() {

            long theFd = 0;

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (fd != theFd) {
                    return super.mmap(fd, len, offset, flags, memoryTag);
                }

                theFd = 0;
                return -1;
            }

            @Override
            public long openRW(LPSZ name) {
                long fd = super.openRW(name);
                if (Chars.endsWith(name, "1970-01-06.14" + Files.SEPARATOR + "v8.d") && counter.decrementAndGet() == 0) {
                    theFd = fd;
                }
                return fd;
            }
        });
    }

    @Test
    public void testColumnTopMidMergeBlankFailRetryMergeFixMapRWContended() throws Exception {
        counter.set(1);
        executeWithPool(0, O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, new FilesFacadeImpl() {

            long theFd = 0;

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (fd != theFd) {
                    return super.mmap(fd, len, offset, flags, memoryTag);
                }

                theFd = 0;
                return -1;
            }

            @Override
            public long openRW(LPSZ name) {
                long fd = super.openRW(name);
                if (Chars.endsWith(name, "1970-01-06.14" + Files.SEPARATOR + "v8.d") && counter.decrementAndGet() == 0) {
                    theFd = fd;
                }
                return fd;
            }
        });
    }

    @Test
    public void testColumnTopMidMergeBlankFailRetryOpenRW() throws Exception {
        counter.set(1);
        executeWithoutPool(O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, ffOpenRW);
    }

    @Test
    public void testColumnTopMidMergeBlankFailRetryOpenRWContended() throws Exception {
        counter.set(1);
        executeWithPool(0, O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, ffOpenRW);
    }

    @Test
    public void testColumnTopMidMergeBlankFailRetryOpenRw() throws Exception {
        counter.set(3);
        executeWithoutPool(O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "1970-01-06" + Files.SEPARATOR + "m.d") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testColumnTopMidMergeBlankFailRetryOpenRw2() throws Exception {
        counter.set(3);
        executeWithoutPool(O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "1970-01-06" + Files.SEPARATOR + "b.d") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testColumnTopMidMergeBlankFailRetryOpenRw2Contended() throws Exception {
        counter.set(3);
        executeWithPool(0, O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "1970-01-06" + Files.SEPARATOR + "b.d") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testColumnTopMidMergeBlankFailRetryOpenRwContended() throws Exception {
        counter.set(3);
        executeWithPool(0, O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "1970-01-06" + Files.SEPARATOR + "m.d") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testFailOnResizingIndexContended() throws Exception {
        // this places break point on resize of key file
        counter.set(144);
        executeWithPool(0, O3FailureTest::testPartitionedDataAppendOODataNotNullStrTailFailRetry0, ffAllocateFailure);
    }

    @Test
    public void testFailOnTruncateKeyIndexContended() throws Exception {
        counter.set(82);
        executeWithPool(0, O3FailureTest::testColumnTopLastOOOPrefixFailRetry0, new FilesFacadeImpl() {

            @Override
            public boolean truncate(long fd, long size) {
                if (counter.decrementAndGet() == 0) {
                    return false;
                }
                return super.truncate(fd, size);
            }
        });
    }

    @Test
    public void testFailOnTruncateKeyValueContended() throws Exception {
        counter.set(82);
        executeWithPool(0, O3FailureTest::testColumnTopLastOOOPrefixFailRetry0, new FilesFacadeImpl() {
            @Override
            public boolean truncate(long fd, long size) {
                if (counter.decrementAndGet() == 0) {
                    return false;
                }
                return super.truncate(fd, size);
            }
        });
    }

    @Test
    public void testInsertAsSelectNegativeTimestamp() throws Exception {
        executeWithPool(0, O3FailureTest::testInsertAsSelectNegativeTimestamp0);
    }

    @Test
    public void testInsertAsSelectNulls() throws Exception {
        executeWithPool(0, O3FailureTest::testInsertAsSelectNulls0);
    }

    @Test
    public void testOOOFollowedByAnotherOOO() throws Exception {
        counter.set(1);
        final AtomicBoolean restoreDiskSpace = new AtomicBoolean(false);
        executeWithPool(0,
                (engine, compiler, sqlExecutionContext) -> testOooFollowedByAnotherOOO0(engine, compiler, sqlExecutionContext, restoreDiskSpace),
                new FilesFacadeImpl() {

                    long theFd = 0;
                    boolean armageddon = false;

                    @Override
                    public boolean close(long fd) {
                        if (fd == theFd) {
                            theFd = 0;
                        }
                        return super.close(fd);
                    }

                    @Override
                    public long openRW(LPSZ name) {
                        long fd = super.openRW(name);
                        if (Chars.endsWith(name, "x" + Files.SEPARATOR + "1970-01-01.1" + Files.SEPARATOR + "m.d")) {
                            if (counter.decrementAndGet() == 0) {
                                theFd = fd;
                            }
                        }
                        return fd;
                    }

                    @Override
                    public boolean allocate(long fd, long size) {
                        if (restoreDiskSpace.get()) {
                            return super.allocate(fd, size);
                        }

                        if (armageddon) {
                            return false;
                        }
                        if (fd == theFd) {
                            theFd = 0;
                            armageddon = true;
                            return false;
                        }
                        return super.allocate(fd, size);
                    }
                });
    }

    @Test
    public void testOutOfFileHandles() throws Exception {
        counter.set(1536);
        executeWithPool(4, O3FailureTest::testOutOfFileHandles0, new FilesFacadeImpl() {
            @Override
            public boolean close(long fd) {
                counter.incrementAndGet();
                return super.close(fd);
            }

            @Override
            public long openAppend(LPSZ name) {
                if (counter.decrementAndGet() < 0) {
                    return -1;
                }
                return super.openAppend(name);
            }

            @Override
            public long openRO(LPSZ name) {
                if (counter.decrementAndGet() < 0) {
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public long openRW(LPSZ name) {
                if (counter.decrementAndGet() < 0) {
                    return -1;
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testPartitionedAllocateLastPartitionFail() throws Exception {
        counter.set(2);
        executeWithoutPool(O3FailureTest::testPartitionedDataAppendOOPrependOODataFailRetry0, ffFailToAllocateIndex);
    }

    @Test
    public void testPartitionedAllocateLastPartitionFailNoReopen() throws Exception {
        counter.set(2);
        executeWithPool(0, O3FailureTest::testPartitionedDataAppendOOPrependOODataFailRetryNoReopen, ffFailToAllocateIndex);
    }

    @Test
    public void testPartitionedCreateDirFail() throws Exception {
        counter.set(1);
        executeWithoutPool(O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, ffMkDirFailure);
    }

    @Test
    public void testPartitionedCreateDirFailContended() throws Exception {
        counter.set(1);
        executeWithPool(0, O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, ffMkDirFailure);
    }

    @Test
    public void testPartitionedDataAppendOOData() throws Exception {
        counter.set(4);
        executeWithoutPool(O3FailureTest::testPartitionedDataAppendOODataFailRetry0, new FilesFacadeImpl() {

            private final AtomicInteger mapCounter = new AtomicInteger(2);
            private long theFd = 0;

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (theFd == fd && mapCounter.decrementAndGet() == 0) {
                    theFd = 0;
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name) {
                long fd = super.openRW(name);
                if (Chars.endsWith(name, "ts.d") && counter.decrementAndGet() == 0) {
                    theFd = fd;
                }
                return fd;
            }
        });
    }

    @Test
    public void testPartitionedDataAppendOODataContended() throws Exception {
        counter.set(4);
        executeWithPool(0, O3FailureTest::testPartitionedDataAppendOODataFailRetry0, new FilesFacadeImpl() {

            private final AtomicInteger mapCounter = new AtomicInteger(2);
            private long theFd = 0;

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (theFd == fd && mapCounter.decrementAndGet() == 0) {
                    theFd = 0;
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name) {
                long fd = super.openRW(name);
                if (Chars.endsWith(name, "ts.d") && counter.decrementAndGet() == 0) {
                    theFd = fd;
                }
                return fd;
            }
        });
    }

    @Test
    public void testPartitionedDataAppendOODataIndexed() throws Exception {
        counter.set(3);
        executeWithoutPool(O3FailureTest::testPartitionedDataAppendOODataIndexedFailRetry0, new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "1970-01-06" + Files.SEPARATOR + "timestamp.d") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testPartitionedDataAppendOODataIndexedContended() throws Exception {
        counter.set(3);
        executeWithPool(0, O3FailureTest::testPartitionedDataAppendOODataIndexedFailRetry0, new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "1970-01-06" + Files.SEPARATOR + "timestamp.d") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testPartitionedDataAppendOODataNotNullStrTail() throws Exception {
        counter.set(174);
        executeWithoutPool(O3FailureTest::testPartitionedDataAppendOODataNotNullStrTailFailRetry0, ffAllocateFailure);
    }

    @Test
    public void testSetAppendPositionFails() throws Exception {
        counter.set(182);
        executeWithoutPool(O3FailureTest::testPartitionedDataAppendOODataNotNullStrTailFailRetry0, ffAllocateFailure);
    }

    @Test
    public void testPartitionedDataAppendOODataNotNullStrTailContended() throws Exception {
        counter.set(174);
        executeWithPool(0, O3FailureTest::testPartitionedDataAppendOODataNotNullStrTailFailRetry0, ffAllocateFailure);
    }

    @Test
    public void testPartitionedDataAppendOODataNotNullStrTailIndexAllocateFail() throws Exception {
        counter.set(2);
        executeWithoutPool(O3FailureTest::testPartitionedDataAppendOODataNotNullStrTailFailRetry0, ffIndexAllocateFailure);
    }

    @Test
    public void testPartitionedDataAppendOODataNotNullStrTailIndexAllocateFailContended() throws Exception {
        counter.set(2);
        executeWithPool(0, O3FailureTest::testPartitionedDataAppendOODataNotNullStrTailFailRetry0, ffIndexAllocateFailure);
    }

    @Test
    public void testPartitionedDataAppendOODataNotNullStrTailParallel() throws Exception {
        counter.set(174);
        executeWithPool(2, O3FailureTest::testPartitionedDataAppendOODataNotNullStrTailFailRetry0, ffAllocateFailure);
    }

    @Test
    public void testPartitionedDataAppendOOPrependOODatThenRegularAppend() throws Exception {
        counter.set(162);
        executeWithPool(0, O3FailureTest::testPartitionedDataAppendOOPrependOODatThenRegularAppend0, ffAllocateFailure);
    }

    @Test
    public void testPartitionedDataAppendOOPrependOOData() throws Exception {
        counter.set(162);
        executeWithoutPool(O3FailureTest::testPartitionedDataAppendOOPrependOODataFailRetry0, ffAllocateFailure);
    }

    @Test
    public void testPartitionedDataAppendOOPrependOODataContended() throws Exception {
        counter.set(162);
        executeWithPool(0, O3FailureTest::testPartitionedDataAppendOOPrependOODataFailRetry0, ffAllocateFailure);
    }

    @Test
    public void testPartitionedDataAppendOOPrependOODataMapVar() throws Exception {
        counter.set(3);
        executeWithoutPool(O3FailureTest::testPartitionedDataAppendOOPrependOODataFailRetry0, new FilesFacadeImpl() {

            private long theFd = 0;

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (theFd == fd) {
                    theFd = 0;
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name) {
                long fd = super.openRW(name);
                if (Chars.endsWith(name, "1970-01-06" + Files.SEPARATOR + "m.d") && counter.decrementAndGet() == 0) {
                    theFd = fd;
                }
                return fd;
            }
        });
    }

    @Test
    public void testPartitionedDataAppendOOPrependOODataMapVarContended() throws Exception {
        counter.set(3);
        executeWithPool(0, O3FailureTest::testPartitionedDataAppendOOPrependOODataFailRetry0, new FilesFacadeImpl() {

            private long theFd = 0;

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (theFd == fd) {
                    theFd = 0;
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name) {
                long fd = super.openRW(name);
                if (Chars.endsWith(name, "1970-01-06" + Files.SEPARATOR + "m.d") && counter.decrementAndGet() == 0) {
                    theFd = fd;
                }
                return fd;
            }
        });
    }

    @Test
    public void testPartitionedDataAppendOOPrependOODataParallel() throws Exception {
        counter.set(193);
        executeWithPool(4, O3FailureTest::testPartitionedDataAppendOOPrependOODataFailRetry0, ffAllocateFailure);
    }

    @Test
    public void testPartitionedDataAppendOOPrependOODataParallelNoReopen() throws Exception {
        counter.set(162);
        executeWithPool(4, O3FailureTest::testPartitionedDataAppendOOPrependOODataFailRetryNoReopen, ffAllocateFailure);
    }

    @Test
    public void testPartitionedOOPrefixesExistingPartitions() throws Exception {
        counter.set(1);
        executeWithoutPool(O3FailureTest::testPartitionedOOPrefixesExistingPartitionsFailRetry0, ffOpenIndexFailure);
    }

    @Test
    public void testPartitionedOOPrefixesExistingPartitionsContended() throws Exception {
        counter.set(1);
        executeWithPool(0, O3FailureTest::testPartitionedOOPrefixesExistingPartitionsFailRetry0, ffOpenIndexFailure);
    }

    @Test
    public void testPartitionedOOPrefixesExistingPartitionsCreateDirs() throws Exception {
        counter.set(2);
        executeWithoutPool(O3FailureTest::testPartitionedOOPrefixesExistingPartitionsFailRetry0, new FilesFacadeImpl() {
            @Override
            public int mkdirs(LPSZ path, int mode) {
                if (Chars.contains(path, "1970-01-01") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.mkdirs(path, mode);
            }
        });
    }

    @Test
    public void testPartitionedOOPrefixesExistingPartitionsCreateDirsContended() throws Exception {
        counter.set(2);
        executeWithPool(0, O3FailureTest::testPartitionedOOPrefixesExistingPartitionsFailRetry0, new FilesFacadeImpl() {
            @Override
            public int mkdirs(LPSZ path, int mode) {
                if (Chars.contains(path, "1970-01-01") && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.mkdirs(path, mode);
            }
        });
    }

    @Test
    public void testPartitionedOpenTimestampFail() throws Exception {
        counter.set(3);
        executeWithoutPool(O3FailureTest::testPartitionedDataAppendOOPrependOODataFailRetry0, ffOpenFailure);
    }

    @Test
    public void testPartitionedOpenTimestampFailContended() throws Exception {
        counter.set(3);
        executeWithPool(0, O3FailureTest::testPartitionedDataAppendOOPrependOODataFailRetry0, ffOpenFailure);
    }

    @Test
    public void testTwoRowsConsistency() throws Exception {
        executeWithPool(0, O3FailureTest::testTwoRowsConsistency0);
    }

    private static void testInsertAsSelectNulls0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i, " +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " timestamp_sequence(500000000000L,1000000L) ts," +
                        " cast(x as short) l" +
                        " from long_sequence(50)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile(
                "create table top as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " case WHEN x < 2 THEN CAST(NULL as TIMESTAMP) ELSE CAST(x as TIMESTAMP) END ts," +
                        " cast(x + 1000 as short)  l" +
                        " from long_sequence(100)" +
                        ")",
                sqlExecutionContext
        );

        try {
            compiler.compile("insert into x select * from top", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ex) {
            Chars.contains(ex.getFlyweightMessage(), "timestamps before 1970-01-01");
        }

        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from top where ts >= 0 union all select * from x)",
                "insert into x select * from top where ts >= 0"
        );
        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testInsertAsSelectNegativeTimestamp0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i, " +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " timestamp_sequence(500000000000L,1000000L) ts," +
                        " cast(x as short) l" +
                        " from long_sequence(50)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile(
                "create table top as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " timestamp_sequence(-500,10L) ts," +
                        " cast(x + 1000 as short)  l" +
                        " from long_sequence(100)" +
                        ")",
                sqlExecutionContext
        );

        try {
            compiler.compile("insert into x select * from top", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ex) {
            Chars.contains(ex.getFlyweightMessage(), "timestamps before 1970-01-01");
        }

        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from top where ts >= 0 union all select * from x)",
                "insert into x select * from top where ts >= 0"
        );
        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedOOPrefixesExistingPartitionsFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        compiler.compile(
                "create table top as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(15000000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(1000)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        try {
            compiler.compile("insert into x select * from top", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignored) {
        }

        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from x union all select * from top)",
                "insert into x select * from top"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedDataAppendOODataNotNullStrTailFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " cast(null as binary) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(510)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(518300000010L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        try {
            compiler.compile("insert into x select * from append", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException | CairoError ignored) {
        }

        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "insert into x select * from append"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedDataAppendOODataIndexedFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(518300000010L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        try {
            compiler.compile("insert into x select * from append", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignored) {
        }

        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "insert into x select * from append"
        );
    }

    private static void testColumnTopLastDataOOODataFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {

        //
        // ----- last partition
        //
        // +-----------+
        // |   empty   |
        // |           |
        // +-----------+   <-- top -->       +---------+
        // |           |                     |   data  |
        // |           |   +----------+      +---------+
        // |           |   |   OOO    |      |   ooo   |
        // |           | < |  block   |  ==  +---------+
        // |           |   | (narrow) |      |   data  |
        // |           |   +----------+      +---------+
        // +-----------+
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(549920000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(500)",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(549920000000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        try {
            compiler.compile("insert into x select * from append", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignored) {
        }

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "/o3/testColumnTopLastDataOOOData.txt"
        );

    }

    private static void testColumnTopMidDataMergeDataFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {

        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(549920000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(1000)",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(549900000000L,50000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        try {
            compiler.compile("insert into x select * from append", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignored) {
        }

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "/o3/testColumnTopMidDataMergeData.txt"
        );
    }

    private static void testColumnTopLastOOOPrefixFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,330000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(664670000000L,10000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(500)",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(604800000000L,10000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(400)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        try {
            compiler.compile("insert into x select * from append", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignored) {
        }

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "/o3/testColumnTopLastOOOPrefix.txt"
        );
    }

    private static void assertO3DataConsistency(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
        // create third table, which will contain both X and 1AM
        compiler.compile("create table y as (x union all append)", sqlExecutionContext);
        compiler.compile("insert into x select * from append", sqlExecutionContext);

        assertSqlResultAgainstFile(compiler, sqlExecutionContext, "/o3/testColumnTopMidAppendColumn.txt");
        engine.releaseAllReaders();
        assertSqlResultAgainstFile(compiler, sqlExecutionContext, "/o3/testColumnTopMidAppendColumn.txt");
    }

    private static void assertSqlResultAgainstFile(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String resourceName
    ) throws URISyntaxException, SqlException {
        printSqlResult(compiler, sqlExecutionContext, "x");

        URL url = O3FailureTest.class.getResource(resourceName);
        Assert.assertNotNull(url);
        TestUtils.assertEquals(new File(url.toURI()), sink);
    }

    private static void testPartitionedDataAppendOODataFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws SqlException {
        // create table with roughly 2AM data
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                executionContext
        );

        compiler.compile(
                "create table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(518300000010L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        try {
            compiler.compile("insert into x select * from append", executionContext);
            Assert.fail();
        } catch (CairoException ignored) {
        }
        // create third table, which will contain both X and 1AM
        assertO3DataConsistency(
                engine,
                compiler,
                executionContext,
                "create table y as (x union all append)",
                "insert into x select * from append"
        );

        assertIndexConsistency(compiler, executionContext);
    }

    private static void testColumnTopMidAppendBlankColumnFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws SqlException {
        // create table with roughly 2AM data
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                executionContext
        );

        compiler.compile("alter table x add column v double", executionContext);
        compiler.compile("alter table x add column v1 float", executionContext);
        compiler.compile("alter table x add column v2 int", executionContext);
        compiler.compile("alter table x add column v3 byte", executionContext);
        compiler.compile("alter table x add column v4 short", executionContext);
        compiler.compile("alter table x add column v5 boolean", executionContext);
        compiler.compile("alter table x add column v6 date", executionContext);
        compiler.compile("alter table x add column v7 timestamp", executionContext);
        compiler.compile("alter table x add column v8 symbol", executionContext);
        compiler.compile("alter table x add column v10 char", executionContext);
        compiler.compile("alter table x add column v11 string", executionContext);
        compiler.compile("alter table x add column v12 binary", executionContext);
        compiler.compile("alter table x add column v9 long", executionContext);

        compiler.compile(
                "create table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(518300000010L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        //  ------------------- new columns ------------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        try {
            compiler.compile("insert into x select * from append", executionContext);
            Assert.fail();
        } catch (CairoException ignored) {
        }

        // create third table, which will contain both X and 1AM
        assertO3DataConsistency(
                engine,
                compiler,
                executionContext,
                "create table y as (x union all append)",
                "insert into x select * from append"
        );

        assertIndexConsistency(compiler, executionContext);
    }

    private static void testColumnTopMidMergeBlankColumnFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        // create table with roughly 2AM data
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol index", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "create table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(518300000000L-1000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        //  ------------------- new columns ------------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        try {
            compiler.compile("insert into x select * from append", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignored) {
        }

        // create third table, which will contain both X and 1AM
        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "insert into x select * from append"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testColumnTopMidAppendColumnFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(549900000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        // ---- new columns ----
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(1000)" +
                        "",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(604700000001,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        // --------- new columns -----------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        try {
            compiler.compile("insert into x select * from append", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignore) {
        }

        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedDataAppendOOPrependOODataFailRetryNoReopen(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        testPartitionedDataAppendOOPrependOODataFailRetry0(
                engine,
                compiler,
                sqlExecutionContext,
                false
        );
    }

    private static void testPartitionedDataAppendOOPrependOODataFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        testPartitionedDataAppendOOPrependOODataFailRetry0(
                engine,
                compiler,
                sqlExecutionContext,
                true
        );
    }

    private static void testPartitionedDataAppendOOPrependOODataFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            boolean reopenTableWriter
    ) throws SqlException {
        // create table with roughly 2AM data
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " cast(null as binary) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(510)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // all records but one is appended to middle partition
        // last record is prepended to the last partition
        compiler.compile(
                "create table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(518390000000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(101)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        try {
            compiler.compile("insert into x select * from append", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignored) {
        }

        if (reopenTableWriter) {
            engine.releaseAllWriters();
        }

        // create third table, which will contain both X and 1AM
        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "insert into x select * from append"
        );

        assertIndexConsistency(
                compiler,
                sqlExecutionContext
        );
    }

    private static void testPartitionedDataAppendOOPrependOODatThenRegularAppend0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        // create table with roughly 2AM data
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " cast(null as binary) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(510)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // all records but one is appended to middle partition
        // last record is prepended to the last partition
        compiler.compile(
                "create table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(518390000000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(101)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        try {
            compiler.compile("insert into x select * from append", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignored) {
        }

        // all records but one is appended to middle partition
        // last record is prepended to the last partition
        compiler.compile(
                "create table append2 as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(551000000000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(101)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );


        // create third table, which will contain both X and 1AM
        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append2)",
                "insert into x select * from append2"
        );

        assertIndexConsistency(
                compiler,
                sqlExecutionContext
        );
    }

    private static void testOooFollowedByAnotherOOO0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            AtomicBoolean restoreDiskSpace
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(10000000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        printSqlResult(
                compiler,
                sqlExecutionContext,
                "x"
        );

        // create table with 1AM data

        compiler.compile(
                "create table 1am as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(9993000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(507)" +
                        ")",
                sqlExecutionContext
        );

        compiler.compile(
                "create table tail as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(9997000010L,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        compiler.compile("create table y as (x union all 1am union all tail)", sqlExecutionContext);

        try {
            compiler.compile("insert into x select * from 1am", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignore) {
            // ignore "no disk space left" error and keep going
        }

        try {
            compiler.compile("insert into x select * from tail", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignore) {
        }

        restoreDiskSpace.set(true);

        // check that table data is intact using "cached" table reader
        // e.g. one that had files already open
        TestUtils.printSql(compiler, sqlExecutionContext, "x", sink2);
        TestUtils.assertEquals(sink, sink2);

        engine.releaseAllReaders();

        // now check that "fresh" table reader can also see consistent data
        TestUtils.printSql(compiler, sqlExecutionContext, "x", sink2);
        TestUtils.assertEquals(sink, sink2);

        // now perform two OOO inserts
        compiler.compile("insert into x select * from 1am", sqlExecutionContext);
        compiler.compile("insert into x select * from tail", sqlExecutionContext);

        printSqlResult(compiler, sqlExecutionContext, "y order by ts");
        TestUtils.printSql(compiler, sqlExecutionContext, "x", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private static void testTwoRowsConsistency0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x (ts timestamp, block_nr long) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "x",
                sink,
                "ts\tblock_nr\n"
        );

        TestUtils.insert(
                compiler,
                sqlExecutionContext,
                "insert into x values(cast('2010-02-04T21:43:14.000000Z' as timestamp), 38304)"
        );

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "x",
                sink,
                "ts\tblock_nr\n" +
                        "2010-02-04T21:43:14.000000Z\t38304\n"
        );

        TestUtils.insert(
                compiler,
                sqlExecutionContext,
                "insert into x values(cast('2010-02-14T23:52:59.000000Z' as timestamp), 40320)"
        );

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "x",
                sink,
                "ts\tblock_nr\n" +
                        "2010-02-04T21:43:14.000000Z\t38304\n" +
                        "2010-02-14T23:52:59.000000Z\t40320\n"
        );

    }

    private static void testOutOfFileHandles0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " rnd_str(5,16,2) i," +
                        " rnd_str(5,16,2) sym," +
                        " rnd_str(5,16,2) amt," +
                        " rnd_str(5,16,2) timestamp," +
                        " rnd_str(5,16,2) b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_str(5,16,2) d," +
                        " rnd_str(5,16,2) e," +
                        " rnd_str(5,16,2) f," +
                        " rnd_str(5,16,2) g," +
                        " rnd_str(5,16,2) ik," +
                        " rnd_str(5,16,2) j," +
                        " timestamp_sequence(500000000000L,100000000L) ts," +
                        " rnd_str(5,16,2) l," +
                        " rnd_str(5,16,2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_str(5,16,2) t," +
                        " rnd_str(5,16,2) l256" +
                        " from long_sequence(10000)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        compiler.compile("create table x1 as (x) timestamp(ts) partition by DAY", executionContext);

        compiler.compile(
                "create table y as (" +
                        "select" +
                        " rnd_str(5,16,2) i," +
                        " rnd_str(5,16,2) sym," +
                        " rnd_str(5,16,2) amt," +
                        " rnd_str(5,16,2) timestamp," +
                        " rnd_str(5,16,2) b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_str(5,16,2) d," +
                        " rnd_str(5,16,2) e," +
                        " rnd_str(5,16,2) f," +
                        " rnd_str(5,16,2) g," +
                        " rnd_str(5,16,2) ik," +
                        " rnd_str(5,16,2) j," +
                        " timestamp_sequence(500000080000L,79999631L) ts," +
                        " rnd_str(5,16,2) l," +
                        " rnd_str(5,16,2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_str(5,16,2) t," +
                        " rnd_str(5,16,2) l256" +
                        " from long_sequence(10000)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        compiler.compile("create table y1 as (y)", executionContext);

        // create expected result sets
        compiler.compile("create table z as (x union all y)", executionContext);

        // create another compiler to be used by second pool
        try (SqlCompiler compiler2 = new SqlCompiler(engine)) {

            final CyclicBarrier barrier = new CyclicBarrier(2);
            final SOCountDownLatch haltLatch = new SOCountDownLatch(2);
            final AtomicInteger errorCount = new AtomicInteger();

            // we have two pairs of tables (x,y) and (x1,y1)
            WorkerPool pool1 = new WorkerPool(new WorkerPoolAwareConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1};
                }

                @Override
                public int getWorkerCount() {
                    return 1;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }

                @Override
                public boolean isEnabled() {
                    return true;
                }
            });

            pool1.assign(new Job() {
                private boolean toRun = true;

                @Override
                public boolean run(int workerId) {
                    if (toRun) {
                        try {
                            toRun = false;
                            barrier.await();
                            compiler.compile("insert into x select * from y", executionContext);
                        } catch (Throwable e) {
                            e.printStackTrace();
                            errorCount.incrementAndGet();
                        } finally {
                            haltLatch.countDown();
                        }
                    }
                    return false;
                }
            });
            pool1.assignCleaner(Path.CLEANER);

            final WorkerPool pool2 = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1};
                }

                @Override
                public int getWorkerCount() {
                    return 1;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }
            });

            pool2.assign(new Job() {
                private boolean toRun = true;

                @Override
                public boolean run(int workerId) {
                    if (toRun) {
                        try {
                            toRun = false;
                            barrier.await();
                            compiler2.compile("insert into x1 select * from y1", executionContext);
                        } catch (Throwable e) {
                            e.printStackTrace();
                            errorCount.incrementAndGet();
                        } finally {
                            haltLatch.countDown();
                        }
                    }
                    return false;
                }
            });

            pool2.assignCleaner(Path.CLEANER);

            pool1.start(null);
            pool2.start(null);

            haltLatch.await();

            pool1.halt();
            pool2.halt();
            Assert.assertTrue(errorCount.get() > 0);
        }
    }

    private void executeWithoutPool(O3Runnable runnable, FilesFacade ff) throws Exception {
        executeVanilla(() -> {
            final CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };
            execute(null, runnable, configuration);
        });
    }
}
