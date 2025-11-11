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

package io.questdb.test.cairo.o3;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.Job;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.vm.Vm.getStorageLength;
import static io.questdb.test.AbstractCairoTest.replaceTimestampSuffix1;

public class O3FailureTest extends AbstractO3Test {
    private final static AtomicInteger counter = new AtomicInteger(0);
    private static final FilesFacade ffOpenIndexFailure = new TestFilesFacadeImpl() {
        @Override
        public long openRW(LPSZ name, int opts) {
            if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "sym.v") && Utf8s.containsAscii(name, "1970-01-02") && counter.decrementAndGet() == 0) {
                return -1;
            }
            return super.openRW(name, opts);
        }
    };
    private final static AtomicBoolean fixFailure = new AtomicBoolean(true);
    private static final FilesFacade ffMapRW = new TestFilesFacadeImpl() {
        @Override
        public boolean close(long fd) {
            if (fd > 0 && fd == this.fd) {
                this.fd = -1;
            }
            return super.close(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            if (!fixFailure.get() || this.fd == fd) {
                fixFailure.set(false);
                this.fd = -1;
                return -1;
            }
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            long fd = super.openRW(name, opts);
            if (Utf8s.endsWithAscii(name, "1970-01-06.16" + Files.SEPARATOR + "i.d") && counter.decrementAndGet() == 0) {
                this.fd = fd;
            }
            return fd;
        }
    };
    private static final FilesFacade ffMkDirFailure = new TestFilesFacadeImpl() {
        @Override
        public int mkdirs(Path path, int mode) {
            if (!fixFailure.get() || (Utf8s.containsAscii(path, "1970-01-06.16") && counter.decrementAndGet() == 0)) {
                fixFailure.set(false);
                return -1;
            }
            return super.mkdirs(path, mode);
        }
    };
    private static final FilesFacade ffOpenRW = new TestFilesFacadeImpl() {
        @Override
        public long openRW(LPSZ name, int opts) {
            if (!fixFailure.get() || (Utf8s.endsWithAscii(name, "1970-01-06.16" + Files.SEPARATOR + "i.d") && counter.decrementAndGet() == 0)) {
                fixFailure.set(false);
                return -1;
            }
            return super.openRW(name, opts);
        }
    };

    @Override
    @Before
    public void setUp() {
        super.setUp();
        fixFailure.set(true);
        commitMode = CommitMode.NOSYNC;
    }

    @Test
    public void testAllocateFailsAtO3OpenColumn() throws Exception {
        // Failing to allocate concrete file is more stable than failing on a counter
        String fileName = "1970-01-06" + Files.SEPARATOR + "ts.d";
        executeWithPool(
                0, O3FailureTest::testAllocateFailsAtO3OpenColumn0, new TestFilesFacadeImpl() {
                    @Override
                    public boolean allocate(long fd, long size) {
                        if (fd == this.fd && size == 1472) {
                            this.fd = -1;
                            return false;
                        }
                        return super.allocate(fd, size);
                    }

                    @Override
                    public boolean close(long fd) {
                        if (fd > 0 && fd == this.fd) {
                            boolean result = super.close(fd);
                            this.fd = 0;
                            return result;
                        }
                        return super.close(fd);
                    }

                    @Override
                    public long length(long fd) {
                        long len = super.length(fd);
                        if (fd == this.fd) {
                            if (len == Files.PAGE_SIZE) {
                                return 0;
                            }
                        }
                        return len;
                    }

                    @Override
                    public long openRW(LPSZ name, int opts) {
                        long fd = TestFilesFacadeImpl.INSTANCE.openRW(name, opts);
                        if (this.fd >= 0 && fd > 0 && Utf8s.endsWithAscii(name, fileName)) {
                            this.fd = fd;
                            return fd;
                        }
                        return fd;
                    }

                    {
                        this.fd = 0;
                    }
                }
        );
    }

    @Test
    public void testAllocateToResizeLastPartition() throws Exception {
        // Failing to allocate concrete file is more stable than failing on a counter
        String fileName = "1970-01-06" + Files.SEPARATOR + "ts.d";
        executeWithPool(
                0, O3FailureTest::testAllocateToResizeLastPartition0, new TestFilesFacadeImpl() {

                    @Override
                    public boolean allocate(long fd, long size) {
                        if (fd == this.fd) {
                            if (size == 1480) {
                                this.fd = -1;
                                return false;
                            }
                        }
                        return super.allocate(fd, size);
                    }

                    @Override
                    public boolean close(long fd) {
                        if (fd > 0 && fd == this.fd) {
                            this.fd = -1;
                        }
                        return super.close(fd);
                    }

                    @Override
                    public long length(long fd) {
                        long len = super.length(fd);
                        if (fd == this.fd) {
                            if (len == Files.PAGE_SIZE) {
                                return 0;
                            }
                        }
                        return len;
                    }

                    @Override
                    public long openRW(LPSZ name, int opts) {
                        long fd = TestFilesFacadeImpl.INSTANCE.openRW(name, opts);
                        if (fd > 0 && Utf8s.endsWithAscii(name, fileName)) {
                            this.fd = fd;
                            return fd;
                        }
                        return fd;
                    }
                }
        );
    }

    @Test
    public void testColumnTopLastDataOOODataFailRetryCantWriteTop() throws Exception {
        counter.set(1);
        executeWithoutPool(
                O3FailureTest::testColumnTopLastDataOOODataFailRetry0,
                failToMMap("1970-01-07" + Files.SEPARATOR + "v.d.1")
        );
    }

    @Test
    public void testColumnTopLastDataOOODataFailRetryCantWriteTopContended() throws Exception {
        counter.set(1);
        executeWithPool(
                0,
                O3FailureTest::testColumnTopLastDataOOODataFailRetry0,
                failToMMap("1970-01-07" + Files.SEPARATOR + "v.d.1")
        );
    }

    @Test
    public void testColumnTopLastDataOOODataFailRetryMapRo() throws Exception {
        counter.set(1);
        executeWithoutPool(
                O3FailureTest::testColumnTopLastDataOOODataFailRetry0, new TestFilesFacadeImpl() {


                    @Override
                    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                        if (fd == this.fd && flags == Files.MAP_RO) {
                            this.fd = -1;
                            return -1;
                        }
                        return super.mmap(fd, len, offset, flags, memoryTag);
                    }

                    @Override
                    public long openRW(LPSZ name, int opts) {
                        long fd = super.openRW(name, opts);
                        if (Utf8s.containsAscii(name, "1970-01-07" + Files.SEPARATOR + "v11.d") && counter.decrementAndGet() == 0) {
                            this.fd = fd;
                        }
                        return fd;
                    }
                }
        );
    }

    @Test
    public void testColumnTopLastDataOOODataFailRetryMapRoContended() throws Exception {
        counter.set(1);
        executeWithPool(
                0, O3FailureTest::testColumnTopLastDataOOODataFailRetry0, new TestFilesFacadeImpl() {


                    @Override
                    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                        if (fd == this.fd && flags == Files.MAP_RO) {
                            this.fd = -1;
                            return -1;
                        }
                        return super.mmap(fd, len, offset, flags, memoryTag);
                    }

                    @Override
                    public long openRW(LPSZ name, int opts) {
                        long fd = super.openRW(name, opts);
                        if (Utf8s.containsAscii(name, "1970-01-07" + Files.SEPARATOR + "v11.d") && counter.decrementAndGet() == 0) {
                            this.fd = fd;
                        }
                        return fd;
                    }
                }
        );
    }

    @Test
    public void testColumnTopLastDataOOODataFailToSyncContended() throws Exception {
        counter.set(1);
        commitMode = CommitMode.SYNC;
        executeWithPool(
                0,
                O3FailureTest::testColumnTopLastDataOOODataFailRetry0,
                failToFSync("1970-01-07.17" + Files.SEPARATOR + "i.d")
        );
    }

    @Test
    public void testColumnTopLastDataOOODataFailToSyncParallel() throws Exception {
        counter.set(1);
        commitMode = CommitMode.SYNC;
        executeWithPool(
                4,
                O3FailureTest::testColumnTopLastDataOOODataFailRetry0,
                failToFSync("1970-01-07.17" + Files.SEPARATOR + "i.d")
        );
    }

    @Test
    public void testColumnTopMidAppend() throws Exception {
        counter.set(3);
        executeWithoutPool(
                O3FailureTest::testColumnTopMidAppendColumnFailRetry0, new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (Utf8s.containsAscii(name, "1970-01-07" + Files.SEPARATOR + "v12.d") && counter.decrementAndGet() == 0) {
                            return -1;
                        }
                        return super.openRW(name, opts);
                    }
                }
        );
    }

    @Test
    public void testColumnTopMidAppendBlank() throws Exception {
        counter.set(1);
        executeWithoutPool(
                O3FailureTest::testColumnTopMidAppendBlankColumnFailRetry0,
                failOnOpenRW("v.d.1", 2)
        );
    }

    @Test
    public void testColumnTopMidAppendBlankContended() throws Exception {
        counter.set(1);
        executeWithPool(
                0,
                O3FailureTest::testColumnTopMidAppendBlankColumnFailRetry0,
                failOnOpenRW("v.d.1", 2)
        );
    }

    @Test
    public void testColumnTopMidAppendContended() throws Exception {
        counter.set(3);
        executeWithPool(
                0, O3FailureTest::testColumnTopMidAppendColumnFailRetry0, new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (Utf8s.containsAscii(name, "1970-01-07" + Files.SEPARATOR + "v12.d") && counter.decrementAndGet() == 0) {
                            return -1;
                        }
                        return super.openRW(name, opts);
                    }
                }
        );
    }

    @Test
    public void testColumnTopMidDataMergeDataFailRetryReadTop() throws Exception {
        counter.set(13);
        executeWithoutPool(O3FailureTest::testColumnTopMidDataMergeDataFailRetry0, failOnOpenRW("v2.d.3", 3));
    }

    @Test
    public void testColumnTopMidDataMergeDataFailRetryReadTopContended() throws Exception {
        counter.set(13);
        executeWithPool(0, O3FailureTest::testColumnTopMidDataMergeDataFailRetry0, failOnOpenRW("v2.d.3", 3));
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
        executeWithoutPool(
                O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, new TestFilesFacadeImpl() {

                    @Override
                    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                        if (fixFailure.get() && fd != this.fd) {
                            return super.mmap(fd, len, offset, flags, memoryTag);
                        }
                        fixFailure.set(false);
                        this.fd = -1;
                        return -1;
                    }

                    @Override
                    public long openRW(LPSZ name, int opts) {
                        long fd = super.openRW(name, opts);
                        if (Utf8s.containsAscii(name, "1970-01-06.16" + Files.SEPARATOR + "v8.d") && counter.decrementAndGet() == 0) {
                            this.fd = fd;
                        }
                        return fd;
                    }
                }
        );
    }

    @Test
    public void testColumnTopMidMergeBlankFailRetryMergeFixMapRWContended() throws Exception {
        counter.set(1);
        executeWithPool(
                0, O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, new TestFilesFacadeImpl() {

                    @Override
                    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                        if (fixFailure.get() && fd != this.fd) {
                            return super.mmap(fd, len, offset, flags, memoryTag);
                        }

                        fixFailure.set(false);
                        this.fd = -1;
                        return -1;
                    }

                    @Override
                    public long openRW(LPSZ name, int opts) {
                        long fd = super.openRW(name, opts);
                        if (Utf8s.containsAscii(name, "1970-01-06.16" + Files.SEPARATOR + "v8.d") && counter.decrementAndGet() == 0) {
                            this.fd = fd;
                        }
                        return fd;
                    }
                }
        );
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
        executeWithoutPool(
                O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (!fixFailure.get() || (Utf8s.endsWithAscii(name, "1970-01-06" + Files.SEPARATOR + "m.d") && counter.decrementAndGet() == 0)) {
                            fixFailure.set(false);
                            return -1;
                        }
                        return super.openRW(name, opts);
                    }
                }
        );
    }

    @Test
    public void testColumnTopMidMergeBlankFailRetryOpenRw2() throws Exception {
        counter.set(3);
        executeWithoutPool(
                O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (!fixFailure.get() || (Utf8s.endsWithAscii(name, "1970-01-06" + Files.SEPARATOR + "b.d") && counter.decrementAndGet() == 0)) {
                            fixFailure.set(false);
                            return -1;
                        }
                        return super.openRW(name, opts);
                    }
                }
        );
    }

    @Test
    public void testColumnTopMidMergeBlankFailRetryOpenRw2Contended() throws Exception {
        counter.set(3);
        executeWithPool(
                4, O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (!fixFailure.get() || (Utf8s.endsWithAscii(name, "1970-01-06" + Files.SEPARATOR + "b.d") && counter.decrementAndGet() == 0)) {
                            fixFailure.set(false);
                            return -1;
                        }
                        return super.openRW(name, opts);
                    }
                }
        );
    }

    @Test
    public void testColumnTopMidMergeBlankFailRetryOpenRwContended() throws Exception {
        counter.set(3);
        executeWithPool(
                0, O3FailureTest::testColumnTopMidMergeBlankColumnFailRetry0, new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (!fixFailure.get() || (Utf8s.endsWithAscii(name, "1970-01-06" + Files.SEPARATOR + "m.d") && counter.decrementAndGet() == 0)) {
                            fixFailure.set(false);
                            return -1;
                        }
                        return super.openRW(name, opts);
                    }
                }
        );
    }

    @Test
    public void testFailMergeWalFixIntoLagContended() throws Exception {
        executeWithPool(0, O3FailureTest::testFailMergeWalFixIntoLag0);
    }

    @Test
    public void testFailMergeWalFixIntoLagParallel() throws Exception {
        executeWithPool(2, O3FailureTest::testFailMergeWalFixIntoLag0);
    }

    @Test
    public void testFailMergeWalVarIntoLagContended() throws Exception {
        executeWithPool(0, O3FailureTest::testFailMergeWalVarIntoLag0);
    }

    @Test
    public void testFailMergeWalVarIntoLagParallel() throws Exception {
        executeWithPool(2, O3FailureTest::testFailMergeWalVarIntoLag0);
    }

    @Test
    public void testFailMoveUncommittedContended() throws Exception {
        executeWithPool(0, O3FailureTest::testFailMoveUncommitted0);
    }

    @Test
    public void testFailMoveUncommittedParallel() throws Exception {
        executeWithPool(2, O3FailureTest::testFailMoveUncommitted0);
    }

    @Test
    public void testFailMoveWalToLagContended() throws Exception {
        executeWithPool(0, O3FailureTest::testFailMoveWalToLag0);
    }

    @Test
    public void testFailMoveWalToLagParallel() throws Exception {
        executeWithPool(2, O3FailureTest::testFailMoveWalToLag0);
    }

    @Test
    public void testFailOnTruncateKeyIndexContended() throws Exception {
        counter.set(0);
        executeWithPool(
                0, O3FailureTest::testColumnTopLastOOOPrefixFailRetry0, new TestFilesFacadeImpl() {
                    @Override
                    public boolean truncate(long fd, long size) {
                        // First two calls to truncate are for varchar column
                        if (size == 0 && counter.getAndIncrement() == 3) {
                            return false;
                        }
                        return super.truncate(fd, size);
                    }
                }
        );
    }

    @Test
    public void testFailOnTruncateKeyValueContended() throws Exception {
        counter.set(0);
        // different number of calls to "truncate" on Windows and *Nix
        // the number targets truncate of key file in BitmapIndexWriter
        executeWithPool(
                0, O3FailureTest::testColumnTopLastOOOPrefixFailRetry0, new TestFilesFacadeImpl() {
                    @Override
                    public boolean truncate(long fd, long size) {
                        // First two calls to truncate are for varchar column
                        if (size == 0 && counter.getAndIncrement() == 3) {
                            return false;
                        }
                        return super.truncate(fd, size);
                    }
                }
        );
    }

    @Test
    public void testFixedColumnCopyPrefixFails() throws Exception {
        int storageLength = 8;
        long records = 500;

        executeWithPool(
                0,
                (engine, compiler, sqlExecutionContext, timestampTypeAndName) -> {
                    Assume.assumeTrue(engine.getConfiguration().isWriterMixedIOEnabled());

                    String tableName = "testFixedColumnCopyPrefixFails";
                    engine.execute(
                            "create atomic table " + tableName + " as ( " +
                                    "select " +
                                    "x, " +
                                    " timestamp_sequence('2022-02-24', 1000)::" + timestampTypeAndName + " ts" +
                                    " from long_sequence(" + records + ")" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    long maxTimestamp = MICRO_DRIVER.parseFloorLiteral("2022-02-24") + records * 1000L;
                    CharSequence o3Ts = MICRO_DRIVER.toMSecString(maxTimestamp - 2000);

                    try {
                        engine.execute("insert into " + tableName + " VALUES(-1, '" + o3Ts + "')", sqlExecutionContext);
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }

                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext,
                            "select * from " + tableName + " limit -5,5",
                            sink,
                            replaceTimestampSuffix1("""
                                    x\tts
                                    496\t2022-02-24T00:00:00.495000Z
                                    497\t2022-02-24T00:00:00.496000Z
                                    498\t2022-02-24T00:00:00.497000Z
                                    499\t2022-02-24T00:00:00.498000Z
                                    500\t2022-02-24T00:00:00.499000Z
                                    """, timestampTypeAndName)
                    );

                    // Insert ok after failure
                    o3Ts = MICRO_DRIVER.toMSecString(maxTimestamp - 3000);
                    engine.execute("insert into " + tableName + " VALUES(-1, '" + o3Ts + "')", sqlExecutionContext);
                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext, "select * from " + tableName + " limit -5,5",
                            sink,
                            replaceTimestampSuffix1("""
                                    x\tts
                                    497\t2022-02-24T00:00:00.496000Z
                                    498\t2022-02-24T00:00:00.497000Z
                                    -1\t2022-02-24T00:00:00.497000Z
                                    499\t2022-02-24T00:00:00.498000Z
                                    500\t2022-02-24T00:00:00.499000Z
                                    """, timestampTypeAndName)
                    );
                },
                new TestFilesFacadeImpl() {
                    @Override
                    public long write(long fd, long address, long len, long offset) {
                        if (offset == 0 && len == storageLength * (records - 1)) {
                            return -1;
                        }
                        return super.write(fd, address, len, offset);
                    }
                }
        );
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
        executeWithPool(
                0,
                (engine, compiler, sqlExecutionContext, timestampTypeName) -> testOooFollowedByAnotherOOO0(engine, compiler, sqlExecutionContext, restoreDiskSpace, timestampTypeName),
                new TestFilesFacadeImpl() {
                    boolean armageddon = false;
                    long theFd = 0;

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

                    @Override
                    public boolean close(long fd) {
                        if (fd == theFd) {
                            theFd = 0;
                        }
                        return super.close(fd);
                    }

                    @Override
                    public long openRW(LPSZ name, int opts) {
                        long fd = super.openRW(name, opts);
                        if (Utf8s.endsWithAscii(name, "x" + TableUtils.SYSTEM_TABLE_NAME_SUFFIX + Files.SEPARATOR + "1970-01-01.1" + Files.SEPARATOR + "m.d")) {
                            if (counter.decrementAndGet() == 0) {
                                theFd = fd;
                            }
                        }
                        return fd;
                    }
                }
        );
    }

    @Test
    public void testOutOfFileHandles() throws Exception {
        counter.set(600);
        executeWithPool(
                4, O3FailureTest::testOutOfFileHandles0, new TestFilesFacadeImpl() {
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
                    public long openRW(LPSZ name, int opts) {
                        if (counter.decrementAndGet() < 0) {
                            return -1;
                        }
                        return super.openRW(name, opts);
                    }
                }
        );
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
        executeWithoutPool(
                O3FailureTest::testPartitionedDataAppendOODataFailRetry0, new TestFilesFacadeImpl() {
                    private final AtomicInteger mapCounter = new AtomicInteger(2);
                    private long theFd = 0;

                    @Override
                    public boolean close(long fd) {
                        if (fd > 0 && fd == theFd) {
                            theFd = 0;
                        }
                        return super.close(fd);
                    }

                    @Override
                    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                        if (!fixFailure.get() || (theFd == fd && mapCounter.decrementAndGet() == 0)) {
                            fixFailure.set(false);
                            theFd = 0;
                            return -1;
                        }
                        return super.mmap(fd, len, offset, flags, memoryTag);
                    }

                    @Override
                    public long openRW(LPSZ name, int opts) {
                        long fd = super.openRW(name, opts);
                        if (Utf8s.endsWithAscii(name, "ts.d") && counter.decrementAndGet() == 0) {
                            theFd = fd;
                        }
                        return fd;
                    }
                }
        );
    }

    @Test
    public void testPartitionedDataAppendOODataContended() throws Exception {
        counter.set(4);
        executeWithPool(
                0, O3FailureTest::testPartitionedDataAppendOODataFailRetry0, new TestFilesFacadeImpl() {
                    private final AtomicInteger mapCounter = new AtomicInteger(2);
                    private long theFd = 0;

                    @Override
                    public boolean close(long fd) {
                        if (fd > 0 && fd == theFd) {
                            theFd = 0;
                        }
                        return super.close(fd);
                    }

                    @Override
                    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                        if (!fixFailure.get() || (theFd == fd && mapCounter.decrementAndGet() == 0)) {
                            fixFailure.set(false);
                            theFd = 0;
                            return -1;
                        }
                        return super.mmap(fd, len, offset, flags, memoryTag);
                    }

                    @Override
                    public long openRW(LPSZ name, int opts) {
                        long fd = super.openRW(name, opts);
                        if (Utf8s.endsWithAscii(name, "ts.d") && counter.decrementAndGet() == 0) {
                            theFd = fd;
                        }
                        return fd;
                    }
                }
        );
    }

    @Test
    public void testPartitionedDataAppendOODataIndexed() throws Exception {
        counter.set(3);
        executeWithoutPool(
                O3FailureTest::testPartitionedDataAppendOODataIndexedFailRetry0, new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (Utf8s.endsWithAscii(name, "1970-01-06" + Files.SEPARATOR + "timestamp.d") && counter.decrementAndGet() == 0) {
                            return -1;
                        }
                        return super.openRW(name, opts);
                    }
                }
        );
    }

    @Test
    public void testPartitionedDataAppendOODataIndexedContended() throws Exception {
        counter.set(3);
        executeWithPool(
                0, O3FailureTest::testPartitionedDataAppendOODataIndexedFailRetry0, new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (Utf8s.endsWithAscii(name, "1970-01-06" + Files.SEPARATOR + "timestamp.d") && counter.decrementAndGet() == 0) {
                            return -1;
                        }
                        return super.openRW(name, opts);
                    }
                }
        );
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
        executeWithoutPool(
                O3FailureTest::testPartitionedOOPrefixesExistingPartitionsFailRetry0, new TestFilesFacadeImpl() {
                    @Override
                    public int mkdirs(Path path, int mode) {
                        if (Utf8s.containsAscii(path, "1970-01-01") && counter.decrementAndGet() == 0) {
                            return -1;
                        }
                        return super.mkdirs(path, mode);
                    }
                }
        );
    }

    @Test
    public void testPartitionedOOPrefixesExistingPartitionsCreateDirsContended() throws Exception {
        counter.set(2);
        executeWithPool(
                0, O3FailureTest::testPartitionedOOPrefixesExistingPartitionsFailRetry0, new TestFilesFacadeImpl() {
                    @Override
                    public int mkdirs(Path path, int mode) {
                        if (Utf8s.containsAscii(path, "1970-01-01") && counter.decrementAndGet() == 0) {
                            return -1;
                        }
                        return super.mkdirs(path, mode);
                    }
                }
        );
    }

    @Test
    public void testPartitionedWithAllocationCallLimit() throws Exception {
        counter.set(0);
        executeWithPool(
                0, O3FailureTest::testPartitionedWithAllocationCallLimit0, new TestFilesFacadeImpl() {
                    @Override
                    public boolean allocate(long fd, long size) {
                        // This tests that BitmapIndexWriter allocates value file in configured incremental pages
                        // instead of allocating block by block.
                        // If allocation block by block happens, number of calls is very big here and failure is simulated.
                        if (counter.incrementAndGet() > 200) {
                            return false;
                        }
                        return super.allocate(fd, size);
                    }
                }
        );
    }

    @Test
    public void testTwoRowsConsistency() throws Exception {
        executeWithPool(0, O3FailureTest::testTwoRowsConsistency0);
    }

    @Test
    public void testVarColumnCopyPrefixFails() throws Exception {
        String strColVal = "[srcDataMax=165250000]";
        int storageLength = getStorageLength(strColVal);
        long records = 500;

        executeWithPool(
                0,
                (engine, compiler, sqlExecutionContext, timestampTypeAndName) -> {
                    Assume.assumeTrue(engine.getConfiguration().isWriterMixedIOEnabled());

                    String tableName = "testVarColumnCopyPrefixFails";
                    engine.execute(
                            "create atomic table " + tableName + " as ( " +
                                    "select " +
                                    "'" + strColVal + "' as str, " +
                                    " timestamp_sequence('2022-02-24', 1000)::" + timestampTypeAndName + "  ts" +
                                    " from long_sequence(" + records + ")" +
                                    ") timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    long maxTimestamp = MICRO_DRIVER.parseFloorLiteral("2022-02-24") + records * 1000L;
                    CharSequence o3Ts = MICRO_DRIVER.toMSecString(maxTimestamp - 2000);

                    try {
                        engine.execute("insert into " + tableName + " VALUES('abcd', '" + o3Ts + "')", sqlExecutionContext);
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }

                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext,
                            "select * from " + tableName + " limit -5,5",
                            sink,
                            replaceTimestampSuffix1("str\tts\n" +
                                    strColVal + "\t2022-02-24T00:00:00.495000Z\n" +
                                    strColVal + "\t2022-02-24T00:00:00.496000Z\n" +
                                    strColVal + "\t2022-02-24T00:00:00.497000Z\n" +
                                    strColVal + "\t2022-02-24T00:00:00.498000Z\n" +
                                    strColVal + "\t2022-02-24T00:00:00.499000Z\n", timestampTypeAndName)
                    );

                    // Insert ok after failure
                    o3Ts = MICRO_DRIVER.toMSecString(maxTimestamp - 3000);
                    engine.execute("insert into " + tableName + " VALUES('abcd', '" + o3Ts + "')", sqlExecutionContext);
                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext, "select * from " + tableName + " limit -5,5",
                            sink,
                            replaceTimestampSuffix1("str\tts\n" +
                                    strColVal + "\t2022-02-24T00:00:00.496000Z\n" +
                                    strColVal + "\t2022-02-24T00:00:00.497000Z\n" +
                                    "abcd\t2022-02-24T00:00:00.497000Z\n" +
                                    strColVal + "\t2022-02-24T00:00:00.498000Z\n" +
                                    strColVal + "\t2022-02-24T00:00:00.499000Z\n", timestampTypeAndName)
                    );
                },
                new TestFilesFacadeImpl() {
                    @Override
                    public long write(long fd, long address, long len, long offset) {
                        if (offset == 0 && len == storageLength * (records - 1)) {
                            return -1;
                        }
                        return super.write(fd, address, len, offset);
                    }
                }
        );
    }

    @Test
    public void testVarColumnStress() throws Exception {
        dataAppendPageSize = 1024 * 1024;
        executeWithPool(
                8, O3FailureTest::testVarColumnStress, new TestFilesFacadeImpl() {
                    boolean tooManyFiles = false;

                    @Override
                    public int mkdirs(Path path, int mode) {
                        return super.mkdirs(path, mode);
                    }

                    @Override
                    public long openRO(LPSZ name) {
                        if (tooManyFiles) {
                            return -1;
                        }
                        return super.openRO(name);
                    }

                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (Utf8s.containsAscii(name, "1970-01-01.4" + Files.SEPARATOR + "g.d")) {
                            tooManyFiles = true;
                            return -1;
                        }
                        return super.openRW(name, opts);
                    }
                }
        );
    }

    private static void assertO3DataConsistency(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        // create third table, which will contain both X and 1AM
        engine.execute("create atomic table y as (x union all append)", sqlExecutionContext);
        engine.execute("insert atomic into x select * from append", sqlExecutionContext);

        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                sqlExecutionContext,
                null,
                null
        );

        engine.releaseAllReaders();

        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                sqlExecutionContext,
                null,
                null
        );
    }

    private static void assertXCountAndMax(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            CharSequence expectedMaxTimestamp
    ) throws SqlException {
        assertXCount(compiler, sqlExecutionContext);
        assertMaxTimestamp(engine, expectedMaxTimestamp);
    }

    private static void assertXCountAndMax(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            CharSequence expectedCount,
            CharSequence expectedMaxTimestamp
    ) throws SqlException {
        sink2.clear();
        sink2.put(expectedCount);
        assertXCount(compiler, sqlExecutionContext);
        assertMaxTimestamp(engine, expectedMaxTimestamp);
    }

    private static FilesFacade failOnOpenRW(String fileName, int count) {
        AtomicInteger counter = new AtomicInteger(count);
        return new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, fileName) && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
    }

    @NotNull
    private static String prepareCountAndMaxTimestampSinks(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                "select count() from x",
                sink2
        );

        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                "select max(ts) from x",
                sink
        );

        return Chars.toString(sink);
    }

    private static void putRndStr(Rnd rnd, RecordMetadata metadata, TableWriter.Row r, int col, int len, Utf8StringSink sink) {
        switch (metadata.getColumnType(col)) {
            case ColumnType.STRING:
                r.putStr(col, rnd.nextChars(len));
                break;
            case ColumnType.VARCHAR:
                sink.clear();
                rnd.nextUtf8Str(len, sink);
                r.putVarchar(col, sink);
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static void testAllocateFailsAtO3OpenColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        // create table with roughly 2AM data
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + "  ts" +
                        " from long_sequence(500)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        final String expected = replaceTimestampSuffix1("""
                i\tj\tts\tv
                1\t4689592037643856\t1970-01-06T18:53:20.000000Z\tnull
                2\t4729996258992366\t1970-01-06T18:55:00.000000Z\tnull
                3\t7746536061816329025\t1970-01-06T18:56:40.000000Z\tnull
                4\t-6945921502384501475\t1970-01-06T18:58:20.000000Z\tnull
                5\t8260188555232587029\t1970-01-06T19:00:00.000000Z\tnull
                6\t8920866532787660373\t1970-01-06T19:01:40.000000Z\tnull
                7\t-7611843578141082998\t1970-01-06T19:03:20.000000Z\tnull
                8\t-5354193255228091881\t1970-01-06T19:05:00.000000Z\tnull
                9\t-2653407051020864006\t1970-01-06T19:06:40.000000Z\tnull
                10\t-1675638984090602536\t1970-01-06T19:08:20.000000Z\tnull
                11\t8754899401270281932\t1970-01-06T19:10:00.000000Z\tnull
                12\t3614738589890112276\t1970-01-06T19:11:40.000000Z\tnull
                13\t7513930126251977934\t1970-01-06T19:13:20.000000Z\tnull
                14\t-7489826605295361807\t1970-01-06T19:15:00.000000Z\tnull
                15\t-4094902006239100839\t1970-01-06T19:16:40.000000Z\tnull
                16\t-4474835130332302712\t1970-01-06T19:18:20.000000Z\tnull
                17\t-6943924477733600060\t1970-01-06T19:20:00.000000Z\tnull
                18\t8173439391403617681\t1970-01-06T19:21:40.000000Z\tnull
                19\t3394168647660478011\t1970-01-06T19:23:20.000000Z\tnull
                20\t5408639942391651698\t1970-01-06T19:25:00.000000Z\tnull
                21\t7953532976996720859\t1970-01-06T19:26:40.000000Z\tnull
                22\t-8968886490993754893\t1970-01-06T19:28:20.000000Z\tnull
                23\t6236292340460979716\t1970-01-06T19:30:00.000000Z\tnull
                24\t8336855953317473051\t1970-01-06T19:31:40.000000Z\tnull
                25\t-3985256597569472057\t1970-01-06T19:33:20.000000Z\tnull
                26\t-8284534269888369016\t1970-01-06T19:35:00.000000Z\tnull
                27\t9116006198143953886\t1970-01-06T19:36:40.000000Z\tnull
                28\t-6856503215590263904\t1970-01-06T19:38:20.000000Z\tnull
                29\t-8671107786057422727\t1970-01-06T19:40:00.000000Z\tnull
                30\t5539350449504785212\t1970-01-06T19:41:40.000000Z\tnull
                31\t4086802474270249591\t1970-01-06T19:43:20.000000Z\tnull
                32\t7039584373105579285\t1970-01-06T19:45:00.000000Z\tnull
                33\t-4485747798769957016\t1970-01-06T19:46:40.000000Z\tnull
                34\t-4100339045953973663\t1970-01-06T19:48:20.000000Z\tnull
                35\t-7475784581806461658\t1970-01-06T19:50:00.000000Z\tnull
                36\t5926373848160552879\t1970-01-06T19:51:40.000000Z\tnull
                37\t375856366519011353\t1970-01-06T19:53:20.000000Z\tnull
                38\t2811900023577169860\t1970-01-06T19:55:00.000000Z\tnull
                39\t8416773233910814357\t1970-01-06T19:56:40.000000Z\tnull
                40\t6600081143067978388\t1970-01-06T19:58:20.000000Z\tnull
                41\t8349358446893356086\t1970-01-06T20:00:00.000000Z\tnull
                42\t7700030475747712339\t1970-01-06T20:01:40.000000Z\tnull
                43\t8000176386900538697\t1970-01-06T20:03:20.000000Z\tnull
                44\t-8479285918156402508\t1970-01-06T20:05:00.000000Z\tnull
                45\t3958193676455060057\t1970-01-06T20:06:40.000000Z\tnull
                46\t9194293009132827518\t1970-01-06T20:08:20.000000Z\tnull
                47\t7759636733976435003\t1970-01-06T20:10:00.000000Z\tnull
                48\t8942747579519338504\t1970-01-06T20:11:40.000000Z\tnull
                49\t-7166640824903897951\t1970-01-06T20:13:20.000000Z\tnull
                50\t7199909180655756830\t1970-01-06T20:15:00.000000Z\tnull
                51\t-8889930662239044040\t1970-01-06T20:16:40.000000Z\tnull
                52\t-4442449726822927731\t1970-01-06T20:18:20.000000Z\tnull
                53\t-3546540271125917157\t1970-01-06T20:20:00.000000Z\tnull
                54\t6404066507400987550\t1970-01-06T20:21:40.000000Z\tnull
                55\t6854658259142399220\t1970-01-06T20:23:20.000000Z\tnull
                56\t-4842723177835140152\t1970-01-06T20:25:00.000000Z\tnull
                57\t-5986859522579472839\t1970-01-06T20:26:40.000000Z\tnull
                58\t8573481508564499209\t1970-01-06T20:28:20.000000Z\tnull
                59\t5476540218465058302\t1970-01-06T20:30:00.000000Z\tnull
                60\t7709707078566863064\t1970-01-06T20:31:40.000000Z\tnull
                61\t6270672455202306717\t1970-01-06T20:33:20.000000Z\tnull
                62\t-8480005421611953360\t1970-01-06T20:35:00.000000Z\tnull
                63\t-8955092533521658248\t1970-01-06T20:36:40.000000Z\tnull
                64\t1205595184115760694\t1970-01-06T20:38:20.000000Z\tnull
                65\t3619114107112892010\t1970-01-06T20:40:00.000000Z\tnull
                66\t8325936937764905778\t1970-01-06T20:41:40.000000Z\tnull
                67\t-7723703968879725602\t1970-01-06T20:43:20.000000Z\tnull
                68\t-6186964045554120476\t1970-01-06T20:45:00.000000Z\tnull
                69\t-4986232506486815364\t1970-01-06T20:46:40.000000Z\tnull
                70\t-7885528361265853230\t1970-01-06T20:48:20.000000Z\tnull
                71\t-6794405451419334859\t1970-01-06T20:50:00.000000Z\tnull
                72\t-6253307669002054137\t1970-01-06T20:51:40.000000Z\tnull
                73\t6820495939660535106\t1970-01-06T20:53:20.000000Z\tnull
                74\t3152466304308949756\t1970-01-06T20:55:00.000000Z\tnull
                75\t3705833798044144433\t1970-01-06T20:56:40.000000Z\tnull
                76\t6993925225312419449\t1970-01-06T20:58:20.000000Z\tnull
                77\t7304706257487484767\t1970-01-06T21:00:00.000000Z\tnull
                78\t6179044593759294347\t1970-01-06T21:01:40.000000Z\tnull
                79\t4238042693748641409\t1970-01-06T21:03:20.000000Z\tnull
                80\t5334238747895433003\t1970-01-06T21:05:00.000000Z\tnull
                81\t-7439145921574737517\t1970-01-06T21:06:40.000000Z\tnull
                82\t-7153335833712179123\t1970-01-06T21:08:20.000000Z\tnull
                83\t7392877322819819290\t1970-01-06T21:10:00.000000Z\tnull
                84\t5536695302686527374\t1970-01-06T21:11:40.000000Z\tnull
                85\t-8811278461560712840\t1970-01-06T21:13:20.000000Z\tnull
                86\t-4371031944620334155\t1970-01-06T21:15:00.000000Z\tnull
                87\t-5228148654835984711\t1970-01-06T21:16:40.000000Z\tnull
                88\t6953604195888525841\t1970-01-06T21:18:20.000000Z\tnull
                89\t7585187984144261203\t1970-01-06T21:20:00.000000Z\tnull
                90\t-6919361415374675248\t1970-01-06T21:21:40.000000Z\tnull
                91\t5942480340792044027\t1970-01-06T21:23:20.000000Z\tnull
                92\t2968650253814730084\t1970-01-06T21:25:00.000000Z\tnull
                93\t9036423629723776443\t1970-01-06T21:26:40.000000Z\tnull
                94\t-7316123607359392486\t1970-01-06T21:28:20.000000Z\tnull
                95\t7641144929328646356\t1970-01-06T21:30:00.000000Z\tnull
                96\t8171230234890248944\t1970-01-06T21:31:40.000000Z\tnull
                97\t-7689224645273531603\t1970-01-06T21:33:20.000000Z\tnull
                98\t-7611030538224290496\t1970-01-06T21:35:00.000000Z\tnull
                99\t-7266580375914176030\t1970-01-06T21:36:40.000000Z\tnull
                100\t-5233802075754153909\t1970-01-06T21:38:20.000000Z\tnull
                101\t-4692986177227268943\t1970-01-06T21:40:00.000000Z\tnull
                102\t7528475600160271422\t1970-01-06T21:41:40.000000Z\tnull
                103\t6473208488991371747\t1970-01-06T21:43:20.000000Z\tnull
                104\t-4091897709796604687\t1970-01-06T21:45:00.000000Z\tnull
                105\t-3107239868490395663\t1970-01-06T21:46:40.000000Z\tnull
                106\t7522482991756933150\t1970-01-06T21:48:20.000000Z\tnull
                107\t5866052386674669514\t1970-01-06T21:50:00.000000Z\tnull
                108\t8831607763082315932\t1970-01-06T21:51:40.000000Z\tnull
                109\t3518554007419864093\t1970-01-06T21:53:20.000000Z\tnull
                110\t571924429013198086\t1970-01-06T21:55:00.000000Z\tnull
                111\t5271904137583983788\t1970-01-06T21:56:40.000000Z\tnull
                112\t-6487422186320825289\t1970-01-06T21:58:20.000000Z\tnull
                113\t-5935729153136649272\t1970-01-06T22:00:00.000000Z\tnull
                114\t-5028301966399563827\t1970-01-06T22:01:40.000000Z\tnull
                115\t-4608960730952244094\t1970-01-06T22:03:20.000000Z\tnull
                116\t-7387846268299105911\t1970-01-06T22:05:00.000000Z\tnull
                117\t7848851757452822827\t1970-01-06T22:06:40.000000Z\tnull
                118\t6373284943859989837\t1970-01-06T22:08:20.000000Z\tnull
                119\t4014104627539596639\t1970-01-06T22:10:00.000000Z\tnull
                120\t5867661438830308598\t1970-01-06T22:11:40.000000Z\tnull
                121\t-6365568807668711866\t1970-01-06T22:13:20.000000Z\tnull
                122\t-3214230645884399728\t1970-01-06T22:15:00.000000Z\tnull
                123\t9029468389542245059\t1970-01-06T22:16:40.000000Z\tnull
                124\t4349785000461902003\t1970-01-06T22:18:20.000000Z\tnull
                125\t-8081265393416742311\t1970-01-06T22:20:00.000000Z\tnull
                126\t-8663526666273545842\t1970-01-06T22:21:40.000000Z\tnull
                127\t7122109662042058469\t1970-01-06T22:23:20.000000Z\tnull
                128\t6079275973105085025\t1970-01-06T22:25:00.000000Z\tnull
                129\t8155981915549526575\t1970-01-06T22:26:40.000000Z\tnull
                130\t-4908948886680892316\t1970-01-06T22:28:20.000000Z\tnull
                131\t8587391969565958670\t1970-01-06T22:30:00.000000Z\tnull
                132\t4167328623064065836\t1970-01-06T22:31:40.000000Z\tnull
                133\t-8906871108655466881\t1970-01-06T22:33:20.000000Z\tnull
                134\t-5512653573876168745\t1970-01-06T22:35:00.000000Z\tnull
                135\t-6161552193869048721\t1970-01-06T22:36:40.000000Z\tnull
                136\t-8425379692364264520\t1970-01-06T22:38:20.000000Z\tnull
                137\t9131882544462008265\t1970-01-06T22:40:00.000000Z\tnull
                138\t-6626590012581323602\t1970-01-06T22:41:40.000000Z\tnull
                139\t8654368763944235816\t1970-01-06T22:43:20.000000Z\tnull
                140\t1504966027220213191\t1970-01-06T22:45:00.000000Z\tnull
                141\t2474001847338644868\t1970-01-06T22:46:40.000000Z\tnull
                142\t8977823376202838087\t1970-01-06T22:48:20.000000Z\tnull
                143\t-7995393784734742820\t1970-01-06T22:50:00.000000Z\tnull
                144\t-6190031864817509934\t1970-01-06T22:51:40.000000Z\tnull
                145\t8702525427024484485\t1970-01-06T22:53:20.000000Z\tnull
                146\t2762535352290012031\t1970-01-06T22:55:00.000000Z\tnull
                147\t-8408704077728333147\t1970-01-06T22:56:40.000000Z\tnull
                148\t-4116381468144676168\t1970-01-06T22:58:20.000000Z\tnull
                149\t8611582118025429627\t1970-01-06T23:00:00.000000Z\tnull
                150\t2235053888582262602\t1970-01-06T23:01:40.000000Z\tnull
                151\t915906628308577949\t1970-01-06T23:03:20.000000Z\tnull
                152\t1761725072747471430\t1970-01-06T23:05:00.000000Z\tnull
                153\t5407260416602246268\t1970-01-06T23:06:40.000000Z\tnull
                154\t-5710210982977201267\t1970-01-06T23:08:20.000000Z\tnull
                155\t-9128506055317587235\t1970-01-06T23:10:00.000000Z\tnull
                156\t9063592617902736531\t1970-01-06T23:11:40.000000Z\tnull
                157\t-2406077911451945242\t1970-01-06T23:13:20.000000Z\tnull
                158\t-6003256558990918704\t1970-01-06T23:15:00.000000Z\tnull
                159\t6623443272143014835\t1970-01-06T23:16:40.000000Z\tnull
                160\t-8082754367165748693\t1970-01-06T23:18:20.000000Z\tnull
                161\t-1438352846894825721\t1970-01-06T23:20:00.000000Z\tnull
                162\t-5439556746612026472\t1970-01-06T23:21:40.000000Z\tnull
                163\t-7256514778130150964\t1970-01-06T23:23:20.000000Z\tnull
                164\t-2605516556381756042\t1970-01-06T23:25:00.000000Z\tnull
                165\t-7103100524321179064\t1970-01-06T23:26:40.000000Z\tnull
                166\t9144172287200792483\t1970-01-06T23:28:20.000000Z\tnull
                167\t-5024542231726589509\t1970-01-06T23:30:00.000000Z\tnull
                168\t-2768987637252864412\t1970-01-06T23:31:40.000000Z\tnull
                169\t-3289070757475856942\t1970-01-06T23:33:20.000000Z\tnull
                170\t7277991313017866925\t1970-01-06T23:35:00.000000Z\tnull
                171\t6574958665733670985\t1970-01-06T23:36:40.000000Z\tnull
                172\t-5817309269683380708\t1970-01-06T23:38:20.000000Z\tnull
                173\t-8910603140262731534\t1970-01-06T23:40:00.000000Z\tnull
                174\t7035958104135945276\t1970-01-06T23:41:40.000000Z\tnull
                175\t9169223215810156269\t1970-01-06T23:43:20.000000Z\tnull
                176\t7973684666911773753\t1970-01-06T23:45:00.000000Z\tnull
                177\t9143800334706665900\t1970-01-06T23:46:40.000000Z\tnull
                178\t8907283191913183400\t1970-01-06T23:48:20.000000Z\tnull
                179\t7505077128008208443\t1970-01-06T23:50:00.000000Z\tnull
                180\t6624299878707135910\t1970-01-06T23:51:40.000000Z\tnull
                181\t4990844051702733276\t1970-01-06T23:53:20.000000Z\tnull
                182\t3446015290144635451\t1970-01-06T23:55:00.000000Z\tnull
                183\t3393210801760647293\t1970-01-06T23:56:40.000000Z\tnull
                184\t-8193596495481093333\t1970-01-06T23:58:20.000000Z\tnull
                10\t3500000\t1970-01-06T23:58:20.000000Z\t10.2
                185\t9130722816060153827\t1970-01-07T00:00:00.000000Z\tnull
                186\t4385246274849842834\t1970-01-07T00:01:40.000000Z\tnull
                187\t-7709579215942154242\t1970-01-07T00:03:20.000000Z\tnull
                188\t-6912707344119330199\t1970-01-07T00:05:00.000000Z\tnull
                189\t-6265628144430971336\t1970-01-07T00:06:40.000000Z\tnull
                190\t-2656704586686189855\t1970-01-07T00:08:20.000000Z\tnull
                191\t-5852887087189258121\t1970-01-07T00:10:00.000000Z\tnull
                192\t-5616524194087992934\t1970-01-07T00:11:40.000000Z\tnull
                193\t8889492928577876455\t1970-01-07T00:13:20.000000Z\tnull
                194\t5398991075259361292\t1970-01-07T00:15:00.000000Z\tnull
                195\t-4947578609540920695\t1970-01-07T00:16:40.000000Z\tnull
                196\t-1550912036246807020\t1970-01-07T00:18:20.000000Z\tnull
                197\t-3279062567400130728\t1970-01-07T00:20:00.000000Z\tnull
                198\t-6187389706549636253\t1970-01-07T00:21:40.000000Z\tnull
                199\t-5097437605148611401\t1970-01-07T00:23:20.000000Z\tnull
                200\t-9053195266501182270\t1970-01-07T00:25:00.000000Z\tnull
                201\t1064753200933634719\t1970-01-07T00:26:40.000000Z\tnull
                202\t2155318342410845737\t1970-01-07T00:28:20.000000Z\tnull
                203\t4437331957970287246\t1970-01-07T00:30:00.000000Z\tnull
                204\t8152044974329490473\t1970-01-07T00:31:40.000000Z\tnull
                205\t6108846371653428062\t1970-01-07T00:33:20.000000Z\tnull
                206\t4641238585508069993\t1970-01-07T00:35:00.000000Z\tnull
                207\t-5315599072928175674\t1970-01-07T00:36:40.000000Z\tnull
                208\t-8755128364143858197\t1970-01-07T00:38:20.000000Z\tnull
                209\t5294917053935522538\t1970-01-07T00:40:00.000000Z\tnull
                210\t5824745791075827139\t1970-01-07T00:41:40.000000Z\tnull
                211\t-8757007522346766135\t1970-01-07T00:43:20.000000Z\tnull
                212\t-1620198143795539853\t1970-01-07T00:45:00.000000Z\tnull
                213\t9161691782935400339\t1970-01-07T00:46:40.000000Z\tnull
                214\t5703149806881083206\t1970-01-07T00:48:20.000000Z\tnull
                215\t-6071768268784020226\t1970-01-07T00:50:00.000000Z\tnull
                216\t-5336116148746766654\t1970-01-07T00:51:40.000000Z\tnull
                217\t8009040003356908243\t1970-01-07T00:53:20.000000Z\tnull
                218\t5292387498953709416\t1970-01-07T00:55:00.000000Z\tnull
                219\t-6786804316219531143\t1970-01-07T00:56:40.000000Z\tnull
                220\t-1798101751056570485\t1970-01-07T00:58:20.000000Z\tnull
                221\t-8323443786521150653\t1970-01-07T01:00:00.000000Z\tnull
                222\t-7714378722470181347\t1970-01-07T01:01:40.000000Z\tnull
                223\t-2888119746454814889\t1970-01-07T01:03:20.000000Z\tnull
                224\t-8546113611224784332\t1970-01-07T01:05:00.000000Z\tnull
                225\t7158971986470055172\t1970-01-07T01:06:40.000000Z\tnull
                226\t5746626297238459939\t1970-01-07T01:08:20.000000Z\tnull
                227\t7574443524652611981\t1970-01-07T01:10:00.000000Z\tnull
                228\t-8994301462266164776\t1970-01-07T01:11:40.000000Z\tnull
                229\t4099611147050818391\t1970-01-07T01:13:20.000000Z\tnull
                230\t-9147563299122452591\t1970-01-07T01:15:00.000000Z\tnull
                231\t-7400476385601852536\t1970-01-07T01:16:40.000000Z\tnull
                232\t-8642609626818201048\t1970-01-07T01:18:20.000000Z\tnull
                233\t-2000273984235276379\t1970-01-07T01:20:00.000000Z\tnull
                234\t-166300099372695016\t1970-01-07T01:21:40.000000Z\tnull
                235\t-3416748419425937005\t1970-01-07T01:23:20.000000Z\tnull
                236\t6351664568801157821\t1970-01-07T01:25:00.000000Z\tnull
                237\t3084117448873356811\t1970-01-07T01:26:40.000000Z\tnull
                238\t6601850686822460257\t1970-01-07T01:28:20.000000Z\tnull
                239\t7759595275644638709\t1970-01-07T01:30:00.000000Z\tnull
                240\t4360855047041000285\t1970-01-07T01:31:40.000000Z\tnull
                241\t6087087705757854416\t1970-01-07T01:33:20.000000Z\tnull
                242\t-5103414617212558357\t1970-01-07T01:35:00.000000Z\tnull
                243\t8574802735490373479\t1970-01-07T01:36:40.000000Z\tnull
                244\t2387397055355257412\t1970-01-07T01:38:20.000000Z\tnull
                245\t8072168822566640807\t1970-01-07T01:40:00.000000Z\tnull
                246\t-3293392739929464726\t1970-01-07T01:41:40.000000Z\tnull
                247\t-8749723816463910031\t1970-01-07T01:43:20.000000Z\tnull
                248\t6127579245089953588\t1970-01-07T01:45:00.000000Z\tnull
                249\t-6883412613642983200\t1970-01-07T01:46:40.000000Z\tnull
                250\t-7153690499922882896\t1970-01-07T01:48:20.000000Z\tnull
                251\t7107508275327837161\t1970-01-07T01:50:00.000000Z\tnull
                252\t-8260644133007073640\t1970-01-07T01:51:40.000000Z\tnull
                253\t-7336930007738575369\t1970-01-07T01:53:20.000000Z\tnull
                254\t5552835357100545895\t1970-01-07T01:55:00.000000Z\tnull
                255\t4534912711595148130\t1970-01-07T01:56:40.000000Z\tnull
                256\t-7228011205059401944\t1970-01-07T01:58:20.000000Z\tnull
                257\t-6703401424236463520\t1970-01-07T02:00:00.000000Z\tnull
                258\t-8857660828600848720\t1970-01-07T02:01:40.000000Z\tnull
                259\t-3105499275013799956\t1970-01-07T02:03:20.000000Z\tnull
                260\t-8371487291073160693\t1970-01-07T02:05:00.000000Z\tnull
                261\t2383285963471887250\t1970-01-07T02:06:40.000000Z\tnull
                262\t1488156692375549016\t1970-01-07T02:08:20.000000Z\tnull
                263\t2151565237758036093\t1970-01-07T02:10:00.000000Z\tnull
                264\t4107109535030235684\t1970-01-07T02:11:40.000000Z\tnull
                265\t-8534688874718947140\t1970-01-07T02:13:20.000000Z\tnull
                266\t-3491277789316049618\t1970-01-07T02:15:00.000000Z\tnull
                267\t8815523022464325728\t1970-01-07T02:16:40.000000Z\tnull
                268\t4959459375462458218\t1970-01-07T02:18:20.000000Z\tnull
                269\t7037372650941669660\t1970-01-07T02:20:00.000000Z\tnull
                270\t4502522085684189707\t1970-01-07T02:21:40.000000Z\tnull
                271\t8850915006829016608\t1970-01-07T02:23:20.000000Z\tnull
                272\t-8095658968635787358\t1970-01-07T02:25:00.000000Z\tnull
                273\t-6716055087713781882\t1970-01-07T02:26:40.000000Z\tnull
                274\t-8425895280081943671\t1970-01-07T02:28:20.000000Z\tnull
                275\t8880550034995457591\t1970-01-07T02:30:00.000000Z\tnull
                276\t8464194176491581201\t1970-01-07T02:31:40.000000Z\tnull
                277\t6056145309392106540\t1970-01-07T02:33:20.000000Z\tnull
                278\t6121305147479698964\t1970-01-07T02:35:00.000000Z\tnull
                279\t2282781332678491916\t1970-01-07T02:36:40.000000Z\tnull
                280\t3527911398466283309\t1970-01-07T02:38:20.000000Z\tnull
                281\t6176277818569291296\t1970-01-07T02:40:00.000000Z\tnull
                282\t-8656750634622759804\t1970-01-07T02:41:40.000000Z\tnull
                283\t7058145725055366226\t1970-01-07T02:43:20.000000Z\tnull
                284\t-8849142892360165671\t1970-01-07T02:45:00.000000Z\tnull
                285\t-1134031357796740497\t1970-01-07T02:46:40.000000Z\tnull
                286\t-6782883555378798844\t1970-01-07T02:48:20.000000Z\tnull
                287\t6405448934035934123\t1970-01-07T02:50:00.000000Z\tnull
                288\t-8425483167065397721\t1970-01-07T02:51:40.000000Z\tnull
                289\t-8719797095546978745\t1970-01-07T02:53:20.000000Z\tnull
                290\t9089874911309539983\t1970-01-07T02:55:00.000000Z\tnull
                291\t-7202923278768687325\t1970-01-07T02:56:40.000000Z\tnull
                292\t-6571406865336879041\t1970-01-07T02:58:20.000000Z\tnull
                293\t-3396992238702724434\t1970-01-07T03:00:00.000000Z\tnull
                294\t-8205259083320287108\t1970-01-07T03:01:40.000000Z\tnull
                295\t-9029407334801459809\t1970-01-07T03:03:20.000000Z\tnull
                296\t-4058426794463997577\t1970-01-07T03:05:00.000000Z\tnull
                297\t6517485707736381444\t1970-01-07T03:06:40.000000Z\tnull
                298\t579094601177353961\t1970-01-07T03:08:20.000000Z\tnull
                299\t750145151786158348\t1970-01-07T03:10:00.000000Z\tnull
                300\t5048272224871876586\t1970-01-07T03:11:40.000000Z\tnull
                301\t-4547802916868961458\t1970-01-07T03:13:20.000000Z\tnull
                302\t-1832315370633201942\t1970-01-07T03:15:00.000000Z\tnull
                303\t-8888027247206813045\t1970-01-07T03:16:40.000000Z\tnull
                304\t3352215237270276085\t1970-01-07T03:18:20.000000Z\tnull
                305\t6937484962759020303\t1970-01-07T03:20:00.000000Z\tnull
                306\t7797019568426198829\t1970-01-07T03:21:40.000000Z\tnull
                307\t2691623916208307891\t1970-01-07T03:23:20.000000Z\tnull
                308\t6184401532241477140\t1970-01-07T03:25:00.000000Z\tnull
                309\t-8653777305694768077\t1970-01-07T03:26:40.000000Z\tnull
                310\t8756159220596318848\t1970-01-07T03:28:20.000000Z\tnull
                311\t4579251508938058953\t1970-01-07T03:30:00.000000Z\tnull
                312\t425369166370563563\t1970-01-07T03:31:40.000000Z\tnull
                313\t5478379480606573987\t1970-01-07T03:33:20.000000Z\tnull
                314\t-4284648096271470489\t1970-01-07T03:35:00.000000Z\tnull
                315\t-1741953200710332294\t1970-01-07T03:36:40.000000Z\tnull
                316\t-4450383397583441126\t1970-01-07T03:38:20.000000Z\tnull
                317\t8984932460293088377\t1970-01-07T03:40:00.000000Z\tnull
                318\t9058067501760744164\t1970-01-07T03:41:40.000000Z\tnull
                319\t8490886945852172597\t1970-01-07T03:43:20.000000Z\tnull
                320\t-8841102831894340636\t1970-01-07T03:45:00.000000Z\tnull
                321\t8503557900983561786\t1970-01-07T03:46:40.000000Z\tnull
                322\t1508637934261574620\t1970-01-07T03:48:20.000000Z\tnull
                323\t663602980874300508\t1970-01-07T03:50:00.000000Z\tnull
                324\t788901813531436389\t1970-01-07T03:51:40.000000Z\tnull
                325\t6793615437970356479\t1970-01-07T03:53:20.000000Z\tnull
                326\t6380499796471875623\t1970-01-07T03:55:00.000000Z\tnull
                327\t2006083905706813287\t1970-01-07T03:56:40.000000Z\tnull
                328\t5513479607887040119\t1970-01-07T03:58:20.000000Z\tnull
                329\t5343275067392229138\t1970-01-07T04:00:00.000000Z\tnull
                330\t4527121849171257172\t1970-01-07T04:01:40.000000Z\tnull
                331\t4847320715984654162\t1970-01-07T04:03:20.000000Z\tnull
                332\t7092246624397344208\t1970-01-07T04:05:00.000000Z\tnull
                333\t6445007901796870697\t1970-01-07T04:06:40.000000Z\tnull
                334\t1669226447966988582\t1970-01-07T04:08:20.000000Z\tnull
                335\t5953039264407551685\t1970-01-07T04:10:00.000000Z\tnull
                336\t7592940205308166826\t1970-01-07T04:11:40.000000Z\tnull
                337\t-7414829143044491558\t1970-01-07T04:13:20.000000Z\tnull
                338\t-6819946977256689384\t1970-01-07T04:15:00.000000Z\tnull
                339\t-7186310556474199346\t1970-01-07T04:16:40.000000Z\tnull
                340\t-8814330552804983713\t1970-01-07T04:18:20.000000Z\tnull
                341\t-8960406850507339854\t1970-01-07T04:20:00.000000Z\tnull
                342\t-8793423647053878901\t1970-01-07T04:21:40.000000Z\tnull
                343\t5941398229034918748\t1970-01-07T04:23:20.000000Z\tnull
                344\t5980197440602572628\t1970-01-07T04:25:00.000000Z\tnull
                345\t2106240318003963024\t1970-01-07T04:26:40.000000Z\tnull
                346\t9200214878918264613\t1970-01-07T04:28:20.000000Z\tnull
                347\t-8211260649542902334\t1970-01-07T04:30:00.000000Z\tnull
                348\t5068939738525201696\t1970-01-07T04:31:40.000000Z\tnull
                349\t3820631780839257855\t1970-01-07T04:33:20.000000Z\tnull
                350\t-9219078548506735248\t1970-01-07T04:35:00.000000Z\tnull
                351\t8737100589707440954\t1970-01-07T04:36:40.000000Z\tnull
                352\t9044897286885345735\t1970-01-07T04:38:20.000000Z\tnull
                353\t-7381322665528955510\t1970-01-07T04:40:00.000000Z\tnull
                354\t6174532314769579955\t1970-01-07T04:41:40.000000Z\tnull
                355\t-8930904012891908076\t1970-01-07T04:43:20.000000Z\tnull
                356\t-6765703075406647091\t1970-01-07T04:45:00.000000Z\tnull
                357\t8810110521992874823\t1970-01-07T04:46:40.000000Z\tnull
                358\t7570866088271751947\t1970-01-07T04:48:20.000000Z\tnull
                359\t-7274175842748412916\t1970-01-07T04:50:00.000000Z\tnull
                360\t6753412894015940665\t1970-01-07T04:51:40.000000Z\tnull
                361\t2106204205501581842\t1970-01-07T04:53:20.000000Z\tnull
                362\t2307279172463257591\t1970-01-07T04:55:00.000000Z\tnull
                363\t812677186520066053\t1970-01-07T04:56:40.000000Z\tnull
                364\t4621844195437841424\t1970-01-07T04:58:20.000000Z\tnull
                365\t-7724577649125721868\t1970-01-07T05:00:00.000000Z\tnull
                366\t-7171265782561774995\t1970-01-07T05:01:40.000000Z\tnull
                367\t6966461743143051249\t1970-01-07T05:03:20.000000Z\tnull
                368\t7629109032541741027\t1970-01-07T05:05:00.000000Z\tnull
                369\t-7212878484370155026\t1970-01-07T05:06:40.000000Z\tnull
                370\t5963775257114848600\t1970-01-07T05:08:20.000000Z\tnull
                371\t3771494396743411509\t1970-01-07T05:10:00.000000Z\tnull
                372\t8798087869168938593\t1970-01-07T05:11:40.000000Z\tnull
                373\t8984775562394712402\t1970-01-07T05:13:20.000000Z\tnull
                374\t3792128300541831563\t1970-01-07T05:15:00.000000Z\tnull
                375\t7101009950667960843\t1970-01-07T05:16:40.000000Z\tnull
                376\t-6460532424840798061\t1970-01-07T05:18:20.000000Z\tnull
                377\t-5044078842288373275\t1970-01-07T05:20:00.000000Z\tnull
                378\t-3323322733858034601\t1970-01-07T05:21:40.000000Z\tnull
                379\t-7665470829783532891\t1970-01-07T05:23:20.000000Z\tnull
                380\t6738282533394287579\t1970-01-07T05:25:00.000000Z\tnull
                381\t6146164804821006241\t1970-01-07T05:26:40.000000Z\tnull
                382\t-7398902448022205322\t1970-01-07T05:28:20.000000Z\tnull
                383\t-2471456524133707236\t1970-01-07T05:30:00.000000Z\tnull
                384\t9041413988802359580\t1970-01-07T05:31:40.000000Z\tnull
                385\t5922689877598858022\t1970-01-07T05:33:20.000000Z\tnull
                386\t5168847330186110459\t1970-01-07T05:35:00.000000Z\tnull
                387\t8987698540484981038\t1970-01-07T05:36:40.000000Z\tnull
                388\t-7228768303272348606\t1970-01-07T05:38:20.000000Z\tnull
                389\t5700115585432451578\t1970-01-07T05:40:00.000000Z\tnull
                390\t7879490594801163253\t1970-01-07T05:41:40.000000Z\tnull
                391\t-5432682396344996498\t1970-01-07T05:43:20.000000Z\tnull
                392\t-3463832009795858033\t1970-01-07T05:45:00.000000Z\tnull
                393\t-8555544472620366464\t1970-01-07T05:46:40.000000Z\tnull
                394\t5205180235397887203\t1970-01-07T05:48:20.000000Z\tnull
                395\t2364286642781155412\t1970-01-07T05:50:00.000000Z\tnull
                396\t5494476067484139960\t1970-01-07T05:51:40.000000Z\tnull
                397\t7357244054212773895\t1970-01-07T05:53:20.000000Z\tnull
                398\t-8506266080452644687\t1970-01-07T05:55:00.000000Z\tnull
                399\t-1905597357123382478\t1970-01-07T05:56:40.000000Z\tnull
                400\t-5496131157726548905\t1970-01-07T05:58:20.000000Z\tnull
                401\t-7474351066761292033\t1970-01-07T06:00:00.000000Z\tnull
                402\t-6482694999745905510\t1970-01-07T06:01:40.000000Z\tnull
                403\t-8026283444976158481\t1970-01-07T06:03:20.000000Z\tnull
                404\t5804262091839668360\t1970-01-07T06:05:00.000000Z\tnull
                405\t7297601774924170699\t1970-01-07T06:06:40.000000Z\tnull
                406\t-4229502740666959541\t1970-01-07T06:08:20.000000Z\tnull
                407\t8842585385650675361\t1970-01-07T06:10:00.000000Z\tnull
                408\t7046578844650327247\t1970-01-07T06:11:40.000000Z\tnull
                409\t8070302167413932495\t1970-01-07T06:13:20.000000Z\tnull
                410\t4480750444572460865\t1970-01-07T06:15:00.000000Z\tnull
                411\t6205872689407104125\t1970-01-07T06:16:40.000000Z\tnull
                412\t9029088579359707814\t1970-01-07T06:18:20.000000Z\tnull
                413\t-8737543979347648559\t1970-01-07T06:20:00.000000Z\tnull
                414\t-6522954364450041026\t1970-01-07T06:21:40.000000Z\tnull
                415\t-6221841196965409356\t1970-01-07T06:23:20.000000Z\tnull
                416\t6484482332827923784\t1970-01-07T06:25:00.000000Z\tnull
                417\t7036584259400395476\t1970-01-07T06:26:40.000000Z\tnull
                418\t-6795628328806886847\t1970-01-07T06:28:20.000000Z\tnull
                419\t7576110962745644701\t1970-01-07T06:30:00.000000Z\tnull
                420\t8537223925650740475\t1970-01-07T06:31:40.000000Z\tnull
                421\t8737613628813682249\t1970-01-07T06:33:20.000000Z\tnull
                422\t4598876523645326656\t1970-01-07T06:35:00.000000Z\tnull
                423\t6436453824498875972\t1970-01-07T06:36:40.000000Z\tnull
                424\t4634177780953489481\t1970-01-07T06:38:20.000000Z\tnull
                425\t6390608559661380246\t1970-01-07T06:40:00.000000Z\tnull
                426\t8282637062702131151\t1970-01-07T06:41:40.000000Z\tnull
                427\t5360746485515325739\t1970-01-07T06:43:20.000000Z\tnull
                428\t-7910490643543561037\t1970-01-07T06:45:00.000000Z\tnull
                429\t8321277364671502705\t1970-01-07T06:46:40.000000Z\tnull
                430\t3987576220753016999\t1970-01-07T06:48:20.000000Z\tnull
                431\t3944678179613436885\t1970-01-07T06:50:00.000000Z\tnull
                432\t6153381060986313135\t1970-01-07T06:51:40.000000Z\tnull
                433\t8278953979466939153\t1970-01-07T06:53:20.000000Z\tnull
                434\t6831200789490300310\t1970-01-07T06:55:00.000000Z\tnull
                435\t5175638765020222775\t1970-01-07T06:56:40.000000Z\tnull
                436\t7090323083171574792\t1970-01-07T06:58:20.000000Z\tnull
                437\t6598154038796950493\t1970-01-07T07:00:00.000000Z\tnull
                438\t6418970788912980120\t1970-01-07T07:01:40.000000Z\tnull
                439\t-7518902569991053841\t1970-01-07T07:03:20.000000Z\tnull
                440\t6083279743811422804\t1970-01-07T07:05:00.000000Z\tnull
                441\t7459338290943262088\t1970-01-07T07:06:40.000000Z\tnull
                442\t7657422372928739370\t1970-01-07T07:08:20.000000Z\tnull
                443\t6235849401126045090\t1970-01-07T07:10:00.000000Z\tnull
                444\t8227167469487474861\t1970-01-07T07:11:40.000000Z\tnull
                445\t4794469881975683047\t1970-01-07T07:13:20.000000Z\tnull
                446\t3861637258207773908\t1970-01-07T07:15:00.000000Z\tnull
                447\t8485507312523128674\t1970-01-07T07:16:40.000000Z\tnull
                448\t-5106801657083469087\t1970-01-07T07:18:20.000000Z\tnull
                449\t-7069883773042994098\t1970-01-07T07:20:00.000000Z\tnull
                450\t7415337004567900118\t1970-01-07T07:21:40.000000Z\tnull
                451\t9026435187365103026\t1970-01-07T07:23:20.000000Z\tnull
                452\t-6517956255651384489\t1970-01-07T07:25:00.000000Z\tnull
                453\t-5611837907908424613\t1970-01-07T07:26:40.000000Z\tnull
                454\t-4036499202601723677\t1970-01-07T07:28:20.000000Z\tnull
                455\t8197069319221391729\t1970-01-07T07:30:00.000000Z\tnull
                456\t1732923061962778685\t1970-01-07T07:31:40.000000Z\tnull
                457\t1737550138998374432\t1970-01-07T07:33:20.000000Z\tnull
                458\t1432925274378784738\t1970-01-07T07:35:00.000000Z\tnull
                459\t4698698969091611703\t1970-01-07T07:36:40.000000Z\tnull
                460\t3843127285248668146\t1970-01-07T07:38:20.000000Z\tnull
                461\t2004830221820243556\t1970-01-07T07:40:00.000000Z\tnull
                462\t5341431345186701123\t1970-01-07T07:41:40.000000Z\tnull
                463\t-8490120737538725244\t1970-01-07T07:43:20.000000Z\tnull
                464\t9158482703525773397\t1970-01-07T07:45:00.000000Z\tnull
                465\t7702559600184398496\t1970-01-07T07:46:40.000000Z\tnull
                466\t-6167105618770444067\t1970-01-07T07:48:20.000000Z\tnull
                467\t-6141734738138509500\t1970-01-07T07:50:00.000000Z\tnull
                468\t-7300976680388447983\t1970-01-07T07:51:40.000000Z\tnull
                469\t6260580881559018466\t1970-01-07T07:53:20.000000Z\tnull
                470\t1658444875429025955\t1970-01-07T07:55:00.000000Z\tnull
                471\t7920520795110290468\t1970-01-07T07:56:40.000000Z\tnull
                472\t-5701911565963471026\t1970-01-07T07:58:20.000000Z\tnull
                473\t-6446120489339099836\t1970-01-07T08:00:00.000000Z\tnull
                474\t6527501025487796136\t1970-01-07T08:01:40.000000Z\tnull
                475\t1851817982979037709\t1970-01-07T08:03:20.000000Z\tnull
                476\t2439907409146962686\t1970-01-07T08:05:00.000000Z\tnull
                477\t4160567228070722087\t1970-01-07T08:06:40.000000Z\tnull
                478\t3250595453661431788\t1970-01-07T08:08:20.000000Z\tnull
                479\t7780743197986640723\t1970-01-07T08:10:00.000000Z\tnull
                480\t-3261700233985485037\t1970-01-07T08:11:40.000000Z\tnull
                481\t-3578120825657825955\t1970-01-07T08:13:20.000000Z\tnull
                482\t7443603913302671026\t1970-01-07T08:15:00.000000Z\tnull
                483\t7794592287856397845\t1970-01-07T08:16:40.000000Z\tnull
                484\t-5391587298431311641\t1970-01-07T08:18:20.000000Z\tnull
                485\t9202397484277640888\t1970-01-07T08:20:00.000000Z\tnull
                486\t-6951348785425447115\t1970-01-07T08:21:40.000000Z\tnull
                487\t-4645139889518544281\t1970-01-07T08:23:20.000000Z\tnull
                488\t-7924422932179070052\t1970-01-07T08:25:00.000000Z\tnull
                489\t-6861664727068297324\t1970-01-07T08:26:40.000000Z\tnull
                490\t-6251867197325094983\t1970-01-07T08:28:20.000000Z\tnull
                491\t8177920927333375630\t1970-01-07T08:30:00.000000Z\tnull
                492\t8210594435353205032\t1970-01-07T08:31:40.000000Z\tnull
                493\t8417830123562577846\t1970-01-07T08:33:20.000000Z\tnull
                494\t6785355388782691241\t1970-01-07T08:35:00.000000Z\tnull
                495\t-5892588302528885225\t1970-01-07T08:36:40.000000Z\tnull
                496\t-1185822981454562836\t1970-01-07T08:38:20.000000Z\tnull
                497\t-5296023984443079410\t1970-01-07T08:40:00.000000Z\tnull
                498\t6829382503979752449\t1970-01-07T08:41:40.000000Z\tnull
                499\t3669882909701240516\t1970-01-07T08:43:20.000000Z\tnull
                500\t8068645982235546347\t1970-01-07T08:45:00.000000Z\tnull
                10\t3500000\t1970-01-07T08:45:00.000000Z\t10.2
                """, timestampTypeName);

        try (TableWriter w = TestUtils.getWriter(engine, "x")) {

            // Adding column is essential, columns open in writer's constructor will have
            // mapped memory, whereas newly added column does not
            w.addColumn("v", ColumnType.DOUBLE);

            // stash copy of X, in case X is corrupt
            engine.execute("create atomic table y as (select * from x)", executionContext);

            testAllocateFailsAtO3OpenColumnAppendRows(w);

            // this should fail
            try {
                w.commit();
                Assert.fail();
            } catch (CairoException ignored) {
                w.rollback();
            }

            // check that X and Y are the same
            TestUtils.assertEquals(
                    compiler,
                    executionContext,
                    "x",
                    "y"
            );

            // repeat the same rows
            testAllocateFailsAtO3OpenColumnAppendRows(w);
            w.commit();
        }

        TestUtils.printSql(
                compiler,
                executionContext,
                "x",
                sink2
        );

        TestUtils.assertEquals(expected, sink2);

        sink2.clear();
        sink2.put(
                """
                        count
                        502
                        """
        );

        assertXCount(
                compiler,
                executionContext
        );

    }

    private static void testAllocateFailsAtO3OpenColumnAppendRows(TableWriter w) {
        TableWriter.Row row;
        // this row goes into a non-recent partition
        // triggering O3
        TimestampDriver driver = ColumnType.getTimestampDriver(w.getTimestampType());
        row = w.newRow(driver.fromMicros(518300000000L));
        row.putInt(0, 10);
        row.putLong(1, 3500000L);
        // skip over the timestamp
        row.putDouble(3, 10.2);
        row.append();

        // another O3 row, this time it is appended to last partition
        row = w.newRow(driver.fromMicros(549900000000L));
        row.putInt(0, 10);
        row.putLong(1, 3500000L);
        // skip over the timestamp
        row.putDouble(3, 10.2);
        row.append();
    }

    private static void testAllocateToResizeLastPartition0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        // create table with roughly 2AM data
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + "  ts" +
                        " from long_sequence(500)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        try (TableWriter w = TestUtils.getWriter(engine, "x")) {

            // stash copy of X, in case X is corrupt
            engine.execute("create atomic table y as (select * from x)", executionContext);
            TimestampDriver driver = ColumnType.getTimestampDriver(w.getTimestampType());
            TableWriter.Row row;
            // this row goes into a non-recent partition
            // triggering O3
            row = w.newRow(driver.fromMicros(518300000000L));
            row.putInt(0, 10);
            row.putLong(1, 3500000L);
            row.append();

            // here we need enough rows to saturate existing page
            // same timestamp is ok
            for (int i = 0; i < 4_000_000; i++) {
                row = w.newRow(driver.fromMicros(549900000000L));
                row.putInt(0, 10);
                row.putLong(1, 3500000L);
                row.append();
            }

            // this should fail
            try {
                w.commit();
                Assert.fail();
            } catch (CairoException ignored) {
                w.rollback();
            }
        }

        // check that X and Y are the same
        TestUtils.assertSqlCursors(
                compiler,
                executionContext,
                "x",
                "y",
                LOG
        );

        engine.execute(
                "create atomic table z as (select rnd_int() i, rnd_long() j, timestamp_sequence(549900000000L-4000000L, 10)::" + timestampTypeName + " ts from long_sequence(3000000))",
                executionContext
        );

        engine.execute(
                "insert atomic into x select * from z",
                executionContext
        );

        TestUtils.assertSqlCursors(
                compiler,
                executionContext,
                "x",
                "(y union all z) order by ts",
                LOG,
                true
        );

        TestUtils.printSql(
                compiler,
                executionContext,
                "select max(ts), count() from (y union all z)",
                sink2
        );

        TestUtils.printSql(
                compiler,
                executionContext,
                "select max(ts), count() from x",
                sink
        );
        TestUtils.assertEquals(sink2, sink);
    }

    private static void testColumnTopLastDataOOODataFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {

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
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " 1 as commit," +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("alter table x add column v double", sqlExecutionContext);
        engine.execute("alter table x add column v1 float", sqlExecutionContext);
        engine.execute("alter table x add column v2 int", sqlExecutionContext);
        engine.execute("alter table x add column v3 byte", sqlExecutionContext);
        engine.execute("alter table x add column v4 short", sqlExecutionContext);
        engine.execute("alter table x add column v5 boolean", sqlExecutionContext);
        engine.execute("alter table x add column v6 date", sqlExecutionContext);
        engine.execute("alter table x add column v7 timestamp", sqlExecutionContext);
        engine.execute("alter table x add column v8 symbol", sqlExecutionContext);
        engine.execute("alter table x add column v10 char", sqlExecutionContext);
        engine.execute("alter table x add column v11 string", sqlExecutionContext);
        engine.execute("alter table x add column v12 binary", sqlExecutionContext);
        engine.execute("alter table x add column v9 long", sqlExecutionContext);
        engine.execute("alter table x add column v13 varchar", sqlExecutionContext);
        engine.execute("alter table x add column v14 varchar", sqlExecutionContext);


        engine.execute(
                "insert into x " +
                        "select" +
                        " 2 as commit," +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(549920000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
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
                        " rnd_long() v9," +
                        " rnd_varchar(1,40,1) v13," +
                        " rnd_varchar(1,1,1) v14" +
                        " from long_sequence(500)",
                sqlExecutionContext
        );

        engine.execute(
                "create atomic table append as (" +
                        "select" +
                        " 3 as commit," +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(549930000000L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
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
                        " rnd_long() v9," +
                        " rnd_varchar(1,40,1) v13," +
                        " rnd_varchar(1,1,1) v14" +
                        " from long_sequence(50)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        final String expectedMaxTimestamp = prepareCountAndMaxTimestampSinks(compiler, sqlExecutionContext);

        try {
            engine.execute("insert atomic into x select * from append", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignored) {
        }

        assertXCountAndMax(engine, compiler, sqlExecutionContext, expectedMaxTimestamp);

        engine.execute("create atomic table y as (x union all append)", sqlExecutionContext);
        engine.execute("insert atomic into x select * from append", sqlExecutionContext);

        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                sqlExecutionContext,
                null,
                null
        );
    }

    private static void testColumnTopLastOOOPrefixFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " 0 as commit," +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + "   timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " (timestamp_sequence(500000000000L,330000000L))::" + timestampTypeName + "   ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("alter table x add column v double", sqlExecutionContext);
        engine.execute("alter table x add column v1 float", sqlExecutionContext);
        engine.execute("alter table x add column v2 int", sqlExecutionContext);
        engine.execute("alter table x add column v3 byte", sqlExecutionContext);
        engine.execute("alter table x add column v4 short", sqlExecutionContext);
        engine.execute("alter table x add column v5 boolean", sqlExecutionContext);
        engine.execute("alter table x add column v6 date", sqlExecutionContext);
        engine.execute("alter table x add column v7 timestamp", sqlExecutionContext);
        engine.execute("alter table x add column v8 symbol", sqlExecutionContext);
        engine.execute("alter table x add column v10 char", sqlExecutionContext);
        engine.execute("alter table x add column v11 string", sqlExecutionContext);
        engine.execute("alter table x add column v12 binary", sqlExecutionContext);
        engine.execute("alter table x add column v9 long", sqlExecutionContext);
        engine.execute("alter table x add column v13 varchar", sqlExecutionContext);
        engine.execute("alter table x add column v14 varchar", sqlExecutionContext);

        engine.execute("create table w as (select * from x)", sqlExecutionContext);

        engine.execute(
                "create table append1 as (" +
                        "select" +
                        " 1 as commit," +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(664670000000L,10000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
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
                        " rnd_long() v9," +
                        " rnd_varchar(1,40,1) v13," +
                        " rnd_varchar(1,1,1) v14" +
                        " from long_sequence(500)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create atomic table append2 as (" +
                        "select" +
                        " 2 as commit," +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(604800000000L,10000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
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
                        " rnd_long() v9," +
                        " rnd_varchar(1,40,1) v13," +
                        " rnd_varchar(1,1,1) v14" +
                        " from long_sequence(400)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("insert into x select * from append1", sqlExecutionContext);
        final String expectedMaxTimestamp = prepareCountAndMaxTimestampSinks(compiler, sqlExecutionContext);

        try {
            engine.execute("insert into x select * from append2", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignored) {
        }

        assertXCountAndMax(engine, compiler, sqlExecutionContext, expectedMaxTimestamp);

        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from w union all append1 union all append2)",
                "insert into x select * from append2"
        );
    }

    private static void testColumnTopMidAppendBlankColumnFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        // create table with roughly 2AM data
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + "   timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + "  ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                executionContext
        );

        engine.execute("alter table x add column v double", executionContext);
        engine.execute("alter table x add column v1 float", executionContext);
        engine.execute("alter table x add column v2 int", executionContext);
        engine.execute("alter table x add column v3 byte", executionContext);
        engine.execute("alter table x add column v4 short", executionContext);
        engine.execute("alter table x add column v5 boolean", executionContext);
        engine.execute("alter table x add column v6 date", executionContext);
        engine.execute("alter table x add column v7 timestamp", executionContext);
        engine.execute("alter table x add column v8 symbol", executionContext);
        engine.execute("alter table x add column v10 char", executionContext);
        engine.execute("alter table x add column v11 string", executionContext);
        engine.execute("alter table x add column v12 binary", executionContext);
        engine.execute("alter table x add column v9 long", executionContext);
        engine.execute("alter table x add column v13 varchar", executionContext);
        engine.execute("alter table x add column v14 varchar", executionContext);

        engine.execute(
                "create atomic table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + "   timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(518300000010L,100000L)::" + timestampTypeName + "   ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
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
                        " rnd_long() v9," +
                        " rnd_varchar(1,40,1) v13," +
                        " rnd_varchar(1,1,1) v14" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        final String expectedMaxTimestamp = prepareCountAndMaxTimestampSinks(compiler, executionContext);

        try {
            engine.execute("insert into x select * from append", executionContext);
            Assert.fail();
        } catch (CairoException ignored) {
        }

        assertXCountAndMax(engine, compiler, executionContext, expectedMaxTimestamp);

        // create third table, which will contain both X and 1AM
        assertO3DataConsistency(
                engine,
                compiler,
                executionContext,
                "create atomic table y as (x union all append)",
                "insert atomic into x select * from append"
        );

        assertIndexConsistency(compiler, executionContext, engine);
        assertXCountY(engine, compiler, executionContext);
    }

    private static void testColumnTopMidAppendColumnFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " 0 as commit," +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + "   timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + "   ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("alter table x add column v double", sqlExecutionContext);
        engine.execute("alter table x add column v1 float", sqlExecutionContext);
        engine.execute("alter table x add column v2 int", sqlExecutionContext);
        engine.execute("alter table x add column v3 byte", sqlExecutionContext);
        engine.execute("alter table x add column v4 short", sqlExecutionContext);
        engine.execute("alter table x add column v5 boolean", sqlExecutionContext);
        engine.execute("alter table x add column v6 date", sqlExecutionContext);
        engine.execute("alter table x add column v7 timestamp", sqlExecutionContext);
        engine.execute("alter table x add column v8 symbol", sqlExecutionContext);
        engine.execute("alter table x add column v10 char", sqlExecutionContext);
        engine.execute("alter table x add column v11 string", sqlExecutionContext);
        engine.execute("alter table x add column v12 binary", sqlExecutionContext);
        engine.execute("alter table x add column v9 long", sqlExecutionContext);
        engine.execute("alter table x add column v13 varchar", sqlExecutionContext);
        engine.execute("alter table x add column v14 varchar", sqlExecutionContext);

        engine.execute(
                "insert atomic into x " +
                        "select" +
                        " 1 as commit," +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(549900000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
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
                        " rnd_long() v9," +
                        " rnd_varchar(1,40,1) v13," +
                        " rnd_varchar(1,1,1) v14" +
                        " from long_sequence(1000)",
                sqlExecutionContext
        );

        engine.execute(
                "create atomic table append as (" +
                        "select" +
                        " 2 as commit," +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(604700000001,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
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
                        " rnd_long() v9," +
                        " rnd_varchar(1,40,1) v13," +
                        " rnd_varchar(1,1,1) v14" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        final String expectedMaxTimestamp = prepareCountAndMaxTimestampSinks(compiler, sqlExecutionContext);
        try {
            engine.execute("insert atomic into x select * from append", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignore) {
        }

        assertXCountAndMax(engine, compiler, sqlExecutionContext, expectedMaxTimestamp);

        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);
    }

    private static void testColumnTopMidDataMergeDataFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {

        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " 0 as commit," +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + "   timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + "   ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("alter table x add column v double", sqlExecutionContext);
        engine.execute("alter table x add column v1 float", sqlExecutionContext);
        engine.execute("alter table x add column v2 int", sqlExecutionContext);
        engine.execute("alter table x add column v3 byte", sqlExecutionContext);
        engine.execute("alter table x add column v4 short", sqlExecutionContext);
        engine.execute("alter table x add column v5 boolean", sqlExecutionContext);
        engine.execute("alter table x add column v6 date", sqlExecutionContext);
        engine.execute("alter table x add column v7 timestamp", sqlExecutionContext);
        engine.execute("alter table x add column v8 symbol", sqlExecutionContext);
        engine.execute("alter table x add column v10 char", sqlExecutionContext);
        engine.execute("alter table x add column v11 string", sqlExecutionContext);
        engine.execute("alter table x add column v12 binary", sqlExecutionContext);
        engine.execute("alter table x add column v9 long", sqlExecutionContext);
        engine.execute("alter table x add column v13 varchar", sqlExecutionContext);
        engine.execute("alter table x add column v14 varchar", sqlExecutionContext);

        engine.execute("create table w as (select * from x)", sqlExecutionContext);

        engine.execute(
                "create atomic table append1 as (" +
                        "select" +
                        " 1 as commit," +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(549920000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
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
                        " rnd_long() v9," +
                        " rnd_varchar(1,40,1) v13," +
                        " rnd_varchar(1,1,1) v14" +
                        " from long_sequence(1000)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create atomic table append2 as (" +
                        "select" +
                        " 2 as commit," +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(549900000000L,50000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
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
                        " rnd_long() v9," +
                        " rnd_varchar(1,40,1) v13," +
                        " rnd_varchar(1,1,1) v14" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("insert into x select * from append1", sqlExecutionContext);

        final String expectedMaxTimestamp = prepareCountAndMaxTimestampSinks(compiler, sqlExecutionContext);

        try {
            engine.execute("insert atomic into x select * from append2", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignored) {
        }

        assertXCountAndMax(engine, compiler, sqlExecutionContext, expectedMaxTimestamp);

        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                sqlExecutionContext,
                "create atomic table y as (select * from w union all append1 union all append2)",
                "insert atomic into x select * from append2"
        );
    }

    private static void testColumnTopMidMergeBlankColumnFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        // create table with roughly 2AM data
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + "   timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + "   ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("alter table x add column v double", sqlExecutionContext);
        engine.execute("alter table x add column v1 float", sqlExecutionContext);
        engine.execute("alter table x add column v2 int", sqlExecutionContext);
        engine.execute("alter table x add column v3 byte", sqlExecutionContext);
        engine.execute("alter table x add column v4 short", sqlExecutionContext);
        engine.execute("alter table x add column v5 boolean", sqlExecutionContext);
        engine.execute("alter table x add column v6 date", sqlExecutionContext);
        engine.execute("alter table x add column v7 timestamp", sqlExecutionContext);
        engine.execute("alter table x add column v8 symbol index", sqlExecutionContext);
        engine.execute("alter table x add column v10 char", sqlExecutionContext);
        engine.execute("alter table x add column v11 string", sqlExecutionContext);
        engine.execute("alter table x add column v12 binary", sqlExecutionContext);
        engine.execute("alter table x add column v9 long", sqlExecutionContext);
        engine.execute("alter table x add column v13 varchar", sqlExecutionContext);
        engine.execute("alter table x add column v14 varchar", sqlExecutionContext);

        engine.execute(
                "create atomic table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(518300000000L-1000L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
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
                        " rnd_long() v9," +
                        " rnd_varchar(1,40,1) v13," +
                        " rnd_varchar(1,1,1) v14" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        final String expectedMaxTimestamp = prepareCountAndMaxTimestampSinks(compiler, sqlExecutionContext);

        for (int i = 0; i < 10; i++) {
            try {
                engine.execute("insert atomic into x select * from append", sqlExecutionContext);
                Assert.fail();
            } catch (CairoException ignored) {
            }
        }

        fixFailure.set(true);

        assertXCountAndMax(engine, compiler, sqlExecutionContext, expectedMaxTimestamp);

        // create third table, which will contain both X and 1AM
        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create atomic table y as (x union all append)",
                "insert atomic into x select * from append"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);
        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testFailMergeWalFixIntoLag0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        o3MemMaxPages = 1;
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_long() j," +
                        " rnd_long256(5) l256," +
                        " timestamp_sequence('2020-02-24T01',1000L)::" + timestampTypeName + "   ts" +
                        " from long_sequence(20)" +
                        ") timestamp (ts) partition by DAY WAL",
                executionContext
        );

        engine.execute(
                "insert atomic into x " +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_long() j," +
                        " rnd_long256(5) l256," +
                        " timestamp_sequence('2020-02-24',100L) ts" +
                        " from long_sequence(50000)",
                executionContext
        );


        drainWalQueue(engine);
        Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

        engine.releaseInactive();

        o3MemMaxPages = Integer.MAX_VALUE;
        engine.execute("ALTER TABLE x RESUME WAL", executionContext);

        drainWalQueue(engine);
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

        assertXCountAndMax(
                engine,
                compiler,
                executionContext,
                """
                        count
                        50020
                        """,
                replaceTimestampSuffix1("""
                        max
                        2020-02-24T01:00:00.019000Z
                        """, timestampTypeName)
        );
    }

    private static void testFailMergeWalVarIntoLag0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        o3MemMaxPages = 1;
        cairoCommitLatency = Long.MAX_VALUE;
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_long() j," +
                        " rnd_str(5,16,2) str," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
                        " timestamp_sequence('2020-02-24T01',1000L)::" + timestampTypeName + "   ts" +
                        " from long_sequence(20)" +
                        ") timestamp (ts) partition by DAY WAL",
                executionContext
        );

        engine.execute(
                "insert atomic into x " +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_long() j," +
                        " rnd_str(5,160,2) str," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
                        " timestamp_sequence('2020-02-24',100L) ts" +
                        " from long_sequence(50000)",
                executionContext
        );


        drainWalQueue(engine);
        Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

        engine.releaseInactive();

        o3MemMaxPages = Integer.MAX_VALUE;
        engine.execute("ALTER TABLE x RESUME WAL", executionContext);

        drainWalQueue(engine);
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

        assertXCountAndMax(
                engine,
                compiler,
                executionContext,
                """
                        count
                        50020
                        """,
                replaceTimestampSuffix1("""
                        max
                        2020-02-24T01:00:00.019000Z
                        """, timestampTypeName)
        );
    }

    private static void testFailMoveUncommitted0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        o3MemMaxPages = 1;
        // create table with roughly 2AM data
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_long() j," +
                        " timestamp_sequence('2020-02-24',1000L)::" + timestampTypeName + "   ts" +
                        " from long_sequence(500000)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        try {
            engine.execute(
                    "insert atomic into x " +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_long() j," +
                            " timestamp_sequence('2020-02-26',100L) ts" +
                            " from long_sequence(500000)" +
                            "union all " +
                            "select -2, -2, CAST('2020-02-24T00:00:00.000000Z' as TIMESTAMP) from long_sequence(1)",
                    executionContext
            );
            Assert.fail();
        } catch (CairoException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "commit failed");
        }

        engine.execute(
                "insert atomic into x " +
                        "select -2, -2, CAST('2020-02-24T00:00:00.000000Z' as TIMESTAMP) from long_sequence(1)" +
                        "union all select -2, -2, CAST('2020-02-25T00:00:00.000000Z' as TIMESTAMP) from long_sequence(1)",
                executionContext
        );

        assertXCountAndMax(
                engine,
                compiler,
                executionContext,
                """
                        count
                        500002
                        """,
                replaceTimestampSuffix1("""
                        max
                        2020-02-25T00:00:00.000000Z
                        """, timestampTypeName)
        );

    }

    private static void testFailMoveWalToLag0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        o3MemMaxPages = 1;
        // create table with roughly 2AM data
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_long() j," +
                        " timestamp_sequence('2020-02-24',1000L)::" + timestampTypeName + "   ts" +
                        " from long_sequence(20)" +
                        ") timestamp (ts) partition by DAY WAL",
                executionContext
        );

        engine.execute(
                "insert atomic into x " +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_long() j," +
                        " timestamp_sequence('2020-02-26',100L) ts" +
                        " from long_sequence(500000)" +
                        "union all " +
                        "select -2, -2, CAST('2020-02-24T00:00:00.000000Z' as TIMESTAMP) from long_sequence(1)",
                executionContext
        );

        engine.execute(
                "insert atomic into x " +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_long() j," +
                        " timestamp_sequence('2020-02-26',100L) ts" +
                        " from long_sequence(500000)" +
                        "union all " +
                        "select -2, -2, CAST('2020-02-24T00:00:00.000000Z' as TIMESTAMP) from long_sequence(1)",
                executionContext
        );


        drainWalQueue(engine);
        Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

        engine.releaseInactive();
        o3MemMaxPages = Integer.MAX_VALUE;

        engine.execute("ALTER TABLE x RESUME WAL", executionContext);
        drainWalQueue(engine);

        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

        assertXCountAndMax(
                engine,
                compiler,
                executionContext,
                """
                        count
                        1000022
                        """,
                replaceTimestampSuffix1("""
                        max
                        2020-02-26T00:00:49.999900Z
                        """, timestampTypeName)
        );
    }

    private static void testInsertAsSelectNegativeTimestamp0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " cast(x as int) i, " +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " timestamp_sequence(500000000000L,1000000L)::" + timestampTypeName + "   ts," +
                        " cast(x as short) l" +
                        " from long_sequence(50)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute(
                "create atomic table top as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " timestamp_sequence(-500,10L)::" + timestampTypeName + "   ts," +
                        " cast(x + 1000 as short)  l" +
                        " from long_sequence(100)" +
                        ")",
                sqlExecutionContext
        );

        final String expectedMaxTimestamp = prepareCountAndMaxTimestampSinks(compiler, sqlExecutionContext);

        try {
            engine.execute("insert atomic into x select * from top", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ex) {
            Chars.contains(ex.getFlyweightMessage(), "timestamps before 1970-01-01");
        }

        assertXCountAndMax(engine, compiler, sqlExecutionContext, expectedMaxTimestamp);

        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create atomic table y as (select * from top where ts >= 0 union all select * from x)",
                "insert atomic into x select * from top where ts >= 0"
        );
        assertIndexConsistency(compiler, sqlExecutionContext, engine);
        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testInsertAsSelectNulls0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " cast(x as int) i, " +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " timestamp_sequence(500000000000L,1000000L)::" + timestampTypeName + "   ts," +
                        " cast(x as short) l" +
                        " from long_sequence(50)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute(
                "create atomic table top as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " case WHEN x < 2 THEN CAST(NULL as TIMESTAMP) ELSE CAST(x as TIMESTAMP) END ts," +
                        " cast(x + 1000 as short)  l" +
                        " from long_sequence(100)" +
                        ")",
                sqlExecutionContext
        );

        final String expectedMaxTimestamp = prepareCountAndMaxTimestampSinks(compiler, sqlExecutionContext);

        try {
            engine.execute("insert atomic into x select * from top", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ex) {
            Chars.contains(ex.getFlyweightMessage(), "timestamps before 1970-01-01");
        }

        assertXCountAndMax(engine, compiler, sqlExecutionContext, expectedMaxTimestamp);

        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create atomic table y as (select * from top where ts >= 0 union all select * from x)",
                "insert atomic into x select * from top where ts >= 0"
        );
        assertIndexConsistency(compiler, sqlExecutionContext, engine);
        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testOooFollowedByAnotherOOO0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            AtomicBoolean restoreDiskSpace,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " 1 as commit," +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + "   timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(10000000000,1000000L)::" + timestampTypeName + "   ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
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

        engine.execute(
                "create atomic table 1am as (" +
                        "select" +
                        " 2 as commit," +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(9993000000,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
                        " from long_sequence(507)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create atomic table tail as (" +
                        "select" +
                        " 3 as commit," +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(9997000010L,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        engine.execute("create atomic table y as (x union all 1am union all tail)", sqlExecutionContext);

        try {
            engine.execute("insert atomic into x select * from 1am", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignore) {
            // ignore "no disk space left" error and keep going
        }

        try {
            engine.execute("insert atomic into x select * from tail", sqlExecutionContext);
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
        engine.execute("insert atomic into x select * from 1am", sqlExecutionContext);
        engine.execute("insert atomic into x select * from tail", sqlExecutionContext);

        engine.print("y order by ts, commit", sink, sqlExecutionContext);
        engine.print("x", sink2, sqlExecutionContext);
        TestUtils.assertEquals(sink, sink2);

        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testOutOfFileHandles0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create atomic table x as (" +
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
                        " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + "   ts," +
                        " rnd_str(5,16,2) l," +
                        " rnd_str(5,16,2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_str(5,16,2) t," +
                        " rnd_str(5,16,2) l256" +
                        " from long_sequence(10000)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        engine.execute("create atomic table x1 as (x) timestamp(ts) partition by DAY", executionContext);

        engine.execute(
                "create atomic table y as (" +
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
                        " timestamp_sequence(500000080000L,79999631L)::" + timestampTypeName + "   ts," +
                        " rnd_str(5,16,2) l," +
                        " rnd_str(5,16,2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_str(5,16,2) t," +
                        " rnd_str(5,16,2) l256" +
                        " from long_sequence(10000)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        engine.execute("create table y1 as (y) timestamp(ts) partition by DAY", executionContext);

        // create another compiler to be used by second pool
        try (SqlCompiler compiler2 = engine.getSqlCompiler()) {
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final SOCountDownLatch haltLatch = new SOCountDownLatch(2);
            final AtomicInteger errorCount = new AtomicInteger();

            // we have two pairs of tables (x,y) and (x1,y1)
            try (WorkerPool pool1 = new WorkerPool(() -> 1)) {
                pool1.assign(new Job() {
                    private boolean toRun = true;

                    @Override
                    public boolean run(int workerId, @NotNull RunStatus runStatus) {
                        if (toRun) {
                            try {
                                toRun = false;
                                barrier.await();
                                engine.execute("insert atomic into x select * from y", executionContext);
                            } catch (Throwable e) {
                                //noinspection CallToPrintStackTrace
                                e.printStackTrace();
                                errorCount.incrementAndGet();
                            } finally {
                                haltLatch.countDown();
                            }
                        }
                        return false;
                    }
                });

                try (final WorkerPool pool2 = new TestWorkerPool(1)) {
                    pool2.assign(new Job() {
                        private boolean toRun = true;

                        @Override
                        public boolean run(int workerId, @NotNull RunStatus runStatus) {
                            if (toRun) {
                                try {
                                    toRun = false;
                                    barrier.await();
                                    try (InsertOperation op = compiler2.compile("insert atomic into x1 select * from y1", executionContext).popInsertOperation()) {
                                        op.execute(executionContext);
                                    }
                                } catch (Throwable e) {
                                    //noinspection CallToPrintStackTrace
                                    e.printStackTrace();
                                    errorCount.incrementAndGet();
                                } finally {
                                    haltLatch.countDown();
                                }
                            }
                            return false;
                        }
                    });

                    pool1.start();
                    pool2.start();
                    haltLatch.await();

                    pool2.halt();
                }
                pool1.halt();
            }

            Assert.assertTrue(errorCount.get() > 0);
        }
    }

    private static void testPartitionedDataAppendOODataFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        // create table with roughly 2AM data
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + "  timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + "  ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                executionContext
        );

        engine.execute(
                "create atomic table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + "   timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(518300000010L,100000L)::" + timestampTypeName + "  ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        final String expectedMaxTimestamp = prepareCountAndMaxTimestampSinks(compiler, executionContext);

        for (int i = 0; i < 10; i++) {
            try {
                engine.execute("insert atomic into x select * from append", executionContext);
                Assert.fail();
            } catch (CairoException ignored) {
            }
        }

        fixFailure.set(true);

        assertXCountAndMax(engine, compiler, executionContext, expectedMaxTimestamp);

        // create third table, which will contain both X and 1AM
        assertO3DataConsistency(
                engine,
                compiler,
                executionContext,
                "create atomic table y as (x union all append)",
                "insert atomic into x select * from append"
        );

        assertIndexConsistency(compiler, executionContext, engine);
        assertXCountY(engine, compiler, executionContext);
    }

    private static void testPartitionedDataAppendOODataIndexedFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + "   timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + "   ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute(
                "create atomic table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + "   timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(518300000010L,100000L)::" + timestampTypeName + "   ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        final String expectedMaxTimestamp = prepareCountAndMaxTimestampSinks(compiler, sqlExecutionContext);

        try {
            engine.execute("insert atomic into x select * from append", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignored) {
        }

        assertXCountAndMax(engine, compiler, sqlExecutionContext, expectedMaxTimestamp);

        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create atomic table y as (x union all append)",
                "insert atomic into x select * from append"
        );

        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testPartitionedOOPrefixesExistingPartitionsFailRetry0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + "   timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,1000000L)::" + timestampTypeName + "   ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        engine.execute(
                "create atomic table top as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(15000000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) vc1," +
                        " rnd_varchar(1,1,1) vc2," +
                        " from long_sequence(1000)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        final String expectedMaxTimestamp = prepareCountAndMaxTimestampSinks(compiler, sqlExecutionContext);

        try {
            engine.execute("insert atomic into x select * from top", sqlExecutionContext);
            Assert.fail();
        } catch (CairoException ignored) {
        }

        assertXCountAndMax(engine, compiler, sqlExecutionContext, expectedMaxTimestamp);

        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create atomic table y as (select * from x union all select * from top)",
                "insert atomic into x select * from top"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);
        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testPartitionedWithAllocationCallLimit0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create atomic table x as (" +
                        "select" +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + "   timestamp," +
                        " timestamp_sequence(500000000000L,1000000L)::" + timestampTypeName + "   ts" +
                        " from long_sequence(100000L)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute(
                "create atomic table append as (" +
                        "select" +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + "   timestamp," +
                        " timestamp_sequence(518300000010L,100000L)::" + timestampTypeName + "   ts" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("insert atomic into x select * from append", sqlExecutionContext);
        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create atomic table y as (x union all append)",
                "insert atomic into x select * from append"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);
        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testTwoRowsConsistency0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x (ts " + timestampTypeName + ", block_nr long) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "x",
                sink,
                "ts\tblock_nr\n"
        );

        engine.execute("insert into x values(cast('2010-02-04T21:43:14.000000Z' as timestamp), 38304)", sqlExecutionContext);

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "x",
                sink,
                replaceTimestampSuffix1("""
                        ts\tblock_nr
                        2010-02-04T21:43:14.000000Z\t38304
                        """, timestampTypeName)
        );

        engine.execute("insert into x values(cast('2010-02-14T23:52:59.000000Z' as timestamp), 40320)", sqlExecutionContext);

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "x",
                sink,
                replaceTimestampSuffix1("""
                        ts\tblock_nr
                        2010-02-04T21:43:14.000000Z\t38304
                        2010-02-14T23:52:59.000000Z\t40320
                        """, timestampTypeName)
        );

    }

    private static void testVarColumnStress(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {

        engine.execute("create table x (f symbol index, a string, b string, c string, d string, vc1 varchar, vc2 varchar, e symbol index, g int, t " + timestampTypeName + ") timestamp (t) partition by DAY", executionContext);
        // max timestamp should be 100_000
        engine.execute("insert atomic into x select rnd_symbol('aa', 'bb', 'cc'), rnd_str(4,4,1), rnd_str(4,4,1), rnd_str(4,4,1), rnd_str(4,4,1), rnd_varchar(1,40,1), rnd_varchar(1,1,1), rnd_symbol('aa', 'bb', 'cc'), rnd_int(), timestamp_sequence(0, 100) from long_sequence(3000000)", executionContext);

        String[] symbols = new String[]{"ppp", "wrre", "0ppd", "l22z", "wwe32", "pps", "oop2", "00kk"};
        final int symbolLen = symbols.length;


        Rnd rnd = TestUtils.generateRandom(LOG);
        int batches = 0;
        int batchCount = 75;

        Utf8StringSink utf8Sink = new Utf8StringSink();
        while (batches < batchCount) {
            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                for (int i = 0; i < batchCount; i++) {
                    batches++;
                    for (int k = 0; k < 1000; k++) {
                        TableWriter.Row r = w.newRow(rnd.nextPositiveInt() % 100_000);
                        r.putSym(0, symbols[rnd.nextInt(symbolLen)]);
                        putRndStr(rnd, w.getMetadata(), r, 1, 7, utf8Sink);
                        putRndStr(rnd, w.getMetadata(), r, 2, 8, utf8Sink);
                        putRndStr(rnd, w.getMetadata(), r, 3, 4, utf8Sink);
                        putRndStr(rnd, w.getMetadata(), r, 4, 6, utf8Sink);

                        putRndStr(rnd, w.getMetadata(), r, 5, 40, utf8Sink);
                        putRndStr(rnd, w.getMetadata(), r, 6, 1, utf8Sink);

                        r.putSym(7, symbols[rnd.nextInt(symbolLen)]);
                        r.putInt(8, rnd.nextInt());

                        r.append();
                    }
                    try {
                        w.ic();
                    } catch (Throwable e) {
                        try {
                            w.rollback();
                        } catch (Throwable ex) {
                            // ignore
                        }
                    }
                }
            }
        }
    }

    private void executeWithoutPool(CustomisableRunnableWithTimestampType runnable, FilesFacade ff) throws Exception {
        executeVanilla(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            };
            TestUtils.execute(
                    null,
                    (engine, compiler, sqlExecutionContext) -> runnable.run(engine, compiler, sqlExecutionContext, timestampType.getTypeName()),
                    configuration,
                    LOG
            );
        });
    }

    private FilesFacade failToFSync(String fileName) {
        AtomicLong targetFd = new AtomicLong();
        AtomicInteger counter = new AtomicInteger(1);

        return new TestFilesFacadeImpl() {

            @Override
            public void fsync(long fd) {
                if (fd == targetFd.get()) {
                    targetFd.set(0);
                    throw CairoException.critical(22).put("cannot fsync");
                }
                super.fsync(fd);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (Utf8s.endsWithAscii(name, fileName) && counter.decrementAndGet() == 0) {
                    targetFd.set(fd);
                }
                return fd;
            }
        };
    }

    private FilesFacade failToMMap(String fileName) {
        AtomicLong targetFd = new AtomicLong();
        AtomicInteger counter = new AtomicInteger(2);

        return new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (fd == targetFd.get() && counter.decrementAndGet() == 0) {
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (Utf8s.endsWithAscii(name, fileName)) {
                    targetFd.set(fd);
                }
                return fd;
            }
        };
    }

    protected static void drainWalQueue(CairoEngine engine) {
        try (final ApplyWal2TableJob walApplyJob = createWalApplyJob(engine)) {
            walApplyJob.drain(0);
            new CheckWalTransactionsJob(engine).run(0);
            // run once again as there might be notifications to handle now
            walApplyJob.drain(0);
        }
    }
}
