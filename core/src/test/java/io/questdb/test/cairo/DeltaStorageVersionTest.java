/*+*****************************************************************************
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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A table holding Delta partitions stamps {@link ColumnType#DELTA_STORAGE_VERSION} into {@code _meta}
 * before the {@code _txn} commit that publishes the first Delta-write bit, so an older build refuses the
 * table instead of serving only its base rows.
 */
public class DeltaStorageVersionTest extends AbstractCairoTest {

    @Test
    public void testCheckpointImageCarriesDeltaStamp() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createTable();
            final TableToken token = engine.verifyTableName("t");
            try (TableWriter writer = engine.getWriter(token, "test")) {
                setDeltaActive(writer);
                writer.commitSeqTxn(writer.getTxWriter().getSeqTxn() + 1);
            }
            execute("CHECKPOINT CREATE");
            try (Path path = new Path()) {
                path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory())
                        .concat(token).concat(TableUtils.META_FILE_NAME);
                Assert.assertEquals(ColumnType.DELTA_STORAGE_VERSION, readVersion(path));
            } finally {
                execute("CHECKPOINT RELEASE");
            }
        });
    }

    @Test
    public void testDeltaBitStaysInvisibleWhenStampFails() throws Exception {
        final AtomicBoolean armed = new AtomicBoolean();
        assertMemoryLeak(metaFsyncFailureFacade(armed), () -> {
            createTable();
            final TableToken token = engine.verifyTableName("t");
            try (TableWriter writer = engine.getWriter(token, "test")) {
                setDeltaActive(writer);
                final long seqTxn = writer.getTxWriter().getSeqTxn() + 1;
                armed.set(true);
                try {
                    writer.commitSeqTxn(seqTxn);
                    Assert.fail("fault did not fire");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not fsync");
                }
                // A crash here leaves a stamped _meta and an unchanged _txn: safe, because this build
                // reads the table and an older build refuses it; the retry publishes the bit.
                Assert.assertFalse("the Delta bit became visible before _meta was stamped", diskTxnHasDeltaActive(token));

                writer.commitSeqTxn(seqTxn);
                Assert.assertTrue(diskTxnHasDeltaActive(token));
                Assert.assertEquals(ColumnType.DELTA_STORAGE_VERSION, metaVersion(token));
            }
        });
    }

    @Test
    public void testOlderBuildsRefuseDeltaStamp() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final TableToken token = engine.verifyTableName("t");
            try (TableWriter writer = engine.getWriter(token, "test")) {
                setDeltaActive(writer);
                writer.commitSeqTxn(writer.getTxWriter().getSeqTxn() + 1);
            }
            try (
                    MemoryMR mem = Vm.getCMRInstance();
                    Path path = new Path()
            ) {
                path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
                mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
                // The check a build that knows composite stamps but not Delta runs on open.
                try {
                    TableUtils.validateMetaVersion(path.$(), mem, TableUtils.META_OFFSET_VERSION, ColumnType.VERSION, ColumnType.MAX_STORAGE_VERSION);
                    Assert.fail("a build that stops at MAX_STORAGE_VERSION accepted a Delta table");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "metadata version does not match runtime version");
                }
            }
        });
    }

    private static void createTable() throws Exception {
        execute("CREATE TABLE t AS (SELECT cast(x AS int) i, timestamp_sequence('2024-01-01', 3_600_000_000L) ts FROM long_sequence(48)) TIMESTAMP(ts) PARTITION BY DAY");
    }

    private static boolean diskTxnHasDeltaActive(TableToken token) {
        try (
                TxReader txReader = new TxReader(configuration.getFilesFacade());
                Path path = new Path()
        ) {
            path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.TXN_FILE_NAME);
            txReader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
            Assert.assertTrue(txReader.unsafeLoadAll());
            return txReader.hasAnyDeltaActive();
        }
    }

    private static FilesFacade metaFsyncFailureFacade(AtomicBoolean armed) {
        return new TestFilesFacadeImpl() {
            private long metaFd = -1;

            @Override
            public void fsync(long fd) {
                if (fd == metaFd && armed.compareAndSet(true, false)) {
                    metaFd = -1;
                    throw CairoException.critical(5).put("could not fsync [fd=").put(fd).put(']');
                }
                super.fsync(fd);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (armed.get() && Utf8s.endsWithAscii(name, "_meta") && stackHas("writeStorageVersionToMeta")) {
                    metaFd = fd;
                }
                return fd;
            }
        };
    }

    private static int metaVersion(TableToken token) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
            return readVersion(path);
        }
    }

    private static int readVersion(Path path) {
        try (MemoryMR mem = Vm.getCMRInstance()) {
            mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
            return mem.getInt(TableUtils.META_OFFSET_VERSION);
        }
    }

    private static void setDeltaActive(TableWriter writer) {
        writer.getTxWriter().setPartitionDeltaActiveByTimestamp(MicrosFormatUtils.parseUTCTimestamp("2024-01-01T00:00:00.000000Z"));
    }

    private static boolean stackHas(String method) {
        return StackWalker.getInstance().walk(s -> s.anyMatch(f -> f.getMethodName().equals(method)));
    }
}
