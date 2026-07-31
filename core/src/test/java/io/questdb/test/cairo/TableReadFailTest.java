/*+*****************************************************************************
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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

public class TableReadFailTest extends AbstractCairoTest {
    @Test
    public void testMetaFileCannotOpenConstructor() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 1);
        spinLockTimeout = 1;
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, TableUtils.META_FILE_NAME)) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertConstructorFail(ff);
    }

    @Test
    public void testMetaFileMissingConstructor() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 1);
        spinLockTimeout = 1;
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ path) {
                if (Utf8s.endsWithAscii(path, TableUtils.META_FILE_NAME)) {
                    return -1;
                }
                return super.openRO(path);
            }
        };
        assertConstructorFail(ff);
    }

    @Test
    public void testReloadTimeout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 1);
            spinLockTimeout = 1;
            String x = "x";
            TableModel model = new TableModel(configuration, x, PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .timestamp();
            AbstractCairoTest.create(model);

            try (
                    Path path = new Path();
                    TableReader reader = newOffPoolReader(configuration, x);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader);
                    MemoryCMARW mem = Vm.getCMARWInstance()
            ) {
                final Rnd rnd = new Rnd();
                final int N = 1000;

                // home path at txn file
                TableToken tableToken = engine.verifyTableName(x);
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$();

                try (TableWriter writer = newOffPoolWriter(configuration, x)) {
                    for (int i = 0; i < N; i++) {
                        TableWriter.Row r = writer.newRow();
                        r.putInt(0, rnd.nextInt());
                        r.putLong(1, rnd.nextLong());
                        r.append();
                    }
                    writer.commit();
                }

                Assert.assertTrue(reader.reload());

                final Record record = cursor.getRecord();
                rnd.reset();
                int count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(rnd.nextInt(), record.getInt(0));
                    Assert.assertEquals(rnd.nextLong(), record.getLong(1));
                    count++;
                }

                Assert.assertEquals(N, count);

                mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);

                // keep txn file parameters
                long offset = configuration.getFilesFacade().length(mem.getFd());

                // corrupt the txn file
                long txn = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                int recOffset = txn % 2 == 0 ? mem.getInt(TableUtils.TX_BASE_OFFSET_A_32) : mem.getInt(TableUtils.TX_BASE_OFFSET_B_32);
                mem.jumpTo(recOffset + TableUtils.TX_OFFSET_TXN_64);
                mem.putLong(txn + 123);
                mem.putLong(TableUtils.TX_BASE_OFFSET_VERSION_64, txn + 2);
                mem.jumpTo(offset);
                mem.close();

                // The corruption leaves a STABLE version pointing at a torn primary area (record txn no longer
                // matches) and an uninitialised fallback area -- permanent corruption, not a concurrent
                // mid-commit whose version would keep churning. The branch's _txn A/B body-checksum guard
                // fail-fasts on that (correct fail-loud) rather than spinning to a spin-lock timeout. (The
                // retry/timeout path still applies when the version actually changes under the reader.)
                try {
                    spinLockTimeout = 100;
                    reader.reload();
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "checksum mismatch");
                }

                // restore txn file to its former glory

                mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
                mem.jumpTo(recOffset + TableUtils.TX_OFFSET_TXN_64);
                mem.putLong(txn + 2);
                mem.putLong(recOffset + TableUtils.TX_OFFSET_BODY_CHECKSUM_64, 0L);
                mem.jumpTo(offset);
                mem.close();
                mem.close();

                // Make sure reload functions correctly. Txn changed from 1 to 3, reload should return true
                Assert.assertTrue(reader.reload());

                try (TableWriter writer = newOffPoolWriter(configuration, x)) {
                    // add more data
                    for (int i = 0; i < N; i++) {
                        TableWriter.Row r = writer.newRow();
                        r.putInt(0, rnd.nextInt());
                        r.putLong(1, rnd.nextLong());
                        r.append();
                    }
                    writer.commit();
                }

                // does positive reload work?
                Assert.assertTrue(reader.reload());

                // can reader still see the correct data?
                cursor.toTop();
                rnd.reset();
                count = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals(rnd.nextInt(), record.getInt(0));
                    Assert.assertEquals(rnd.nextLong(), record.getLong(1));
                    count++;
                }

                Assert.assertEquals(2 * N, count);
            }
            engine.clear();
        });
    }

    @Test
    public void testTornLiveTxnAreaIsNamedRatherThanBlamedOnContention() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 100);
            spinLockTimeout = 100;
            String x = "x";
            TableModel model = new TableModel(configuration, x, PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .timestamp();
            AbstractCairoTest.create(model);
            TableToken tableToken = engine.verifyTableName(x);

            // Two commits, so the OTHER A/B area holds a complete, checksum-valid previous record --
            // the one the fallback will land on.
            commitOneRow(x, 0);
            commitOneRow(x, 1);

            // A reader that has already seen the current txn pins the scoreboard max there. That is what
            // makes the fallback record unusable below: it carries the PREVIOUS txn, which the scoreboard
            // then refuses. The scoreboard is malloc'd, so this models corruption discovered while the
            // engine is live -- after a restart the fallback would simply succeed.
            try (TableReader pin = newOffPoolReader(configuration, x)) {
                Assert.assertTrue(pin.getTxn() > 0);
            }

            tearLiveTxnArea(tableToken);

            TxReader.resetBodyChecksumFallbackCount();
            try (TableReader ignore = newOffPoolReader(configuration, x)) {
                Assert.fail("a torn live _txn area must not open a reader");
            } catch (CairoException e) {
                // The defect this pins: TxReader correctly detects the torn area and falls back to the
                // intact previous one, but the scoreboard refuses that older txn, so the reader spun to
                // its deadline and reported a contention-flavoured "Transaction read timeout". Name the
                // torn _txn instead -- an operator must not go hunting for reader contention.
                TestUtils.assertContains(e.getFlyweightMessage(), "_txn live area is torn");
                TestUtils.assertContains(e.getFlyweightMessage(), "table=" + x);
            }
            // Prove the diagnosis ran on the path it claims: the A/B fallback must actually have fired.
            Assert.assertTrue(
                    "the body-checksum fallback must have fired, else this test proves nothing",
                    TxReader.getBodyChecksumFallbackCount() > 0
            );

            TxReader.resetBodyChecksumFallbackCount();
            engine.clear();
        });
    }

    /**
     * A torn live area is TERMINAL, so the diagnosis must not depend on time passing.
     * <p>
     * The retry loop's only exit was a wall-clock deadline. Freezing the clock is ordinary practice in this
     * suite -- any test that drives time by hand does it, and {@code AbstractEntColdStorageFsTest} pins
     * {@code currentMicros} for the whole class -- and {@code CairoTestConfiguration.getMillisecondClock}
     * derives from that same frozen source. So the deadline could never arrive, and a condition the code
     * already knew how to name became an unbounded 100%-CPU spin: enterprise CI burned its 20-minute
     * per-test limit on it across the linux, mac and Replication jobs rather than failing in milliseconds.
     * <p>
     * Waiting never repairs a torn area, so there is nothing to wait for. The same error, without the wait.
     */
    @Test(timeout = 60_000)
    public void testTornLiveTxnAreaIsNamedEvenWhenTheClockCannotAdvance() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Frozen: every getTicks() from here returns the same value, so no deadline can ever elapse.
            currentMicros = 1_000_000L;
            node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 100);
            spinLockTimeout = 100;
            String x = "clockfrozen";
            TableModel model = new TableModel(configuration, x, PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .timestamp();
            AbstractCairoTest.create(model);
            TableToken tableToken = engine.verifyTableName(x);

            commitOneRow(x, 0);
            commitOneRow(x, 1);

            // Pins the scoreboard max at the current txn, so the intact PREVIOUS record the fallback lands
            // on is refused and the loop cannot converge -- the live-engine corruption case.
            try (TableReader pin = newOffPoolReader(configuration, x)) {
                Assert.assertTrue(pin.getTxn() > 0);
            }

            tearLiveTxnArea(tableToken);

            TxReader.resetBodyChecksumFallbackCount();
            try (TableReader ignore = newOffPoolReader(configuration, x)) {
                Assert.fail("a torn live _txn area must not open a reader");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_txn live area is torn");
                TestUtils.assertContains(e.getFlyweightMessage(), "table=" + x);
            }
            Assert.assertTrue(
                    "the body-checksum fallback must have fired, else this test proves nothing",
                    TxReader.getBodyChecksumFallbackCount() > 0
            );

            TxReader.resetBodyChecksumFallbackCount();
            currentMicros = -1;
            engine.clear();
        });
    }

    @Test
    public void testTornLiveTxnAreaIsNamedRatherThanBlamedOnContentionForMetadataReader() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 100);
            spinLockTimeout = 100;
            // WAL, because getTableMetadata() propagates the failure for a WAL table. For a non-WAL one it
            // retries through tryRepairTable, which would swallow the very diagnosis under test.
            execute("create table w (ts timestamp, a int) timestamp(ts) partition by day wal");
            final TableToken tableToken = engine.verifyTableName("w");

            execute("insert into w values ('2024-01-01T00:00:00.000000Z', 0)");
            drainWalQueue();

            // Seed the metadata pool at THIS txn: the later get() then sees a moved version and goes down
            // refresh() -> reloadSlow() -> readTxnSlow, the second call site with the same blind spot.
            // (A tenant seeded at the corrupted txn would keep its still-valid snapshot and never reload.)
            engine.getTableMetadata(tableToken).close();

            execute("insert into w values ('2024-01-02T00:00:00.000000Z', 1)");
            drainWalQueue();

            tearLiveTxnArea(tableToken);

            // This site needs no scoreboard pin to stall: the metadata tenant's own acquireTxn() requires the
            // loaded version to still match the file's, which the fallback record's never can.
            TxReader.resetBodyChecksumFallbackCount();
            try (TableMetadata ignore = engine.getTableMetadata(tableToken)) {
                Assert.fail("a torn live _txn area must not refresh a metadata reader, got " + ignore.getTableToken());
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_txn live area is torn");
                TestUtils.assertContains(e.getFlyweightMessage(), "src=metadata");
            }
            Assert.assertTrue(
                    "the body-checksum fallback must have fired, else this test proves nothing",
                    TxReader.getBodyChecksumFallbackCount() > 0
            );
            TxReader.resetBodyChecksumFallbackCount();
            engine.clear();
        });
    }

    @Test
    public void testTxnFileCannotOpenConstructor() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, TableUtils.TXN_FILE_NAME)) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertConstructorFail(ff);
    }

    @Test
    public void testTxnFileMissingConstructor() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long length(LPSZ name) {
                return Utf8s.endsWithAscii(name, TableUtils.TXN_FILE_NAME) ? 0 : super.length(name);
            }
        };
        assertConstructorFail(ff);
    }

    private void assertConstructorFail(FilesFacade ff) throws Exception {
        CreateTableTestUtils.createAllTable(engine, PartitionBy.DAY, ColumnType.TIMESTAMP_MICRO);
        assertMemoryLeak(() -> {
            try {
                newOffPoolReader(
                        new DefaultTestCairoConfiguration(root) {
                            @Override
                            public @NotNull FilesFacade getFilesFacade() {
                                return ff;
                            }

                            @Override
                            public long getSpinLockTimeout() {
                                return 1;
                            }
                        }, "all"
                ).close();
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }

    private void commitOneRow(String table, int seq) {
        try (TableWriter writer = newOffPoolWriter(configuration, table)) {
            TableWriter.Row r = writer.newRow(seq * 1_000_000L);
            r.putInt(0, seq);
            r.append();
            writer.commit();
        }
    }

    /**
     * Pokes a checksum-covered scalar in the version-selected (live) area of {@code _txn} and deliberately
     * does NOT restamp the body checksum, leaving the area torn exactly as a partial writeback would. The
     * area's internal txn guard still matches, so only the body checksum can catch this -- which sends
     * TxReader to the intact previous A/B area, and the reader into its retry loop.
     */
    private void tearLiveTxnArea(TableToken tableToken) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME);
            try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
                final long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                final int recOffset = (version & 1) == 0
                        ? mem.getInt(TableUtils.TX_BASE_OFFSET_A_32)
                        : mem.getInt(TableUtils.TX_BASE_OFFSET_B_32);
                mem.putLong(recOffset + TableUtils.TX_OFFSET_MAX_TIMESTAMP_64, 987_654_321L);
                mem.close(false);
            }
        }
    }
}
