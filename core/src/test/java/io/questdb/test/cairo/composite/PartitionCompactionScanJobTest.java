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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.ColumnTopRecorder;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coverage for {@link PartitionCompactionScanJob}: the interval gate on its own (mirroring
 * {@link io.questdb.test.cairo.wal.WalPurgeJobTest}'s own interval test), then end-to-end sweeps that must
 * compact only genuinely idle composite/Parquet partitions and leave everything else - plain partitions,
 * already-compact ones, and anything too recent - untouched.
 */
public class PartitionCompactionScanJobTest extends AbstractCairoTest {

    /**
     * Mirrors {@link io.questdb.test.cairo.wal.WalPurgeJobTest}'s own interval-gate test: wraps the files
     * facade to count how many times the sweep actually touches a table's {@code _txn} file, and checks
     * that count only ever moves on a {@link PartitionCompactionScanJob#run()} call made after the
     * configured interval has elapsed, never before, and never twice for the same elapsed interval.
     */
    @Test
    public void testInterval() throws Exception {
        final AtomicInteger counter = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                counter.incrementAndGet();
                return super.exists(path);
            }
        };

        assertMemoryLeak(ff, () -> {
            final String tableName = testName.getMethodName();
            execute("create table " + tableName + "(" +
                    "x long," +
                    "ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL");
            drainWalQueue();

            final long interval = engine.getConfiguration().getPartitionCompactionCheckInterval() * 1000; // ms to us.
            setCurrentMicros(1); // Some point in time that's not 0.

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, ff, configuration.getMicrosecondClock())) {
                counter.set(0);

                // last == 0 at construction; not enough simulated time has passed yet.
                job.run();
                Assert.assertEquals("no sweep should run before the interval elapses", 0, counter.get());

                setCurrentMicros(currentMicros + interval + 1);
                job.run();
                final int afterFirstTrigger = counter.get();
                Assert.assertTrue("expected a sweep to have run", afterFirstTrigger > 0);

                // No clock movement: must not sweep again.
                job.run();
                job.run();
                Assert.assertEquals("no extra sweep without the clock advancing", afterFirstTrigger, counter.get());

                setCurrentMicros(currentMicros + interval + 1);
                job.run();
                final int afterSecondTrigger = counter.get();
                Assert.assertTrue("expected a second sweep to have run", afterSecondTrigger > afterFirstTrigger);

                // A large jump still triggers only once per run() call.
                setCurrentMicros(currentMicros + 10 * interval);
                job.run();
                Assert.assertTrue("expected a third sweep to have run", counter.get() > afterSecondTrigger);
            }
        });
    }

    /**
     * A configuration-seeded random start keeps a table with a large backlog from deterministically hiding
     * every table behind it. The one-byte IO budget permits one oversized rebuild per sweep, and busy writers
     * keep every backlog intact. Over a bounded deterministic sequence of starts, every table gets selected.
     */
    @Test
    public void testRandomSweepStartsReachTablesWithSaturatedIoBudget() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IO_BUDGET, "1");
        node1.setProperty(PropertyKey.CAIRO_WRITER_COMMAND_QUEUE_CAPACITY, 64);

        final ObjList<String> tableNames = new ObjList<>();
        tableNames.add("ca");
        tableNames.add("cb");
        tableNames.add("cc");
        final int tableCount = tableNames.size();
        // Filled in as the tables are created; the facade below reads whatever is there at the time, which
        // is nothing until the fixture is built.
        final ObjList<TableToken> tokens = new ObjList<>();
        final ObjList<AtomicInteger> stagedCopies = new ObjList<>();
        for (int i = 0; i < tableCount; i++) {
            stagedCopies.add(new AtomicInteger());
        }
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                if (Utf8s.containsAscii(path, TableUtils.COMPACTING_DIR_MARKER)) {
                    for (int i = 0, n = tokens.size(); i < n; i++) {
                        if (Utf8s.containsAscii(path, tokens.getQuick(i).getDirName())) {
                            stagedCopies.getQuick(i).incrementAndGet();
                            break;
                        }
                    }
                }
                return super.mkdirs(path, mode);
            }
        };

        assertMemoryLeak(ff, () -> {
            // 41 day partitions of 288 rows each - over the 1K split floor, and small enough that 40 of them
            // per table stay cheap to build and to copy.
            final String base = "SELECT x::INT i, timestamp_sequence('2020-01-01', 300*1000000L) ts FROM long_sequence(11808)";
            // One row inside each of the first 40 days, so every one of those turns composite while the 41st
            // stays plain and active.
            final String backfill = "SELECT x::INT + 70_000 i," +
                    " ('2020-01-01T04:00:07'::timestamp + (x - 1) * 86_400_000_000L)::timestamp ts" +
                    " FROM long_sequence(40)";

            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            for (int i = 0; i < tableCount; i++) {
                final String tableName = tableNames.getQuick(i);
                execute("CREATE TABLE " + tableName + " AS (" + base + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
                drainWalQueue();
                execute("INSERT INTO " + tableName + " " + backfill);
                drainWalQueue();
                tokens.add(engine.verifyTableName(tableName));
            }

            // Each table has enough work to keep consuming a one-dispatch sweep whenever it starts first.
            for (int i = 0; i < tableCount; i++) {
                try (TableReader reader = engine.getReader(tokens.getQuick(i))) {
                    final TxReader tx = reader.getTxFile();
                    Assert.assertEquals(41, tx.getPartitionCount());
                    int compositeCount = 0;
                    for (int p = 0, n = tx.getPartitionCount(); p < n; p++) {
                        if (tx.isPartitionComposite(p)) {
                            compositeCount++;
                        }
                    }
                    Assert.assertTrue(
                            "table " + tableNames.getQuick(i) + " must offer a backlog, got " + compositeCount,
                            compositeCount > 10
                    );
                }
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            final long interval = engine.getConfiguration().getPartitionCompactionCheckInterval() * 1000;
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-02-15T00:10:00.000000Z"));

            // Holding every writer on this thread keeps each swap on a queue instead of applying it, so no
            // table's backlog ever shrinks and the leader saturates the budget on every sweep rather than
            // only the first. The sweeps run on a thread of their own, so the job sees those writers as busy,
            // and on ONE job instance, so whatever the job carries between sweeps is exercised.
            try (TableWriter wa = engine.getWriter(tokens.getQuick(0), "test");
                 TableWriter wb = engine.getWriter(tokens.getQuick(1), "test");
                 TableWriter wc = engine.getWriter(tokens.getQuick(2), "test")) {
                Assert.assertNotNull(wa);
                Assert.assertNotNull(wb);
                Assert.assertNotNull(wc);
                final Throwable[] failure = new Throwable[1];
                final Thread sweeper = new Thread(() -> {
                    try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, ff, configuration.getMicrosecondClock())) {
                        for (int sweep = 0; sweep < 20; sweep++) {
                            setCurrentMicros(currentMicros + interval + 1);
                            job.run();
                        }
                    } catch (Throwable e) {
                        failure[0] = e;
                    } finally {
                        // What WorkerPool's worker-halt cleaners do for the compaction pool's own thread.
                        Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                        Path.clearThreadLocals();
                    }
                });
                sweeper.start();
                sweeper.join();
                if (failure[0] != null) {
                    throw new AssertionError("the sweep failed on its own thread", failure[0]);
                }
            }

            for (int i = 0; i < tableCount; i++) {
                Assert.assertTrue(
                        "table " + tableNames.getQuick(i) + " was never dispatched in 20 sweeps; staged copies per table were "
                                + stagedCopies,
                        stagedCopies.getQuick(i).get() > 0
                );
            }

            // The queued swaps still land once the writers go back, and no rows move either way.
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            for (int i = 0; i < tableCount; i++) {
                assertQuery("SELECT count() c FROM " + tableNames.getQuick(i))
                        .noRandomAccess().expectSize().returns("c\n11848\n");
            }
        });
    }

    /**
     * The Enterprise storage policy can freeze a lone COMPOSITE partition ahead of its parquet conversion, and asks
     * for the fold the conversion needs. The request must fold it - here a REWRITE, with the sweep off - and the
     * rewritten folder must stay READ ONLY, or the policy asks forever.
     */
    @Test
    public void testRequestFoldsAReadOnlyCompositePartitionAndKeepsItReadOnly() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createCompositeDayTable("ro_cr");
            createCompositeDayTable("ro_cr_oracle");
            final TableToken token = engine.verifyTableName("ro_cr");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            final long nameTxnBefore;
            final long liveRowsBefore;
            try (TableWriter writer = engine.getWriter(token, "test-freeze")) {
                Assert.assertTrue("2020-01-01 should be composite", writer.getTxWriter().isPartitionComposite(0));
                Assert.assertTrue("fixture must require REWRITE, not MAKE-PLAIN", writer.getGeometry().getPieceCount(0) > 1);
                writer.getTxWriter().setPartitionReadOnly(0, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
                nameTxnBefore = writer.getTxWriter().getPartitionNameTxn(0);
                liveRowsBefore = writer.getTxWriter().getPartitionSize(0);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(token, day));
                Assert.assertTrue("the request must dispatch the fold", job.run());
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertFalse("the request must fold a read-only composite partition", tx.isPartitionComposite(0));
                Assert.assertNotEquals("a REWRITE lands in a new directory", nameTxnBefore, tx.getPartitionNameTxn(0));
                Assert.assertTrue("the folded partition must stay READ ONLY", tx.isPartitionReadOnly(0));
                Assert.assertEquals(liveRowsBefore, tx.getPartitionSize(0));
            }
            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext, "SELECT * FROM ro_cr_oracle ORDER BY ts, i", "SELECT * FROM ro_cr ORDER BY ts, i", LOG
            );
        });
    }

    /**
     * The cold-storage upload asks for a parquet partition only when {@link
     * PartitionCompactionScanJob#isParquetPartitionCompact} says it is not compact, and the request path decides on
     * that same predicate. So a compact parquet partition, and an uploaded one - which the job never rewrites, and the
     * predicate therefore calls compact - are both left exactly as they were.
     */
    @Test
    public void testRequestLeavesACompactOrUploadedParquetPartitionAlone() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken uploaded = createIdleDirtyParquetTable("rq_up_px");
            final long uploadedNameTxnBefore;
            try (TableWriter writer = engine.getWriter(uploaded, "test-upload")) {
                writer.getTxWriter().setPartitionRemote(0, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
                uploadedNameTxnBefore = writer.getTxWriter().getPartitionNameTxn(0);
            }

            execute("CREATE TABLE rq_clean_px (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO rq_clean_px(a, ts) VALUES (1, '2020-01-01T00:00:00.000Z'), (2, '2020-01-01T01:00:00.000Z')");
            // Pusher day, so 2020-01-01 is inactive by the time it is converted below.
            execute("INSERT INTO rq_clean_px(a, ts) VALUES (90, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();
            execute("ALTER TABLE rq_clean_px CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            final TableToken clean = engine.verifyTableName("rq_clean_px");
            final long cleanNameTxnBefore;
            try (TableReader reader = engine.getReader(clean)) {
                cleanNameTxnBefore = reader.getTxFile().getPartitionNameTxn(0);
            }
            Assert.assertTrue("a freshly converted partition is compact", isParquetPartitionCompact(clean, 0));
            Assert.assertTrue("the job never rewrites an uploaded partition, so it counts as compact", isParquetPartitionCompact(uploaded, 0));
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(uploaded, day));
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(clean, day));
                Assert.assertFalse("neither request may dispatch a rebuild", job.run());
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertPartitionUntouched(uploaded, uploadedNameTxnBefore, "an uploaded");
            try (TableReader reader = engine.getReader(uploaded)) {
                Assert.assertTrue("the UPLOADED bit must survive the request", reader.getTxFile().isPartitionRemote(0));
            }
            try (TableReader reader = engine.getReader(clean)) {
                Assert.assertEquals("a compact partition must keep its directory", cleanNameTxnBefore, reader.getTxFile().getPartitionNameTxn(0));
            }
        });
    }

    /**
     * See {@link #assertReadOnlySplitDayMerges}: the READ ONLY flag sits on the day's main folder.
     */
    @Test
    public void testRequestMergesAReadOnlySplitDayAndKeepsItReadOnly() throws Exception {
        assertReadOnlySplitDayMerges(0, true);
    }

    /**
     * See {@link #assertReadOnlySplitDayMerges}: the READ ONLY flag sits only on the split, which the merge
     * folds away - the merged folder has to take the flag over from it.
     */
    @Test
    public void testRequestMergesASplitDayWhoseSplitIsReadOnlyAndKeepsItReadOnly() throws Exception {
        assertReadOnlySplitDayMerges(1, true);
    }

    @Test
    public void testRequestMergesANonWalSplitDayAroundAnOpenTransaction() throws Exception {
        assertNonWalSplitDayMergeAroundOpenTransaction(false);
    }

    /**
     * The writer commits an open non-WAL transaction that opened a new partition before it merges, so the O3 purge
     * job never sees that partition's directory unlisted below the merge's txn.
     */
    @Test
    public void testRequestMergeCommitsAnOpenTransactionThatOpenedAPartition() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createSplitDayTable("nx");
            execute("ALTER TABLE nx SET TYPE BYPASS WAL");
            engine.releaseInactive();
            engine.load();
            final TableToken token = engine.verifyTableName("nx");
            Assert.assertFalse(token.isWal());
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            final long newDay = MicrosFormatUtils.parseTimestamp("2020-01-04T00:00:00.000000Z");
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals("the O3 insert must have split 2020-01-01", 3, reader.getTxFile().getPartitionCount());
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
            try (
                    TableWriter ownerWriter = engine.getWriter(token, "owner");
                    O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1);
                    Path path = new Path()
            ) {
                appendRow(ownerWriter, "2020-01-03T01:00:00.000000Z", 80001);
                appendRow(ownerWriter, "2020-01-04T00:00:00.000000Z", 80002);
                appendRow(ownerWriter, "2020-01-04T01:00:00.000000Z", 80003);
                Assert.assertTrue(ownerWriter.inTransaction());
                Assert.assertFalse(ownerWriter.hasO3());

                path.of(configuration.getDbRoot()).concat(token.getDirName());
                TableUtils.setPathForNativePartition(
                        path,
                        ColumnType.TIMESTAMP,
                        PartitionBy.DAY,
                        newDay,
                        ownerWriter.getPartitionNameTxnByPartitionTimestamp(newDay)
                );
                final FilesFacade ff = configuration.getFilesFacade();
                Assert.assertTrue("the open transaction must have created its partition directory", ff.exists(path.$()));

                // A reader on the committed state makes a merge hand its old folders to the purge job.
                try (TableReader ignore = engine.getReader(token)) {
                    Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(token, day));
                    runSweepOnAnotherThread(ff);
                    ownerWriter.tick();
                    Assert.assertFalse("the merge must commit the open transaction first", ownerWriter.inTransaction());
                    try (TableReader reader = engine.getReader(token)) {
                        Assert.assertEquals("the day's two folders must have become one", 3, reader.getTxFile().getPartitionCount());
                    }
                }
                engine.releaseAllReaders();
                purgeJob.drain(0);
                Assert.assertTrue("the purge must keep the committed partition directory", ff.exists(path.$()));
            }

            assertQuery("SELECT count() c FROM nx").noRandomAccess().expectSize().returns("c\n6013\n");
            assertQuery("SELECT i, ts FROM nx WHERE ts >= '2020-01-04'").timestamp("ts").returns("""
                    i\tts
                    80002\t2020-01-04T00:00:00.000000Z
                    80003\t2020-01-04T01:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testRequestedMergeIsRefusedByANonWalTransactionWritingTheDay() throws Exception {
        assertNonWalSplitDayMergeAroundOpenTransaction(true);
    }

    /**
     * A request skips every idle threshold - the requester decided the partition is cold - and is served with
     * the sweep itself disabled: here a split day is merged into one folder under timeouts it is nowhere near.
     */
    @Test
    public void testRequestMergesASplitDayWithTheSweepOffAndTheTimeoutsUnmet() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createSplitDayTable("cx");
            final TableToken token = engine.verifyTableName("cx");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");

            final long liveRowsBefore;
            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals("the O3 insert must have split 2020-01-01", 3, tx.getPartitionCount());
                liveRowsBefore = tx.getPartitionSize(0) + tx.getPartitionSize(1);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1000h");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_SQUASH_IDLE_TIMEOUT, "1000h");
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                Assert.assertFalse("nothing is requested yet and the sweep is off", job.run());
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(token, day));
                Assert.assertTrue("the request must dispatch the merge", job.run());
                Assert.assertFalse("the queue must be empty after the request", job.run());
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals("the day's two folders must have become one", 2, tx.getPartitionCount());
                Assert.assertFalse("the merged folder must be plain", tx.isPartitionComposite(0));
                Assert.assertEquals("the merge must not change the row count", liveRowsBefore, tx.getPartitionSize(0));
            }
            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n6010\n");
        });
    }

    /**
     * A requester keeps asking until it sees the fold land, so a request can arrive for a partition that is
     * already one plain folder; one run drains them all. A non-WAL composite partition is folded by a REWRITE, as
     * MAKE-PLAIN commits the writer itself.
     */
    @Test
    public void testRequestFoldsEachCompositePartitionOnce() throws Exception {
        final AtomicInteger stagingMkdirs = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                if (Utf8s.containsAscii(path, TableUtils.COMPACTING_DIR_MARKER)) {
                    stagingMkdirs.incrementAndGet();
                }
                return super.mkdirs(path, mode);
            }
        };

        assertMemoryLeak(ff, () -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createCompositeDayTable("cn");
            createCompositeDayTable("cw");
            execute("ALTER TABLE cn SET TYPE BYPASS WAL");
            engine.releaseInactive();
            engine.load();

            final TableToken nonWalToken = engine.verifyTableName("cn");
            final TableToken walToken = engine.verifyTableName("cw");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            try (TableReader reader = engine.getReader(nonWalToken)) {
                Assert.assertTrue(reader.getTxFile().isPartitionComposite(0));
            }
            try (TableReader reader = engine.getReader(walToken)) {
                Assert.assertTrue(reader.getTxFile().isPartitionComposite(0));
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, ff, configuration.getMicrosecondClock())) {
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(nonWalToken, day));
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(walToken, day));
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(walToken, day));
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(nonWalToken, day));
                Assert.assertTrue("the non-WAL request must dispatch the fold", job.run());
                Assert.assertTrue("the WAL request must dispatch the fold", job.run());
                Assert.assertFalse("the repeats must be drained as no-ops", job.run());
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(walToken)) {
                Assert.assertFalse("the WAL partition must be folded", reader.getTxFile().isPartitionComposite(0));
            }
            try (TableReader reader = engine.getReader(nonWalToken)) {
                Assert.assertFalse("the non-WAL partition must be folded", reader.getTxFile().isPartitionComposite(0));
            }
            engine.releaseAllReaders();
            Assert.assertEquals("exactly one staged copy per table is expected", 2, stagingMkdirs.get());

            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext, "SELECT * FROM cn ORDER BY ts, i", "SELECT * FROM cw ORDER BY ts, i", LOG
            );
        });
    }

    /**
     * The cold-storage upload refuses a parquet partition that is not compact and asks for it here. The request
     * rebuilds a lone parquet folder whatever its age - its file was written moments ago on the real clock, which
     * holds the sweep's idle gate shut - and with the sweep off, so neither the idle threshold nor the sweep's
     * budgets can keep the upload waiting.
     */
    @Test
    public void testRequestRebuildsALoneParquetPartitionThatIsNotCompact() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createIdleDirtyParquetTable("rq_px");
            final long nameTxnBefore;
            try (TableReader reader = engine.getReader(token)) {
                nameTxnBefore = reader.getTxFile().getPartitionNameTxn(0);
            }
            Assert.assertFalse("dead row groups make the partition not compact", isParquetPartitionCompact(token, 0));
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(token, day));
                Assert.assertTrue("the request must dispatch the rebuild", job.run());
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertNotEquals("the rebuild lands in a new directory", nameTxnBefore, tx.getPartitionNameTxn(0));
                Assert.assertTrue("the rebuilt partition must stay parquet", tx.isPartitionParquet(0));
            }
            assertUnusedBytes(token, 0, false);
            Assert.assertTrue("the rebuilt partition must be compact", isParquetPartitionCompact(token, 0));
            assertQuery("SELECT count() c FROM rq_px").noRandomAccess().expectSize().returns("c\n16\n");
        });
    }

    /**
     * A column added after the copy's build moves only the metadata version, which the install must check.
     */
    /**
     * The open transaction opened a new day and holds an O3 row for an earlier one, so the install commits it first
     * and must look the day up again: the commit moves it from index 0 to 1.
     */
    @Test
    public void testParquetCopyInstallCommitsAnOpenTransactionThatOpenedAPartition() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createParquetCopyTable("gp");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
                final String stagedBuild = stageParquetCopy(ownerWriter, token, day);
                appendRow(ownerWriter, "2020-01-04T00:00:00.000000Z", 70001);
                appendRow(ownerWriter, "2019-12-31T00:00:00.000000Z", 70002);
                Assert.assertTrue("the fixture must hold an O3 row", ownerWriter.hasO3());
                Assert.assertEquals("the in-order row must have opened a partition", 3, ownerWriter.getPartitionCount());

                ownerWriter.tick();
                Assert.assertFalse("the install must commit the open transaction first", ownerWriter.inTransaction());
                Assert.assertEquals("the commit must have moved the day", 1, ownerWriter.getTxWriter().getPartitionIndex(day));
                Assert.assertEquals(
                        "the install must take the copy",
                        "nameTxn=-1, rows=100, columnVersion=-1, metadataVersion=0, active=false, split=false," +
                                " parquet=false, copy=true, composite=false, remote=false",
                        parquetCopyInputs(ownerWriter, day)
                );
                Assert.assertFalse("no staged build may outlive the install", dirExists(configuration.getFilesFacade(), stagedBuild));
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertQuery("SELECT count() c FROM gp").noRandomAccess().expectSize().returns("c\n112\n");
        });
    }

    /**
     * The install's commit of the owner's open transaction fails in O3. The command queue swallows the failure, so the
     * writer must be left distressed: the owner's next commit has to fail rather than roll its rows back silently.
     */
    @Test
    public void testParquetCopyInstallDistressesTheWriterWhenItsCommitFails() throws Exception {
        final AtomicBoolean isO3Failing = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isO3Failing.get() && Utf8s.containsAscii(name, Files.SEPARATOR + "2019-12-31")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            final TableToken token = createParquetCopyTable("gf");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
                stageParquetCopy(ownerWriter, token, day);
                appendRow(ownerWriter, "2020-01-04T00:00:00.000000Z", 70_001);
                appendRow(ownerWriter, "2019-12-31T00:00:00.000000Z", 70_002);
                Assert.assertEquals("the in-order row must have opened a partition", 3, ownerWriter.getPartitionCount());

                isO3Failing.set(true);
                ownerWriter.tick();
                Assert.assertTrue("a failed commit must distress the writer", ownerWriter.isDistressed());
                try {
                    ownerWriter.commit();
                    Assert.fail("the owner's commit must not report success");
                } catch (CairoError e) {
                    TestUtils.assertContains(e.getMessage(), "distressed");
                }
                isO3Failing.set(false);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertQuery("SELECT count() c FROM gf").noRandomAccess().expectSize().returns("c\n110\n");
            execute("INSERT INTO gf VALUES (70_003, '2020-01-05T00:00:00.000000Z')");
            assertQuery("SELECT count() c FROM gf").noRandomAccess().expectSize().returns("c\n111\n");
        });
    }

    @Test
    public void testParquetCopyInstallRefusesAColumnAddedAfterItsBuild() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createParquetCopyTable("gm");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
                final String stagedBuild = stageParquetCopy(ownerWriter, token, day);
                ownerWriter.addColumn("j", ColumnType.INT, AllowAllSecurityContext.INSTANCE);
                Assert.assertEquals(
                        "only the metadata version must have moved",
                        "nameTxn=-1, rows=100, columnVersion=-1, metadataVersion=1, active=false, split=false," +
                                " parquet=false, copy=false, composite=false, remote=false",
                        parquetCopyInputs(ownerWriter, day)
                );
                assertParquetCopyRefused(ownerWriter, stagedBuild);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertParquetCopyRebuilt(token, day, 100);
            assertQuery("SELECT count() c, count(j) j FROM gm").noRandomAccess().expectSize().returns("c\tj\n110\t0\n");
        });
    }

    /**
     * An O3 row past the day's last row is appended in place, so only the row count tells the copy is stale.
     */
    @Test
    public void testParquetCopyInstallRefusesADayAppendedToAfterItsBuild() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createParquetCopyTable("ga");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
                final String stagedBuild = stageParquetCopy(ownerWriter, token, day);
                appendRow(ownerWriter, "2020-01-01T12:00:00.000000Z", 70001);
                ownerWriter.commit();
                Assert.assertEquals(
                        "only the row count must have moved",
                        "nameTxn=-1, rows=101, columnVersion=-1, metadataVersion=0, active=false, split=false," +
                                " parquet=false, copy=false, composite=false, remote=false",
                        parquetCopyInputs(ownerWriter, day)
                );
                assertParquetCopyRefused(ownerWriter, stagedBuild);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertParquetCopyRebuilt(token, day, 101);
            assertQuery("SELECT count() c FROM ga").noRandomAccess().expectSize().returns("c\n111\n");
        });
    }

    @Test
    public void testParquetCopyInstallRefusesADayDroppedAfterItsBuild() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createParquetCopyTable("gd");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
                final String stagedBuild = stageParquetCopy(ownerWriter, token, day);
                Assert.assertTrue(ownerWriter.removePartition(day));
                Assert.assertEquals("the day must be gone", "dropped", parquetCopyInputs(ownerWriter, day));
                assertParquetCopyRefused(ownerWriter, stagedBuild);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertParquetCopyRequestDeclined(token, day);
            assertQuery("SELECT count() c FROM gd").noRandomAccess().expectSize().returns("c\n10\n");
        });
    }

    /**
     * A WAL UPSERT replacing rows of a settled day merge-appends them: only the composite flag shows the copy stale.
     */
    @Test
    public void testParquetCopyInstallRefusesADayMadeCompositeAfterItsBuild() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        final AtomicInteger builtCopies = new AtomicInteger();
        final AtomicInteger upsertCount = new AtomicInteger();
        final AtomicReference<String> inputsAfterUpsert = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long length(LPSZ name) {
                final long length = super.length(name);
                if (length > 0
                        && Utf8s.containsAscii(name, TableUtils.GENERATING_DIR_MARKER)
                        && Utf8s.endsWithAscii(name, TableUtils.PARQUET_PARTITION_NAME)) {
                    builtCopies.incrementAndGet();
                }
                return length;
            }

            @Override
            public int mkdirs(Path path, int mode) {
                final int result = super.mkdirs(path, mode);
                if (upsertCount.get() == 0 && Utf8s.containsAscii(path, TableUtils.GENERATING_DIR_MARKER)) {
                    upsertCount.incrementAndGet();
                    // The build already holds its reader snapshot; the UPSERT lands before its install.
                    try {
                        execute("INSERT INTO gp SELECT x::INT + 5000 i," +
                                " timestamp_sequence('2020-01-01T00:10:00', 60*1000000L) ts FROM long_sequence(10)");
                        drainWalQueue();
                        try (TableWriter writer = engine.getWriter(engine.verifyTableName("gp"), "inputs")) {
                            inputsAfterUpsert.set(parquetCopyInputs(writer, MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z")));
                        }
                    } catch (Exception e) {
                        throw new AssertionError("UPSERT failed during the copy's build", e);
                    }
                }
                return result;
            }
        };

        assertMemoryLeak(ff, () -> {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            execute("CREATE TABLE gp AS (SELECT x::INT i," +
                    " timestamp_sequence('2020-01-01', 60*1000000L) ts FROM long_sequence(100))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
            execute("INSERT INTO gp SELECT x::INT + 90000 i," +
                    " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(10)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("gp");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            final String stagedBuild;
            try (TableWriter writer = engine.getWriter(token, "inputs")) {
                Assert.assertEquals(
                        "the day must start as one plain folder",
                        "nameTxn=-1, rows=100, columnVersion=-1, metadataVersion=0, active=false, split=false," +
                                " parquet=false, copy=false, composite=false, remote=false",
                        parquetCopyInputs(writer, day)
                );
                stagedBuild = generatingDir(token, ColumnType.TIMESTAMP, PartitionBy.DAY, day, writer.getTxWriter().getPartitionNameTxn(0));
            }

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetGeneration(token, day));
                job.run();
            }
            Assert.assertEquals("the build must reach the UPSERT interleaving", 1, upsertCount.get());
            Assert.assertTrue("the build must have produced the copy the install refuses", builtCopies.get() >= 1);
            Assert.assertEquals(
                    "only the composite flag must have moved",
                    "nameTxn=-1, rows=100, columnVersion=-1, metadataVersion=0, active=false, split=false," +
                            " parquet=false, copy=false, composite=true, remote=false",
                    inputsAfterUpsert.get()
            );
            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertFalse("the install must refuse the stale copy", tx.isPartitionParquetGenerated(0));
                Assert.assertTrue("the refused install must leave the day composite", tx.isPartitionComposite(0));
            }
            Assert.assertFalse("the refused install must remove the staged build", dirExists(ff, stagedBuild));
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertParquetCopyFoldsFirst(token, day, 100);
            assertQuery("SELECT count() c, min(i) lo, max(i) hi FROM gp WHERE ts < '2020-01-02'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\tlo\thi\n100\t1\t5010\n");
        });
    }

    /**
     * A parquet round trip after the build moves only the day's name txn.
     */
    @Test
    public void testParquetCopyInstallRefusesADayRewrittenAfterItsBuild() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createParquetCopyTable("gw");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
                final String stagedBuild = stageParquetCopy(ownerWriter, token, day);
                Assert.assertTrue(ownerWriter.convertPartitionNativeToParquet(day, null, Double.NaN));
                Assert.assertTrue(ownerWriter.convertPartitionParquetToNative(day));
                Assert.assertEquals(
                        "only the name txn must have moved",
                        "nameTxn=3, rows=100, columnVersion=-1, metadataVersion=0, active=false, split=false," +
                                " parquet=false, copy=false, composite=false, remote=false",
                        parquetCopyInputs(ownerWriter, day)
                );
                assertParquetCopyRefused(ownerWriter, stagedBuild);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertParquetCopyRebuilt(token, day, 100);
            assertQuery("SELECT count() c FROM gw").noRandomAccess().expectSize().returns("c\n110\n");
        });
    }

    /**
     * A copy is installed only on a day that is one plain folder; a split after the build makes the install refuse.
     */
    @Test
    public void testParquetCopyInstallRefusesADaySplitAfterItsBuild() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            execute("CREATE TABLE gn AS (SELECT x::INT i," +
                    " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760))" +
                    " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO gn SELECT x::INT + 90000 i," +
                    " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)");
            final TableToken token = engine.verifyTableName("gn");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            final String stagedBuild;
            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals("2020-01-01 must start as one folder", 2, tx.getPartitionCount());
                stagedBuild = generatingDir(
                        token, reader.getMetadata().getTimestampType(), reader.getPartitionedBy(), day, tx.getPartitionNameTxn(0)
                );
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
            final FilesFacade ff = configuration.getFilesFacade();
            try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetGeneration(token, day));
                runSweepOnAnotherThread(ff);
                Assert.assertTrue("the copy must be built and staged", dirExists(ff, stagedBuild));

                // Rows near the END of 2020-01-01 split it. A pinned reader keeps the commit from squashing the split.
                try (TableReader ignore = engine.getReader(token)) {
                    final long base = MicrosFormatUtils.parseTimestamp("2020-01-01T22:00:07.000000Z");
                    for (int k = 0; k < 200; k++) {
                        final TableWriter.Row row = ownerWriter.newRow(base + k * 5_000_000L);
                        row.putInt(0, 70_000 + k);
                        row.append();
                    }
                    ownerWriter.commit();
                }
                Assert.assertEquals("the O3 rows must have split 2020-01-01", 3, ownerWriter.getTxWriter().getPartitionCount());

                ownerWriter.tick();
                Assert.assertFalse("the install must refuse the split day", ownerWriter.getTxWriter().isPartitionParquetGenerated(0));
                Assert.assertEquals("the refused install must leave the day split", 3, ownerWriter.getTxWriter().getPartitionCount());
                Assert.assertFalse("the refused install must remove the staged build", dirExists(ff, stagedBuild));
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertParquetCopyFoldsFirst(token, day, 5960);
            assertQuery("SELECT count() c FROM gn").noRandomAccess().expectSize().returns("c\n6010\n");
        });
    }

    /**
     * A copy is never installed on the active partition, whose files take rows the copy never sees.
     */
    @Test
    public void testParquetCopyInstallRefusesADayThatBecameActiveAfterItsBuild() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createParquetCopyTable("gt");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
                final String stagedBuild = stageParquetCopy(ownerWriter, token, day);
                Assert.assertTrue(ownerWriter.removePartition(MicrosFormatUtils.parseTimestamp("2020-01-03T00:00:00.000000Z")));
                Assert.assertEquals(
                        "only the day's place in the table must have moved",
                        "nameTxn=-1, rows=100, columnVersion=-1, metadataVersion=0, active=true, split=false," +
                                " parquet=false, copy=false, composite=false, remote=false",
                        parquetCopyInputs(ownerWriter, day)
                );
                assertParquetCopyRefused(ownerWriter, stagedBuild);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertParquetCopyRequestDeclined(token, day);
            assertQuery("SELECT count() c FROM gt").noRandomAccess().expectSize().returns("c\n100\n");
        });
    }

    @Test
    public void testParquetCopyInstallRefusesADayUpdatedAfterItsBuild() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createParquetCopyTable("gu");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
                final String stagedBuild = stageParquetCopy(ownerWriter, token, day);
                try (
                        SqlCompiler compiler = engine.getSqlCompiler();
                        UpdateOperation update = compiler.compile(
                                "UPDATE gu SET i = i + 1000 WHERE ts < '2020-01-02'", sqlExecutionContext
                        ).getUpdateOperation()
                ) {
                    update.withContext(sqlExecutionContext);
                    Assert.assertEquals(100, ownerWriter.apply(update));
                }
                Assert.assertEquals(
                        "only the column version must have moved",
                        "nameTxn=-1, rows=100, columnVersion=2, metadataVersion=0, active=false, split=false," +
                                " parquet=false, copy=false, composite=false, remote=false",
                        parquetCopyInputs(ownerWriter, day)
                );
                assertParquetCopyRefused(ownerWriter, stagedBuild);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertParquetCopyRebuilt(token, day, 100);
            assertQuery("SELECT count() c, min(i) lo, max(i) hi FROM gu WHERE ts < '2020-01-02'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\tlo\thi\n100\t1001\t1100\n");
        });
    }

    /**
     * The upload's end state (remote native folder, no copy) is set the way Enterprise's
     * {@code StoragePolicyWriterCommand} sets it.
     */
    @Test
    public void testParquetCopyInstallRefusesADayUploadedAfterItsBuild() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createParquetCopyTable("gr");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
                final String stagedBuild = stageParquetCopy(ownerWriter, token, day);
                ownerWriter.getTxWriter().setPartitionRemote(0, true);
                ownerWriter.bumpPartitionTableVersion();
                Assert.assertEquals(
                        "only the uploaded flag must have moved",
                        "nameTxn=-1, rows=100, columnVersion=-1, metadataVersion=0, active=false, split=false," +
                                " parquet=false, copy=false, composite=false, remote=true",
                        parquetCopyInputs(ownerWriter, day)
                );
                assertParquetCopyRefused(ownerWriter, stagedBuild);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertParquetCopyRequestDeclined(token, day);
            assertQuery("SELECT count() c FROM gr").noRandomAccess().expectSize().returns("c\n110\n");
        });
    }

    /**
     * Two builds into the same staging directory can both be queued; the second install must refuse rather than clear
     * the installed {@code data.parquet}.
     */
    @Test
    public void testParquetCopyInstallRefusesADayWithACopyInstalledAfterItsBuild() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createParquetCopyTable("gc");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
                final String stagedBuild = stageParquetCopy(ownerWriter, token, day);
                Assert.assertEquals(stagedBuild, stageParquetCopy(ownerWriter, token, day));
                ownerWriter.tick();
                Assert.assertEquals(
                        "the first install must take the copy",
                        "nameTxn=-1, rows=100, columnVersion=-1, metadataVersion=0, active=false, split=false," +
                                " parquet=false, copy=true, composite=false, remote=false",
                        parquetCopyInputs(ownerWriter, day)
                );
                Assert.assertFalse("no staged build may outlive the installs", dirExists(configuration.getFilesFacade(), stagedBuild));
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertParquetCopyInstalled(token, day, 100);
            assertQuery("SELECT count() c FROM gc").noRandomAccess().expectSize().returns("c\n110\n");
        });
    }

    @Test
    public void testParquetCopyInstallRefusesAnOpenTransactionWritingTheDay() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createParquetCopyTable("go");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
                final String stagedBuild = stageParquetCopy(ownerWriter, token, day);
                appendRow(ownerWriter, "2020-01-01T00:30:30.000000Z", 70001);
                Assert.assertTrue(ownerWriter.hasO3());
                Assert.assertEquals(
                        "the open transaction must leave the committed day as the copy saw it",
                        "nameTxn=-1, rows=100, columnVersion=-1, metadataVersion=0, active=false, split=false," +
                                " parquet=false, copy=false, composite=false, remote=false",
                        parquetCopyInputs(ownerWriter, day)
                );
                assertParquetCopyRefused(ownerWriter, stagedBuild);
                Assert.assertTrue("the refused install must leave the transaction open", ownerWriter.inTransaction());
                ownerWriter.commit();
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertParquetCopyRebuilt(token, day, 101);
            assertQuery("SELECT count() c FROM go").noRandomAccess().expectSize().returns("c\n111\n");
        });
    }

    @Test
    public void testParquetCopyRequestFoldsASplitDayFirst() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createSplitDayTable("gx");
            final TableToken token = engine.verifyTableName("gx");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            final long liveRows;
            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals("the O3 insert must have split 2020-01-01", 3, tx.getPartitionCount());
                liveRows = tx.getPartitionSize(0) + tx.getPartitionSize(1);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
            assertParquetCopyFoldsFirst(token, day, liveRows);
            assertQuery("SELECT count() c FROM gx").noRandomAccess().expectSize().returns("c\n6010\n");
        });
    }

    /**
     * The stray-dir purge at writer open must keep a copy staging dir whose folder is still native and copy-less,
     * including a folder with no name txn suffix.
     */
    @Test
    public void testPurgeReclaimsAbandonedParquetCopyStagingButKeepsAnInFlightOne() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cg (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO cg VALUES (1, '2020-01-01T00:00:00'), (2, '2020-01-02T00:00:00')");

            final TableToken token = engine.verifyTableName("cg");
            final String inFlightDir;
            final String abandonedDir;
            try (TableReader reader = engine.getReader(token)) {
                final TxReader txReader = reader.getTxFile();
                final long partitionTimestamp = txReader.getPartitionTimestampByIndex(0);
                final long nameTxn = txReader.getPartitionNameTxn(0);
                Assert.assertEquals("the folder must never have moved", -1, nameTxn);
                final int timestampType = reader.getMetadata().getTimestampType();
                final int partitionBy = reader.getPartitionedBy();
                inFlightDir = generatingDir(token, timestampType, partitionBy, partitionTimestamp, nameTxn);
                // A name txn the live folder does not carry: the install would be refused as stale.
                abandonedDir = generatingDir(token, timestampType, partitionBy, partitionTimestamp, nameTxn + 5);
            }

            final FilesFacade ff = configuration.getFilesFacade();
            stageWithContent(ff, inFlightDir);
            stageWithContent(ff, abandonedDir);

            engine.releaseInactive();
            try (TableWriter ignore = engine.getWriter(token, "test")) {
                Assert.assertNotNull(ignore);
            }

            Assert.assertFalse("abandoned parquet copy staging directory survived the purge", dirExists(ff, abandonedDir));
            Assert.assertTrue("in-flight parquet copy staging directory was purged", dirExists(ff, inFlightDir));

            try (Path path = new Path()) {
                ff.rmdir(path.of(inFlightDir), false);
            }
        });
    }

    /**
     * A composite partition REWRITE stages its copy in a {@code .compacting<writerTxn>} directory that
     * {@link TableWriter#swapCompactedCompositePartition} either renames in or deletes. A crash between
     * the two leaves a full copy of the partition on disk that nothing ever adopts: its name carries a
     * writer txn rather than a partition name txn, so no later sweep reuses it. The writer's stray-dir
     * purge reclaims exactly the ones a swap could no longer accept, and leaves a build still in flight
     * (one whose writer txn still matches the live partition) alone.
     */
    @Test
    public void testPurgeReclaimsAbandonedStagingDirectoryButKeepsAnInFlightOne() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            execute("CREATE TABLE cx AS (SELECT x::INT i," +
                    " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            // A later, plain day, so 2020-01-01 is never the active partition and the backfill below
            // goes through the O3 path instead of an append.
            execute("INSERT INTO cx SELECT x::INT + 90000 i," +
                    " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)");
            drainWalQueue();
            execute("INSERT INTO cx SELECT x::INT + 70000 i," +
                    " timestamp_sequence('2020-01-01T04:00:07', 5*1000000L) ts FROM long_sequence(200)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("cx");
            final String inFlightDir;
            final String abandonedDir;
            try (TableReader reader = engine.getReader(token)) {
                final TxReader txReader = reader.getTxFile();
                Assert.assertTrue("2020-01-01 should be composite", txReader.isPartitionComposite(0));
                final long partitionTimestamp = txReader.getPartitionTimestampByIndex(0);
                final long nameTxn = txReader.getPartitionNameTxn(0);
                final long writerTxn = reader.getGeometry().getWriterTxn(0);
                final int timestampType = reader.getMetadata().getTimestampType();
                final int partitionBy = reader.getPartitionedBy();
                inFlightDir = stagingDir(token, timestampType, partitionBy, partitionTimestamp, nameTxn, writerTxn);
                // A writer txn the live partition no longer carries: this build's swap would be rejected
                // as stale, so nothing can ever adopt the directory.
                abandonedDir = stagingDir(token, timestampType, partitionBy, partitionTimestamp, nameTxn, writerTxn - 1);
            }

            final FilesFacade ff = configuration.getFilesFacade();
            stageWithContent(ff, inFlightDir);
            stageWithContent(ff, abandonedDir);

            // The stray-partition-dir purge runs when a writer opens the table.
            engine.releaseInactive();
            try (TableWriter ignore = engine.getWriter(token, "test")) {
                Assert.assertNotNull(ignore);
            }

            Assert.assertFalse("abandoned staging directory survived the purge", dirExists(ff, abandonedDir));
            Assert.assertTrue("in-flight staging directory was purged", dirExists(ff, inFlightDir));

            // Leave nothing behind for the suite's own checks.
            try (Path path = new Path()) {
                ff.rmdir(path.of(inFlightDir), false);
            }
        });
    }

    /**
     * Builds one composite partition in each of two SEPARATE tables: {@code cx}'s 2020-01-01 gets its
     * pieces at a long-ago simulated "wall clock" write time, {@code cy}'s 2020-01-09 only 20 minutes
     * before the job runs below - genuinely recent by BOTH measures at once, its own data timestamps AND
     * {@code _geometry}'s {@code lastWriteMicros}. Kept as two tables, not two partitions of one, because
     * {@link #dispatchComposite} runs through {@link TableWriter#compactPartitionNoCommit(int)} plus a
     * real {@link TableWriter#commit()}: that commit runs the writer's OWN per-commit housekeeping too,
     * which would freely sweep up any OTHER wall-clock-idle composite partition on the SAME writer,
     * regardless of this job's own recency filter - a separate table means {@code cx}'s compaction commit
     * cannot reach {@code cy} at all. A plain (never split) partition sits alongside each as a control.
     */
    @Test
    public void testScanCompactsIdleCompositePartitionButLeavesRecentAndPlainAlone() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            // cx's pieces land at this long-ago simulated wall-clock instant.
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));

            // One day at 15s, so the partition holds 5760 rows before anything backdated lands.
            final String dayABase = "SELECT x::INT i, timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760)";
            // A later, plain day - pushes the max timestamp forward so 2020-01-01 is never the active
            // partition, and the backfill below goes through the O3 path instead of an append.
            final String dayBPlain = "SELECT x::INT + 90000 i, timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)";
            // Lands ONLY inside 2020-01-01, cutting it into pieces (composite).
            final String dayABackfill = "SELECT x::INT + 70000 i, timestamp_sequence('2020-01-01T04:00:07', 5*1000000L) ts FROM long_sequence(200)";

            final String dayCBase = "SELECT x::INT + 200000 i, timestamp_sequence('2020-01-09', 15*1000000L) ts FROM long_sequence(5760)";
            final String dayDPlain = "SELECT x::INT + 290000 i, timestamp_sequence('2020-01-11', 60*1000000L) ts FROM long_sequence(50)";
            final String dayCBackfill = "SELECT x::INT + 270000 i, timestamp_sequence('2020-01-09T04:00:07', 5*1000000L) ts FROM long_sequence(200)";

            execute("CREATE TABLE cx AS (" + dayABase + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO cx " + dayBPlain);
            drainWalQueue();
            execute("INSERT INTO cx " + dayABackfill);
            drainWalQueue();

            // cy's pieces land only 20 minutes before the job runs below - well inside the 1-hour idle
            // window on both measures at once. A separate table, so this advance cannot make cx's OWN
            // composite partition look any more idle than it already is.
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-09T23:50:00.000000Z"));
            execute("CREATE TABLE cy AS (" + dayCBase + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO cy " + dayDPlain);
            drainWalQueue();
            execute("INSERT INTO cy " + dayCBackfill);
            drainWalQueue();

            final TableToken cxToken = engine.verifyTableName("cx");
            final TableToken cyToken = engine.verifyTableName("cy");
            final long liveRowsABefore;
            final long liveRowsCBefore;
            try (TableReader cxReader = engine.getReader(cxToken); TableReader cyReader = engine.getReader(cyToken)) {
                final TxReader cxTx = cxReader.getTxFile();
                Assert.assertEquals(2, cxTx.getPartitionCount());
                Assert.assertTrue("2020-01-01 should be composite", cxTx.isPartitionComposite(0));
                Assert.assertTrue("2020-01-01 should have more than one piece", cxReader.getGeometry().getPieceCount(0) > 1);
                Assert.assertFalse("2020-01-03 is plain, never split", cxTx.isPartitionComposite(1));

                final TxReader cyTx = cyReader.getTxFile();
                Assert.assertEquals(2, cyTx.getPartitionCount());
                Assert.assertTrue("2020-01-09 should be composite", cyTx.isPartitionComposite(0));
                Assert.assertTrue("2020-01-09 should have more than one piece", cyReader.getGeometry().getPieceCount(0) > 1);
                Assert.assertFalse("2020-01-11 is plain, never split", cyTx.isPartitionComposite(1));

                liveRowsABefore = cxTx.getPartitionSize(0);
                liveRowsCBefore = cyTx.getPartitionSize(0);
            }
            Assert.assertEquals(5960, liveRowsABefore);
            Assert.assertEquals(5960, liveRowsCBefore);

            // 1 hour: long enough that anything written back at 2020-01-01T00:00 is idle by the time the
            // job runs, short enough that cy's pieces - built only 20 minutes before "now" below - still
            // count as too recent, on both the data-timestamp recency check and the writer's own
            // _geometry-based age rule.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader cxReader = engine.getReader(cxToken); TableReader cyReader = engine.getReader(cyToken)) {
                final TxReader cxTx = cxReader.getTxFile();
                Assert.assertFalse("2020-01-01 is idle, should have been compacted", cxTx.isPartitionComposite(0));
                Assert.assertEquals(1, cxReader.getGeometry().getPieceCount(0));
                Assert.assertEquals("compaction must not change the row count", liveRowsABefore, cxTx.getPartitionSize(0));
                Assert.assertFalse("2020-01-03 is plain, must stay untouched", cxTx.isPartitionComposite(1));

                final TxReader cyTx = cyReader.getTxFile();
                Assert.assertTrue("2020-01-09 is too recent, must stay composite", cyTx.isPartitionComposite(0));
                Assert.assertTrue(cyReader.getGeometry().getPieceCount(0) > 1);
                Assert.assertEquals("a skipped partition's row count must be unchanged", liveRowsCBefore, cyTx.getPartitionSize(0));
                Assert.assertFalse("2020-01-11 is plain, must stay untouched", cyTx.isPartitionComposite(1));
            }

            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n6010\n");
            assertQuery("SELECT count() c FROM cy").noRandomAccess().expectSize().returns("c\n6010\n");

            execute("CREATE TABLE cx_oracle AS (SELECT i, ts FROM (" +
                    dayABase + " UNION ALL " + dayBPlain + " UNION ALL " + dayABackfill +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext, "SELECT * FROM cx_oracle ORDER BY ts, i", "SELECT * FROM cx ORDER BY ts, i", LOG
            );

            execute("CREATE TABLE cy_oracle AS (SELECT i, ts FROM (" +
                    dayCBase + " UNION ALL " + dayDPlain + " UNION ALL " + dayCBackfill +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext, "SELECT * FROM cy_oracle ORDER BY ts, i", "SELECT * FROM cy ORDER BY ts, i", LOG
            );
        });
    }

    /**
     * TWO idle composite partitions, both larger than the table's small last partition, both compacted by
     * a single sweep, with the writer taken fresh from the pool so
     * {@code TableWriter.restorePostingIndexersToLastPartition} finds no open active partition to rebind
     * the posting indexers to after each REWRITE's reseal.
     * <p>
     * That stale binding is real here (the restore does bail out), so this pins the shape in which it
     * could do damage: whatever the next partition's swap commits must not disturb the chain the previous
     * REWRITE just rebuilt. A seeded {@code DedupInsertFuzzTest} run on the pre-{@code
     * CompositePartitionSwapCommand} compaction job hit exactly that - the job's own trailing {@code
     * commit()} ran {@code updateIndexes}, refreshing the stale-bound indexer with the LAST partition's
     * row range and truncating the rebuilt chain at that row count, so index-backed scans silently lost
     * every row above it. The non-blocking swap commits through {@code
     * commitTxWriterAndPublishPendingPostingSealPurges} instead and never reaches {@code updateIndexes},
     * which is why this test passes with or without the zero-row guards in {@code SymbolColumnIndexer} and
     * {@code TableWriter.updateIndexesSlow} - it is coverage for the shape, not a regression test that
     * fails when those guards are removed.
     */
    @Test
    public void testScanCompactingTwoCompositePartitionsKeepsBothPostingIndexesIntact() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));

            // Two big days, each 5760 rows - both far larger than the 50-row last partition whose row
            // count the stale refresh clamps a rebuilt chain to.
            final String dayOneBase = "SELECT x::INT i, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760)";
            final String dayTwoBase = "SELECT x::INT + 10000 i, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence('2020-01-02', 15*1000000L) ts FROM long_sequence(5760)";
            // A later, tiny, plain day - keeps both days above out of the active slot so their backfills
            // go through the O3 composite path.
            final String dayPlain = "SELECT x::INT + 90000 i, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence('2020-01-05', 60*1000000L) ts FROM long_sequence(50)";
            // One backfill per big day, cutting each into pieces.
            final String dayOneBackfill = "SELECT x::INT + 70000 i, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence('2020-01-01T04:00:07', 5*1000000L) ts FROM long_sequence(200)";
            final String dayTwoBackfill = "SELECT x::INT + 80000 i, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence('2020-01-02T04:00:07', 5*1000000L) ts FROM long_sequence(200)";

            execute("CREATE TABLE cx AS (" + dayOneBase + "), index(sym_posting TYPE POSTING)" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO cx " + dayTwoBase);
            execute("INSERT INTO cx " + dayPlain);
            drainWalQueue();
            execute("INSERT INTO cx " + dayOneBackfill);
            execute("INSERT INTO cx " + dayTwoBackfill);
            drainWalQueue();

            final TableToken cxToken = engine.verifyTableName("cx");
            try (TableReader cxReader = engine.getReader(cxToken)) {
                final TxReader cxTx = cxReader.getTxFile();
                Assert.assertTrue("2020-01-01 should be composite", cxTx.isPartitionComposite(0));
                Assert.assertTrue("2020-01-02 should be composite", cxTx.isPartitionComposite(1));
            }

            // Drop the writer so the sweep below takes a fresh one from the pool, with no open active
            // partition - the state in which restorePostingIndexersToLastPartition cannot rebind.
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader cxReader = engine.getReader(cxToken)) {
                final TxReader cxTx = cxReader.getTxFile();
                Assert.assertFalse("2020-01-01 is idle, should have been REWRITE-compacted", cxTx.isPartitionComposite(0));
                Assert.assertFalse("2020-01-02 is idle, should have been REWRITE-compacted", cxTx.isPartitionComposite(1));
            }

            execute("CREATE TABLE cx_oracle AS (SELECT i, sym_posting, ts FROM (" +
                    dayOneBase + " UNION ALL " + dayTwoBase + " UNION ALL " + dayPlain +
                    " UNION ALL " + dayOneBackfill + " UNION ALL " + dayTwoBackfill +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            // The indexed scan must return exactly what the unindexed oracle holds. A truncated chain
            // shows up here as missing rows from whichever partition the refresh clamped.
            for (int k = 0; k < 7; k++) {
                final String value = "sym" + k;
                TestUtils.assertSqlCursors(
                        engine, sqlExecutionContext,
                        "SELECT i, sym_posting::varchar sym_posting, ts FROM cx_oracle" +
                                " WHERE sym_posting = '" + value + "' ORDER BY ts, i",
                        "SELECT i, sym_posting::varchar sym_posting, ts FROM cx" +
                                " WHERE sym_posting = '" + value + "' ORDER BY ts, i",
                        LOG
                );
            }
        });
    }

    /**
     * Same fixture shape as {@link #testScanCompactsIdleCompositePartitionButLeavesRecentAndPlainAlone}, but
     * the composite partition also carries a BITMAP-indexed symbol column and a POSTING-indexed one.
     * {@link TableWriter#compactPartitionNoCommit(int)} - the job's one dispatch path for a composite
     * partition - must leave both indexes able to find every row after the REWRITE, not just the row count
     * and the raw column bytes right.
     */
    @Test
    public void testScanCompactsIdleCompositePartitionPreservesIndexes() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));

            final String dayBase = "SELECT x::INT i, ('sym' || (x % 7))::symbol sym_bitmap, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760)";
            // Later, plain day - pushes the max timestamp forward so 2020-01-01 is never the active
            // partition, and the backfill below goes through the O3 path instead of an append.
            final String dayPlain = "SELECT x::INT + 90000 i, ('sym' || (x % 7))::symbol sym_bitmap, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)";
            // Lands ONLY inside 2020-01-01, cutting it into pieces (composite).
            final String dayBackfill = "SELECT x::INT + 70000 i, ('sym' || (x % 7))::symbol sym_bitmap, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence('2020-01-01T04:00:07', 5*1000000L) ts FROM long_sequence(200)";

            execute("CREATE TABLE cx AS (" + dayBase + "), index(sym_bitmap), index(sym_posting TYPE POSTING)" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO cx " + dayPlain);
            drainWalQueue();
            execute("INSERT INTO cx " + dayBackfill);
            drainWalQueue();

            final TableToken cxToken = engine.verifyTableName("cx");
            try (TableReader cxReader = engine.getReader(cxToken)) {
                final TxReader cxTx = cxReader.getTxFile();
                Assert.assertTrue("2020-01-01 should be composite", cxTx.isPartitionComposite(0));
                Assert.assertTrue("2020-01-01 should have more than one piece", cxReader.getGeometry().getPieceCount(0) > 1);
            }

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader cxReader = engine.getReader(cxToken)) {
                Assert.assertFalse("2020-01-01 is idle, should have been REWRITE-compacted", cxReader.getTxFile().isPartitionComposite(0));
            }

            execute("CREATE TABLE cx_oracle AS (SELECT i, sym_bitmap, sym_posting, ts FROM (" +
                    dayBase + " UNION ALL " + dayPlain + " UNION ALL " + dayBackfill +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            for (int k = 0; k < 7; k++) {
                final String value = "sym" + k;
                // UNION ALL of independently symbol-typed subqueries widens the column to VARCHAR in the
                // oracle, so both sides are cast to VARCHAR here purely to line up the comparison's column
                // types - the filter itself still exercises each table's own indexed SYMBOL column.
                TestUtils.assertSqlCursors(
                        engine, sqlExecutionContext,
                        "SELECT i, sym_bitmap::varchar sym_bitmap, sym_posting::varchar sym_posting, ts FROM cx_oracle" +
                                " WHERE sym_bitmap = '" + value + "' ORDER BY ts, i",
                        "SELECT i, sym_bitmap::varchar sym_bitmap, sym_posting::varchar sym_posting, ts FROM cx" +
                                " WHERE sym_bitmap = '" + value + "' ORDER BY ts, i",
                        LOG
                );
                TestUtils.assertSqlCursors(
                        engine, sqlExecutionContext,
                        "SELECT i, sym_bitmap::varchar sym_bitmap, sym_posting::varchar sym_posting, ts FROM cx_oracle" +
                                " WHERE sym_posting = '" + value + "' ORDER BY ts, i",
                        "SELECT i, sym_bitmap::varchar sym_bitmap, sym_posting::varchar sym_posting, ts FROM cx" +
                                " WHERE sym_posting = '" + value + "' ORDER BY ts, i",
                        LOG
                );
            }
        });
    }

    /**
     * Same as {@link #testScanCompactsIdleCompositePartitionPreservesIndexes}, except the composite
     * partition being compacted is the table's only - and therefore ACTIVE - partition, exercising
     * {@link TableWriter#compactPartitionNoCommit(int)}'s other branch: closing and reopening the writer's
     * own column handles against the freshly compacted directory (mirrors
     * {@code UpdateTest#testUpdateOnActiveCompositePartition}, which checks the writer stays usable for
     * plain appends afterward but carries no indexed column). Here the check is the index, not the append:
     * both the BITMAP and the POSTING index must still find every row afterward.
     */
    @Test
    public void testScanCompactsIdleActiveCompositePartitionPreservesIndexes() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("1970-01-01T00:00:00.000000Z"));

            // Small partition, well under a day - mirrors UpdateTest#testUpdateOnActiveCompositePartition's
            // proven recipe for a composite ACTIVE partition (a single PARTITION BY DAY partition is, by
            // construction, the active one throughout).
            final String dayBase = "SELECT x::INT i, ('sym' || (x % 7))::symbol sym_bitmap, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence(0, 15*1000000L) ts FROM long_sequence(400)";
            // A small batch landing well inside the partition's own range - forces a merge-append there,
            // leaving the ACTIVE partition composite.
            final String dayBackfill = "SELECT x::INT + 70000 i, ('sym' || (x % 7))::symbol sym_bitmap, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence('1970-01-01T00:20:07', 1000000L) ts FROM long_sequence(3)";

            execute("CREATE TABLE cx AS (" + dayBase + "), index(sym_bitmap), index(sym_posting TYPE POSTING)" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("INSERT INTO cx " + dayBackfill);
            drainWalQueue();

            final TableToken cxToken = engine.verifyTableName("cx");
            try (TableReader cxReader = engine.getReader(cxToken)) {
                final TxReader cxTx = cxReader.getTxFile();
                Assert.assertEquals("cx must have exactly one, the active, partition", 1, cxTx.getPartitionCount());
                Assert.assertTrue("1970-01-01 should be composite", cxTx.isPartitionComposite(0));
                Assert.assertTrue("1970-01-01 should have more than one piece", cxReader.getGeometry().getPieceCount(0) > 1);
            }

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("1970-01-01T03:00:00.000000Z"));

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader cxReader = engine.getReader(cxToken)) {
                Assert.assertFalse(
                        "the active partition is idle past the timeout, should have been REWRITE-compacted",
                        cxReader.getTxFile().isPartitionComposite(0)
                );
            }

            // The writer must still be usable for an ordinary append after reopening its active-partition
            // column handles - see UpdateTest#testUpdateOnActiveCompositePartition for the same check.
            final String dayAppend = "SELECT x::INT + 90000 i, ('sym' || (x % 7))::symbol sym_bitmap, ('sym' || (x % 7))::symbol sym_posting," +
                    " timestamp_sequence('1970-01-01T04:00:00', 1000000L) ts FROM long_sequence(5)";
            execute("INSERT INTO cx " + dayAppend);
            drainWalQueue();

            execute("CREATE TABLE cx_oracle AS (SELECT i, sym_bitmap, sym_posting, ts FROM (" +
                    dayBase + " UNION ALL " + dayBackfill + " UNION ALL " + dayAppend +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            for (int k = 0; k < 7; k++) {
                final String value = "sym" + k;
                TestUtils.assertSqlCursors(
                        engine, sqlExecutionContext,
                        "SELECT i, sym_bitmap::varchar sym_bitmap, sym_posting::varchar sym_posting, ts FROM cx_oracle" +
                                " WHERE sym_bitmap = '" + value + "' ORDER BY ts, i",
                        "SELECT i, sym_bitmap::varchar sym_bitmap, sym_posting::varchar sym_posting, ts FROM cx" +
                                " WHERE sym_bitmap = '" + value + "' ORDER BY ts, i",
                        LOG
                );
                TestUtils.assertSqlCursors(
                        engine, sqlExecutionContext,
                        "SELECT i, sym_bitmap::varchar sym_bitmap, sym_posting::varchar sym_posting, ts FROM cx_oracle" +
                                " WHERE sym_posting = '" + value + "' ORDER BY ts, i",
                        "SELECT i, sym_bitmap::varchar sym_bitmap, sym_posting::varchar sym_posting, ts FROM cx" +
                                " WHERE sym_posting = '" + value + "' ORDER BY ts, i",
                        LOG
                );
            }
        });
    }

    /**
     * A composite ACTIVE partition leaves {@code columns[]} closed, so {@code TableWriter#finishO3Commit}
     * skips its usual {@code openPartition} call for it - the one call that reconfigures every indexer's
     * live writer. POSTING indexers get their own fixup there ({@code sealPostingIndexesForO3Partitions},
     * chain-based, independent of {@code columns[]}); a BITMAP indexer has no such path and relied solely
     * on {@code openPartition}, so its underlying {@code BitmapIndexWriter} was left exactly as an earlier
     * {@code closeActivePartition} call left it - closed, its key-file memory unmapped.
     * <p>
     * Shape: {@code sym_late} (BITMAP) is added while day0 is still plain, a real follower gets wired up.
     * A backfill inside day0 then forces a merge-append there, making it composite while still active -
     * closing that follower with nothing to reopen it, since day0 stays the active partition. day1 is then
     * created and becomes composite shortly after its own birth (a row landing earlier within day1 itself).
     * With day1 now active and day0 idle, {@link PartitionCompactionScanJob} REWRITE-compacts day0 (the
     * non-active branch, which does not reopen the ACTIVE partition's indexers) and commits - and that
     * commit's own indexing pass dereferences {@code sym_late}'s still-unmapped {@code BitmapIndexWriter}.
     */
    @Test
    public void testScanCompactsIdleNonActiveCompositePartitionSurvivesStaleBitmapIndexerAcrossPartitionSwitch() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("1970-01-01T00:00:00.000000Z"));

            final String day0Base = "SELECT x::INT i, timestamp_sequence(0, 15*1000000L) ts FROM long_sequence(400)";

            execute("CREATE TABLE cx AS (" + day0Base + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // Column added while day0 is still plain - a real follower gets wired up.
            execute("ALTER TABLE cx ADD COLUMN sym_late SYMBOL INDEX");
            drainWalQueue();

            // Backfill inside day0 - forces a merge-append there, leaving it composite while still active.
            execute("INSERT INTO cx SELECT x::INT + 70000 i, timestamp_sequence('1970-01-01T00:20:07', 1000000L) ts," +
                    " NULL sym_late FROM long_sequence(3)");
            drainWalQueue();

            final TableToken cxToken = engine.verifyTableName("cx");
            try (TableReader cxReader = engine.getReader(cxToken)) {
                final TxReader cxTx = cxReader.getTxFile();
                Assert.assertEquals("cx must have exactly one, the active, partition", 1, cxTx.getPartitionCount());
                Assert.assertTrue("day0 should be composite", cxTx.isPartitionComposite(0));
            }

            // day1's own first commit creates it; the second lands earlier within day1 itself - a real
            // merge-append there, so day1 is composite from shortly after its own birth, not plain.
            execute("INSERT INTO cx SELECT x::INT + 80000 i, timestamp_sequence('1970-01-02T00:00:10', 1000000L) ts," +
                    " NULL sym_late FROM long_sequence(50)");
            drainWalQueue();
            execute("INSERT INTO cx SELECT x::INT + 90000 i, timestamp_sequence('1970-01-02T00:00:00', 1000000L) ts," +
                    " NULL sym_late FROM long_sequence(3)");
            drainWalQueue();

            try (TableReader cxReader = engine.getReader(cxToken)) {
                final TxReader cxTx = cxReader.getTxFile();
                Assert.assertEquals("cx must have two partitions now", 2, cxTx.getPartitionCount());
                Assert.assertTrue("day0 should still be composite", cxTx.isPartitionComposite(0));
            }

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("1970-01-05T00:00:00.000000Z"));

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            final TableToken cxTokenAfter = engine.verifyTableName("cx");
            Assert.assertFalse("the compaction commit suspended the table",
                    engine.getTableSequencerAPI().isSuspended(cxTokenAfter));
        });
    }

    /**
     * A cold-opened {@link TableWriter} - the writer pool had evicted it, so the next access builds a fresh
     * instance - never configures a BITMAP indexer for its own ACTIVE partition when that partition is
     * ALREADY composite at open time: {@code initLastPartition} calls
     * {@code openLastPartitionAndSetAppendPosition}, which is a no-op for a composite last partition (see
     * its own javadoc - nothing appends to one in place), so {@code openPartition}'s
     * {@code indexer.configureFollowerAndWriter} call never runs. But {@code initLastPartition} calls
     * {@code populateDenseIndexerList()} right after regardless, which adds every non-null
     * {@code ColumnIndexer} to {@code denseIndexers} without checking whether it was ever configured - so
     * the still-pristine indexer, its {@code BitmapIndexWriter} never {@code of()}-ed, lands in the set
     * {@code commit()} indexes on. Any later commit on this writer - even
     * {@link PartitionCompactionScanJob#dispatchComposite}'s, compacting a wholly different, non-active,
     * idle partition - reaches {@code updateIndexesSlow} and dereferences that indexer's unmapped key file,
     * an {@link AssertionError} deep in {@code AbstractMemoryCR.addressOf}.
     */
    @Test
    public void testScanCommitCrashesOnColdOpenedWriterWithComposedActivePartition() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("1970-01-01T00:00:00.000000Z"));

            execute("CREATE TABLE cx AS (" +
                    "SELECT x::INT i, timestamp_sequence(0, 15*1000000L) ts FROM long_sequence(400)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // Added after the table already holds rows - carries a real column top, matching a
            // fuzz-generated table's own "_top" column idiom.
            execute("ALTER TABLE cx ADD COLUMN sym_top SYMBOL INDEX");
            drainWalQueue();

            // Backfill inside day0 - forces a merge-append there, leaving it composite. day0 is still the
            // only, active, partition at this point.
            execute("INSERT INTO cx SELECT x::INT + 70000 i, timestamp_sequence('1970-01-01T00:20:07', 1000000L) ts," +
                    " ('sym' || (x % 7))::symbol sym_top FROM long_sequence(3)");
            drainWalQueue();

            // day1's own first commit creates it, pushing day0 out of the active slot. The second lands
            // earlier within day1 itself - a real merge-append there, so the NEW active partition, day1,
            // is ALSO composite - the shape a cold-opened writer never initializes an indexer for.
            execute("INSERT INTO cx SELECT x::INT + 80000 i, timestamp_sequence('1970-01-02T00:00:10', 1000000L) ts," +
                    " ('sym' || (x % 7))::symbol sym_top FROM long_sequence(50)");
            drainWalQueue();
            execute("INSERT INTO cx SELECT x::INT + 90000 i, timestamp_sequence('1970-01-02T00:00:00', 1000000L) ts," +
                    " ('sym' || (x % 7))::symbol sym_top FROM long_sequence(3)");
            drainWalQueue();

            final TableToken cxToken = engine.verifyTableName("cx");
            try (TableReader cxReader = engine.getReader(cxToken)) {
                final TxReader cxTx = cxReader.getTxFile();
                Assert.assertEquals("cx must have two partitions now", 2, cxTx.getPartitionCount());
                Assert.assertTrue("day0 should be composite", cxTx.isPartitionComposite(0));
                Assert.assertTrue("day1, the active partition, should also be composite", cxTx.isPartitionComposite(1));
            }

            // Evict the writer from the pool - the writer pool does this itself once WAL apply's own
            // time-quota ejects a table mid-fuzz-run (see the "WriterPool closed [... reason=IDLE]" log
            // line that precedes the crash in the wild). The NEXT access below cold-opens a fresh
            // TableWriter while day1 is still composite.
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("1970-01-05T00:00:00.000000Z"));

            // Only day0 (non-active, composite, idle) is a candidate here - day1 is still the active
            // partition and stays well inside the recency window. dispatchComposite cold-opens the writer
            // (day1 still composite at that point), compacts day0 (unrelated to day1), then commits - the
            // commit is where indexing dereferences day1's never-configured sym_top indexer.
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            final TableToken cxTokenAfter = engine.verifyTableName("cx");
            Assert.assertFalse("the compaction commit suspended the table",
                    engine.getTableSequencerAPI().isSuspended(cxTokenAfter));
        });
    }

    /**
     * A Parquet partition with dead row-group bytes below the automatic rewrite ratio gets rewritten once
     * idle; a second, never-updated Parquet partition - equally idle, but with nothing to reclaim - is left
     * exactly as it was (same name txn, zero dead bytes throughout).
     */
    @Test
    public void testScanCompactsIdleDirtyParquetPartitionButLeavesCleanOneAlone() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));

            execute("CREATE TABLE px (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    "INSERT INTO px(a, ts) VALUES" +
                            "(1,  '2020-01-01T00:00:00.000Z')," +
                            "(2,  '2020-01-01T01:00:00.000Z')," +
                            "(3,  '2020-01-01T02:00:00.000Z')," +
                            "(4,  '2020-01-01T03:00:00.000Z')," +
                            "(5,  '2020-01-01T04:00:00.000Z')," +
                            "(6,  '2020-01-01T05:00:00.000Z')," +
                            "(7,  '2020-01-01T06:00:00.000Z')," +
                            "(8,  '2020-01-01T07:00:00.000Z')," +
                            "(9,  '2020-01-01T08:00:00.000Z')," +
                            "(10, '2020-01-01T09:00:00.000Z')," +
                            "(11, '2020-01-01T10:00:00.000Z')," +
                            "(12, '2020-01-01T11:00:00.000Z')"
            );
            // Pusher day, so 2020-01-01 is inactive by the time it is converted below.
            execute("INSERT INTO px(a, ts) VALUES (90, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE px CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Three in-place O3 updates: each appends a merged row group and leaves the one it replaced
            // as dead bytes. Ratio/max-bytes thresholds are disabled above, so none auto-rewrites.
            execute("INSERT INTO px(a, ts) VALUES (101, '2020-01-01T01:30:00.000Z')");
            drainWalQueue();
            execute("INSERT INTO px(a, ts) VALUES (102, '2020-01-01T02:30:00.000Z')");
            drainWalQueue();
            execute("INSERT INTO px(a, ts) VALUES (103, '2020-01-01T03:30:00.000Z')");
            drainWalQueue();

            // A second, CLEAN Parquet partition: converted, never touched by an O3 update afterward.
            execute("INSERT INTO px(a, ts) VALUES (200, '2020-01-03T00:00:00.000Z')");
            execute("INSERT INTO px(a, ts) VALUES (400, '2020-01-04T00:00:00.000Z')"); // pusher
            drainWalQueue();
            execute("ALTER TABLE px CONVERT PARTITION TO PARQUET LIST '2020-01-03'");
            drainWalQueue();

            final TableToken pxToken = engine.verifyTableName("px");
            final long cleanNameTxnBefore;
            try (TableReader reader = engine.getReader(pxToken)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertTrue(tx.isPartitionParquet(0));
                Assert.assertTrue(tx.isPartitionParquet(2));
                cleanNameTxnBefore = tx.getPartitionNameTxn(2);
            }
            assertUnusedBytes(pxToken, 0, true);
            assertUnusedBytes(pxToken, 2, false);

            // The ratio/max-bytes thresholds above were disabled only so the 3 O3 updates could build up
            // dead bytes without the automatic mid-commit rewrite already reclaiming them. The idle scan
            // job reuses these exact same keys (see PartitionCompactionScanJob), so re-tighten them now,
            // after every write above has already landed, to the values the job itself should act on.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "0.01");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, "1");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));

            // The parquet branch's idle gate reads the .parquet file's modification time, which is real
            // wall-clock time no matter what the simulated clock says. The three O3 updates above wrote
            // the file moments ago, so a sweep on the real clock leaves it alone - see
            // ParquetPartitionCompactionTest#testIdleSweepSkipsAPartitionWrittenToWithinTheIdleTimeout -
            // and the job's own clock has to sit past the idle timeout for the partition to count as idle.
            final Clock pastTheIdleTimeout = () -> MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, configuration.getFilesFacade(), pastTheIdleTimeout)) {
                job.run();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertUnusedBytes(pxToken, 0, false);
            assertUnusedBytes(pxToken, 2, false);
            try (TableReader reader = engine.getReader(pxToken)) {
                Assert.assertEquals(
                        "clean partition must not have been rewritten",
                        cleanNameTxnBefore, reader.getTxFile().getPartitionNameTxn(2)
                );
            }

            assertQuery("SELECT count() c FROM px").noRandomAccess().expectSize().returns("c\n18\n");
        });
    }

    /**
     * A frozen partition is compacted like any other - the freeze keeps writes out, not the sweep - and the swap
     * carries the READ ONLY flag over to the rebuilt directory's {@code _txn} entry, in the commit that swaps it in.
     */
    @Test
    public void testScanCompactsAReadOnlyParquetPartitionAndKeepsItReadOnly() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createIdleDirtyParquetTable("ro_px");
            final long nameTxnBefore;
            try (TableWriter writer = engine.getWriter(token, "test-freeze")) {
                writer.getTxWriter().setPartitionReadOnly(0, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
                nameTxnBefore = writer.getTxWriter().getPartitionNameTxn(0);
            }
            assertUnusedBytes(token, 0, true);

            sweepPastTheIdleTimeout();

            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertNotEquals("the sweep must rebuild a read-only partition", nameTxnBefore, tx.getPartitionNameTxn(0));
                Assert.assertTrue("the rebuilt partition must be parquet", tx.isPartitionParquet(0));
                Assert.assertTrue("the rebuilt partition must stay READ ONLY", tx.isPartitionReadOnly(0));
                Assert.assertFalse("the next day must not take the flag", tx.isPartitionReadOnly(1));
            }
            assertUnusedBytes(token, 0, false);
            assertQuery("SELECT count() c FROM ro_px").noRandomAccess().expectSize().returns("c\n16\n");
        });
    }

    /**
     * Composite twin of {@link #testScanCompactsAReadOnlyParquetPartitionAndKeepsItReadOnly}: the REWRITE lands
     * through {@link TableWriter#swapCompactedCompositePartition}, which must carry the flag over the same way.
     */
    @Test
    public void testScanCompactsAReadOnlyCompositePartitionAndKeepsItReadOnly() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));

            final String dayBase = "SELECT x::INT i, timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760)";
            final String dayPlain = "SELECT x::INT + 90000 i, timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)";
            final String dayBackfill = "SELECT x::INT + 70000 i, timestamp_sequence('2020-01-01T04:00:07', 5*1000000L) ts FROM long_sequence(200)";

            execute("CREATE TABLE ro_cx AS (" + dayBase + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO ro_cx " + dayPlain);
            drainWalQueue();
            execute("INSERT INTO ro_cx " + dayBackfill);
            drainWalQueue();

            final TableToken token = engine.verifyTableName("ro_cx");
            final long liveRowsBefore;
            final long nameTxnBefore;
            try (TableWriter writer = engine.getWriter(token, "test-freeze")) {
                Assert.assertTrue("2020-01-01 should be composite", writer.getTxWriter().isPartitionComposite(0));
                Assert.assertTrue("fixture must require REWRITE, not MAKE-PLAIN", writer.getGeometry().getPieceCount(0) > 1);
                writer.getTxWriter().setPartitionReadOnly(0, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
                liveRowsBefore = writer.getTxWriter().getPartitionSize(0);
                nameTxnBefore = writer.getTxWriter().getPartitionNameTxn(0);
            }
            Assert.assertEquals(5960, liveRowsBefore);

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertFalse("the sweep must REWRITE a read-only composite partition", tx.isPartitionComposite(0));
                Assert.assertNotEquals("a REWRITE lands in a new directory", nameTxnBefore, tx.getPartitionNameTxn(0));
                Assert.assertTrue("the rewritten partition must stay READ ONLY", tx.isPartitionReadOnly(0));
                Assert.assertFalse("the next day must not take the flag", tx.isPartitionReadOnly(1));
                Assert.assertEquals(liveRowsBefore, tx.getPartitionSize(0));
            }
            assertQuery("SELECT count() c FROM ro_cx").noRandomAccess().expectSize().returns("c\n6010\n");
        });
    }

    @Test
    public void testScanLeavesASoftLinkedParquetPartitionAlone() throws Exception {
        // Windows: ff.isSoftLink() is always false, so ATTACH does not mark a soft-linked partition READ ONLY.
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            final TableToken token = createIdleDirtyParquetTable("sl_px");
            execute("ALTER TABLE sl_px DETACH PARTITION LIST '2020-01-01'");
            drainWalQueue();
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            final FilesFacade ff = configuration.getFilesFacade();
            final String otherVolume = temp.newFolder("sl_px_other_volume").getAbsolutePath();
            try (Path src = new Path(); Path dst = new Path()) {
                src.of(configuration.getDbRoot()).concat(token).concat("2020-01-01").put(TableUtils.DETACHED_DIR_MARKER);
                dst.of(otherVolume).concat("2020-01-01").put(TableUtils.DETACHED_DIR_MARKER);
                Assert.assertEquals(0, ff.rename(src.$(), dst.$()));
                src.of(configuration.getDbRoot()).concat(token).concat("2020-01-01").put(configuration.getAttachPartitionSuffix());
                Assert.assertEquals(0, ff.softLink(dst.$(), src.$()));
            }
            execute("ALTER TABLE sl_px ATTACH PARTITION LIST '2020-01-01'");
            drainWalQueue();
            engine.releaseAllReaders();

            final long nameTxnBefore;
            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertTrue(tx.isPartitionReadOnly(0));
                Assert.assertTrue(tx.isPartitionParquet(0));
                nameTxnBefore = tx.getPartitionNameTxn(0);
            }
            assertUnusedBytes(token, 0, true);

            sweepPastTheIdleTimeout();

            assertPartitionUntouched(token, nameTxnBefore, "a soft-linked");
            try (TableReader reader = engine.getReader(token); Path dir = new Path()) {
                dir.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForNativePartition(
                        dir,
                        reader.getMetadata().getTimestampType(),
                        reader.getPartitionedBy(),
                        reader.getTxFile().getPartitionTimestampByIndex(0),
                        nameTxnBefore
                );
                Assert.assertTrue("the partition must stay a soft link", ff.isSoftLink(dir.$()));
            }
            assertQuery("SELECT count() c FROM sl_px").noRandomAccess().expectSize().returns("c\n16\n");
        });
    }

    /**
     * A partition whose bytes already live in a remote object store. It keeps reading UPLOADED after a rewrite,
     * so nothing downstream notices the manifest row now describes content that moved.
     */
    @Test
    public void testScanLeavesAnUploadedParquetPartitionAlone() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createIdleDirtyParquetTable("up_px");
            final long nameTxnBefore;
            try (TableWriter writer = engine.getWriter(token, "test-upload")) {
                writer.getTxWriter().setPartitionRemote(0, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
                nameTxnBefore = writer.getTxWriter().getPartitionNameTxn(0);
            }
            assertUnusedBytes(token, 0, true);

            sweepPastTheIdleTimeout();

            assertPartitionUntouched(token, nameTxnBefore, "an uploaded");
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertTrue("the UPLOADED bit must survive the sweep", reader.getTxFile().isPartitionRemote(0));
            }
            assertQuery("SELECT count() c FROM up_px").noRandomAccess().expectSize().returns("c\n16\n");
        });
    }

    /**
     * A logical partition holding an uploaded (REMOTE) folder is never folded, frozen or not, by the sweep or on
     * request: rewriting it would leave the bucket copy describing a directory that no longer exists.
     */
    @Test
    public void testScanAndRequestLeaveASplitDayWithAnUploadedFolderAlone() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createSplitDayTable("up_cx");
            final TableToken token = engine.verifyTableName("up_cx");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            final long mainNameTxnBefore;
            final long splitNameTxnBefore;
            try (TableWriter writer = engine.getWriter(token, "test-upload")) {
                Assert.assertEquals("the O3 insert must have split 2020-01-01", 3, writer.getTxWriter().getPartitionCount());
                writer.getTxWriter().setPartitionReadOnly(0, true);
                writer.getTxWriter().setPartitionRemote(1, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
                mainNameTxnBefore = writer.getTxWriter().getPartitionNameTxn(0);
                splitNameTxnBefore = writer.getTxWriter().getPartitionNameTxn(1);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_SQUASH_IDLE_TIMEOUT, "30m");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));
            final Clock pastTheSquashTimeout = () -> MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(
                    engine,
                    configuration.getFilesFacade(),
                    pastTheSquashTimeout
            )) {
                job.run();
                engine.releaseAllReaders();
                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(token, day));
                Assert.assertFalse("the request must not dispatch a fold", job.run());
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals("the day must stay split", 3, tx.getPartitionCount());
                Assert.assertEquals(mainNameTxnBefore, tx.getPartitionNameTxn(0));
                Assert.assertEquals(splitNameTxnBefore, tx.getPartitionNameTxn(1));
                Assert.assertTrue("the main folder must stay READ ONLY", tx.isPartitionReadOnly(0));
                Assert.assertTrue("the UPLOADED bit must survive", tx.isPartitionRemote(1));
            }
            assertQuery("SELECT count() c FROM up_cx").noRandomAccess().expectSize().returns("c\n6010\n");
        });
    }

    /**
     * See {@link #assertReadOnlySplitDayMerges}: the READ ONLY flag sits on the day's main folder.
     */
    @Test
    public void testScanMergesAReadOnlySplitDayAndKeepsItReadOnly() throws Exception {
        assertReadOnlySplitDayMerges(0, false);
    }

    /**
     * See {@link #assertReadOnlySplitDayMerges}: the READ ONLY flag sits only on the split.
     */
    @Test
    public void testScanMergesASplitDayWhoseSplitIsReadOnlyAndKeepsItReadOnly() throws Exception {
        assertReadOnlySplitDayMerges(1, false);
    }

    private void assertNonWalSplitDayMergeAroundOpenTransaction(boolean isDayWritten) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createSplitDayTable("nx");
            execute("ALTER TABLE nx SET TYPE BYPASS WAL");
            engine.releaseInactive();
            engine.load();
            final TableToken token = engine.verifyTableName("nx");
            Assert.assertFalse(token.isWal());
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals("the O3 insert must have split 2020-01-01", 3, reader.getTxFile().getPartitionCount());
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
            try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
                appendRow(ownerWriter, "2020-01-03T01:00:00.000000Z", 80001);
                appendRow(ownerWriter, "2020-01-03T00:00:30.000000Z", 80003);
                if (isDayWritten) {
                    appendRow(ownerWriter, "2020-01-01T12:00:01.000000Z", 80004);
                }
                Assert.assertTrue(ownerWriter.inTransaction());
                Assert.assertTrue("the fixture must hold O3 rows", ownerWriter.hasO3());

                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(token, day));
                runSweepOnAnotherThread(configuration.getFilesFacade());
                ownerWriter.tick();
                Assert.assertTrue("the swap must leave the transaction open", ownerWriter.inTransaction());

                try (TableReader reader = engine.getReader(token)) {
                    final TxReader tx = reader.getTxFile();
                    Assert.assertEquals(
                            isDayWritten ? "a refused merge must leave the day split" : "the day's two folders must have become one",
                            isDayWritten ? 3 : 2,
                            tx.getPartitionCount()
                    );
                }
                assertQuery("SELECT count() c FROM nx").noLeakCheck().noRandomAccess().expectSize().returns("c\n6010\n");
                engine.releaseAllReaders();
                ownerWriter.commit();
            }

            execute("CREATE TABLE nx_oracle AS (SELECT i, ts FROM (" +
                    "SELECT x::INT i, timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760)" +
                    " UNION ALL SELECT x::INT + 90000 i, timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)" +
                    " UNION ALL SELECT x::INT + 70000 i, timestamp_sequence('2020-01-01T22:00:07', 5*1000000L) ts FROM long_sequence(200)" +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO nx_oracle VALUES (80001, '2020-01-03T01:00:00.000000Z'), " +
                    "(80003, '2020-01-03T00:00:30.000000Z')" + (isDayWritten ? ", (80004, '2020-01-01T12:00:01.000000Z')" : ""));
            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext, "SELECT * FROM nx_oracle ORDER BY ts, i", "SELECT * FROM nx ORDER BY ts, i", LOG
            );
        });
    }

    /**
     * The first request folds the day and installs no copy; the second builds and installs the copy.
     */
    private void assertParquetCopyFoldsFirst(TableToken token, long day, long liveRows) {
        try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
            Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetGeneration(token, day));
            Assert.assertTrue("the first request must dispatch the fold", job.run());
            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals("the day's folders must have become one", 2, tx.getPartitionCount());
                Assert.assertFalse("the merged folder must be plain", tx.isPartitionComposite(0));
                Assert.assertFalse("a fold installs no copy", tx.isPartitionParquetGenerated(0));
                Assert.assertEquals("the fold must keep every row", liveRows, tx.getPartitionSize(0));
            }

            Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetGeneration(token, day));
            Assert.assertTrue("the second request must dispatch the copy", job.run());
        }
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        assertParquetCopyInstalled(token, day, liveRows);
    }

    private void assertParquetCopyInstalled(TableToken token, long day, long liveRows) {
        try (TableReader reader = engine.getReader(token); Path path = new Path()) {
            final TxReader tx = reader.getTxFile();
            Assert.assertTrue("the copy must be installed", tx.isPartitionParquetGenerated(0));
            Assert.assertFalse("the folder must stay native", tx.isPartitionParquet(0));
            path.of(configuration.getDbRoot()).concat(token.getDirName());
            TableUtils.setPathForNativePartition(
                    path, reader.getMetadata().getTimestampType(), reader.getPartitionedBy(), day, tx.getPartitionNameTxn(0)
            );
            final int partitionDirLen = path.size();
            final FilesFacade ff = configuration.getFilesFacade();
            final long parquetFileSize = ff.length(path.concat(TableUtils.PARQUET_PARTITION_NAME).$());
            Assert.assertTrue("the copy's data.parquet must be installed", parquetFileSize > 0);
            path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME);
            final ParquetMetaFileReader pmReader = new ParquetMetaFileReader();
            final long addr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), pmReader);
            Assert.assertTrue("the copy's _pm must be installed", addr != 0);
            final long mappedSize = pmReader.getFileSize();
            try {
                Assert.assertTrue("the _pm must describe the installed data.parquet", pmReader.resolveFooter(parquetFileSize));
                Assert.assertEquals("the copy must hold every row of the day", liveRows, pmReader.getPartitionRowCount());
            } finally {
                pmReader.clear();
                ff.munmap(addr, mappedSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
            }
        }
    }

    /**
     * Uses a fresh job, so the request is the only thing that can dispatch the copy.
     */
    private void assertParquetCopyRebuilt(TableToken token, long day, long liveRows) {
        try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
            Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetGeneration(token, day));
            Assert.assertTrue("the next request must dispatch a fresh copy", job.run());
        }
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        assertParquetCopyInstalled(token, day, liveRows);
    }

    private void assertParquetCopyRefused(TableWriter ownerWriter, String stagedBuild) {
        ownerWriter.tick();
        final TxWriter tx = ownerWriter.getTxWriter();
        for (int partitionIndex = 0, n = tx.getPartitionCount(); partitionIndex < n; partitionIndex++) {
            Assert.assertFalse("the install must refuse the stale copy", tx.isPartitionParquetGenerated(partitionIndex));
        }
        Assert.assertFalse("the refused install must remove the staged build", dirExists(configuration.getFilesFacade(), stagedBuild));
    }

    private void assertParquetCopyRequestDeclined(TableToken token, long day) {
        try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
            Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetGeneration(token, day));
            Assert.assertFalse("the request must dispatch nothing", job.run());
        }
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        try (TableReader reader = engine.getReader(token)) {
            final TxReader tx = reader.getTxFile();
            for (int partitionIndex = 0, n = tx.getPartitionCount(); partitionIndex < n; partitionIndex++) {
                Assert.assertFalse("no folder may carry a copy", tx.isPartitionParquetGenerated(partitionIndex));
            }
        }
    }

    private void assertPartitionUntouched(TableToken tableToken, long nameTxnBefore, String why) throws Exception {
        try (TableReader reader = engine.getReader(tableToken)) {
            Assert.assertEquals(
                    why + " partition must keep its directory",
                    nameTxnBefore, reader.getTxFile().getPartitionNameTxn(0)
            );
        }
        // Still there: the sweep declined, rather than reclaiming them by some other route.
        assertUnusedBytes(tableToken, 0, true);
    }

    /**
     * A logical partition holding a READ ONLY folder is merged like any other - by the sweep past its idle
     * thresholds, or on an explicit request with the sweep off and the thresholds unmet, which is how the
     * Enterprise storage policy folds a frozen partition ahead of its parquet conversion - and the merged folder
     * stays READ ONLY.
     *
     * @param readOnlyIndex the {@code _txn} index of the one source folder to freeze: 0 the main folder, 1 the split
     * @param isRequested   true to merge through a request, false through the sweep
     */
    private void assertReadOnlySplitDayMerges(int readOnlyIndex, boolean isRequested) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createSplitDayTable("ro_cx");
            final TableToken token = engine.verifyTableName("ro_cx");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");

            final long liveRowsBefore;
            try (TableWriter writer = engine.getWriter(token, "test-freeze")) {
                Assert.assertEquals("the O3 insert must have split 2020-01-01", 3, writer.getTxWriter().getPartitionCount());
                writer.getTxWriter().setPartitionReadOnly(readOnlyIndex, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
                liveRowsBefore = writer.getTxWriter().getPartitionSize(0) + writer.getTxWriter().getPartitionSize(1);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            if (isRequested) {
                // The same setup testRequestMergesASplitDayWithTheSweepOffAndTheTimeoutsUnmet merges this day under.
                node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
                node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1000h");
                node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_SQUASH_IDLE_TIMEOUT, "1000h");
                try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                    Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(token, day));
                    Assert.assertTrue("the request must dispatch the merge", job.run());
                }
            } else {
                // The same thresholds testScanMergesAnIdleLogicalPartitionOfPlainSplits merges this day under.
                node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
                node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_SQUASH_IDLE_TIMEOUT, "30m");
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));
                final Clock pastTheSquashTimeout = () -> MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
                try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(
                        engine,
                        configuration.getFilesFacade(),
                        pastTheSquashTimeout
                )) {
                    job.run();
                }
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals("the day's two folders must have become one", 2, tx.getPartitionCount());
                Assert.assertEquals(day, tx.getPartitionTimestampByIndex(0));
                Assert.assertFalse("the merged folder must be plain", tx.isPartitionComposite(0));
                Assert.assertTrue("the merged folder must stay READ ONLY", tx.isPartitionReadOnly(0));
                Assert.assertFalse("the next day must not take the flag", tx.isPartitionReadOnly(1));
                Assert.assertEquals("the merge must not change the row count", liveRowsBefore, tx.getPartitionSize(0));
            }
            execute("CREATE TABLE ro_cx_oracle AS (SELECT i, ts FROM (" +
                    "SELECT x::INT i, timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760)" +
                    " UNION ALL SELECT x::INT + 90000 i, timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)" +
                    " UNION ALL SELECT x::INT + 70000 i, timestamp_sequence('2020-01-01T22:00:07', 5*1000000L) ts FROM long_sequence(200)" +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext, "SELECT * FROM ro_cx_oracle ORDER BY ts, i", "SELECT * FROM ro_cx ORDER BY ts, i", LOG
            );
        });
    }

    private void assertUnusedBytes(TableToken tableToken, int partitionIndex, boolean expectPositive) throws Exception {
        try (TableReader reader = engine.getReader(tableToken)) {
            reader.openPartition(partitionIndex);
            final long unusedBytes = reader.getAndInitParquetPartitionDecoder(partitionIndex).metadata().getUnusedBytes();
            if (expectPositive) {
                Assert.assertTrue("expected dead row-group bytes, got " + unusedBytes, unusedBytes > 0);
            } else {
                Assert.assertEquals("expected no dead row-group bytes", 0, unusedBytes);
            }
        }
    }

    /**
     * Builds {@code tableName} with an idle Parquet partition at 2020-01-01 holding dead row-group bytes, behind
     * a later plain partition so it is never the active one. Control fixture for the gate tests, which differ
     * from it only in the one partition flag they set;
     * {@link #testScanCompactsIdleDirtyParquetPartitionButLeavesCleanOneAlone} proves a sweep DOES compact it.
     */
    private TableToken createIdleDirtyParquetTable(String tableName) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));

        execute("CREATE TABLE " + tableName + " (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute(
                "INSERT INTO " + tableName + "(a, ts) VALUES" +
                        "(1,  '2020-01-01T00:00:00.000Z')," +
                        "(2,  '2020-01-01T01:00:00.000Z')," +
                        "(3,  '2020-01-01T02:00:00.000Z')," +
                        "(4,  '2020-01-01T03:00:00.000Z')," +
                        "(5,  '2020-01-01T04:00:00.000Z')," +
                        "(6,  '2020-01-01T05:00:00.000Z')," +
                        "(7,  '2020-01-01T06:00:00.000Z')," +
                        "(8,  '2020-01-01T07:00:00.000Z')," +
                        "(9,  '2020-01-01T08:00:00.000Z')," +
                        "(10, '2020-01-01T09:00:00.000Z')," +
                        "(11, '2020-01-01T10:00:00.000Z')," +
                        "(12, '2020-01-01T11:00:00.000Z')"
        );
        // Pusher day, so 2020-01-01 is inactive by the time it is converted below.
        execute("INSERT INTO " + tableName + "(a, ts) VALUES (90, '2020-01-02T00:00:00.000Z')");
        drainWalQueue();

        execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
        drainWalQueue();

        // Three in-place O3 updates: each appends a merged row group and leaves the one it replaced as
        // dead bytes. The ratio/max-bytes thresholds above are disabled, so none auto-rewrites.
        execute("INSERT INTO " + tableName + "(a, ts) VALUES (101, '2020-01-01T01:30:00.000Z')");
        drainWalQueue();
        execute("INSERT INTO " + tableName + "(a, ts) VALUES (102, '2020-01-01T02:30:00.000Z')");
        drainWalQueue();
        execute("INSERT INTO " + tableName + "(a, ts) VALUES (103, '2020-01-01T03:30:00.000Z')");
        drainWalQueue();

        final TableToken tableToken = engine.verifyTableName(tableName);
        try (TableReader reader = engine.getReader(tableToken)) {
            Assert.assertTrue(reader.getTxFile().isPartitionParquet(0));
        }
        assertUnusedBytes(tableToken, 0, true);
        return tableToken;
    }

    /**
     * {@link PartitionCompactionScanJob#isParquetPartitionCompact} on the reader's own view of the partition.
     */
    private boolean isParquetPartitionCompact(TableToken tableToken, int partitionIndex) {
        try (TableReader reader = engine.getReader(tableToken)) {
            reader.openPartition(partitionIndex);
            return PartitionCompactionScanJob.isParquetPartitionCompact(
                    reader.getTxFile(),
                    partitionIndex,
                    reader.getMetadata(),
                    reader.getColumnVersionReader(),
                    reader.getAndInitParquetPartitionDecoder(partitionIndex).metadata()
            );
        }
    }

    /**
     * Re-tightens the dead-space thresholds - deliberately disabled while the dead bytes were built up - and
     * runs one sweep on a clock past the idle timeout. The parquet branch's idle gate reads the
     * {@code .parquet} file's modification time, which is real wall-clock time whatever the simulated clock
     * says, so the job's own clock has to be shifted rather than the simulated one.
     */
    private void sweepPastTheIdleTimeout() {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "0.01");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, "1");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
        setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));

        final Clock pastTheIdleTimeout = () -> MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
        try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, configuration.getFilesFacade(), pastTheIdleTimeout)) {
            job.run();
        }
        engine.releaseAllReaders();
        engine.releaseAllWriters();
    }

    /**
     * A column added AFTER a partition is already full carries a column top equal to that partition's row
     * count. A REWRITE copies only the live rows into a fresh directory, so every top has to be
     * republished against the new layout - here the live piece sits entirely ABOVE the old top, and the
     * rewritten directory's top must therefore come out as 0.
     */
    @Test
    public void testScanCompactsCompositePartitionRepublishesColumnTops() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));

            execute("CREATE TABLE cx AS (" +
                    "SELECT x::INT i, timestamp_sequence('2020-01-01', 60*1000000L) ts FROM long_sequence(240)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
            // A later, plain day keeps 2020-01-01 out of the active slot, so the backfill below goes
            // through the O3 composite path instead of an append.
            execute("INSERT INTO cx SELECT x::INT + 90000 i," +
                    " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)");
            drainWalQueue();

            // s arrives once 2020-01-01 already holds all 240 of its rows, so the partition records a
            // column top of 240 for it.
            execute("ALTER TABLE cx ADD COLUMN s VARCHAR");
            drainWalQueue();

            // The same 240 timestamps again, this time carrying s. DEDUP replaces every original row, so
            // the partition's only live piece is the merged image parked at file row 240 - entirely above
            // the column top the REWRITE below has to republish.
            execute("INSERT INTO cx SELECT x::INT + 70000 i," +
                    " timestamp_sequence('2020-01-01', 60*1000000L) ts," +
                    " ('v' || x)::varchar s FROM long_sequence(240)");
            drainWalQueue();

            final TableToken cxToken = engine.verifyTableName("cx");
            try (TableReader reader = engine.getReader(cxToken)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertTrue("2020-01-01 should be composite", tx.isPartitionComposite(0));
                Assert.assertEquals(240, tx.getPartitionSize(0));
                Assert.assertTrue("the live piece should sit above the column top",
                        reader.getGeometry().getPieceRowOffset(0, 0) >= 240);
            }

            assertQuery("SELECT count() c FROM cx WHERE ts IN '2020-01-01' AND s IS NOT NULL")
                    .noRandomAccess().expectSize().returns("c\n240\n");

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(cxToken)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertFalse("2020-01-01 is idle, should have been compacted", tx.isPartitionComposite(0));
                Assert.assertEquals("compaction must not change the row count", 240, tx.getPartitionSize(0));
            }

            // No row lost its value, and none of them shifted onto a neighbour's.
            assertQuery("SELECT count() c FROM cx WHERE ts IN '2020-01-01' AND s IS NOT NULL")
                    .noRandomAccess().expectSize().returns("c\n240\n");
            assertQuery("SELECT count() c FROM cx WHERE ts IN '2020-01-01'" +
                    " AND s <> ('v' || (i - 70000))::varchar")
                    .noRandomAccess().expectSize().returns("c\n0\n");
        });
    }

    /**
     * The multi-piece counterpart of {@link #testScanCompactsCompositePartitionRepublishesColumnTops}. A
     * REWRITE copies one piece at a time into the same target frame, so each piece after the first has to
     * see the top the previous pieces left, not the source directory's own. The partition below crosses
     * that top part way through the copy, where re-resolving it from the source skews every remaining
     * piece by the difference.
     */
    @Test
    public void testScanCompactsMultiPieceCompositePartitionRepublishesColumnTops() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));

            execute("CREATE TABLE cx AS (" +
                    "SELECT x::INT i, timestamp_sequence('2020-01-01', 60*1000000L) ts FROM long_sequence(1440)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            // A later, plain day keeps 2020-01-01 out of the active slot.
            execute("INSERT INTO cx SELECT x::INT + 900000 i," +
                    " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)");
            drainWalQueue();

            // s arrives once 2020-01-01 holds all 1440 of its rows, so it records a column top of 1440.
            execute("ALTER TABLE cx ADD COLUMN s VARCHAR");
            drainWalQueue();

            // Three separate backfills, each its own commit and its own sub-range of the day, so the
            // partition ends up with several pieces. Together they add 900 rows - enough that the copy
            // below crosses the 1440-row top part way through, which is what the stale re-resolve needs
            // to go wrong.
            for (int b = 0; b < 3; b++) {
                execute("INSERT INTO cx SELECT x::INT + " + (200000 + b * 100000) + " i," +
                        " timestamp_sequence('2020-01-01T0" + (2 + b * 3) + ":00:07', 5*1000000L) ts," +
                        " ('v' || (x + " + (200000 + b * 100000) + "))::varchar s FROM long_sequence(300)");
                drainWalQueue();
            }

            final TableToken cxToken = engine.verifyTableName("cx");
            try (TableReader reader = engine.getReader(cxToken)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertTrue("2020-01-01 should be composite", tx.isPartitionComposite(0));
                Assert.assertTrue("expected several pieces, got " + reader.getGeometry().getPieceCount(0),
                        reader.getGeometry().getPieceCount(0) > 2);
                Assert.assertEquals(2340, tx.getPartitionSize(0));
            }

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(cxToken)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertFalse("2020-01-01 is idle, should have been compacted", tx.isPartitionComposite(0));
                Assert.assertEquals("compaction must not change the row count", 2340, tx.getPartitionSize(0));
            }

            // The 1440 original rows predate s and stay null; the 900 backfilled ones keep their own
            // value, none of them shifted onto a neighbour's.
            assertQuery("SELECT count() c FROM cx WHERE ts IN '2020-01-01' AND s IS NULL")
                    .noRandomAccess().expectSize().returns("c\n1440\n");
            assertQuery("SELECT count() c FROM cx WHERE ts IN '2020-01-01'" +
                    " AND s IS NOT NULL AND s <> ('v' || i)::varchar")
                    .noRandomAccess().expectSize().returns("c\n0\n");
        });
    }

    /**
     * The swap RETIRES the source directory, and the disk it holds is the whole point of compacting.
     * Draining the remove candidate at swap time is what actually reclaims it: parking it on
     * {@code partitionRemoveCandidates} does not, because the next commit CLEARS that list without
     * draining it, so the retired copy sat on disk until the writer was recycled - right after a
     * compaction that ran to reclaim space, the partition cost twice its size. The writer is deliberately
     * left live here, since that is the steady state of a continuously ingesting table.
     */
    @Test
    public void testScanDeletesTheRetiredDirectoryWhileTheWriterStaysLive() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            execute("CREATE TABLE cx AS (SELECT x::INT i," +
                    " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            // A later, plain day, so 2020-01-01 is never the active partition and the backfill is O3.
            execute("INSERT INTO cx SELECT x::INT + 90000 i," +
                    " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)");
            drainWalQueue();
            execute("INSERT INTO cx SELECT x::INT + 70000 i," +
                    " timestamp_sequence('2020-01-01T04:00:07', 5*1000000L) ts FROM long_sequence(200)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("cx");
            final String retiredDir;
            try (TableReader reader = engine.getReader(token)) {
                final TxReader txReader = reader.getTxFile();
                Assert.assertTrue("2020-01-01 should be composite", txReader.isPartitionComposite(0));
                retiredDir = partitionDir(
                        token,
                        reader.getMetadata().getTimestampType(),
                        reader.getPartitionedBy(),
                        txReader.getPartitionTimestampByIndex(0),
                        txReader.getPartitionNameTxn(0)
                );
            }
            final FilesFacade ff = configuration.getFilesFacade();
            Assert.assertTrue("fixture is wrong, source directory does not exist", dirExists(ff, retiredDir));

            // Only readers: a live writer is exactly the condition this test is about.
            engine.releaseAllReaders();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }

            try (TableReader reader = engine.getReader(token)) {
                Assert.assertFalse("2020-01-01 should have been compacted", reader.getTxFile().isPartitionComposite(0));
            }
            engine.releaseAllReaders();
            Assert.assertFalse("the retired directory is still on disk", dirExists(ff, retiredDir));
            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n6010\n");
        });
    }

    /**
     * The compaction sweep owns a dedicated single-thread pool precisely so a REWRITE copies its partition
     * inline on that thread, never fanning the per-column work onto sharedPoolWrite - the pool running WAL
     * apply and O3 (see ServerMain, where the pool is created). This asserts that dispatch target directly:
     * the shared column-task publish sequence, which sharedPoolWrite's {@code ColumnTaskJob} drains, must
     * not advance across a sweep that actually rewrites a multi-column composite partition. If the sweep
     * built its frames off the engine's shared factory instead, the copy would publish one task per live
     * column and the sequence would move.
     */
    @Test
    public void testScanRewriteDoesNotPublishColumnTasksToTheSharedWritePool() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            // Two data columns plus the designated timestamp: enough that a shared-factory copy would
            // dispatch parallel per-column tasks (columnCount > 1), which is exactly what must not happen.
            execute("CREATE TABLE cx AS (SELECT x::INT i, (x * 2)::LONG j," +
                    " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            // A later, plain day, so 2020-01-01 is never the active partition and the backfill is O3.
            execute("INSERT INTO cx SELECT x::INT + 90000 i, (x * 2)::LONG j," +
                    " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)");
            drainWalQueue();
            execute("INSERT INTO cx SELECT x::INT + 70000 i, (x * 2)::LONG j," +
                    " timestamp_sequence('2020-01-01T04:00:07', 5*1000000L) ts FROM long_sequence(200)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("cx");
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertTrue("2020-01-01 should be composite", reader.getTxFile().isPartitionComposite(0));
                Assert.assertTrue("2020-01-01 should have more than one piece", reader.getGeometry().getPieceCount(0) > 1);
            }
            engine.releaseAllReaders();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                // The shared column-task producer sequence is the single seam between a frame copy and
                // sharedPoolWrite: every parallel per-column task passes through it. Nothing else publishes
                // to it while this single-threaded sweep runs on the test thread.
                final long pubSeqBefore = engine.getMessageBus().getColumnTaskPubSeq().current();
                job.run();
                final long pubSeqAfter = engine.getMessageBus().getColumnTaskPubSeq().current();
                Assert.assertEquals(
                        "the compaction REWRITE published per-column tasks to the shared write pool's queue",
                        pubSeqBefore,
                        pubSeqAfter
                );
            }

            try (TableReader reader = engine.getReader(token)) {
                Assert.assertFalse(
                        "2020-01-01 is idle, the sweep should have REWRITE-compacted it",
                        reader.getTxFile().isPartitionComposite(0)
                );
            }
            engine.releaseAllReaders();
            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n6010\n");
        });
    }

    /**
     * A swap handed to a BUSY writer is queued and applied later, on the writer's own thread. The sweep
     * used to record nothing about that, so the next interval built the whole staging copy over again -
     * one redundant full-partition copy per interval, of which only one swap could ever be used.
     * <p>
     * A MOVE-TAIL leaves a day as two composite directories - the front, and a sibling split at a later
     * timestamp inside the same day - and both floor to the same logical partition timestamp. The sweep keyed
     * its pending-swap record by that floor, so the sibling's turn found the front's record, took it for a
     * stale one and dropped it, then dispatched by the floor - which resolves to the FRONT. The rebuild
     * removed the front's staging directory while the swap queued on the writer still owned it, and that
     * swap then renamed a half-built copy into place.
     * <p>
     * Both folders are idle past the squash threshold here, so the sweep now dispatches one whole-partition
     * MERGE for the day rather than a per-folder REWRITE. The guard is the same either way - one record per
     * LOGICAL partition, checked before dispatch - so the count below covers both staging markers.
     */
    @Test
    public void testScanDoesNotRebuildAQueuedSwapWhenASiblingSplitSharesTheDay() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 16);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

        // Both markers: the day's two folders can be idle enough for the sweep to dispatch a whole-partition
        // MERGE (.merging) instead of a single-folder REWRITE (.compacting), and counting only the latter
        // would make the assertion below pass by never seeing a staging directory at all.
        final ObjList<String> dayStagingMkdirs = new ObjList<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                if (Utf8s.containsAscii(path, Files.SEPARATOR + "2024-01-01")
                        && (Utf8s.containsAscii(path, TableUtils.COMPACTING_DIR_MARKER)
                        || Utf8s.containsAscii(path, TableUtils.MERGING_DIR_MARKER))) {
                    dayStagingMkdirs.add(path.toString());
                }
                return super.mkdirs(path, mode);
            }
        };

        assertMemoryLeak(ff, () -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-01T00:00:00.000000Z"));
            execute("CREATE TABLE x AS (SELECT x::INT i," +
                    " timestamp_sequence('2024-01-01', 1_000_000L) ts" +
                    " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            buildMoveTailWaste();
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, Long.MAX_VALUE / 8);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 2);

            final TableToken token = engine.verifyTableName("x");
            final long day = MicrosFormatUtils.parseTimestamp("2024-01-01T00:00:00.000000Z");
            // A pinned reader keeps MAKE-PLAIN declining, so the front stays composite.
            try (TableReader pinned = engine.getReader(token)) {
                Assert.assertNotNull(pinned);
                for (int i = 0; i < 6; i++) {
                    execute("INSERT INTO x SELECT x::INT + 800_000 + " + (i * 10) + " i," +
                            " timestamp_sequence('2024-03-0" + (1 + i) + "', 60_000_000L) ts FROM long_sequence(2)");
                    drainWalQueue();
                }
                // A merge into the sibling split makes it composite too, and not in the MAKE-PLAIN shape.
                final long siblingTs;
                try (TableReader reader = engine.getReader(token)) {
                    final TxReader tx = reader.getTxFile();
                    Assert.assertTrue("front must be composite", tx.isPartitionComposite(0));
                    Assert.assertEquals("MOVE-TAIL must leave a sibling split", day, tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(1)));
                    siblingTs = tx.getPartitionTimestampByIndex(1);
                }
                execute("INSERT INTO x SELECT x::INT + 600_000 i," +
                        " timestamp_sequence(" + (siblingTs + 500_000) + ", 1_000_000L) ts FROM long_sequence(20)");
                drainWalQueue();
                try (TableReader reader = engine.getReader(token)) {
                    final TxReader tx = reader.getTxFile();
                    Assert.assertTrue("front must stay composite", tx.isPartitionComposite(0));
                    Assert.assertTrue("sibling split must be composite", tx.isPartitionComposite(1));
                }
            }
            engine.releaseAllReaders();
            final long expectedCount;
            try (TableReader reader = engine.getReader(token)) {
                expectedCount = reader.size();
            }
            engine.releaseAllReaders();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            final long interval = engine.getConfiguration().getPartitionCompactionCheckInterval() * 1000;
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-04-01T00:00:00.000000Z"));

            // Holding the writer makes every swap queue instead of applying inline.
            try (TableWriter writer = engine.getWriter(token, "test");
                 PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, ff, configuration.getMicrosecondClock())) {
                for (int i = 0; i < 3; i++) {
                    setCurrentMicros(currentMicros + interval + 1);
                    job.run();
                }
                Assert.assertEquals("the day must take exactly one staging copy across three sweeps - the first"
                                + " sweep's, whose swap is queued on the held writer: " + dayStagingMkdirs,
                        1, dayStagingMkdirs.size());
                writer.tick(true);
            }

            engine.releaseAllWriters();
            engine.releaseAllReaders();
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize().returns("c\n" + expectedCount + "\n");
        });
    }

    @Test
    public void testScanKeepsAQueuedRewriteWhenASiblingSplitChanges() throws Exception {
        // A REWRITE of the day's front folder F is queued on a busy writer, which then commits into F's sibling
        // split G without ticking. The next sweep must not rebuild F into the staging directory the queued
        // command still owns: that command re-checks only F, so it would rename a half-built copy into place.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

        final ObjList<String> dayStagingMkdirs = new ObjList<>();
        final AtomicReference<TableWriter> tickOnStagingOpen = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                if (Utf8s.containsAscii(path, Files.SEPARATOR + "2020-01-01") && Utf8s.containsAscii(path, TableUtils.COMPACTING_DIR_MARKER)) {
                    dayStagingMkdirs.add(path.toString());
                }
                return super.mkdirs(path, mode);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                // Lands the queued command mid-copy, the way the writer's own thread would, should a rebuild
                // ever start filling the staging directory that command owns.
                if (Utf8s.containsAscii(name, TableUtils.COMPACTING_DIR_MARKER) && Utf8s.endsWithAscii(name, Files.SEPARATOR + "ts.d")) {
                    final TableWriter writer = tickOnStagingOpen.getAndSet(null);
                    if (writer != null) {
                        writer.tick(false);
                    }
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            execute("CREATE TABLE x AS (SELECT x::INT i," +
                    " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x SELECT x::INT + 30000 i," +
                    " timestamp_sequence('2020-01-02', 15*1000000L) ts FROM long_sequence(5760)");
            execute("INSERT INTO x SELECT x::INT + 90000 i," +
                    " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)");
            drainWalQueue();
            // Splits 2020-01-01: F is the front folder, G the sibling split.
            execute("INSERT INTO x SELECT x::INT + 70000 i," +
                    " timestamp_sequence('2020-01-01T22:00:07', 5*1000000L) ts FROM long_sequence(200)");
            drainWalQueue();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // Makes 2020-01-02 an older idle composite day, so the writer's own per-commit compaction on the
            // commit into G picks it rather than F.
            execute("INSERT INTO x SELECT x::INT + 40000 i," +
                    " timestamp_sequence('2020-01-02T04:00:07', 5*1000000L) ts FROM long_sequence(200)");
            drainWalQueue();
            setCurrentMicros(currentMicros + 10 * Micros.MINUTE_MICROS);
            // Makes F composite.
            execute("INSERT INTO x SELECT x::INT + 50000 i," +
                    " timestamp_sequence('2020-01-01T04:00:07', 5*1000000L) ts FROM long_sequence(200)");
            drainWalQueue();
            engine.releaseAllReaders();

            final TableToken token = engine.verifyTableName("x");
            final long siblingTs;
            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertTrue("F must be composite", tx.isPartitionComposite(0));
                siblingTs = tx.getPartitionTimestampByIndex(1);
                Assert.assertEquals("G must share F's day",
                        MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"), tx.getLogicalPartitionTimestamp(siblingTs));
            }
            engine.releaseAllReaders();
            final String fRangeQuery = "SELECT count() c, sum(i) s FROM x WHERE ts < " + siblingTs;
            final StringSink expectedF = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, fRangeQuery, expectedF);

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));
            final long interval = engine.getConfiguration().getPartitionCompactionCheckInterval() * 1000;
            // Holding the writer makes every swap queue instead of applying inline.
            try (TableWriter writer = engine.getWriter(token, "test");
                 PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, ff, configuration.getMicrosecondClock())) {
                setCurrentMicros(currentMicros + interval + 1);
                job.run();
                Assert.assertEquals("the first sweep must build F's REWRITE: " + dayStagingMkdirs, 1, dayStagingMkdirs.size());

                // Commits into G through the busy writer, which does not tick its command queue.
                final TableWriter.Row row = writer.newRow(siblingTs + 900_000);
                row.putInt(0, 123_456);
                row.append();
                writer.commit();

                tickOnStagingOpen.set(writer);
                setCurrentMicros(currentMicros + interval + 1);
                job.run();
                Assert.assertEquals("F must not be rebuilt while its REWRITE is still queued: " + dayStagingMkdirs,
                        1, dayStagingMkdirs.size());
                Assert.assertEquals("F's in-flight record must survive a change to its sibling", 1, job.getPendingSwapMemoSize());
                tickOnStagingOpen.set(null);
                writer.tick(true);
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            try (TableReader reader = engine.getReader(token)) {
                Assert.assertFalse("the queued REWRITE must have landed on F", reader.getTxFile().isPartitionComposite(0));
            }
            assertQuery(fRangeQuery).noRandomAccess().expectSize().returns(expectedF.toString());
        });
    }

    @Test
    public void testScanDoesNotRebuildAStagingCopyWhileItsSwapIsStillQueued() throws Exception {
        assertNoRebuildWhileSwapQueued("1h");
    }

    /**
     * The open transaction opened a new day and holds an O3 row for an earlier one, so the REWRITE swap commits it
     * first and must look the partition up again: the commit moves it from index 0 to 1.
     */
    @Test
    public void testCompositeSwapCommitsAnOpenTransactionThatOpenedAPartition() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            execute("CREATE TABLE cc AS (SELECT x::INT i," +
                    " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO cc SELECT x::INT + 90000 i," +
                    " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)");
            drainWalQueue();
            execute("INSERT INTO cc SELECT x::INT + 70000 i," +
                    " timestamp_sequence('2020-01-01T04:00:07', 5*1000000L) ts FROM long_sequence(200)");
            drainWalQueue();
            execute("ALTER TABLE cc SET TYPE BYPASS WAL");
            engine.releaseInactive();
            engine.load();

            final TableToken token = engine.verifyTableName("cc");
            Assert.assertFalse(token.isWal());
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            final long nameTxnBefore;
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertTrue("2020-01-01 should still be composite after the conversion",
                        reader.getTxFile().isPartitionComposite(0));
                nameTxnBefore = reader.getTxFile().getPartitionNameTxn(0);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
            try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
                appendRow(ownerWriter, "2020-01-04T00:00:00.000000Z", 80001);
                appendRow(ownerWriter, "2019-12-31T00:00:00.000000Z", 80002);
                Assert.assertTrue("the fixture must hold an O3 row", ownerWriter.hasO3());
                Assert.assertEquals("the in-order row must have opened a partition", 3, ownerWriter.getPartitionCount());

                Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(token, day));
                runSweepOnAnotherThread(configuration.getFilesFacade());
                ownerWriter.tick();
                Assert.assertFalse("the swap must commit the open transaction first", ownerWriter.inTransaction());
                final TxWriter tx = ownerWriter.getTxWriter();
                final int partitionIndex = tx.getPartitionIndex(day);
                Assert.assertEquals("the commit must have moved the day", 1, partitionIndex);
                Assert.assertFalse("the REWRITE must have landed", tx.isPartitionComposite(partitionIndex));
                Assert.assertNotEquals("the REWRITE must have landed", nameTxnBefore, tx.getPartitionNameTxn(partitionIndex));
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            assertQuery("SELECT count() c FROM cc").noRandomAccess().expectSize().returns("c\n6012\n");
            assertQuery("SELECT count() c FROM cc WHERE ts IN '2020-01-01'").noRandomAccess().expectSize().returns("c\n5960\n");
        });
    }

    /**
     * The composite twin of {@code ParquetPartitionCompactionTest#testSwapInsideAnOpenTransactionIsRefused}:
     * {@link TableWriter#swapCompactedCompositePartition} refuses to run while the writer holds uncommitted
     * rows for the very partition the staged REWRITE replaces, rather than committing those rows to make room
     * for it or swapping over them.
     * <p>
     * A writer can only hold an open transaction across calls on a non-WAL table - {@code
     * TableUpdateDetails.commitIfMaxUncommittedRowsCountReached()} ticks it every {@code
     * cairo.writer.tick.rows.count} rows WITHOUT committing first - and only a WAL table founds a composite
     * partition ({@code O3PartitionJob}'s {@code isCompositeOrWal} gate: the pre-split that pays for the shape
     * hangs off the WAL transaction block). So the table is built as WAL and then converted, which is what
     * leaves a real deployment in this shape.
     * <p>
     * The background sweep no longer reaches a non-WAL table at all - see
     * {@link #testIdleSweepLeavesANonWalCompositeTableAlone} - so nothing queues a swap here any more and the
     * test calls the entry point directly. The guard itself stays: the entry point is public and its contract
     * holds for whoever calls it.
     */
    @Test
    public void testCompositeSwapInsideAnOpenTransactionIsRefused() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            execute("CREATE TABLE cq AS (SELECT x::INT i," +
                    " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            // A later day, so 2020-01-01 is never the active partition and the backfill below is a real O3 write.
            execute("INSERT INTO cq SELECT x::INT + 90000 i," +
                    " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)");
            drainWalQueue();
            // Lands only inside 2020-01-01, cutting it into pieces - composite.
            execute("INSERT INTO cq SELECT x::INT + 70000 i," +
                    " timestamp_sequence('2020-01-01T04:00:07', 5*1000000L) ts FROM long_sequence(200)");
            drainWalQueue();

            execute("ALTER TABLE cq SET TYPE BYPASS WAL");
            engine.releaseInactive();
            engine.load();

            final TableToken token = engine.verifyTableName("cq");
            Assert.assertFalse("the writer can only hold an open transaction on a non-WAL table", token.isWal());
            final long partitionTs = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            final long nameTxnBefore;
            final long writerTxnBefore;
            final long liveRowsBefore;
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertTrue("2020-01-01 should still be composite after the conversion",
                        reader.getTxFile().isPartitionComposite(0));
                nameTxnBefore = reader.getTxFile().getPartitionNameTxn(0);
                writerTxnBefore = reader.getGeometry().getWriterTxn(0);
                liveRowsBefore = reader.getTxFile().getPartitionSize(0);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableWriter ownerWriter = engine.getWriter(token, "owner")) {
                // An O3 row INSIDE the composite partition a staged copy would replace, left uncommitted:
                // exactly the non-WAL lag a swap built off an older snapshot would drop.
                final TableWriter.Row row = ownerWriter.newRow(MicrosFormatUtils.parseTimestamp("2020-01-01T04:00:03.000000Z"));
                row.putInt(0, 55555);
                row.append();
                Assert.assertTrue("the fixture must leave the writer in a transaction", ownerWriter.inTransaction());

                // Every argument below names the LIVE generation, so the open transaction is the only thing
                // that can make this stale. processAsyncWriterCommand() reports the
                // TableReferenceOutOfDateException as READER_OUT_OF_DATE - "rebuild and retry" - rather than
                // as a command error, which is why a queued swap declines instead of failing the tick.
                try {
                    ownerWriter.swapCompactedCompositePartition(
                            partitionTs,
                            nameTxnBefore,
                            writerTxnBefore,
                            ownerWriter.getMetadataVersion(),
                            liveRowsBefore,
                            new ColumnTopRecorder()
                    );
                    Assert.fail("the swap ran inside an open transaction");
                } catch (TableReferenceOutOfDateException ignore) {
                    // The decline every stale swap raises.
                }

                // Declined, not committed: the transaction is still the owner's to finish.
                Assert.assertTrue("the swap committed the writer's open transaction", ownerWriter.inTransaction());

                ownerWriter.commit();
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals("a declined swap must not replace the live partition",
                        nameTxnBefore, reader.getTxFile().getPartitionNameTxn(0));
                Assert.assertTrue("a declined swap must leave the partition composite",
                        reader.getTxFile().isPartitionComposite(0));
            }
            engine.releaseAllReaders();

            // The lag row outlived the decline, landed in the right partition, and took nothing with it.
            assertQuery("SELECT count() c FROM cq").noLeakCheck().noRandomAccess().expectSize().returns("c\n6011\n");
            assertQuery("SELECT count() c FROM cq WHERE ts IN '2020-01-01'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n5961\n");
            assertQuery("SELECT i, ts FROM cq WHERE i = 55555")
                    .noLeakCheck().timestamp("ts")
                    .returns("i\tts\n55555\t2020-01-01T04:00:03.000000Z\n");
        });
    }

    /**
     * The gate that makes the sweep WAL-only, pinned from both sides in one run so a gate that quietly
     * disables compaction for everyone cannot pass: two tables with the same idle composite partition, one
     * WAL and one converted to BYPASS WAL, swept together. The WAL one must be compacted; the non-WAL one
     * must come out untouched - still composite, same nameTxn, no staging directory ever created.
     * <p>
     * Non-WAL tables are out of scope for the out-of-band sweep: their writer holds its transaction open
     * across ticks, so a swap built off a reader snapshot can always arrive at a writer carrying rows that
     * snapshot never saw (see {@link #testCompositeSwapInsideAnOpenTransactionIsRefused}).
     */
    @Test
    public void testIdleSweepLeavesANonWalCompositeTableAlone() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);

        final AtomicInteger stagingMkdirs = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                if (Utf8s.containsAscii(path, TableUtils.COMPACTING_DIR_MARKER)) {
                    stagingMkdirs.incrementAndGet();
                }
                return super.mkdirs(path, mode);
            }
        };

        assertMemoryLeak(ff, () -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createCompositeDayTable("cn");
            createCompositeDayTable("cw");

            // Only "cn" converts, so the two differ in nothing but the WAL flag.
            execute("ALTER TABLE cn SET TYPE BYPASS WAL");
            engine.releaseInactive();
            engine.load();

            final TableToken nonWalToken = engine.verifyTableName("cn");
            final TableToken walToken = engine.verifyTableName("cw");
            Assert.assertFalse(nonWalToken.isWal());
            Assert.assertTrue(walToken.isWal());

            final long nonWalNameTxnBefore;
            try (TableReader reader = engine.getReader(nonWalToken)) {
                Assert.assertTrue("the fixture lost its composite partition in the conversion",
                        reader.getTxFile().isPartitionComposite(0));
                nonWalNameTxnBefore = reader.getTxFile().getPartitionNameTxn(0);
            }
            try (TableReader reader = engine.getReader(walToken)) {
                Assert.assertTrue(reader.getTxFile().isPartitionComposite(0));
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));

            // More passes than the WAL table needs, so "not yet" cannot pass for "never".
            for (int i = 0; i < 3; i++) {
                runSweepOnAnotherThread(ff);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(nonWalToken)) {
                Assert.assertTrue("the sweep compacted a non-WAL table", reader.getTxFile().isPartitionComposite(0));
                Assert.assertEquals("the sweep moved a non-WAL partition", nonWalNameTxnBefore, reader.getTxFile().getPartitionNameTxn(0));
            }
            try (TableReader reader = engine.getReader(walToken)) {
                Assert.assertFalse("the gate disabled compaction for WAL tables too", reader.getTxFile().isPartitionComposite(0));
                Assert.assertEquals(1, reader.getGeometry().getPieceCount(0));
            }
            engine.releaseAllReaders();

            Assert.assertEquals("exactly one staged copy is expected - the WAL table's; a non-WAL partition must never be staged",
                    1, stagingMkdirs.get());

            assertQuery("SELECT count() c FROM cn").noLeakCheck().noRandomAccess().expectSize().returns("c\n6010\n");
            assertQuery("SELECT count() c FROM cw").noLeakCheck().noRandomAccess().expectSize().returns("c\n6010\n");
            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext, "SELECT * FROM cn ORDER BY ts, i", "SELECT * FROM cw ORDER BY ts, i", LOG
            );
        });
    }

    /**
     * A day cut into a prefix and a split by an O3 insert into the middle of it, with merge-append off so both
     * folders come out PLAIN, and a later day so the split day is never the active one.
     */
    private static void buildMoveTailWaste() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1T");
        for (int i = 0; i < 5; i++) {
            execute("INSERT INTO x SELECT x::INT + 500_000 i," +
                    " timestamp_sequence('2024-01-01T05:00:00', 1_000_000L) ts FROM long_sequence(200)");
            drainWalQueue();
        }
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
    }

    private static void createSplitDayTable(String tableName) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
        execute("CREATE TABLE " + tableName + " AS (SELECT x::INT i," +
                " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760))" +
                " TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + tableName + " SELECT x::INT + 90000 i," +
                " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)");
        drainWalQueue();
        // Lands near the END of 2020-01-01, so the prefix dwarfs the merge and the suffix and the O3 path
        // splits the day rather than rewriting it whole.
        execute("INSERT INTO " + tableName + " SELECT x::INT + 70000 i," +
                " timestamp_sequence('2020-01-01T22:00:07', 5*1000000L) ts FROM long_sequence(200)");
        drainWalQueue();
    }

    /**
     * A day made composite by a backfill into the middle of it, with a later day so the composite one is
     * never the active partition.
     */
    private static void createCompositeDayTable(String tableName) throws Exception {
        execute("CREATE TABLE " + tableName + " AS (SELECT x::INT i," +
                " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760))" +
                " TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + tableName + " SELECT x::INT + 90000 i," +
                " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)");
        drainWalQueue();
        // Lands only inside 2020-01-01, cutting it into pieces - composite.
        execute("INSERT INTO " + tableName + " SELECT x::INT + 70000 i," +
                " timestamp_sequence('2020-01-01T04:00:07', 5*1000000L) ts FROM long_sequence(200)");
        drainWalQueue();
    }

    /**
     * Non-WAL: 100 rows in 2020-01-01, 10 in the active 2020-01-03, sweep off so jobs act on requests only.
     */
    private static TableToken createParquetCopyTable(String tableName) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
        setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
        execute("CREATE TABLE " + tableName + " AS (SELECT x::INT i," +
                " timestamp_sequence('2020-01-01', 60*1000000L) ts FROM long_sequence(100))" +
                " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO " + tableName + " SELECT x::INT + 90000 i," +
                " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(10)");
        return engine.verifyTableName(tableName);
    }

    /**
     * The same guard, under an idle timeout short enough to have expired the record that carries it. The
     * timeout says how long a partition must sit still before compacting it is worth it, and a fuzz run
     * sets it sub-millisecond; using it as the pending-swap window too dropped the record on the sweep
     * right after the one that queued it, and the rebuild then re-created the exact directory the queued
     * command was about to rename and reseal - taking the files out from under a copy still appending to
     * them, which surfaced as a SIGBUS in PostingIndexWriter rather than as a wasted copy.
     */
    @Test
    public void testScanDoesNotRebuildAStagingCopyWhileItsSwapIsStillQueuedUnderATinyIdleTimeout() throws Exception {
        assertNoRebuildWhileSwapQueued("1us");
    }

    @Test
    public void testIoBudgetAllowsOneOversizedPartitionPerSweep() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IO_BUDGET, "1");

        final AtomicInteger stagingMkdirs = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                if (Utf8s.containsAscii(path, TableUtils.COMPACTING_DIR_MARKER)) {
                    stagingMkdirs.incrementAndGet();
                }
                return super.mkdirs(path, mode);
            }
        };

        assertMemoryLeak(ff, () -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            execute("""
                    CREATE TABLE cx AS (
                        SELECT x::INT i, timestamp_sequence('2020-01-01', 15 * 1_000_000L) ts
                        FROM long_sequence(11_520)
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO cx
                    SELECT x::INT + 90_000 i, timestamp_sequence('2020-01-04', 60 * 1_000_000L) ts
                    FROM long_sequence(50)
                    """);
            drainWalQueue();
            execute("""
                    INSERT INTO cx
                    SELECT x::INT + 70_000 i, timestamp_sequence('2020-01-01T04:00:07', 5 * 1_000_000L) ts
                    FROM long_sequence(200)
                    """);
            drainWalQueue();
            execute("""
                    INSERT INTO cx
                    SELECT x::INT + 80_000 i, timestamp_sequence('2020-01-02T04:00:07', 5 * 1_000_000L) ts
                    FROM long_sequence(200)
                    """);
            drainWalQueue();

            final TableToken token = engine.verifyTableName("cx");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            final long interval = engine.getConfiguration().getPartitionCompactionCheckInterval() * 1000;
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));

            try (TableWriter writer = engine.getWriter(token, "test");
                 PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, ff, configuration.getMicrosecondClock())) {
                setCurrentMicros(currentMicros + interval + 1);
                job.run();
                Assert.assertEquals("the first oversized partition must run", 1, stagingMkdirs.get());
                Assert.assertEquals(1, job.getPendingSwapMemoSize());

                setCurrentMicros(currentMicros + interval + 1);
                job.run();
                Assert.assertEquals("the byte budget must defer the second partition", 2, stagingMkdirs.get());
                Assert.assertEquals(2, job.getPendingSwapMemoSize());
                writer.tick(true);
            }

            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n11970\n");
        });
    }

    /**
     * The whole logical partition is the unit of work: a day left as a prefix folder plus a split, both PLAIN and
     * both idle past the squash threshold, is copied into one folder and its two {@code _txn} entries become one.
     * Today's job never touches a plain folder at all, so this is the behaviour this design adds.
     * <p>
     * A plain folder's age is its timestamp column file's modification time, which is real wall-clock time
     * whatever the simulated clock says, so the job's own clock is shifted rather than the simulated one - the
     * same trick {@link #sweepPastTheIdleTimeout} plays for parquet.
     */
    @Test
    public void testScanMergesAnIdleLogicalPartitionOfPlainSplits() throws Exception {
        // Merge-append off: the O3 insert then splits the day instead of rewriting it into pieces, which is
        // the shape - two PLAIN folders - today's job never touches.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createSplitDayTable("cx");
            final TableToken token = engine.verifyTableName("cx");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");

            final long liveRowsBefore;
            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals(3, tx.getPartitionCount());
                Assert.assertEquals(day, tx.getPartitionTimestampByIndex(0));
                Assert.assertEquals("the O3 insert must have split 2020-01-01",
                        day, tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(1)));
                Assert.assertNotEquals(day, tx.getPartitionTimestampByIndex(1));
                Assert.assertFalse("the prefix must be plain", tx.isPartitionComposite(0));
                Assert.assertFalse("the split must be plain", tx.isPartitionComposite(1));
                liveRowsBefore = tx.getPartitionSize(0) + tx.getPartitionSize(1);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_SQUASH_IDLE_TIMEOUT, "30m");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));
            final Clock pastTheSquashTimeout = () -> MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(
                    engine,
                    configuration.getFilesFacade(),
                    pastTheSquashTimeout
            )) {
                job.run();
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals("the day's two folders must have become one", 2, tx.getPartitionCount());
                Assert.assertEquals(day, tx.getPartitionTimestampByIndex(0));
                Assert.assertFalse("the merged folder must be plain", tx.isPartitionComposite(0));
                Assert.assertEquals("the merge must not change the row count", liveRowsBefore, tx.getPartitionSize(0));
            }

            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n6010\n");
            execute("CREATE TABLE cx_oracle AS (SELECT i, ts FROM (" +
                    "SELECT x::INT i, timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760)" +
                    " UNION ALL SELECT x::INT + 90000 i, timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)" +
                    " UNION ALL SELECT x::INT + 70000 i, timestamp_sequence('2020-01-01T22:00:07', 5*1000000L) ts FROM long_sequence(200)" +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext, "SELECT * FROM cx_oracle ORDER BY ts, i", "SELECT * FROM cx ORDER BY ts, i", LOG
            );
        });
    }

    /**
     * A sweep MERGE copies a logical partition whose front folder is MOVE-TAIL's leftover: composite, one piece at
     * row 0, dead space above it. The sweep's reader pins a txn at or past that geometry record, which is outside the
     * range MAKE-PLAIN waits on, so the writer is free to MAKE-PLAIN the front and TRIM-FILES its column files while
     * the copy is in flight. The copy must therefore map the front only as far as its live pieces reach, like
     * {@code TableReader#mappedRowCount}: mapping to E reads the var-size aux entry for row E-1 to size the data
     * mapping, and that entry now sits past the end of the trimmed file - SIGBUS.
     * <p>
     * The hook is the staging directory's mkdirs, after the sweep took its reader and before it maps any source.
     */
    @Test
    public void testScanMergeSurvivesMakePlainTrimmingTheFrontItCopies() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 16);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

        final AtomicReference<Runnable> hook = new AtomicReference<>();
        final AtomicInteger fired = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                final Runnable r = hook.get();
                if (r != null
                        && Utf8s.containsAscii(path, Files.SEPARATOR + "2024-01-01")
                        && Utf8s.containsAscii(path, TableUtils.MERGING_DIR_MARKER)
                        && fired.getAndIncrement() == 0) {
                    // Before the directory exists: a writer opening inside the hook must not see it as abandoned.
                    r.run();
                }
                return super.mkdirs(path, mode);
            }
        };

        assertMemoryLeak(ff, () -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-01T00:00:00.000000Z"));
            execute("CREATE TABLE x AS (SELECT x::INT i, ('v' || x)::STRING s," +
                    " timestamp_sequence('2024-01-01', 1_000_000L) ts" +
                    " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1T");
            for (int i = 0; i < 5; i++) {
                execute("INSERT INTO x SELECT x::INT + 500_000 i, ('b' || x)::STRING s," +
                        " timestamp_sequence('2024-01-01T05:00:00', 1_000_000L) ts FROM long_sequence(200)");
                drainWalQueue();
            }
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, Long.MAX_VALUE / 8);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 2);

            final TableToken token = engine.verifyTableName("x");
            final long day = MicrosFormatUtils.parseTimestamp("2024-01-01T00:00:00.000000Z");
            // A pinned reader lets MOVE-TAIL run and keeps MAKE-PLAIN declining, so the front stays composite.
            try (TableReader pinned = engine.getReader(token)) {
                Assert.assertNotNull(pinned);
                for (int i = 0; i < 6; i++) {
                    execute("INSERT INTO x SELECT x::INT + 800_000 + " + (i * 10) + " i, 'c' s," +
                            " timestamp_sequence('2024-03-0" + (1 + i) + "', 60_000_000L) ts FROM long_sequence(2)");
                    drainWalQueue();
                }
            }
            engine.releaseAllReaders();
            final long expectedCount;
            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertTrue("front must be composite", tx.isPartitionComposite(0));
                Assert.assertEquals(1, reader.getGeometry().getPieceCount(0));
                Assert.assertTrue("MOVE-TAIL left no dead space", reader.getPartitionPhysicalRowCount(0) > tx.getPartitionSize(0));
                Assert.assertEquals("MOVE-TAIL must leave a sibling split", day, tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(1)));
                expectedCount = reader.size();
            }
            engine.releaseAllReaders();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_SQUASH_IDLE_TIMEOUT, "30m");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-04-01T00:00:00.000000Z"));
            hook.set(() -> {
                try {
                    // Past the decline backoff, a commit elsewhere lets the writer MAKE-PLAIN the front.
                    setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);
                    execute("INSERT INTO x SELECT x::INT + 900_000 i, 'd' s," +
                            " timestamp_sequence('2024-03-20', 60_000_000L) ts FROM long_sequence(2)");
                    drainWalQueue();
                    try (TableReader reader = engine.getReader(token)) {
                        Assert.assertFalse("the writer did not MAKE-PLAIN the front", reader.getTxFile().isPartitionComposite(0));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            // Plain folders use filesystem modification times, not the simulated clock.
            final Clock pastTheSquashTimeout = () -> MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, ff, pastTheSquashTimeout)) {
                job.run();
            } finally {
                hook.set(null);
            }
            Assert.assertEquals("the sweep did not MERGE the day", 1, fired.get());
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize().returns("c\n" + (expectedCount + 2) + "\n");
        });
    }

    /**
     * Folding a whole split day is gated on {@code squash.idle.timeout}, not on the longer {@code idle.timeout}
     * that compacting one composite folder on its own waits for: a sweep between the two thresholds merges the day,
     * and one before the squash threshold does not.
     */
    @Test
    public void testScanMergeWaitsForTheSquashIdleTimeout() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createSplitDayTable("cx");
            final TableToken token = engine.verifyTableName("cx");
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals("the O3 insert must have split 2020-01-01", 3, reader.getTxFile().getPartitionCount());
            }

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_SQUASH_IDLE_TIMEOUT, "30m");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));
            // Plain folders use filesystem modification times, not the simulated clock.
            final long beforeSquashTimeout = MicrosecondClockImpl.INSTANCE.getTicks() + 15 * Micros.MINUTE_MICROS;
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, configuration.getFilesFacade(), () -> beforeSquashTimeout)) {
                job.run();
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals("folders idle less than squash.idle.timeout must not merge", 3, reader.getTxFile().getPartitionCount());
            }

            final long betweenTimeouts = MicrosecondClockImpl.INSTANCE.getTicks() + 45 * Micros.MINUTE_MICROS;
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, configuration.getFilesFacade(), () -> betweenTimeouts)) {
                job.run();
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals("folders idle past squash.idle.timeout must merge before idle.timeout", 2, reader.getTxFile().getPartitionCount());
            }
            assertQuery("SELECT count() c, sum(i) s FROM cx").noRandomAccess().expectSize()
                    .returns("c\ts\n6010\t" + (5760L * 5761 / 2 + 50L * 51 / 2 + 50L * 90_000 + 200L * 201 / 2 + 200L * 70_000) + "\n");
        });
    }

    /**
     * An UPDATE that commits while the merge copies leaves every word the swap used to re-check standing:
     * the day keeps its two plain folders, their name txns, their (absent) generations, their row counts and
     * the table's metadata version. Only the folders' column versions move, because the UPDATE rewrites the
     * column files under new name txns. The swap has to read that word and decline: the staged copy holds the
     * pre-UPDATE bytes, and publishing it would both lose the UPDATE and leave the merged directory missing
     * the files the live {@code _cv} names.
     */
    @Test
    public void testScanMergePreservesUpdateCommittedDuringBuild() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);

        final AtomicInteger updateCount = new AtomicInteger();
        final AtomicInteger mergedColumnWrites = new AtomicInteger();
        final AtomicInteger mergingRenames = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                // The build writing the merged timestamp column separates "the copy was made and the swap
                // then declined" from "the build itself failed", which the assertions below cannot tell apart.
                if (Utf8s.containsAscii(name, TableUtils.MERGING_DIR_MARKER) && Utf8s.endsWithAscii(name, "ts.d")) {
                    mergedColumnWrites.incrementAndGet();
                }
                return super.openRW(name, opts);
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                // Landing a merge renames the staging directory over the day's folders. Nothing may rename it
                // here: the swap has to decline on the column version the UPDATE moved.
                if (Utf8s.containsAscii(from, TableUtils.MERGING_DIR_MARKER)) {
                    mergingRenames.incrementAndGet();
                }
                return super.rename(from, to);
            }

            @Override
            public int mkdirs(Path path, int mode) {
                final int result = super.mkdirs(path, mode);
                if (updateCount.get() == 0 && Utf8s.containsAscii(path, TableUtils.MERGING_DIR_MARKER)) {
                    updateCount.incrementAndGet();
                    // The merge already holds its reader snapshot; commit UPDATE before it copies those files.
                    try {
                        execute("UPDATE cx SET i = -42 WHERE ts IN '2020-01-01'");
                        drainWalQueue();
                        // The outer scope checks leaks; do not clear the pool while the merge holds its reader.
                        assertQuery("SELECT count() c FROM cx WHERE i = -42")
                                .noLeakCheck()
                                .noRandomAccess()
                                .expectSize()
                                .returns("""
                                        c
                                        5960
                                        """);
                    } catch (Exception e) {
                        throw new AssertionError("UPDATE failed during the merge build", e);
                    }
                }
                return result;
            }
        };

        assertMemoryLeak(ff, () -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createSplitDayTable("cx");
            final TableToken token = engine.verifyTableName("cx");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals(3, tx.getPartitionCount());
                Assert.assertEquals(day, tx.getPartitionTimestampByIndex(0));
                Assert.assertEquals(day, tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(1)));
                Assert.assertFalse("the prefix must be plain", tx.isPartitionComposite(0));
                Assert.assertFalse("the split must be plain", tx.isPartitionComposite(1));
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_SQUASH_IDLE_TIMEOUT, "30m");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));
            // Plain folders use filesystem modification times, not the simulated clock.
            final Clock pastTheSquashTimeout = () -> MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, ff, pastTheSquashTimeout)) {
                job.run();
            }
            Assert.assertEquals("the merge must reach the UPDATE interleaving", 1, updateCount.get());
            Assert.assertTrue("the merge must have built its staging copy, not failed before writing it",
                    mergedColumnWrites.get() > 0);
            Assert.assertEquals("the staged copy holds pre-UPDATE bytes; the swap must decline instead of"
                    + " renaming it into place", 0, mergingRenames.get());
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals("the merge must have been declined, leaving the day's two folders",
                        3, tx.getPartitionCount());
                Assert.assertEquals(day, tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(1)));
            }
            engine.releaseAllReaders();
            // A declined merge owns its staging copy and has to remove it.
            Assert.assertEquals("the declined merge left its staging directory behind",
                    0, countMergeStagingDirs(ff, token));

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT count() c FROM cx WHERE i = -42")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c
                            5960
                            """);
            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n6010\n");
        });
    }

    /**
     * The other side of {@link #testScanMergePreservesUpdateCommittedDuringBuild}: the column-version word is
     * read per folder, so an UPDATE that commits during the build but lands on a DIFFERENT day moves none of
     * the merged run's words and the merge still goes through. A guard keyed on anything table-wide - the
     * {@code _cv} file's own version, say - would stall every merge on an ingesting table instead.
     */
    @Test
    public void testScanMergeSurvivesUpdateToAnotherPartitionDuringBuild() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);

        final AtomicInteger updateCount = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                final int result = super.mkdirs(path, mode);
                if (updateCount.get() == 0 && Utf8s.containsAscii(path, TableUtils.MERGING_DIR_MARKER)) {
                    updateCount.incrementAndGet();
                    // 2020-01-03 is a day of its own, and no folder of it takes part in this merge.
                    try {
                        execute("UPDATE cx SET i = -42 WHERE ts IN '2020-01-03'");
                        drainWalQueue();
                    } catch (Exception e) {
                        throw new AssertionError("UPDATE failed during the merge build", e);
                    }
                }
                return result;
            }
        };

        assertMemoryLeak(ff, () -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createSplitDayTable("cx");
            final TableToken token = engine.verifyTableName("cx");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            final long liveRowsBefore;
            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals(3, tx.getPartitionCount());
                Assert.assertEquals(day, tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(1)));
                liveRowsBefore = tx.getPartitionSize(0) + tx.getPartitionSize(1);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_SQUASH_IDLE_TIMEOUT, "30m");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));
            final Clock pastTheSquashTimeout = () -> MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, ff, pastTheSquashTimeout)) {
                job.run();
            }
            Assert.assertEquals("the merge must reach the UPDATE interleaving", 1, updateCount.get());
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals("an UPDATE to another day must not stop the merge", 2, tx.getPartitionCount());
                Assert.assertEquals(day, tx.getPartitionTimestampByIndex(0));
                Assert.assertEquals("the merge must not change the row count", liveRowsBefore, tx.getPartitionSize(0));
            }
            engine.releaseAllReaders();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT count() c FROM cx WHERE i = -42").noRandomAccess().expectSize().returns("c\n50\n");
            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n6010\n");
        });
    }

    /**
     * A column added after a day was written has no data in that day's first folder and data in the split that
     * came later, so the merged directory's column top is the first folder's row count. The copy records the
     * tops it actually writes and the swap republishes them against the merged layout.
     */
    @Test
    public void testScanMergeRepublishesColumnTops() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            execute("CREATE TABLE cx AS (SELECT x::INT i," +
                    " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO cx SELECT x::INT + 90000 i," +
                    " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)");
            drainWalQueue();
            // Added once 2020-01-01 is already full, so its rows carry no value for it at all.
            execute("ALTER TABLE cx ADD COLUMN v LONG");
            drainWalQueue();
            // Lands near the end of 2020-01-01, splitting it, and the split DOES carry values for v.
            execute("INSERT INTO cx SELECT x::INT + 70000 i," +
                    " timestamp_sequence('2020-01-01T22:00:07', 5*1000000L) ts, x v FROM long_sequence(200)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("cx");
            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            final long prefixRows;
            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals(3, tx.getPartitionCount());
                Assert.assertEquals(day, tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(1)));
                prefixRows = tx.getPartitionSize(0);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_SQUASH_IDLE_TIMEOUT, "30m");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));
            final Clock pastTheSquashTimeout = () -> MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(
                    engine,
                    configuration.getFilesFacade(),
                    pastTheSquashTimeout
            )) {
                job.run();
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(2, reader.getTxFile().getPartitionCount());
                Assert.assertEquals("v starts where the first folder ended",
                        prefixRows, reader.getColumnVersionReader().getColumnTop(day, reader.getMetadata().getColumnIndex("v")));
            }
            assertQuery("SELECT count() c FROM cx WHERE v IS NOT NULL").noRandomAccess().expectSize().returns("c\n200\n");
            assertQuery("SELECT sum(v) s FROM cx").noRandomAccess().expectSize().returns("s\n20100\n");
            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n6010\n");
        });
    }

    /**
     * Columns added after a day was split hold no value in any of its folders, so merging the folders writes none
     * either: the merged directory takes them as column tops. Their files must not be allocated for the rows the
     * tops stand in for.
     */
    @Test
    public void testScanMergeAllocatesNothingForColumnUnderItsTop() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createSplitDayTable("cx");
            execute("ALTER TABLE cx ADD COLUMN k LONG");
            execute("ALTER TABLE cx ADD COLUMN s VARCHAR");
            execute("ALTER TABLE cx ADD COLUMN t STRING");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("cx");
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals("the O3 insert must have split 2020-01-01", 3, reader.getTxFile().getPartitionCount());
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_SQUASH_IDLE_TIMEOUT, "30m");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));
            final Clock pastTheSquashTimeout = () -> MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(
                    engine,
                    configuration.getFilesFacade(),
                    pastTheSquashTimeout
            )) {
                job.run();
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            final long day = MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z");
            final FilesFacade ff = configuration.getFilesFacade();
            try (TableReader reader = engine.getReader(token); Path path = new Path()) {
                final TxReader tx = reader.getTxFile();
                Assert.assertEquals("the day's two folders must have become one", 2, tx.getPartitionCount());
                final String dir = partitionDir(token, reader.getMetadata().getTimestampType(), reader.getPartitionedBy(), day, tx.getPartitionNameTxn(0));
                path.of(dir);
                final int partitionLen = path.size();
                final String[] files = new java.io.File(dir).list();
                Assert.assertNotNull(files);
                int checkedFiles = 0;
                for (String file : files) {
                    if (file.startsWith("k.") || file.startsWith("s.") || file.startsWith("t.")) {
                        path.trimTo(partitionLen).concat(file);
                        Assert.assertEquals(file, 0, ff.length(path.$()));
                        checkedFiles++;
                    }
                }
                Assert.assertTrue("the merge must have created the late columns' files", checkedFiles > 0);
            }
            assertQuery("SELECT count() c, count(k) ck, count(s) cs, count(t) ct FROM cx")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\tck\tcs\tct\n6010\t0\t0\t0\n");
        });
    }

    /**
     * The merge also reclaims dead space: a day MOVE-TAIL left as a COMPOSITE front plus a COMPOSITE sibling
     * split comes back as one plain folder holding only the live rows, so the sweep does the whole logical
     * partition's work in a single copy rather than one copy per folder.
     */
    @Test
    public void testScanMergesAnIdleLogicalPartitionOfCompositeFolders() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 16);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-01T00:00:00.000000Z"));
            execute("CREATE TABLE x AS (SELECT x::INT i," +
                    " timestamp_sequence('2024-01-01', 1_000_000L) ts" +
                    " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            buildMoveTailWaste();
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, Long.MAX_VALUE / 8);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 2);

            final TableToken token = engine.verifyTableName("x");
            final long day = MicrosFormatUtils.parseTimestamp("2024-01-01T00:00:00.000000Z");
            final long siblingTs;
            // A pinned reader keeps MAKE-PLAIN declining, so the per-commit compaction picks MOVE-TAIL, which
            // is what leaves the day as a front plus a sibling split.
            try (TableReader pinned = engine.getReader(token)) {
                Assert.assertNotNull(pinned);
                // Later days, so 2024-01-01 is never the active logical partition.
                for (int i = 0; i < 6; i++) {
                    execute("INSERT INTO x SELECT x::INT + 800_000 + " + (i * 10) + " i," +
                            " timestamp_sequence('2024-03-0" + (1 + i) + "', 60_000_000L) ts FROM long_sequence(2)");
                    drainWalQueue();
                }
                try (TableReader reader = engine.getReader(token)) {
                    final TxReader tx = reader.getTxFile();
                    Assert.assertTrue("the front must be composite", tx.isPartitionComposite(0));
                    Assert.assertEquals("MOVE-TAIL must leave a sibling split",
                            day, tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(1)));
                    siblingTs = tx.getPartitionTimestampByIndex(1);
                }
                // A merge into the sibling split makes it composite too.
                execute("INSERT INTO x SELECT x::INT + 600_000 i," +
                        " timestamp_sequence(" + (siblingTs + 500_000) + ", 1_000_000L) ts FROM long_sequence(20)");
                drainWalQueue();
            }
            engine.releaseAllReaders();

            final long liveRowsBefore;
            final long expectedCount;
            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                Assert.assertTrue("the front must stay composite", tx.isPartitionComposite(0));
                Assert.assertTrue("the sibling split must be composite", tx.isPartitionComposite(1));
                Assert.assertEquals("only the two folders of 2024-01-01 may share the day",
                        day, tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(1)));
                Assert.assertNotEquals(day, tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(2)));
                liveRowsBefore = tx.getPartitionSize(0) + tx.getPartitionSize(1);
                expectedCount = reader.size();
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_SQUASH_IDLE_TIMEOUT, "30m");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-04-01T00:00:00.000000Z"));
            final Clock pastTheSquashTimeout = () -> MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(
                    engine,
                    configuration.getFilesFacade(),
                    pastTheSquashTimeout
            )) {
                job.run();
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(token)) {
                final TxReader tx = reader.getTxFile();
                int folders = 0;
                for (int i = 0, n = tx.getPartitionCount(); i < n; i++) {
                    if (tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(i)) == day) {
                        folders++;
                    }
                }
                Assert.assertEquals("the whole day must have been merged into one folder", 1, folders);
                Assert.assertEquals(day, tx.getPartitionTimestampByIndex(0));
                Assert.assertFalse("the merged folder must be plain", tx.isPartitionComposite(0));
                Assert.assertEquals(1, reader.getGeometry().getPieceCount(0));
                Assert.assertEquals("the merge must not change the row count", liveRowsBefore, tx.getPartitionSize(0));
            }
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize().returns("c\n" + expectedCount + "\n");
        });
    }

    /**
     * The ACTIVE logical partition is out of scope for the merge however cold it looks: the writer holds it open
     * and its files carry the WAL lag rows past the live ones, which no piece accounts for and a copy built off a
     * reader snapshot would silently drop. Its splits are the writer's own squash to fold, on its next commit.
     */
    @Test
    public void testScanLeavesTheActiveLogicalPartitionsSplitsAlone() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            execute("CREATE TABLE cx AS (SELECT x::INT i," +
                    " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            // Splits the one and only day, which is therefore the active one.
            execute("INSERT INTO cx SELECT x::INT + 70000 i," +
                    " timestamp_sequence('2020-01-01T22:00:07', 5*1000000L) ts FROM long_sequence(200)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("cx");
            final int partitionCountBefore;
            try (TableReader reader = engine.getReader(token)) {
                partitionCountBefore = reader.getTxFile().getPartitionCount();
                Assert.assertTrue("the active day must be split in two", partitionCountBefore > 1);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_SQUASH_IDLE_TIMEOUT, "30m");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));
            final Clock pastTheSquashTimeout = () -> MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(
                    engine,
                    configuration.getFilesFacade(),
                    pastTheSquashTimeout
            )) {
                job.run();
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals("the active logical partition must be left to the writer's own squash",
                        partitionCountBefore, reader.getTxFile().getPartitionCount());
            }
            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n5960\n");
        });
    }

    @Test
    public void testScanPrunesQueuedSwapWhenTableIsDropped() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createCompositeDayTable("cx");
            final TableToken token = engine.verifyTableName("cx");
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            final long interval = engine.getConfiguration().getPartitionCompactionCheckInterval() * 1000;
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(
                    engine,
                    configuration.getFilesFacade(),
                    configuration.getMicrosecondClock()
            )) {
                try (TableWriter writer = engine.getWriter(token, "test")) {
                    setCurrentMicros(currentMicros + interval + 1);
                    job.run();
                    Assert.assertEquals(1, job.getPendingSwapMemoSize());
                    writer.destroy();
                }

                execute("DROP TABLE cx");
                setCurrentMicros(currentMicros + interval + 1);
                job.run();
                Assert.assertEquals("dropping a table must remove its in-flight records", 0, job.getPendingSwapMemoSize());
            }
        });
    }

    /**
     * A command the writer consumed WITHOUT moving {@code _txn} - here its rename fails - leaves {@code _txn} exactly
     * as the in-flight record describes it. The command reports that the writer applied it, so the next sweep
     * forgets the record and rebuilds the partition without waiting out the in-flight timeout.
     */
    @Test
    public void testScanRebuildsOnceTheWriterHasAppliedAFailedSwap() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);

        final AtomicInteger stagingMkdirs = new AtomicInteger();
        final AtomicInteger failedRenames = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                if (Utf8s.containsAscii(path, TableUtils.COMPACTING_DIR_MARKER)) {
                    stagingMkdirs.incrementAndGet();
                }
                return super.mkdirs(path, mode);
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (Utf8s.containsAscii(from, TableUtils.COMPACTING_DIR_MARKER) && failedRenames.get() == 0) {
                    failedRenames.incrementAndGet();
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };

        assertMemoryLeak(ff, () -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createCompositeDayTable("cx");
            final TableToken token = engine.verifyTableName("cx");
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            final long interval = engine.getConfiguration().getPartitionCompactionCheckInterval() * 1000;
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, ff, configuration.getMicrosecondClock())) {
                try (TableWriter writer = engine.getWriter(token, "test")) {
                    Assert.assertNotNull(writer);
                    setCurrentMicros(currentMicros + interval + 1);
                    job.run();
                    Assert.assertEquals(1, stagingMkdirs.get());
                    Assert.assertEquals(1, job.getPendingSwapMemoSize());
                }
                // Returning the writer to the pool ticks it, so the queued swap runs here - and its rename
                // fails, which leaves _txn exactly as the record describes it.
                Assert.assertEquals(1, failedRenames.get());

                setCurrentMicros(currentMicros + interval + 1);
                job.run();
                Assert.assertEquals("an applied command must release its record", 0, job.getPendingSwapMemoSize());
                Assert.assertEquals("the partition must be rebuilt once the record is gone", 2, stagingMkdirs.get());
            }

            engine.releaseAllWriters();
            engine.releaseAllReaders();
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertFalse("the partition must have been compacted in the end", reader.getTxFile().isPartitionComposite(0));
            }
            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n6010\n");
        });
    }

    @Test
    public void testScanRebuildsAfterTheWriterThatOwnedAQueuedSwapIsDestroyed() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);

        final AtomicInteger stagingMkdirs = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                if (Utf8s.containsAscii(path, TableUtils.COMPACTING_DIR_MARKER)) {
                    stagingMkdirs.incrementAndGet();
                }
                return super.mkdirs(path, mode);
            }
        };

        assertMemoryLeak(ff, () -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createCompositeDayTable("cx");
            final TableToken token = engine.verifyTableName("cx");
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            final long interval = engine.getConfiguration().getPartitionCompactionCheckInterval() * 1000;
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));

            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(
                    engine,
                    ff,
                    configuration.getMicrosecondClock()
            )) {
                try (TableWriter writer = engine.getWriter(token, "test")) {
                    setCurrentMicros(currentMicros + interval + 1);
                    job.run();
                    Assert.assertEquals(1, stagingMkdirs.get());
                    Assert.assertEquals(1, job.getPendingSwapMemoSize());
                    // destroy() drops the queued command without ticking it. The pool removes this writer
                    // instance when close() runs at the end of the block.
                    writer.destroy();
                }

                setCurrentMicros(currentMicros + interval + 1);
                job.run();
                Assert.assertEquals("a replacement writer must invalidate the old writer's in-flight record",
                        0, job.getPendingSwapMemoSize());
                Assert.assertEquals("the abandoned staging copy must be rebuilt", 2, stagingMkdirs.get());
            }

            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n6010\n");
        });
    }

    private void assertNoRebuildWhileSwapQueued(String idleTimeout) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        final AtomicInteger stagingMkdirs = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                if (Utf8s.containsAscii(path, TableUtils.COMPACTING_DIR_MARKER)) {
                    stagingMkdirs.incrementAndGet();
                }
                return super.mkdirs(path, mode);
            }
        };

        assertMemoryLeak(ff, () -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            execute("CREATE TABLE cx AS (SELECT x::INT i," +
                    " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO cx SELECT x::INT + 90000 i," +
                    " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)");
            drainWalQueue();
            execute("INSERT INTO cx SELECT x::INT + 70000 i," +
                    " timestamp_sequence('2020-01-01T04:00:07', 5*1000000L) ts FROM long_sequence(200)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("cx");
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertTrue("2020-01-01 should be composite", reader.getTxFile().isPartitionComposite(0));
            }
            engine.releaseAllReaders();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, idleTimeout);
            final long interval = engine.getConfiguration().getPartitionCompactionCheckInterval() * 1000;
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));

            // Holding the writer is what makes getWriterOrPublishCommand queue the swap instead of
            // applying it, which is the whole condition under test.
            try (TableWriter writer = engine.getWriter(token, "test");
                 PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, ff, configuration.getMicrosecondClock())) {
                Assert.assertNotNull(writer);
                setCurrentMicros(currentMicros + interval + 1);
                job.run();
                Assert.assertEquals("the first sweep should build the staging copy", 1, stagingMkdirs.get());

                // Cross the old one-hour memo TTL while the same writer and queued command remain live.
                setCurrentMicros(currentMicros + 61 * Micros.MINUTE_MICROS + interval + 1);
                job.run();
                setCurrentMicros(currentMicros + interval + 1);
                job.run();
                Assert.assertEquals("later sweeps must not rebuild a copy whose swap is already queued",
                        1, stagingMkdirs.get());

                writer.tick(true);
                setCurrentMicros(currentMicros + interval + 1);
                job.run();
                Assert.assertEquals("the completed swap record must be pruned", 0, job.getPendingSwapMemoSize());
            }

            // The queued swap landed, and the data is unchanged.
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n6010\n");
        });
    }

    /**
     * A swap queued onto a busy writer applies later, on the writer's own thread, and an
     * {@code ALTER TABLE ... ALTER COLUMN ... TYPE} can land in between - this is the ordinary WAL
     * ordering, since the sweep runs while {@code ApplyWal2TableJob} holds the writer and that job ticks
     * the writer right after applying the ALTER.
     * <p>
     * {@code ConvertOperatorImpl} rewrites the column IN PLACE: same directory, same partition name txn,
     * a new column name txn, and no {@code PartitionGeometry.publish}. So none of the generation terms
     * the swap tests moves, and a swap staged BEFORE the ALTER used to pass the staleness test after it -
     * renaming in a directory that predates the conversion and deleting the only directory holding the
     * converted column's files, which made every query on that partition fail with
     * "could not open, file does not exist". The metadata version is the term that catches it.
     */
    @Test
    public void testQueuedSwapIsDiscardedWhenAColumnTypeChangesBeforeItApplies() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            execute("CREATE TABLE cx AS (SELECT x::INT i," +
                    " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            // A later, plain day, so 2020-01-01 is never the active partition and the backfill is O3.
            execute("INSERT INTO cx SELECT x::INT + 90000 i," +
                    " timestamp_sequence('2020-01-03', 60*1000000L) ts FROM long_sequence(50)");
            drainWalQueue();
            execute("INSERT INTO cx SELECT x::INT + 70000 i," +
                    " timestamp_sequence('2020-01-01T04:00:07', 5*1000000L) ts FROM long_sequence(200)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("cx");
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertTrue("2020-01-01 should be composite", reader.getTxFile().isPartitionComposite(0));
            }
            engine.releaseAllReaders();

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));

            // Holding the writer makes getWriterOrPublishCommand queue the swap instead of applying it.
            try (TableWriter writer = engine.getWriter(token, "test")) {
                try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                    job.run();
                }
                // The ALTER lands between the build and the swap, exactly as the WAL apply loop orders it.
                writer.changeColumnType("i", ColumnType.LONG, 0, false, IndexType.NONE, 0, false, null);
                writer.tick();
            }

            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // The converted column must still be readable, and hold what it held before the ALTER.
            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n6010\n");
            assertQuery("SELECT count() c, sum(i) s FROM cx WHERE ts IN '2020-01-01'")
                    .noRandomAccess().expectSize().returns("c\ts\n5960\t30611780\n");
        });
    }

    /**
     * A composite partition compacted AFTER an ALTER COLUMN TYPE, which retires the converted column's
     * writer index and appends a new one. {@code TableReaderMetadata} is DENSE - it skips the retired
     * index - while {@code _cv} records are keyed by the WRITER index, so the two spaces diverge from
     * that point on. {@link io.questdb.cairo.TableReader} bridges them at every {@code _cv} access
     * ({@code metadata.getWriterIndex(columnIndex)}); {@code FrameImpl} does not, and
     * {@code PartitionCompactionScanJob} is the only caller that hands it reader metadata.
     * <p>
     * A shifted lookup reads some other column's name txn and column top, which is how a compaction can
     * map a retired, empty column file at a live column's row count.
     */
    @Test
    public void testCompactionAfterColumnTypeChangeReadsTheRightColumnFiles() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));

            execute("CREATE TABLE cx AS (" +
                    "SELECT x::INT a, x::INT i, timestamp_sequence('2020-01-01', 15*1000000L) ts " +
                    "FROM long_sequence(5760)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // A later, plain day, so 2020-01-01 is never the active partition.
            execute("INSERT INTO cx SELECT x::INT + 90000, x::INT + 90000, " +
                    "timestamp_sequence('2020-01-03', 60*1000000L) FROM long_sequence(50)");
            drainWalQueue();

            // Added late, so it carries a real column top in both existing partitions.
            execute("ALTER TABLE cx ADD COLUMN late LONG");
            drainWalQueue();

            // Lands only inside 2020-01-01, cutting it into pieces, and gives `late` rows above its top.
            execute("INSERT INTO cx (a, i, ts, late) SELECT x::INT + 70000, x::INT + 70000, " +
                    "timestamp_sequence('2020-01-01T04:00:07', 5*1000000L), x + 500000 FROM long_sequence(200)");
            drainWalQueue();

            // The writer-index hole.
            execute("ALTER TABLE cx ALTER COLUMN a TYPE LONG");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("cx");
            boolean hasIndexHole = false;
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertTrue("2020-01-01 should be composite", reader.getTxFile().isPartitionComposite(0));
                Assert.assertTrue("2020-01-01 should have more than one piece", reader.getGeometry().getPieceCount(0) > 1);
                for (int i = 0, n = reader.getMetadata().getColumnCount(); i < n; i++) {
                    if (reader.getMetadata().getWriterIndex(i) != i) {
                        hasIndexHole = true;
                        break;
                    }
                }
            }
            Assert.assertTrue("the conversion must leave dense != writer index, or this proves nothing", hasIndexHole);

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(token)) {
                Assert.assertFalse("2020-01-01 is idle, should have been compacted", reader.getTxFile().isPartitionComposite(0));
            }

            execute("CREATE TABLE cx_oracle AS (SELECT a::LONG a, i, ts, late FROM (" +
                    "SELECT x::INT a, x::INT i, timestamp_sequence('2020-01-01', 15*1000000L) ts, NULL::LONG late FROM long_sequence(5760)" +
                    " UNION ALL SELECT x::INT + 90000, x::INT + 90000, timestamp_sequence('2020-01-03', 60*1000000L), NULL::LONG FROM long_sequence(50)" +
                    " UNION ALL SELECT x::INT + 70000, x::INT + 70000, timestamp_sequence('2020-01-01T04:00:07', 5*1000000L), x + 500000 FROM long_sequence(200)" +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext,
                    "SELECT * FROM cx_oracle ORDER BY ts, i", "SELECT * FROM cx ORDER BY ts, i", LOG
            );
        });
    }

    /**
     * The directory a partition's data actually lives in.
     */
    private static String partitionDir(
            TableToken token,
            int timestampType,
            int partitionBy,
            long partitionTimestamp,
            long nameTxn
    ) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token.getDirName());
            TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, nameTxn);
            return path.toString();
        }
    }

    /**
     * How many logical-partition-merge staging directories {@code token}'s table folder still holds. A merge
     * that is neither renamed in nor removed leaves one behind.
     */
    private static int countMergeStagingDirs(FilesFacade ff, TableToken token) {
        final AtomicInteger count = new AtomicInteger();
        final Utf8StringSink name = new Utf8StringSink();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token.getDirName());
            ff.iterateDir(path.$(), (pUtf8NameZ, type) -> {
                name.clear();
                Utf8s.utf8ZCopy(pUtf8NameZ, name);
                if (Utf8s.containsAscii(name, TableUtils.MERGING_DIR_MARKER)) {
                    count.incrementAndGet();
                }
            });
        }
        return count.get();
    }

    private static boolean dirExists(FilesFacade ff, String dir) {
        try (Path path = new Path()) {
            return ff.exists(path.of(dir).$());
        }
    }

    private static String generatingDir(TableToken token, int timestampType, int partitionBy, long partitionTimestamp, long nameTxn) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token.getDirName());
            TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, nameTxn);
            path.put(TableUtils.GENERATING_DIR_MARKER);
            return path.toString();
        }
    }

    /**
     * What {@link TableWriter#installGeneratedParquet} re-checks a parquet copy of {@code day} against.
     */
    private static String parquetCopyInputs(TableWriter writer, long day) {
        final TxWriter tx = writer.getTxWriter();
        final int partitionIndex = tx.getPartitionIndex(day);
        if (partitionIndex < 0) {
            return "dropped";
        }
        final int lastIndex = tx.getPartitionCount() - 1;
        return "nameTxn=" + tx.getPartitionNameTxn(partitionIndex)
                + ", rows=" + tx.getPartitionSize(partitionIndex)
                + ", columnVersion=" + writer.getColumnVersionWriter().getMaxPartitionVersion(day)
                + ", metadataVersion=" + writer.getMetadataVersion()
                + ", active=" + (partitionIndex == lastIndex)
                + ", split=" + (partitionIndex < lastIndex
                && tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(partitionIndex + 1)) == day)
                + ", parquet=" + tx.isPartitionParquet(partitionIndex)
                + ", copy=" + tx.isPartitionParquetGenerated(partitionIndex)
                + ", composite=" + tx.isPartitionComposite(partitionIndex)
                + ", remote=" + tx.isPartitionRemote(partitionIndex);
    }

    /**
     * The directory name PartitionCompactionScanJob stages a REWRITE into.
     */
    private static String stagingDir(
            TableToken token,
            int timestampType,
            int partitionBy,
            long partitionTimestamp,
            long nameTxn,
            long writerTxn
    ) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token.getDirName());
            TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, nameTxn);
            path.put(TableUtils.COMPACTING_DIR_MARKER).put(writerTxn);
            return path.toString();
        }
    }

    private static void appendRow(TableWriter writer, String timestamp, int i) throws Exception {
        final TableWriter.Row row = writer.newRow(MicrosFormatUtils.parseTimestamp(timestamp));
        row.putInt(0, i);
        row.append();
    }

    /**
     * One sweep from a thread of its own, so a writer the test thread holds is busy from the job's point of
     * view and the swap goes to the writer's command queue instead of being applied on the spot.
     */
    private static void runSweepOnAnotherThread(FilesFacade ff) throws InterruptedException {
        final Throwable[] failure = new Throwable[1];
        final Thread sweeper = new Thread(() -> {
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, ff, configuration.getMicrosecondClock())) {
                job.run();
            } catch (Throwable e) {
                failure[0] = e;
            } finally {
                // What WorkerPool's worker-halt cleaners do for the compaction pool's own thread.
                Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                Path.clearThreadLocals();
            }
        });
        sweeper.start();
        sweeper.join();
        if (failure[0] != null) {
            throw new AssertionError("the sweep failed on its own thread", failure[0]);
        }
    }

    /**
     * Builds the copy while {@code ownerWriter} is checked out, so its install waits for the owner's next tick.
     */
    private static String stageParquetCopy(TableWriter ownerWriter, TableToken token, long day) throws InterruptedException {
        final TxWriter tx = ownerWriter.getTxWriter();
        final String stagedBuild = generatingDir(
                token, ColumnType.TIMESTAMP, PartitionBy.DAY, day, tx.getPartitionNameTxn(tx.getPartitionIndex(day))
        );
        Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequestParquetGeneration(token, day));
        final FilesFacade ff = configuration.getFilesFacade();
        runSweepOnAnotherThread(ff);
        Assert.assertTrue("the copy must be built and staged", dirExists(ff, stagedBuild));
        return stagedBuild;
    }

    /**
     * A staging directory holds a full copy of the partition, so give it a file to make the purge recurse.
     */
    private static void stageWithContent(FilesFacade ff, String dir) {
        try (Path path = new Path()) {
            TableUtils.createDirsOrFail(ff, path.of(dir).slash(), configuration.getMkDirMode());
            Assert.assertTrue(ff.touch(path.of(dir).concat("i.d").$()));
        }
    }
}
