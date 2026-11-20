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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.Numbers;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The persisted base table identity of a materialized view: the id of the base table that the view's refresh
 * position and cached refresh intervals belong to. The view state event carries it, the {@code _mv.s} state file
 * keeps it across the apply of refresh commits, and the load takes it from the newest view state event the view's
 * WAL has not applied yet, or else from the state file. A view whose state carries no id gets it at its first
 * refresh, which persists it with one view state event before any data commit.
 */
public class MatViewBaseIdentityTest extends AbstractCairoTest {
    private static final String VIEW_SQL = "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h";

    @Test
    public void testApplyFailureOfResumeStateLeavesOldIdAndNextRefreshInvalidates() throws Exception {
        // RESUME positions the view on a re-created base and writes the new id in its state event. When the apply job
        // fails to write _mv.s for that event, the file keeps the old id. After a restart the view's next refresh
        // finds the base table changed and invalidates the view without writing rows.
        final AtomicBoolean isStateWriteFailArmed = new AtomicBoolean();
        final AtomicInteger stateWriteFailures = new AtomicInteger();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isStateWriteFailArmed.get()
                        && Utf8s.endsWithAscii(name, MatViewState.MAT_VIEW_STATE_FILE_NAME)
                        && Utf8s.containsAscii(name, "price_1h")) {
                    isStateWriteFailArmed.set(false);
                    stateWriteFailures.incrementAndGet();
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("MANUAL");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            final int oldBaseTableId = baseTableId();
            assertPersistedBaseTableId(oldBaseTableId);

            execute("DROP TABLE base");
            createBase();
            insertBaseRow("b", 2.0, "2024-01-01T01:10:00.000000Z");
            insertBaseRow("c", 3.0, "2024-01-01T02:10:00.000000Z");
            drainWalAndMatViewQueues();
            Assert.assertTrue(viewState().isInvalid());
            final int newBaseTableId = baseTableId();
            Assert.assertNotEquals(oldBaseTableId, newBaseTableId);

            execute("REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG");
            Assert.assertEquals(newBaseTableId, viewState().getBaseTableId());
            isStateWriteFailArmed.set(true);
            drainWalQueue();
            Assert.assertEquals(1, stateWriteFailures.get());
            assertPersistedBaseTableId(oldBaseTableId);

            restart();
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertEquals(oldBaseTableId, viewState().getBaseTableId());
            final long viewSeqTxn = viewSeqTxn();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", MatViewRefreshJob.BASE_TABLE_CHANGED_REASON);
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    """);
        });
    }

    @Test
    public void testApplyOfDataCommitKeepsIdUnknownWithoutReadableStateFile() throws Exception {
        // The apply of a refresh commit keeps the id that _mv.s holds. Without a state file, or with one it cannot
        // read, it writes the id as unknown, which skips the base identity check: only a stale id is harmful.
        final AtomicBoolean isStateReadFailArmed = new AtomicBoolean();
        final AtomicInteger stateReadFailures = new AtomicInteger();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (isStateReadFailArmed.get()
                        && Utf8s.endsWithAscii(name, MatViewState.MAT_VIEW_STATE_FILE_NAME)
                        && Utf8s.containsAscii(name, "price_1h")) {
                    stateReadFailures.incrementAndGet();
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("MANUAL");
            drainWalQueue();
            // a refresh commit that no view state event precedes, as in the WAL of a previous version
            commitViewRow(1, 1.0);
            drainWalQueue();
            assertPersistedBaseTableId(MatViewState.BASE_TABLE_ID_UNKNOWN);

            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertPersistedBaseTableId(baseTableId());

            isStateReadFailArmed.set(true);
            try {
                commitViewRow(1, 2.0);
                drainWalQueue();
            } finally {
                isStateReadFailArmed.set(false);
            }
            Assert.assertTrue(stateReadFailures.get() > 0);
            assertPersistedBaseTableId(MatViewState.BASE_TABLE_ID_UNKNOWN);
        });
    }

    @Test
    public void testBaseIdSurvivesRefreshCommitsRestartAndCheckpoint() throws Exception {
        // The apply of a refresh commit keeps the id in _mv.s, and the checkpoint's copy of the state keeps it too.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("IMMEDIATE");
            drainWalAndMatViewQueues();
            for (int i = 0; i < 3; i++) {
                insertBaseRow("a", 2.0 + i, "2024-01-01T0" + (i + 1) + ":10:00.000000Z");
                drainWalAndMatViewQueues();
            }
            final int baseTableId = baseTableId();
            Assert.assertEquals(baseTableId, viewState().getBaseTableId());
            assertPersistedBaseTableId(baseTableId);
            restart();
            Assert.assertEquals(baseTableId, viewState().getBaseTableId());

            execute("CHECKPOINT CREATE");
            try {
                try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
                    path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory()).concat(viewToken())
                            .concat(MatViewState.MAT_VIEW_STATE_FILE_NAME);
                    reader.of(path.$());
                    final MatViewStateReader stateReader = new MatViewStateReader().of(reader, viewToken());
                    Assert.assertEquals(baseTableId, stateReader.getBaseTableId());
                    Assert.assertFalse(stateReader.isInvalid());
                }
            } finally {
                execute("CHECKPOINT RELEASE");
            }
        });
    }

    @Test
    public void testBackfillFrontierPersistKeepsId() throws Exception {
        // A user backfill below a view's REFRESH LIMIT makes the WAL apply job rewrite _mv.s with the advanced
        // backfill frontier; the rewrite keeps the base table id.
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-09-13T12:30:00.000000Z");
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('a', 5.0, '2024-09-10T12:00'), ('a', 9.0, '2024-09-13T12:00')");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH MANUAL AS (" + VIEW_SQL + ") PARTITION BY DAY");
            execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 2 DAYS");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertPersistedBaseTableId(baseTableId());
            final long frontierBefore = viewState().getPersistedBackfillFrontier();

            execute("INSERT INTO price_1h VALUES ('2024-09-10T18:00', 'a', 1.0)");
            drainWalQueue();
            Assert.assertTrue(viewState().getPersistedBackfillFrontier() > frontierBefore);
            assertPersistedBaseTableId(baseTableId());
        });
    }

    @Test
    public void testBackfillBeforeFirstRefreshPersistsUnknownId() throws Exception {
        // A user backfill below the REFRESH LIMIT of a view that never completed a refresh makes the WAL apply job
        // write the view's first _mv.s, with the backfill frontier. The view has no base table id yet, so the file
        // carries it as unknown, never another table's, and the view's first refresh then persists it.
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-09-13T12:30:00.000000Z");
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('a', 5.0, '2024-09-10T12:00'), ('a', 9.0, '2024-09-13T12:00')");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH MANUAL DEFERRED AS (" + VIEW_SQL + ") PARTITION BY DAY");
            execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 2 DAYS");
            drainWalAndMatViewQueues();
            try (Path path = new Path()) {
                Assert.assertFalse(TableUtils.isMatViewStateFileExists(configuration, path, viewToken().getDirName()));
            }
            final long frontierBefore = viewState().getPersistedBackfillFrontier();

            execute("INSERT INTO price_1h VALUES ('2024-09-10T18:00', 'a', 1.0)");
            drainWalQueue();
            Assert.assertTrue(viewState().getPersistedBackfillFrontier() > frontierBefore);
            Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, viewState().getBaseTableId());
            assertPersistedBaseTableId(MatViewState.BASE_TABLE_ID_UNKNOWN);

            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
            assertPersistedBaseTableId(baseTableId());
        });
    }

    @Test
    public void testFailingIdStateEventFailsRefreshBeforeData() throws Exception {
        // The view state event that persists the id of a new view's base table is the view's first transaction. When
        // its write fails, the refresh fails before any data commit: the view goes invalid with the write's error,
        // and holds no rows.
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 1);
        final AtomicBoolean isFailArmed = new AtomicBoolean();
        final StringSink failingPart = new StringSink();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isFailArmed.get() && Utf8s.endsWithAscii(name, failingPart)) {
                    isFailArmed.set(false);
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("IMMEDIATE");
            final long viewSeqTxn = viewSeqTxn();
            // with one transaction per part, the next view transaction opens the part of its own
            failingPart.put(viewToken().getDirName()).put(Files.SEPARATOR).put(WalUtils.SEQ_DIR).put(Files.SEPARATOR)
                    .put(WalUtils.TXNLOG_PARTS_DIR).put(Files.SEPARATOR).put(viewSeqTxn);
            isFailArmed.set(true);
            drainWalAndMatViewQueues();
            Assert.assertFalse(isFailArmed.get());
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, viewState().getBaseTableId());
            // the failed state event never reached the sequencer; the invalidation did, and no data commit
            Assert.assertEquals("INVALIDATE", viewTxnTypes());
            assertViewRows("ts\tsym\tprice\n");
        });
    }

    @Test
    public void testFirstRefreshOfNewViewWritesOneStateEventBeforeData() throws Exception {
        // The first refresh of a new view persists its base table id with one view state event before its data
        // commit; later refreshes write no such event. A later incremental refresh writes the state event of its
        // refresh-interval cache and its data commit, as before. A restart right after the first refresh loads the
        // id, also before the view's WAL applies the event.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("IMMEDIATE");
            Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, viewState().getBaseTableId());
            drainWalQueue();
            drainMatViewQueue(engine);
            Assert.assertEquals("INVALIDATE,DATA", viewTxnTypes());
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());

            // the view's WAL is not applied yet: the id comes from the unapplied state event
            restart();
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
            drainWalAndMatViewQueues();
            assertPersistedBaseTableId(baseTableId());
            restart();
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());

            insertBaseRow("a", 2.0, "2024-01-01T01:10:00.000000Z");
            drainWalAndMatViewQueues();
            insertBaseRow("a", 3.0, "2024-01-01T02:10:00.000000Z");
            drainWalAndMatViewQueues();
            // each later refresh: the cache event and the data commit
            Assert.assertEquals("INVALIDATE,DATA,INVALIDATE,DATA,INVALIDATE,DATA", viewTxnTypes());
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\ta\t2.0
                    2024-01-01T02:00:00.000000Z\ta\t3.0
                    """);
        });
    }

    @Test
    public void testFirstRefreshWithoutIdFailsCompileAndPersistsIdWithInvalidation() throws Exception {
        // A view whose state carries no id, and whose first refresh on this version fails to compile, persists the id
        // with its invalidation: the refresh persists the id before it compiles.
        assertMemoryLeak(() -> {
            createViewWithoutBaseId();
            execute("ALTER TABLE base DROP COLUMN price");
            insertBaseRow("a", "2024-01-01T01:10:00.000000Z");
            drainWalQueue();
            final long viewSeqTxn = viewSeqTxn();
            engine.getMatViewStateStore().enqueueIncrementalRefresh(viewToken());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "[21]: Invalid column: price");
            // the id event and the invalidation
            Assert.assertEquals(viewSeqTxn + 2, viewSeqTxn());
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
            restart();
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
            assertPersistedBaseTableId(baseTableId());
        });
    }

    @Test
    public void testFirstRefreshWithoutIdWritesOneStateEventBeforeData() throws Exception {
        // A view whose state carries no id, as one last written by a previous version, gets the id at its first
        // refresh on this version, which writes one view state event before its refresh-interval cache event and its
        // data commit. A restart right after it loads the id, also before the view's WAL applies the event.
        assertMemoryLeak(() -> {
            createViewWithoutBaseId();
            final String typesBefore = viewTxnTypes();
            insertBaseRow("a", 2.0, "2024-01-01T01:10:00.000000Z");
            drainWalQueue();
            drainMatViewQueue(engine);
            // the id event, then the cache event and the data commit of every incremental refresh
            Assert.assertEquals(typesBefore + ",INVALIDATE,INVALIDATE,DATA", viewTxnTypes());
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());

            restart();
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
            drainWalAndMatViewQueues();
            restart();
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());

            insertBaseRow("a", 3.0, "2024-01-01T02:10:00.000000Z");
            drainWalAndMatViewQueues();
            Assert.assertEquals(typesBefore + ",INVALIDATE,INVALIDATE,DATA,INVALIDATE,DATA", viewTxnTypes());
        });
    }

    @Test
    public void testFullAndResumeOntoNewBasePersistNewId() throws Exception {
        // FULL and RESUME position the view on the table now under the base table's name, and persist its id: a
        // restart afterwards keeps the view valid and refreshing.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("IMMEDIATE");
            drainWalAndMatViewQueues();

            for (String recovery : new String[]{"FULL", "RESUME SKIP BACKLOG"}) {
                execute("DROP TABLE base");
                createBase();
                insertBaseRow("b", 2.0, "2024-01-01T01:10:00.000000Z");
                drainWalAndMatViewQueues();
                assertViewStatus("invalid", "base table is dropped or renamed");

                execute("REFRESH MATERIALIZED VIEW price_1h " + recovery);
                drainWalAndMatViewQueues();
                assertViewStatus("valid", "");
                Assert.assertEquals(recovery, baseTableId(), viewState().getBaseTableId());
                assertPersistedBaseTableId(baseTableId());

                restart();
                Assert.assertEquals(recovery, baseTableId(), viewState().getBaseTableId());
                insertBaseRow("c", 3.0, "2024-01-01T02:10:00.000000Z");
                drainWalAndMatViewQueues();
                assertViewStatus("valid", "");
            }
        });
    }

    @Test
    public void testLoadDoesNotOpenAppliedEventsForId() throws Exception {
        // With 10,000 applied refresh commits in the view's current segment, the load opens the event of the newest
        // one for the view's state and no other WAL-e file: the id comes from _mv.s, since no unapplied view state
        // event is newer.
        // the commits need no durability here, and syncing each of them would take most of the test's time
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        final AtomicBoolean isCounting = new AtomicBoolean();
        final AtomicInteger eventOpens = new AtomicInteger();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (isCounting.get() && Utf8s.endsWithAscii(name, WalUtils.EVENT_FILE_NAME) && Utf8s.containsAscii(name, "price_1h")) {
                    eventOpens.incrementAndGet();
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("IMMEDIATE");
            drainWalAndMatViewQueues();
            final long ts = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
            final long hourMicros = 3_600_000_000L;
            try (WalWriter writer = engine.getWalWriter(viewToken())) {
                for (int i = 0; i < 10_000; i++) {
                    final TableWriter.Row row = writer.newRow(ts);
                    row.putSym(1, "a");
                    row.putDouble(2, i);
                    row.append();
                    writer.commitMatView(1, i, Numbers.LONG_NULL, ts, ts + hourMicros);
                }
            }
            drainWalQueue();
            Assert.assertEquals(10_002, viewSeqTxn());

            isCounting.set(true);
            try {
                restart();
            } finally {
                isCounting.set(false);
            }
            Assert.assertEquals(1, eventOpens.get());
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
            Assert.assertFalse(viewState().isInvalid());
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t9999.0
                    """);
        });
    }

    @Test
    public void testIncrementalCheckpointSkipsIntervalsOfReplacedBase() throws Exception {
        // An incremental checkpoint merges the base's WAL gap into each view's cached intervals. A view whose state
        // belongs to another table than the one now under the base's name keeps its state as it is.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("MANUAL");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            final int oldBaseTableId = baseTableId();

            execute("DROP TABLE base");
            // the drop's invalidation is lost, as in a crash
            final MatViewRefreshTask task = new MatViewRefreshTask();
            Assert.assertTrue(engine.getMatViewStateStore().tryDequeueRefreshTask(task));
            Assert.assertEquals(MatViewRefreshTask.INVALIDATE, task.operation);
            createBase();
            insertBaseRow("b", 2.0, "2024-01-01T01:10:00.000000Z");
            insertBaseRow("c", 3.0, "2024-01-01T02:10:00.000000Z");
            drainWalQueue();

            engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);
            try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
                path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory()).concat(viewToken())
                        .concat(MatViewState.MAT_VIEW_STATE_FILE_NAME);
                reader.of(path.$());
                final MatViewStateReader stateReader = new MatViewStateReader().of(reader, viewToken());
                Assert.assertEquals(oldBaseTableId, stateReader.getBaseTableId());
                Assert.assertEquals(-1, stateReader.getRefreshIntervalsBaseTxn());
                Assert.assertEquals(0, stateReader.getRefreshIntervals().size());
            } finally {
                execute("CHECKPOINT RELEASE");
            }
        });
    }

    @Test
    public void testIncrementalRefreshWithoutIdOntoOtherTimestampTypeFailsBeforeData() throws Exception {
        // A view whose state carries no id cannot tell a re-created base from its own, and takes the id of the table
        // under the base's name at its first refresh on this version. When that table has another designated
        // timestamp type, the compile check of the query's timestamp type fails the refresh before any data commit.
        assertMemoryLeak(() -> {
            createViewWithoutBaseId("MANUAL");
            execute("DROP TABLE base");
            drainWalAndMatViewQueues();
            Assert.assertTrue(viewState().isInvalid());
            // the view's invalidation by the drop is lost, as across a restart of a previous version
            try (WalWriter writer = engine.getWalWriter(viewToken())) {
                writer.resetMatViewState(1, 1, false, null, Numbers.LONG_NULL, null, -1);
            }
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insertBaseRow("b", 2.0, "2024-01-01T01:10:00.000000Z");
            insertBaseRow("c", 3.0, "2024-01-01T02:10:00.000000Z");
            drainWalQueue();
            restart();
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, viewState().getBaseTableId());

            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "[-1]: timestamp type mismatch between materialized view and query [view=TIMESTAMP, query=TIMESTAMP_NS]");
            assertViewRows("""
                    ts	sym	price
                    2024-01-01T00:00:00.000000Z	a	1.0
                    """);
        });
    }

    @Test
    public void testIncrementalCheckpointMergesIntervalsOfKnownAndUnknownId() throws Exception {
        // The incremental checkpoint merges the base's WAL gap into the cached intervals of a view whose state knows
        // the id of the table under the base's name, and of a view whose state carries no id.
        assertMemoryLeak(() -> {
            createViewWithoutBaseId("MANUAL");
            execute("CREATE MATERIALIZED VIEW price_2h REFRESH MANUAL AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 2h) PARTITION BY DAY");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_2h INCREMENTAL");
            drainWalAndMatViewQueues();
            final TableToken known = engine.verifyTableName("price_2h");
            Assert.assertEquals(baseTableId(), engine.getMatViewStateStore().getViewState(known).getBaseTableId());
            Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, viewState().getBaseTableId());

            insertBaseRow("b", 2.0, "2024-01-01T01:10:00.000000Z");
            drainWalQueue();
            final long baseSeqTxn = engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("base")).getSeqTxn();
            engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), true);
            try {
                for (TableToken viewToken : new TableToken[]{viewToken(), known}) {
                    try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
                        path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory()).concat(viewToken)
                                .concat(MatViewState.MAT_VIEW_STATE_FILE_NAME);
                        reader.of(path.$());
                        final MatViewStateReader stateReader = new MatViewStateReader().of(reader, viewToken);
                        Assert.assertEquals(viewToken.getTableName(), baseSeqTxn, stateReader.getRefreshIntervalsBaseTxn());
                        Assert.assertEquals(viewToken.getTableName(), 2, stateReader.getRefreshIntervals().size());
                    }
                }
            } finally {
                execute("CHECKPOINT RELEASE");
            }
        });
    }

    @Test
    public void testLegacyStateEventCarriesNoId() throws Exception {
        // A view state event in the legacy format has no trailing fields, so it carries no base table id.
        assertMemoryLeak(() -> {
            createBase();
            createView("MANUAL");
            try (WalWriter writer = engine.getWalWriter(viewToken())) {
                writer.setLegacyMatViewFormat(true);
                try {
                    writer.resetMatViewState(1, 1, true, "legacy", Numbers.LONG_NULL, null, -1, 777);
                } finally {
                    writer.setLegacyMatViewFormat(false);
                }
            }
            Assert.assertEquals(1, viewSeqTxn());
            Assert.assertEquals("INVALIDATE", viewTxnTypes());
            Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, firstEventBaseTableId());
        });
    }

    @Test
    public void testLoadScansPastStructuralChangeForId() throws Exception {
        // The load takes the id from the newest view state event that the view's WAL has not applied, past a
        // structural change of the view that lies between it and the newest refresh commit.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("MANUAL");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            final MatViewState state = viewState();
            try (WalWriter writer = engine.getWalWriter(viewToken())) {
                writer.resetMatViewState(state.getLastRefreshBaseTxn(), 1, false, null, Numbers.LONG_NULL, null, -1, 12_345);
            }
            commitViewRow(1, 2.0);
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra DOUBLE");
            commitViewRow(1, 3.0);
            Assert.assertTrue(viewTxnTypes().endsWith(",INVALIDATE,DATA,STRUCTURE,DATA"));
            restart();
            Assert.assertEquals(12_345, viewState().getBaseTableId());

            // the form without the applied seqTxn takes the id from the state file alone
            try (
                    Path path = new Path();
                    BlockFileReader reader = new BlockFileReader(configuration);
                    WalEventReader eventReader = new WalEventReader(configuration)
            ) {
                final MatViewStateReader stateReader = new MatViewStateReader();
                path.of(configuration.getDbRoot()).concat(viewToken());
                Assert.assertTrue(WalUtils.readMatViewState(
                        path,
                        viewToken(),
                        configuration,
                        io.questdb.cairo.vm.Vm.getCMRInstance(),
                        eventReader,
                        reader,
                        stateReader
                ));
                Assert.assertEquals(baseTableId(), stateReader.getBaseTableId());
            }
        });
    }

    @Test
    public void testLoadReadsIdFromTxnLogParts() throws Exception {
        // The load scans a transaction log kept in parts the same way: the id comes from the newest unapplied view
        // state event, which can lie in an older part than the newest transaction.
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 2);
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("MANUAL");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            final MatViewState state = viewState();
            try (WalWriter writer = engine.getWalWriter(viewToken())) {
                writer.resetMatViewState(state.getLastRefreshBaseTxn(), 1, false, null, Numbers.LONG_NULL, null, -1, 12_345);
            }
            commitViewRow(1, 2.0);
            commitViewRow(1, 3.0);
            Assert.assertEquals("INVALIDATE,DATA,INVALIDATE,DATA,DATA", viewTxnTypes());
            restart();
            Assert.assertEquals(12_345, viewState().getBaseTableId());
            // with the view's WAL applied, the id comes from the state file
            drainWalQueue();
            restart();
            Assert.assertEquals(12_345, viewState().getBaseTableId());
            assertPersistedBaseTableId(12_345);
        });
    }

    @Test
    public void testLoadSkipsUnreadableUnappliedEvent() throws Exception {
        // An unapplied view state event whose WAL-e file is gone gives no id; the load takes it from _mv.s.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("MANUAL");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            final int walId;
            final int segmentId;
            final MatViewState state = viewState();
            try (WalWriter writer = engine.getWalWriter(viewToken())) {
                writer.resetMatViewState(state.getLastRefreshBaseTxn(), 1, false, null, Numbers.LONG_NULL, null, -1, 12_345);
                walId = writer.getWalId();
                segmentId = writer.getSegmentId();
                writer.rollSegment();
                final TableWriter.Row row = writer.newRow(parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z"));
                row.putSym(1, "a");
                row.putDouble(2, 2.0);
                row.append();
                final long ts = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
                writer.commitMatView(1, 1, Numbers.LONG_NULL, ts, ts + 3_600_000_000L);
            }
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(viewToken()).concat(WalUtils.WAL_NAME_BASE).put(walId)
                        .slash().put(segmentId).concat(WalUtils.EVENT_FILE_NAME);
                Assert.assertTrue(configuration.getFilesFacade().removeQuiet(path.$()));
            }
            restart();
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
        });
    }

    @Test
    public void testLoadWithUnreadableTxnScansWholeWal() throws Exception {
        // When the load cannot read the view's applied seqTxn, it scans every transaction of the view's WAL for the id.
        final AtomicBoolean isTxnReadFailArmed = new AtomicBoolean();
        final AtomicInteger eventOpens = new AtomicInteger();
        final StringSink viewTxnFile = new StringSink();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (isTxnReadFailArmed.get() && Utf8s.containsAscii(name, "price_1h")) {
                    if (Utf8s.endsWithAscii(name, viewTxnFile)) {
                        return -1;
                    }
                    if (Utf8s.endsWithAscii(name, WalUtils.EVENT_FILE_NAME)) {
                        eventOpens.incrementAndGet();
                    }
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("IMMEDIATE");
            drainWalAndMatViewQueues();
            insertBaseRow("a", 2.0, "2024-01-01T01:10:00.000000Z");
            drainWalAndMatViewQueues();
            viewTxnFile.put(viewToken().getDirName()).put(Files.SEPARATOR).put(TableUtils.TXN_FILE_NAME);
            isTxnReadFailArmed.set(true);
            try {
                restart();
            } finally {
                isTxnReadFailArmed.set(false);
            }
            // the newest event for the state, and the cache event before it for the id
            Assert.assertEquals(2, eventOpens.get());
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
        });
    }

    @Test
    public void testLoadAfterFailedFirstStateWriteKeepsRowsOverDroppedBasePartition() throws Exception {
        // The apply of the view's first refresh commit fails to write _mv.s, which keeps the never-refreshed position
        // of the base id event before it. After a base DROP PARTITION and an applied view ADD COLUMN, the load takes
        // the position from the refresh commit, so the next refresh keeps the view's rows over the dropped partition
        // instead of rebuilding the view as a first build.
        final AtomicInteger stateOpensToPass = new AtomicInteger(-1);
        final AtomicInteger stateWriteFailures = new AtomicInteger();
        assertMemoryLeak(newStateWriteFailingFacade(stateOpensToPass, stateWriteFailures), () -> {
            createBase();
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, '2024-01-02T00:10:00.000000Z'),
                    ('c', 3.0, '2024-01-03T00:10:00.000000Z')
                    """);
            createView("MANUAL");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainMatViewQueue(engine);
            Assert.assertEquals("INVALIDATE,DATA", viewTxnTypes());
            // the base id event's _mv.s write passes, the refresh commit's fails
            stateOpensToPass.set(1);
            drainWalQueue();
            stateOpensToPass.set(-1);
            Assert.assertEquals(1, stateWriteFailures.get());
            assertPersistedRefreshBaseTxn(-1);
            Assert.assertEquals(1, viewState().getLastRefreshBaseTxn());

            execute("ALTER TABLE base DROP PARTITION LIST '2024-01-02'");
            drainWalQueue();
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra DOUBLE");
            drainWalQueue();
            Assert.assertEquals("INVALIDATE,DATA,STRUCTURE", viewTxnTypes());
            restart();
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertEquals(1, viewState().getLastRefreshBaseTxn());

            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice\textra
                    2024-01-01T00:00:00.000000Z\ta\t1.0\tnull
                    2024-01-02T00:00:00.000000Z\tb\t2.0\tnull
                    2024-01-03T00:00:00.000000Z\tc\t3.0\tnull
                    """);
        });
    }

    @Test
    public void testLoadAfterFailedStateWriteTakesAppliedRefreshCommitBelowPendingAlterAs() throws Exception {
        // The apply of a refresh commit fails to write _mv.s, which keeps the previous refresh's position, and an
        // ALTER ... AS waits in the view's WAL above the commit. The load passes the pending definition and takes the
        // position from the applied refresh commit, so the next refresh leaves the refreshed buckets as they are
        // instead of recomputing them with the new SQL.
        final AtomicInteger stateOpensToPass = new AtomicInteger(-1);
        final AtomicInteger stateWriteFailures = new AtomicInteger();
        assertMemoryLeak(newStateWriteFailingFacade(stateOpensToPass, stateWriteFailures), () -> {
            refreshTwiceFailingSecondStateWrite(stateOpensToPass, stateWriteFailures);
            execute("ALTER MATERIALIZED VIEW price_1h AS (SELECT ts, sym, first(price) AS price FROM base SAMPLE BY 1h)");
            Assert.assertEquals("INVALIDATE,DATA,INVALIDATE,DATA," + WalTxnType.MAT_VIEW_DEFINITION, viewTxnTypes());
            Assert.assertEquals(viewSeqTxn() - 1, engine.getTableSequencerAPI().getTxnTracker(viewToken()).getWriterTxn());
            restart();
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertEquals(2, viewState().getLastRefreshBaseTxn());

            drainWalQueue();
            final long viewSeqTxn = viewSeqTxn();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t5.0
                    """);
        });
    }

    @Test
    public void testLoadAfterFailedStateWriteTakesAppliedRefreshCommitBelowViewColumnChange() throws Exception {
        // The apply of a refresh commit fails to write _mv.s, which keeps the previous refresh's position and the
        // cached intervals that the commit refreshed, and an applied view ADD COLUMN, a structural transaction without
        // view state, lies above the commit. The load passes the column change and takes the state from the applied
        // refresh commit, so the next refresh has nothing to recompute.
        final AtomicInteger stateOpensToPass = new AtomicInteger(-1);
        final AtomicInteger stateWriteFailures = new AtomicInteger();
        assertMemoryLeak(newStateWriteFailingFacade(stateOpensToPass, stateWriteFailures), () -> {
            refreshTwiceFailingSecondStateWrite(stateOpensToPass, stateWriteFailures);
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra DOUBLE");
            drainWalQueue();
            Assert.assertEquals("INVALIDATE,DATA,INVALIDATE,DATA,STRUCTURE", viewTxnTypes());
            restart();
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertEquals(2, viewState().getLastRefreshBaseTxn());
            Assert.assertEquals(0, viewState().getRefreshIntervals().size());
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());

            final long viewSeqTxn = viewSeqTxn();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            assertViewRows("""
                    ts\tsym\tprice\textra
                    2024-01-01T00:00:00.000000Z\ta\t1.0\tnull
                    2024-01-01T01:00:00.000000Z\tb\t5.0\tnull
                    """);
        });
    }

    @Test
    public void testLoadDoesNotOpenAppliedTxnsWithoutState() throws Exception {
        // With 10,000 applied refresh commits and three applied commits without view state above them, the load opens
        // the event of the newest one and takes the state from _mv.s, which holds the applied transactions.
        // the commits need no durability here, and syncing each of them would take most of the test's time
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        final AtomicBoolean isCounting = new AtomicBoolean();
        final AtomicInteger eventOpens = new AtomicInteger();
        assertMemoryLeak(newViewEventOpenCountingFacade(isCounting, eventOpens), () -> {
            createViewWithAppliedRefreshCommits();
            commitViewRowsWithoutState(3);
            drainWalQueue();
            Assert.assertEquals(10_005, viewSeqTxn());

            restartCountingViewEventOpens(isCounting);
            Assert.assertEquals(1, eventOpens.get());
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertEquals(1, viewState().getLastRefreshBaseTxn());
            Assert.assertEquals(9_999, viewState().getLastRefreshFinishTimestampUs());
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
        });
    }

    @Test
    public void testLoadOpensUnappliedTxnsDownToStateEvent() throws Exception {
        // Above 10,000 applied refresh commits, an unapplied view state event and three unapplied commits without view
        // state above it: the load opens the three commits' events and the state event's, takes the state and the id
        // from the state event, and opens no applied transaction's event.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        final AtomicBoolean isCounting = new AtomicBoolean();
        final AtomicInteger eventOpens = new AtomicInteger();
        assertMemoryLeak(newViewEventOpenCountingFacade(isCounting, eventOpens), () -> {
            createViewWithAppliedRefreshCommits();
            try (WalWriter writer = engine.getWalWriter(viewToken())) {
                writer.resetMatViewState(1, 77, true, "test reason", Numbers.LONG_NULL, null, -1, 12_345);
            }
            commitViewRowsWithoutState(3);
            Assert.assertEquals(10_006, viewSeqTxn());

            restartCountingViewEventOpens(isCounting);
            Assert.assertEquals(4, eventOpens.get());
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertEquals("test reason", viewState().getInvalidationReasonForReport());
            Assert.assertEquals(1, viewState().getLastRefreshBaseTxn());
            Assert.assertEquals(77, viewState().getLastRefreshFinishTimestampUs());
            Assert.assertEquals(12_345, viewState().getBaseTableId());
        });
    }

    @Test
    public void testLoadStopsAtNewestAppliedTxnBelowUnappliedTxnsWithoutState() throws Exception {
        // Above 10,000 applied refresh commits, three unapplied commits without view state: the load opens their three
        // events and the newest applied refresh commit's, which gives the state, and no other applied transaction's.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        final AtomicBoolean isCounting = new AtomicBoolean();
        final AtomicInteger eventOpens = new AtomicInteger();
        assertMemoryLeak(newViewEventOpenCountingFacade(isCounting, eventOpens), () -> {
            createViewWithAppliedRefreshCommits();
            commitViewRowsWithoutState(3);
            Assert.assertEquals(10_005, viewSeqTxn());

            restartCountingViewEventOpens(isCounting);
            Assert.assertEquals(4, eventOpens.get());
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertEquals(1, viewState().getLastRefreshBaseTxn());
            Assert.assertEquals(9_999, viewState().getLastRefreshFinishTimestampUs());
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
        });
    }

    @Test
    public void testLoadWithUnknownAppliedTxnAndOnlyTxnsWithoutStateTakesStateFile() throws Exception {
        // A REFRESH LIMIT view that never refreshed holds only transactions without view state: SET REFRESH LIMIT and
        // a backfill. With the view's applied seqTxn unknown, the load takes the state from _mv.s at the first
        // transaction, and finds no state without the file.
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-09-13T12:30:00.000000Z");
            createBase();
            execute("INSERT INTO base VALUES ('a', 5.0, '2024-09-10T12:00'), ('a', 9.0, '2024-09-13T12:00')");
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH MANUAL DEFERRED AS (" + VIEW_SQL + ") PARTITION BY DAY");
            execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 2 DAYS");
            drainWalQueue();
            // The anchor min(max(base) 09-13T12:00, now) less the limit puts the boundary at 09-11T12:00, so the
            // backfill at 09-10T18:00 is frozen, and its apply sets the frontier to its bucket end 09-10T19:00 plus the
            // limit of 2 days.
            execute("INSERT INTO price_1h VALUES ('2024-09-10T18:00', 'a', 1.0)");
            drainWalQueue();
            final long frontier = parseFloorPartialTimestamp("2024-09-12T19:00:00.000000Z");
            Assert.assertEquals(frontier, viewState().getBackfillFrontier());
            Assert.assertEquals(WalTxnType.SQL + ",REPLACE", viewTxnTypes());

            final MatViewStateReader stateReader = new MatViewStateReader();
            Assert.assertTrue(readViewState(-1, stateReader));
            Assert.assertFalse(stateReader.isInvalid());
            Assert.assertEquals(-1, stateReader.getLastRefreshBaseTxn());
            Assert.assertEquals(frontier, stateReader.getBackfillFrontier());

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(viewToken()).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME);
                Assert.assertTrue(configuration.getFilesFacade().removeQuiet(path.$()));
            }
            Assert.assertFalse(readViewState(-1, new MatViewStateReader()));
        });
    }

    @Test
    public void testLoadWithUnknownAppliedTxnTakesNewestStateEvent() throws Exception {
        // With the view's applied seqTxn unknown, every transaction counts as unapplied: the load passes a view DROP
        // PARTITION to RESUME's view state event below it.
        assertMemoryLeak(() -> {
            createBase();
            execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z'), ('b', 2.0, '2024-01-02T00:10:00.000000Z')");
            createView("IMMEDIATE");
            drainWalAndMatViewQueues();
            engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "test invalidation");
            drainWalAndMatViewQueues();
            Assert.assertTrue(viewState().isInvalid());
            execute("REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG");
            execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-01'");
            Assert.assertTrue(viewTxnTypes().endsWith(",INVALIDATE,INVALIDATE," + WalTxnType.SQL));

            final MatViewStateReader stateReader = new MatViewStateReader();
            Assert.assertTrue(readViewState(-1, stateReader));
            Assert.assertFalse(stateReader.isInvalid());
            Assert.assertEquals(1, stateReader.getLastRefreshBaseTxn());
            Assert.assertEquals(baseTableId(), stateReader.getBaseTableId());
        });
    }

    @Test
    public void testLoadWithUnknownAppliedTxnTakesStateFileAtUnreadableEvent() throws Exception {
        // With the view's applied seqTxn unknown, the load passes a commit without view state to the view state event
        // below it, whose WAL-e file is gone: the state comes from _mv.s, and the base table id from the newest view
        // state event it can read, as for any unreadable event.
        assertMemoryLeak(() -> {
            createBase();
            createView("MANUAL");
            final int walId;
            try (WalWriter writer = engine.getWalWriter(viewToken())) {
                walId = writer.getWalId();
                // segment 0
                writer.resetMatViewState(1, 11, false, null, Numbers.LONG_NULL, null, -1, 111);
                writer.rollSegment();
                // segment 1
                writer.resetMatViewState(2, 22, true, "second", Numbers.LONG_NULL, null, -1, 222);
                writer.rollSegment();
            }
            drainWalQueue();
            assertPersistedBaseTableId(222);
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(viewToken()).concat(WalUtils.WAL_NAME_BASE).put(walId)
                        .slash().put(1).concat(WalUtils.EVENT_FILE_NAME);
                Assert.assertTrue(configuration.getFilesFacade().removeQuiet(path.$()));
            }
            // segment 2
            commitViewRowsWithoutState(1);
            Assert.assertEquals(3, viewSeqTxn());

            final MatViewStateReader stateReader = new MatViewStateReader();
            Assert.assertTrue(readViewState(-1, stateReader));
            Assert.assertTrue(stateReader.isInvalid());
            Assert.assertEquals("second", Chars.toString(stateReader.getInvalidationReason()));
            Assert.assertEquals(2, stateReader.getLastRefreshBaseTxn());
            Assert.assertEquals(22, stateReader.getLastRefreshTimestampUs());
            Assert.assertEquals(111, stateReader.getBaseTableId());
        });
    }

    @Test
    public void testPeriodRangeRefreshKeepsIdInStateEvent() throws Exception {
        // The period timer's range refresh writes a view state event with the period it reached; the event carries
        // the base table id.
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH IMMEDIATE PERIOD (LENGTH 1d) AS (" + VIEW_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
            currentMicros = parseFloorPartialTimestamp("2024-01-02T01:00:00.000000Z");
            final io.questdb.cairo.mv.MatViewTimerJob timerJob = new io.questdb.cairo.mv.MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-02T00:00:00.000000Z"), viewState().getLastPeriodHi());
            Assert.assertTrue(viewTxnTypes().endsWith(",REPLACE,INVALIDATE"));
            assertPersistedBaseTableId(baseTableId());
            assertViewRows("""
                    ts	sym	price
                    2024-01-01T00:00:00.000000Z	a	1.0
                    """);
        });
    }

    @Test
    public void testRangeAndCacheUpdatesKeepUnknownIdUnknown() throws Exception {
        // Only an incremental refresh sets an unknown id. RANGE, the refresh-interval cache update and the ATTACH /
        // UPDATE range task leave it unknown, and refresh as usual.
        assertMemoryLeak(() -> {
            createViewWithoutBaseId("MANUAL");
            insertBaseRow("a", 2.0, "2024-01-01T00:20:00.000000Z");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01' TO '2024-01-02'");
            drainWalAndMatViewQueues();
            Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, viewState().getBaseTableId());
            assertViewRows("""
                    ts	sym	price
                    2024-01-01T00:00:00.000000Z	a	2.0
                    """);
            engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken());
            drainWalAndMatViewQueues();
            Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, viewState().getBaseTableId());
            Assert.assertEquals(2, viewState().getRefreshIntervals().size());
            engine.getMatViewStateStore().enqueueAddRefreshRange(
                    viewToken(),
                    engine.verifyTableName("base"),
                    parseFloorPartialTimestamp("2024-01-01T05:00:00.000000Z"),
                    parseFloorPartialTimestamp("2024-01-01T05:59:59.999999Z"),
                    2
            );
            drainWalAndMatViewQueues();
            Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, viewState().getBaseTableId());
            Assert.assertEquals(4, viewState().getRefreshIntervals().size());
            assertViewStatus("valid", "");

            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
        });
    }

    @Test
    public void testShortBaseTableBlockReadsAsUnknown() throws Exception {
        // A base table block shorter than the id, which no version writes, reads as carrying no id.
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
                    BlockFileReader reader = new BlockFileReader(configuration)
            ) {
                path.of(temp.newFolder().getAbsolutePath()).concat("short_mv.s");
                writer.of(path.$());
                AppendableBlock block = writer.append();
                MatViewState.appendState(1, false, null, block);
                block.commit(MatViewState.MAT_VIEW_STATE_FORMAT_MSG_TYPE);
                block = writer.append();
                block.commit(MatViewState.MAT_VIEW_STATE_FORMAT_EXTRA_BASE_TABLE_MSG_TYPE);
                writer.commit();

                reader.of(path.$());
                Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, MatViewStateReader.readBaseTableId(reader));
                final MatViewStateReader stateReader = new MatViewStateReader();
                final TableToken token = new TableToken("x", "x", null, 1, false, false, false);
                Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, stateReader.of(reader, token).getBaseTableId());
                stateReader.setBaseTableId(42);
                stateReader.ofFrozenZoneOverlay(reader);
                Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, stateReader.getBaseTableId());

                // A state file that a previous version wrote has no base table block, and a block of a type that
                // a later version adds is skipped.
                path.of(temp.newFolder().getAbsolutePath()).concat("old_mv.s");
                writer.of(path.$());
                block = writer.append();
                MatViewState.appendState(1, false, null, block);
                block.commit(MatViewState.MAT_VIEW_STATE_FORMAT_MSG_TYPE);
                block = writer.append();
                block.putInt(7);
                block.commit(99);
                writer.commit();
                reader.of(path.$());
                Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, MatViewStateReader.readBaseTableId(reader));
                Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, stateReader.of(reader, token).getBaseTableId());
                Assert.assertEquals(1, stateReader.getLastRefreshBaseTxn());
            }
        });
    }

    @Test
    public void testRestartAfterFullOntoRecreatedBaseLoadsNewId() throws Exception {
        // FULL onto a re-created base writes the new id with its post-truncate state event. A restart before the
        // view's WAL applies it loads the new id from the unapplied event, not the stale one in _mv.s.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("IMMEDIATE");
            drainWalAndMatViewQueues();
            final int oldBaseTableId = baseTableId();

            execute("DROP TABLE base");
            createBase();
            insertBaseRow("b", 2.0, "2024-01-01T01:10:00.000000Z");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table is dropped or renamed");
            final int newBaseTableId = baseTableId();
            Assert.assertNotEquals(oldBaseTableId, newBaseTableId);

            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainMatViewQueue(engine);
            assertPersistedBaseTableId(oldBaseTableId);
            restart();
            Assert.assertEquals(newBaseTableId, viewState().getBaseTableId());
            Assert.assertFalse(viewState().isInvalid());

            drainWalAndMatViewQueues();
            assertPersistedBaseTableId(newBaseTableId);
            insertBaseRow("c", 3.0, "2024-01-01T02:10:00.000000Z");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-01T02:00:00.000000Z\tc\t3.0
                    """);
        });
    }

    @Test
    public void testRestartAfterResumeBeforeApplyLoadsNewId() throws Exception {
        // RESUME onto a re-created base writes the new id in its view state event. A restart before the view's WAL
        // applies it, while that event is the newest of the view's WAL, loads the new id.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("MANUAL");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            final int oldBaseTableId = baseTableId();
            execute("DROP TABLE base");
            createBase();
            insertBaseRow("b", 2.0, "2024-01-01T01:10:00.000000Z");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table is dropped or renamed");

            execute("REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG");
            Assert.assertTrue(viewTxnTypes().endsWith(",INVALIDATE,INVALIDATE"));
            assertPersistedBaseTableId(oldBaseTableId);
            restart();
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
        });
    }

    @Test
    public void testRestartAfterIncrementalRefreshKeepsIdKnown() throws Exception {
        // After an incremental refresh the newest event of the view's WAL is a data commit, which carries no id. A
        // restart keeps the id known through _mv.s.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("IMMEDIATE");
            drainWalAndMatViewQueues();
            insertBaseRow("a", 2.0, "2024-01-01T01:10:00.000000Z");
            drainWalAndMatViewQueues();
            Assert.assertTrue(viewTxnTypes().endsWith(",DATA"));
            restart();
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
        });
    }

    @Test
    public void testRestartAfterResumeOntoRecreatedBaseAndViewChangeLoadsResume() throws Exception {
        // RESUME SKIP BACKLOG positions the view on a re-created base and writes the new id in its view state event. A
        // view DROP EXPIRE, which carries no view state, follows it, and a restart comes before the view's WAL applies
        // them: the view loads valid on the new base, and the new base's next commit reaches it.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("IMMEDIATE");
            drainWalAndMatViewQueues();
            final int oldBaseTableId = baseTableId();
            execute("DROP TABLE base");
            createBase();
            insertBaseRow("b", 2.0, "2024-01-01T01:10:00.000000Z");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table is dropped or renamed");
            final int newBaseTableId = baseTableId();
            Assert.assertNotEquals(oldBaseTableId, newBaseTableId);

            execute("REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG");
            execute("ALTER MATERIALIZED VIEW price_1h DROP EXPIRE");
            Assert.assertTrue(viewTxnTypes().endsWith(",INVALIDATE," + WalTxnType.SQL));
            assertPersistedBaseTableId(oldBaseTableId);
            restart();
            Assert.assertFalse(viewState().isInvalid());
            // RESUME jumped to the new base's applied transaction, the insert of b
            Assert.assertEquals(1, viewState().getLastRefreshBaseTxn());
            Assert.assertEquals(newBaseTableId, viewState().getBaseTableId());

            drainWalAndMatViewQueues();
            assertPersistedBaseTableId(newBaseTableId);
            insertBaseRow("c", 3.0, "2024-01-01T02:10:00.000000Z");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            // b is the gap RESUME SKIP BACKLOG jumped over
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T02:00:00.000000Z\tc\t3.0
                    """);
        });
    }

    @Test
    public void testRestartBeforeFirstRefreshAppliesLoadsIt() throws Exception {
        // A new view's first refresh commits the id state event and the data, and a view SET TTL follows; a restart
        // comes before the view's WAL applies anything, with no _mv.s yet. The view loads the refresh's position and
        // the base table id.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            drainWalQueue();
            createView("IMMEDIATE");
            drainMatViewQueue(engine);
            execute("ALTER MATERIALIZED VIEW price_1h SET TTL 30 DAYS");
            Assert.assertEquals("INVALIDATE,DATA," + WalTxnType.SQL, viewTxnTypes());
            Assert.assertEquals(0, engine.getTableSequencerAPI().getTxnTracker(viewToken()).getWriterTxn());

            restart();
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertEquals(1, viewState().getLastRefreshBaseTxn());
            Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    """);
        });
    }

    @Test
    public void testRestartLoadsUnappliedLegacyStateEventBelowViewChange() throws Exception {
        // A view state event in the legacy format, as a previous version writes it, and a view SET TTL above it, both
        // unapplied: the restart loads the event's invalidation, which carries no base table id.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("MANUAL");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertPersistedBaseTableId(baseTableId());
            try (WalWriter writer = engine.getWalWriter(viewToken())) {
                writer.setLegacyMatViewFormat(true);
                try {
                    writer.resetMatViewState(1, 1, true, "legacy", Numbers.LONG_NULL, null, -1, 777);
                } finally {
                    writer.setLegacyMatViewFormat(false);
                }
            }
            execute("ALTER MATERIALIZED VIEW price_1h SET TTL 30 DAYS");
            Assert.assertTrue(viewTxnTypes().endsWith(",INVALIDATE," + WalTxnType.SQL));

            restart();
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertEquals("legacy", viewState().getInvalidationReasonForReport());
            Assert.assertEquals(1, viewState().getLastRefreshBaseTxn());
            Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, viewState().getBaseTableId());
        });
    }

    @Test
    public void testRestartLoadsStateAtAppliedViewColumnChangeAbovePurgedTxnLogParts() throws Exception {
        // With the view's transaction log in parts of one transaction, the view's newest transaction is an applied view
        // ADD COLUMN, a structural transaction without a WAL event, and the WAL purge deleted the parts below it. A
        // restart takes the state from _mv.s there and reads no deleted part: the view keeps its refresh position, its
        // base table and, in the second round, its invalidation.
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 1);
        assertMemoryLeak(() -> {
            for (int round = 0; round < 2; round++) {
                final boolean isInvalid = round == 1;
                createBase();
                execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z'), ('b', 2.0, '2024-01-02T00:10:00.000000Z')");
                createView("IMMEDIATE");
                drainWalAndMatViewQueues();
                if (isInvalid) {
                    engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "test invalidation");
                    drainWalAndMatViewQueues();
                }
                execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra DOUBLE");
                drainWalQueue();
                Assert.assertEquals(isInvalid ? "INVALIDATE,DATA,INVALIDATE,STRUCTURE" : "INVALIDATE,DATA,STRUCTURE", viewTxnTypes());
                MatViewTxnLogParts.purge(engine, viewToken());
                final long appliedSeqTxn = engine.getTableSequencerAPI().getTxnTracker(viewToken()).getWriterTxn();
                for (long part = 0; part < appliedSeqTxn - 1; part++) {
                    Assert.assertFalse(MatViewTxnLogParts.exists(engine, viewToken(), part));
                }
                Assert.assertTrue(MatViewTxnLogParts.exists(engine, viewToken(), appliedSeqTxn - 1));

                restart();
                Assert.assertEquals(isInvalid, viewState().isInvalid());
                Assert.assertEquals(isInvalid ? "test invalidation" : null, viewState().getInvalidationReasonForReport());
                Assert.assertEquals(1, viewState().getLastRefreshBaseTxn());
                Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
                drainWalAndMatViewQueues();
                insertBaseRow("c", 3.0, "2024-01-02T05:10:00.000000Z");
                drainWalAndMatViewQueues();
                assertQuery("SELECT view_status, refresh_base_table_txn, base_table_txn FROM materialized_views() WHERE view_name = 'price_1h'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns(isInvalid
                                ? """
                                view_status\trefresh_base_table_txn\tbase_table_txn
                                invalid\t1\t2
                                """
                                : """
                                view_status\trefresh_base_table_txn\tbase_table_txn
                                valid\t2\t2
                                """);
                execute("DROP MATERIALIZED VIEW price_1h");
                execute("DROP TABLE base");
                drainWalAndMatViewQueues();
            }
        });
    }

    @Test
    public void testRestartStopsAtAppliedTxnsAbovePurgedTxnLogParts() throws Exception {
        // With the view's transaction log in parts of one transaction, the WAL purge deletes the parts below the one of
        // the view's applied seqTxn, an applied view ADD COLUMN. A view DROP PARTITION follows, unapplied, and in the
        // second round an unapplied view ADD COLUMN lies between them. A restart takes the state from _mv.s at the last
        // unapplied transaction, reads no deleted part, and the view refreshes on.
        setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 1);
        assertMemoryLeak(() -> {
            for (int round = 0; round < 2; round++) {
                createBase();
                execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z'), ('b', 2.0, '2024-01-02T00:10:00.000000Z')");
                createView("IMMEDIATE");
                drainWalAndMatViewQueues();
                execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra DOUBLE");
                drainWalQueue();
                Assert.assertEquals("INVALIDATE,DATA,STRUCTURE", viewTxnTypes());
                MatViewTxnLogParts.purge(engine, viewToken());
                Assert.assertFalse(MatViewTxnLogParts.exists(engine, viewToken(), 0));
                Assert.assertFalse(MatViewTxnLogParts.exists(engine, viewToken(), 1));
                Assert.assertTrue(MatViewTxnLogParts.exists(engine, viewToken(), 2));
                if (round == 1) {
                    execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra2 DOUBLE");
                }
                execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-01'");
                Assert.assertEquals(3, engine.getTableSequencerAPI().getTxnTracker(viewToken()).getWriterTxn());

                restart();
                Assert.assertFalse(viewState().isInvalid());
                Assert.assertEquals(1, viewState().getLastRefreshBaseTxn());
                Assert.assertEquals(baseTableId(), viewState().getBaseTableId());
                drainWalAndMatViewQueues();
                insertBaseRow("c", 3.0, "2024-01-02T05:10:00.000000Z");
                drainWalAndMatViewQueues();
                assertQuery("SELECT view_status, refresh_base_table_txn, base_table_txn FROM materialized_views() WHERE view_name = 'price_1h'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                view_status\trefresh_base_table_txn\tbase_table_txn
                                valid\t2\t2
                                """);
                assertQuery("SELECT ts, sym, price, extra FROM price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns("""
                        ts\tsym\tprice\textra
                        2024-01-02T00:00:00.000000Z\tb\t2.0\tnull
                        2024-01-02T05:00:00.000000Z\tc\t3.0\tnull
                        """);
                execute("DROP MATERIALIZED VIEW price_1h");
                execute("DROP TABLE base");
                drainWalAndMatViewQueues();
            }
        });
    }

    @Test
    public void testReplacedBaseBeforeFirstRefreshAfterRestartInvalidates() throws Exception {
        // A base replaced before the view's first refresh after a restart, with the drop's invalidation lost in the
        // restart, invalidates the view at that refresh with no rows written.
        assertMemoryLeak(() -> {
            createBase();
            insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
            createView("IMMEDIATE");
            drainWalAndMatViewQueues();
            final int oldBaseTableId = baseTableId();

            execute("DROP TABLE base");
            createBase();
            insertBaseRow("b", 2.0, "2024-01-01T01:10:00.000000Z");
            insertBaseRow("c", 3.0, "2024-01-01T02:10:00.000000Z");
            drainWalQueue();
            // the restart loses the queued invalidation of the drop
            restart();
            Assert.assertFalse(viewState().isInvalid());
            Assert.assertEquals(oldBaseTableId, viewState().getBaseTableId());
            final long viewSeqTxn = viewSeqTxn();
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", MatViewRefreshJob.BASE_TABLE_CHANGED_REASON);
            Assert.assertEquals(viewSeqTxn + 1, viewSeqTxn());
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    """);
        });
    }

    @Test
    public void testReplacedBaseInvalidatesRangeRefreshAndIntervalUpdates() throws Exception {
        // RANGE, the refresh-interval cache update and the ATTACH / UPDATE range task check the base identity too.
        assertMemoryLeak(() -> {
            for (int op = 0; op < 3; op++) {
                execute("DROP MATERIALIZED VIEW IF EXISTS price_1h");
                execute("DROP TABLE IF EXISTS base");
                drainWalAndMatViewQueues();
                createBase();
                insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
                createView("MANUAL");
                drainWalQueue();
                execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
                drainWalAndMatViewQueues();
                Assert.assertEquals(baseTableId(), viewState().getBaseTableId());

                execute("DROP TABLE base");
                final MatViewRefreshTask task = new MatViewRefreshTask();
                // the drop's invalidation is lost, as in a crash
                Assert.assertTrue(engine.getMatViewStateStore().tryDequeueRefreshTask(task));
                Assert.assertEquals(MatViewRefreshTask.INVALIDATE, task.operation);
                createBase();
                insertBaseRow("b", 2.0, "2024-01-01T01:10:00.000000Z");
                drainWalAndMatViewQueues();
                Assert.assertFalse(viewState().isInvalid());

                switch (op) {
                    case 0 -> execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01' TO '2024-01-02'");
                    case 1 -> engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken());
                    default -> engine.getMatViewStateStore().enqueueAddRefreshRange(
                            viewToken(),
                            engine.verifyTableName("base"),
                            parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z"),
                            parseFloorPartialTimestamp("2024-01-01T23:59:59.999999Z"),
                            1
                    );
                }
                drainWalAndMatViewQueues();
                assertViewStatus("invalid", MatViewRefreshJob.BASE_TABLE_CHANGED_REASON);
                assertViewRows("""
                        ts\tsym\tprice
                        2024-01-01T00:00:00.000000Z\ta\t1.0
                        """);
            }
        });
    }

    // Fails the open for write of the view's _mv.s once the given number of such opens passed; -1 never fails.
    private static TestFilesFacadeImpl newStateWriteFailingFacade(AtomicInteger stateOpensToPass, AtomicInteger stateWriteFailures) {
        return new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, MatViewState.MAT_VIEW_STATE_FILE_NAME) && Utf8s.containsAscii(name, "price_1h")) {
                    final int opensToPass = stateOpensToPass.get();
                    if (opensToPass == 0) {
                        stateOpensToPass.set(-1);
                        stateWriteFailures.incrementAndGet();
                        return -1;
                    }
                    if (opensToPass > 0) {
                        stateOpensToPass.decrementAndGet();
                    }
                }
                return super.openRW(name, opts);
            }
        };
    }

    // Counts the opens of the view's WAL event files while the flag is set.
    private static TestFilesFacadeImpl newViewEventOpenCountingFacade(AtomicBoolean isCounting, AtomicInteger eventOpens) {
        return new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (isCounting.get() && Utf8s.endsWithAscii(name, WalUtils.EVENT_FILE_NAME) && Utf8s.containsAscii(name, "price_1h")) {
                    eventOpens.incrementAndGet();
                }
                return super.openRO(name);
            }
        };
    }

    private void assertPersistedBaseTableId(int expected) {
        try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
            path.of(configuration.getDbRoot()).concat(viewToken()).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME);
            reader.of(path.$());
            Assert.assertEquals(expected, MatViewStateReader.readBaseTableId(reader));
        }
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    private void assertViewStatus(String status, String reason) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'price_1h'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\n" + status + "\t" + reason + "\n");
    }

    private int baseTableId() {
        return engine.verifyTableName("base").getTableId();
    }

    private void createBase() throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private void createView(String refreshType) throws SqlException {
        execute("CREATE MATERIALIZED VIEW price_1h REFRESH " + refreshType + " AS (" + VIEW_SQL + ") PARTITION BY DAY");
    }

    // A refresh commit of one row in the 00:00 bucket, written straight to the view's WAL.
    private void commitViewRow(long refreshBaseTxn, double price) {
        final long ts = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
        try (WalWriter writer = engine.getWalWriter(viewToken())) {
            final TableWriter.Row row = writer.newRow(ts);
            row.putSym(1, "a");
            row.putDouble(2, price);
            row.append();
            writer.commitMatView(refreshBaseTxn, 1, Numbers.LONG_NULL, ts, ts + 3_600_000_000L);
        }
    }

    private void createViewWithoutBaseId() throws Exception {
        createViewWithoutBaseId("IMMEDIATE");
    }

    // An IMMEDIATE view refreshed from one base row, and 10,000 refresh commits of base txn 1 written to its WAL and
    // applied, the last one with the refresh timestamp 9,999.
    private void createViewWithAppliedRefreshCommits() throws SqlException {
        createBase();
        insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
        createView("IMMEDIATE");
        drainWalAndMatViewQueues();
        final long ts = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
        try (WalWriter writer = engine.getWalWriter(viewToken())) {
            for (int i = 0; i < 10_000; i++) {
                final TableWriter.Row row = writer.newRow(ts);
                row.putSym(1, "a");
                row.putDouble(2, i);
                row.append();
                writer.commitMatView(1, i, Numbers.LONG_NULL, ts, ts + 3_600_000_000L);
            }
        }
        drainWalQueue();
        Assert.assertEquals(10_002, viewSeqTxn());
        Assert.assertEquals(10_002, engine.getTableSequencerAPI().getTxnTracker(viewToken()).getWriterTxn());
    }

    // Commits of one row each in the 00:00 bucket, written straight to the view's WAL without view state, as a range
    // refresh commits.
    private void commitViewRowsWithoutState(int count) {
        final long ts = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
        try (WalWriter writer = engine.getWalWriter(viewToken())) {
            for (int i = 0; i < count; i++) {
                final TableWriter.Row row = writer.newRow(ts);
                row.putSym(1, "a");
                row.putDouble(2, 100 + i);
                row.append();
                writer.commitWithParams(ts, ts + 3_600_000_000L, WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE);
            }
        }
    }

    // Reads the view's state as the load does, with the given applied seqTxn of the view, and returns whether it found
    // one.
    private boolean readViewState(long appliedSeqTxn, MatViewStateReader stateReader) {
        try (
                Path path = new Path();
                BlockFileReader reader = new BlockFileReader(configuration);
                WalEventReader eventReader = new WalEventReader(configuration)
        ) {
            path.of(configuration.getDbRoot()).concat(viewToken());
            return WalUtils.readMatViewState(
                    path,
                    viewToken(),
                    configuration,
                    io.questdb.cairo.vm.Vm.getCMRInstance(),
                    eventReader,
                    reader,
                    stateReader,
                    appliedSeqTxn
            );
        }
    }

    private void restartCountingViewEventOpens(AtomicBoolean isCounting) {
        isCounting.set(true);
        try {
            restart();
        } finally {
            isCounting.set(false);
        }
    }

    // A refreshed view whose persisted state carries no base table id, as one that a previous version wrote: the view
    // state event that a previous version's refresh job writes has no id, and the state file the apply job writes
    // from it neither.
    private void createViewWithoutBaseId(String refreshType) throws Exception {
        createBase();
        insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
        createView(refreshType);
        drainWalQueue();
        if (!"IMMEDIATE".equals(refreshType)) {
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
        }
        drainWalAndMatViewQueues();
        final MatViewState state = viewState();
        try (WalWriter writer = engine.getWalWriter(viewToken())) {
            writer.resetMatViewState(
                    state.getLastRefreshBaseTxn(),
                    state.getLastRefreshFinishTimestampUs(),
                    false,
                    null,
                    state.getLastPeriodHi(),
                    null,
                    -1
            );
        }
        drainWalQueue();
        restart();
        Assert.assertEquals(MatViewState.BASE_TABLE_ID_UNKNOWN, viewState().getBaseTableId());
        assertPersistedBaseTableId(MatViewState.BASE_TABLE_ID_UNKNOWN);
    }

    private void insertBaseRow(String sym, double price, String ts) throws SqlException {
        execute("INSERT INTO base VALUES ('" + sym + "', " + price + ", '" + ts + "')");
    }

    private void insertBaseRow(String sym, String ts) throws SqlException {
        execute("INSERT INTO base VALUES ('" + sym + "', '" + ts + "')");
    }

    // A MANUAL view refreshed at base txn 1, then at base txn 2, whose refresh commit's apply fails to write _mv.s:
    // the file keeps base txn 1 and the cached intervals of base txn 2, while the view's state in memory is at 2.
    private void refreshTwiceFailingSecondStateWrite(AtomicInteger stateOpensToPass, AtomicInteger stateWriteFailures) throws SqlException {
        createBase();
        insertBaseRow("a", 1.0, "2024-01-01T00:10:00.000000Z");
        createView("MANUAL");
        drainWalQueue();
        execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
        drainWalAndMatViewQueues();
        execute("INSERT INTO base VALUES ('b', 2.0, '2024-01-01T01:10:00.000000Z'), ('b', 5.0, '2024-01-01T01:20:00.000000Z')");
        drainWalQueue();
        execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
        drainMatViewQueue(engine);
        Assert.assertEquals("INVALIDATE,DATA,INVALIDATE,DATA", viewTxnTypes());
        // the cache event's _mv.s write passes, the refresh commit's fails
        stateOpensToPass.set(1);
        drainWalQueue();
        stateOpensToPass.set(-1);
        Assert.assertEquals(1, stateWriteFailures.get());
        assertPersistedRefreshBaseTxn(1);
        Assert.assertEquals(2, viewState().getLastRefreshBaseTxn());
    }

    private void assertPersistedRefreshBaseTxn(long expected) {
        try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
            path.of(configuration.getDbRoot()).concat(viewToken()).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME);
            reader.of(path.$());
            Assert.assertEquals(expected, new MatViewStateReader().of(reader, viewToken()).getLastRefreshBaseTxn());
        }
    }

    private void restart() {
        engine.getMatViewStateStore().clear();
        engine.hydrateMatViewStateStore();
    }

    private long viewSeqTxn() {
        return engine.getTableSequencerAPI().getTxnTracker(viewToken()).getSeqTxn();
    }

    private MatViewState viewState() {
        return engine.getMatViewStateStore().getViewState(viewToken());
    }

    private TableToken viewToken() {
        return engine.verifyTableName("price_1h");
    }

    // The base table id of the view state event that is the first transaction of the view's WAL.
    private int firstEventBaseTableId() {
        final TableToken viewToken = viewToken();
        try (
                TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(viewToken, 0);
                WalEventReader eventReader = new WalEventReader(configuration);
                Path path = new Path()
        ) {
            Assert.assertTrue(cursor.hasNext());
            path.of(configuration.getDbRoot()).concat(viewToken).concat(WalUtils.WAL_NAME_BASE).put(cursor.getWalId()).slash().put(cursor.getSegmentId());
            final WalEventCursor eventCursor = eventReader.of(path, cursor.getSegmentTxn());
            Assert.assertEquals(WalTxnType.MAT_VIEW_INVALIDATE, eventCursor.getType());
            return eventCursor.getMatViewInvalidationInfo().getBaseTableId();
        }
    }

    // The types of the view's WAL transactions in seqTxn order: DATA for a refresh data commit, REPLACE for a range
    // refresh commit, INVALIDATE for a view state event, STRUCTURE for a structural change.
    private String viewTxnTypes() {
        final StringSink sink = new StringSink();
        final TableToken viewToken = viewToken();
        try (
                TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(viewToken, 0);
                WalEventReader eventReader = new WalEventReader(configuration);
                Path path = new Path()
        ) {
            while (cursor.hasNext()) {
                if (sink.length() > 0) {
                    sink.put(',');
                }
                final int walId = cursor.getWalId();
                if (walId < 1) {
                    sink.put("STRUCTURE");
                    continue;
                }
                path.of(configuration.getDbRoot()).concat(viewToken).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(cursor.getSegmentId());
                final WalEventCursor eventCursor = eventReader.of(path, cursor.getSegmentTxn());
                switch (eventCursor.getType()) {
                    case WalTxnType.MAT_VIEW_DATA -> sink.put("DATA");
                    case WalTxnType.MAT_VIEW_INVALIDATE -> sink.put("INVALIDATE");
                    case WalTxnType.DATA -> sink.put("REPLACE");
                    default -> sink.put(eventCursor.getType());
                }
            }
        }
        return sink.toString();
    }
}
