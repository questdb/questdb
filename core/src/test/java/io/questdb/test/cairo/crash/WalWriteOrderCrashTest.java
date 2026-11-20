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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SequencerMetadata;
import io.questdb.cairo.mv.MatViewBacklog;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Every write path that appends to a sequencer must make the files the record names durable before the sequencer
 * log is flushed, in the modes that promise durability across a power loss.
 * <p>
 * Each case runs one sequenced operation, then flushes the table's sequencer log as any writer of the table may do
 * at any moment (under a group window a peer's batch flush covers every record in the log). Nothing else is written
 * back: only the product's own barriers make bytes durable. After the power loss, every record the durable log
 * names must be readable from the durable {@code _event} and {@code _event.i} files of the segment it names, the
 * segment {@code _meta} of every data record must declare the record's structure version (the Enterprise uploader
 * reads it to ship the record), the table must apply without suspending, and the operation's effect must be
 * visible.
 * <p>
 * A new write path goes into {@link #data()}: it fails here if it sequences a record before its files are durable.
 */
@RunWith(Parameterized.class)
public class WalWriteOrderCrashTest extends AbstractAdaptiveCrashTest {
    private static final byte CUSTOM_EVENT_TYPE = 64;
    private static final String MAT_VIEW_SQL = "SELECT ts, max(v) AS v FROM t SAMPLE BY 1d";
    private static final String MAT_VIEW_WITH_BASE_SQL = "SELECT ts, sum(v) AS v FROM t2 SAMPLE BY 1d";
    private final String commitMode;
    private final long groupWindowUs;
    private final String path;
    // the base table name and SQL of the last MAT_VIEW_DEFINITION record on disk
    private String lastMatViewBaseTableName;
    // the state of the last MAT_VIEW_INVALIDATE record on disk
    private String lastMatViewInvalidationBacklogLostReason;
    private int lastMatViewInvalidationBaseTableId;
    private boolean lastMatViewInvalidationIsInvalid;
    private String lastMatViewInvalidationReason;
    private String lastMatViewSql;
    private String lastViewSql;

    public WalWriteOrderCrashTest(String commitMode, long groupWindowUs, String path) {
        this.commitMode = commitMode;
        this.groupWindowUs = groupWindowUs;
        this.path = path;
    }

    @Parameterized.Parameters(name = "mode={0}, window={1}, path={2}")
    public static Collection<Object[]> data() {
        final Object[][] modes = {
                {"sync", 0L},
                {"adaptive", 0L},
                {"adaptive", 50_000L}
        };
        final String[] paths = {
                "insert",
                "insert in a new writer",
                "insert after a segment roll",
                "add column on an empty segment, then insert",
                "update",
                "drop partition",
                "truncate",
                "add column",
                "custom event",
                "alter view",
                "alter view in a new segment",
                "alter view in a new segment, directories synced",
                "alter materialized view",
                "alter materialized view with base",
                "resume materialized view"
        };
        final List<Object[]> params = new ArrayList<>();
        for (Object[] mode : modes) {
            for (String path : paths) {
                params.add(new Object[]{mode[0], mode[1], path});
            }
        }
        return params;
    }

    @Test
    public void testSequencedRecordIsOnDiskAfterPowerLoss() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, groupWindowUs);
        node1.setProperty(PropertyKey.CAIRO_WAL_COMMIT_WRITEBACK_DRAIN, false);
        if (path.equals("insert after a segment roll")) {
            // Two rows per segment: the first insert fills segment 0, so the case's row rolls into a new one.
            node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 2);
        }
        runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false;
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES (0, 1), ('1970-01-02', 2)");
            final boolean isView = path.startsWith("alter view");
            final boolean isMatView = path.contains("materialized view");
            if (isView) {
                execute("CREATE VIEW v AS (SELECT ts, v FROM t)");
                if (path.equals("alter view")) {
                    // The view's WAL segment exists and is durable before the case starts.
                    drainWalAndViewQueues();
                    execute("ALTER VIEW v AS (SELECT ts, v FROM t WHERE v > 0)");
                }
            }
            if (isMatView) {
                // A refreshed view, whose WAL segment exists and is durable before the case starts.
                execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, sum(v) AS v FROM t SAMPLE BY 1d) PARTITION BY DAY");
                if (path.equals("alter materialized view with base")) {
                    execute("CREATE TABLE t2 (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    execute("INSERT INTO t2 VALUES ('1970-01-03', 5)");
                }
                drainWalAndMatViewQueues();
                if (path.equals("resume materialized view")) {
                    engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("mv"), "test");
                    drainWalAndMatViewQueues();
                    Assert.assertTrue(engine.getMatViewStateStore().getViewState(engine.verifyTableName("mv")).isInvalid());
                }
            }
            drainWalAndViewQueues();
            if (path.equals("insert in a new writer") || path.startsWith("add column on an empty segment")) {
                // The case's writer opens its segment 0 after the baseline.
                engine.releaseAllWalWriters();
            }
            final TableToken token = engine.verifyTableName(isView ? "v" : (isMatView ? "mv" : "t"));
            final long txnBefore = engine.getTableSequencerAPI().lastTxn(token);
            markDurableBaseline();

            runPath();

            final long seqTxn = engine.getTableSequencerAPI().lastTxn(token);
            // a base change writes the view's invalidation, then its definition
            final int sequencedTxns = path.startsWith("add column on an empty segment") || path.equals("alter materialized view with base") ? 2 : 1;
            Assert.assertEquals("the operation must sequence its txns", txnBefore + sequencedTxns, seqTxn);
            // Any writer may flush the shared log now: a peer's batch flush under a window, or the operation's own.
            engine.getTableSequencerAPI().fdatasyncTxnLog(token);
            if (path.endsWith("directories synced")) {
                // Only the new segment's files then decide the outcome.
                try (Path dir = new Path()) {
                    TableUtils.fsyncDirDurable(crashFf, dir.of(root).concat(token.getDirName()).$());
                    TableUtils.fsyncDirDurable(crashFf, dir.of(root).concat(token.getDirName()).concat(WalUtils.WAL_NAME_BASE).put(1).$());
                }
            }

            final TableToken[] tokens;
            if (path.equals("alter materialized view with base")) {
                tokens = new TableToken[]{engine.verifyTableName("t"), engine.verifyTableName("t2"), token};
            } else if (isView || isMatView) {
                tokens = new TableToken[]{engine.verifyTableName("t"), token};
            } else {
                tokens = new TableToken[]{token};
            }
            simulateRestart(tokens);
            Assert.assertEquals("the flushed log must name the txn", seqTxn, engine.getTableSequencerAPI().lastTxn(token));
            assertEveryNamedRecordOnDisk(token);
            if (path.equals("custom event")) {
                // The apply job has no handler for a test's custom event type: the record on disk is the whole check.
                releaseEngineHandles();
                return;
            }

            new RecoveryCoordinator(engine).recover();
            for (TableToken tt : tokens) {
                engine.notifyWalTxnRepublisher(tt);
            }
            drainWalAndViewQueues();
            Assert.assertFalse(
                    "must not be suspended: " + engine.getTableSequencerAPI().getTxnTracker(token).getErrorMessage(),
                    engine.getTableSequencerAPI().isSuspended(token)
            );
            if (isMatView) {
                // A booting server loads the view states from disk.
                engine.getMatViewStateStore().clear();
                engine.hydrateMatViewStateStore();
            }
            assertEffect();
            releaseEngineHandles();
        });
    }

    private void assertEffect() throws Exception {
        switch (path) {
            case "insert", "insert in a new writer", "insert after a segment roll" ->
                    assertQuery("SELECT v FROM t ORDER BY v").expectSize().returns("v\n1\n2\n3\n");
            case "add column on an empty segment, then insert" ->
                    assertQuery("SELECT v, c FROM t ORDER BY v").expectSize().returns("v\tc\n1\tnull\n2\tnull\n3\t30\n");
            case "update" -> assertQuery("SELECT v FROM t ORDER BY v").expectSize().returns("v\n42\n42\n");
            case "drop partition" -> assertQuery("SELECT v FROM t").expectSize().returns("v\n2\n");
            case "truncate" -> assertQuery("SELECT v FROM t").expectSize().returns("v\n");
            case "add column" ->
                    assertQuery("SELECT v, c FROM t ORDER BY v").expectSize().returns("v\tc\n1\tnull\n2\tnull\n");
            // The harness does not rebuild the view graph as a booting server does: the durable record is the effect.
            case "alter view", "alter view in a new segment", "alter view in a new segment, directories synced" ->
                    Assert.assertEquals("SELECT ts FROM t", lastViewSql);
            // The apply writes the view's definition from the durable record.
            case "alter materialized view" -> {
                Assert.assertEquals("t", lastMatViewBaseTableName);
                Assert.assertEquals(MAT_VIEW_SQL, lastMatViewSql);
                final MatViewDefinition definition = matViewDefinition();
                Assert.assertEquals("t", definition.getBaseTableName());
                Assert.assertEquals(MAT_VIEW_SQL, definition.getMatViewSql());
                Assert.assertFalse(matViewState().isInvalid());
            }
            // The view follows the new base, invalid with a lost backlog, until RESUME SKIP BACKLOG.
            case "alter materialized view with base" -> {
                Assert.assertEquals("t2", lastMatViewBaseTableName);
                Assert.assertEquals(MAT_VIEW_WITH_BASE_SQL, lastMatViewSql);
                Assert.assertTrue(lastMatViewInvalidationIsInvalid);
                Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_CHANGED, lastMatViewInvalidationReason);
                Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_CHANGED, lastMatViewInvalidationBacklogLostReason);
                Assert.assertEquals(engine.verifyTableName("t").getTableId(), lastMatViewInvalidationBaseTableId);
                final MatViewDefinition definition = matViewDefinition();
                Assert.assertEquals("t2", definition.getBaseTableName());
                Assert.assertEquals(MAT_VIEW_WITH_BASE_SQL, definition.getMatViewSql());
                final MatViewState state = matViewState();
                Assert.assertTrue(state.isInvalid());
                Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_CHANGED, state.getBacklogLostReason());
            }
            // The view is valid again, at the base's position, with its backlog skipped.
            case "resume materialized view" -> {
                Assert.assertFalse(lastMatViewInvalidationIsInvalid);
                Assert.assertNull(lastMatViewInvalidationBacklogLostReason);
                final int baseTableId = engine.verifyTableName("t").getTableId();
                Assert.assertEquals(baseTableId, lastMatViewInvalidationBaseTableId);
                final MatViewState state = matViewState();
                Assert.assertFalse(state.isInvalid());
                Assert.assertEquals(baseTableId, state.getBaseTableId());
                Assert.assertNull(state.getBacklogLostReason());
            }
            default -> throw new AssertionError("unknown path: " + path);
        }
    }

    private void assertEveryNamedRecordOnDisk(TableToken token) {
        try (
                TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(token, 0);
                WalEventReader reader = new WalEventReader(engine.getConfiguration());
                SequencerMetadata segmentMeta = new SequencerMetadata(engine.getConfiguration(), true);
                Path segmentPath = new Path()
        ) {
            while (cursor.hasNext()) {
                final int walId = cursor.getWalId();
                if (walId > 0) {
                    final String what = "seqTxn=" + cursor.getTxn() + ", wal=" + walId + ", segment=" + cursor.getSegmentId()
                            + ", segmentTxn=" + cursor.getSegmentTxn();
                    segmentPath.of(root).concat(token.getDirName()).concat(WalUtils.WAL_NAME_BASE).put(walId)
                            .slash().put(cursor.getSegmentId());
                    final WalEventCursor events;
                    try {
                        events = reader.of(segmentPath, cursor.getSegmentTxn());
                    } catch (Throwable th) {
                        throw new AssertionError("the log names a WAL event that is not on disk [" + what + ']', th);
                    }
                    Assert.assertEquals(what, cursor.getSegmentTxn(), events.getTxn());
                    if (WalTxnType.isDataType(events.getType())) {
                        try {
                            segmentMeta.open(segmentPath, segmentPath.size(), token);
                        } catch (Throwable th) {
                            throw new AssertionError("the log names a data record whose segment _meta is not on disk [" + what + ']', th);
                        }
                        Assert.assertTrue(
                                "the segment _meta on disk is older than the data record [" + what + ", recordVersion="
                                        + cursor.getStructureVersion() + ", metaVersion=" + segmentMeta.getMetadataVersion() + ']',
                                segmentMeta.getMetadataVersion() >= cursor.getStructureVersion()
                        );
                    }
                    if (events.getType() == WalTxnType.VIEW_DEFINITION) {
                        lastViewSql = events.getViewDefinitionInfo().getViewSql();
                    }
                    if (events.getType() == WalTxnType.MAT_VIEW_DEFINITION) {
                        final WalEventCursor.MatViewDefinitionInfo info = events.getMatViewDefinitionInfo();
                        lastMatViewBaseTableName = info.getBaseTableName().toString();
                        lastMatViewSql = info.getMatViewSql().toString();
                    }
                    if (events.getType() == WalTxnType.MAT_VIEW_INVALIDATE) {
                        final WalEventCursor.MatViewInvalidationInfo info = events.getMatViewInvalidationInfo();
                        lastMatViewInvalidationIsInvalid = info.isInvalid();
                        lastMatViewInvalidationReason = info.getInvalidationReason().toString();
                        final CharSequence backlogLostReason = info.getBacklogLostReason();
                        lastMatViewInvalidationBacklogLostReason = backlogLostReason != null ? backlogLostReason.toString() : null;
                        lastMatViewInvalidationBaseTableId = info.getBaseTableId();
                    }
                }
            }
        }
    }

    private MatViewDefinition matViewDefinition() {
        return engine.getDependentViewGraph().getViewDefinition(engine.verifyTableName("mv"));
    }

    private MatViewState matViewState() {
        return engine.getMatViewStateStore().getViewState(engine.verifyTableName("mv"));
    }

    private void runPath() throws Exception {
        switch (path) {
            case "insert", "insert in a new writer", "insert after a segment roll" ->
                    execute("INSERT INTO t VALUES ('1970-01-02T01:00:00', 3)");
            case "add column on an empty segment, then insert" -> {
                final TableToken token = engine.verifyTableName("t");
                try (WalWriter writer = getWalWriter("t")) {
                    // The kernel may write the new segment's _meta back at any time; the in-place rewrite below must
                    // still reach the disk before the insert that relies on it is sequenced.
                    crashFf.markFileDurable(root + "/" + token.getDirName() + "/" + writer.getWalName() + "/0/" + TableUtils.META_FILE_NAME);
                }
                execute("ALTER TABLE t ADD COLUMN c INT");
                execute("INSERT INTO t VALUES ('1970-01-02T01:00:00', 3, 30)");
            }
            case "update" -> execute("UPDATE t SET v = 42");
            case "drop partition" -> execute("ALTER TABLE t DROP PARTITION LIST '1970-01-01'");
            case "truncate" -> execute("TRUNCATE TABLE t");
            case "add column" -> execute("ALTER TABLE t ADD COLUMN c INT");
            case "custom event" -> {
                try (WalWriter writer = getWalWriter("t")) {
                    writer.appendCustomEvent(CUSTOM_EVENT_TYPE, mem -> mem.putLong(42L));
                }
            }
            case "alter view", "alter view in a new segment", "alter view in a new segment, directories synced" ->
                    execute("ALTER VIEW v AS (SELECT ts FROM t)");
            case "alter materialized view" -> execute("ALTER MATERIALIZED VIEW mv AS (" + MAT_VIEW_SQL + ")");
            case "alter materialized view with base" ->
                    execute("ALTER MATERIALIZED VIEW mv WITH BASE t2 AS (" + MAT_VIEW_WITH_BASE_SQL + ")");
            case "resume materialized view" -> execute("REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG");
            default -> throw new AssertionError("unknown path: " + path);
        }
    }

}
