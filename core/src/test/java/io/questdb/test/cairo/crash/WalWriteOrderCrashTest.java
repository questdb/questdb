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
 * table must apply without suspending, and the operation's effect must be visible.
 * <p>
 * A new write path goes into {@link #data()}: it fails here if it sequences a record before its files are durable.
 */
@RunWith(Parameterized.class)
public class WalWriteOrderCrashTest extends AbstractAdaptiveCrashTest {
    private static final byte CUSTOM_EVENT_TYPE = 64;
    private final String commitMode;
    private final long groupWindowUs;
    private final String path;
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
                "update",
                "drop partition",
                "truncate",
                "add column",
                "custom event",
                "alter view",
                "alter view in a new segment",
                "alter view in a new segment, directories synced"
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
        runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false;
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES (0, 1), ('1970-01-02', 2)");
            final boolean isView = path.startsWith("alter view");
            if (isView) {
                execute("CREATE VIEW v AS (SELECT ts, v FROM t)");
                if (path.equals("alter view")) {
                    // The view's WAL segment exists and is durable before the case starts.
                    drainWalAndViewQueues();
                    execute("ALTER VIEW v AS (SELECT ts, v FROM t WHERE v > 0)");
                }
            }
            drainWalAndViewQueues();
            final TableToken token = engine.verifyTableName(isView ? "v" : "t");
            final long txnBefore = engine.getTableSequencerAPI().lastTxn(token);
            markDurableBaseline();

            runPath();

            final long seqTxn = engine.getTableSequencerAPI().lastTxn(token);
            Assert.assertEquals("the operation must sequence one txn", txnBefore + 1, seqTxn);
            // Any writer may flush the shared log now: a peer's batch flush under a window, or the operation's own.
            engine.getTableSequencerAPI().fdatasyncTxnLog(token);
            if (path.endsWith("directories synced")) {
                // Only the new segment's files then decide the outcome.
                try (Path dir = new Path()) {
                    TableUtils.fsyncDirDurable(crashFf, dir.of(root).concat(token.getDirName()).$());
                    TableUtils.fsyncDirDurable(crashFf, dir.of(root).concat(token.getDirName()).concat(WalUtils.WAL_NAME_BASE).put(1).$());
                }
            }

            final TableToken[] tokens = isView ? new TableToken[]{engine.verifyTableName("t"), token} : new TableToken[]{token};
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
            assertEffect();
            releaseEngineHandles();
        });
    }

    private void assertEffect() throws Exception {
        switch (path) {
            case "insert" -> assertQuery("SELECT v FROM t ORDER BY v").expectSize().returns("v\n1\n2\n3\n");
            case "update" -> assertQuery("SELECT v FROM t ORDER BY v").expectSize().returns("v\n42\n42\n");
            case "drop partition" -> assertQuery("SELECT v FROM t").expectSize().returns("v\n2\n");
            case "truncate" -> assertQuery("SELECT v FROM t").expectSize().returns("v\n");
            case "add column" ->
                    assertQuery("SELECT v, c FROM t ORDER BY v").expectSize().returns("v\tc\n1\tnull\n2\tnull\n");
            // The harness does not rebuild the view graph as a booting server does: the durable record is the effect.
            case "alter view", "alter view in a new segment", "alter view in a new segment, directories synced" ->
                    Assert.assertEquals("SELECT ts FROM t", lastViewSql);
            default -> throw new AssertionError("unknown path: " + path);
        }
    }

    private void assertEveryNamedRecordOnDisk(TableToken token) {
        try (
                TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(token, 0);
                WalEventReader reader = new WalEventReader(engine.getConfiguration());
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
                    if (events.getType() == WalTxnType.VIEW_DEFINITION) {
                        lastViewSql = events.getViewDefinitionInfo().getViewSql();
                    }
                }
            }
        }
    }

    private void runPath() throws Exception {
        switch (path) {
            case "insert" -> execute("INSERT INTO t VALUES ('1970-01-02T01:00:00', 3)");
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
            default -> throw new AssertionError("unknown path: " + path);
        }
    }
}
