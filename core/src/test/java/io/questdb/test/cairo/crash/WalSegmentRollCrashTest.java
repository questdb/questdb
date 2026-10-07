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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A commit that a concurrent ALTER forces into a new WAL segment must be as durable as any other commit of
 * a mode that promises durability.
 * <p>
 * The writer's commit gets NO_TXN, replays the ALTER, and, because the pending rows are not the first in
 * their segment, rolls them into a new segment: it copies the rows, re-creates the txn's event record there,
 * and only then sequences the txn. Every column DDL that the writer replays does that, not only ADD COLUMN.
 * The pre-sequencing barriers of the commit ran against the OLD segment, so everything the roll wrote needs
 * its own before the sequencer names it: the rolled event record (under ADAPTIVE with a group-commit window
 * it was only MS_ASYNC'd), the new segment's file names and its entry in the WAL directory.
 * <p>
 * Nothing is written back by the kernel here: only the product's own barriers make bytes durable. The txn
 * was acknowledged (ADAPTIVE) or its commit returned (SYNC), so it must survive the crash. So must the
 * ALTER itself once it returns.
 */
@RunWith(Parameterized.class)
public class WalSegmentRollCrashTest extends AbstractAdaptiveCrashTest {
    private final String commitMode;
    private final String ddl;
    private final long groupWindowUs;

    public WalSegmentRollCrashTest(String commitMode, long groupWindowUs, String ddl) {
        this.commitMode = commitMode;
        this.groupWindowUs = groupWindowUs;
        this.ddl = ddl;
    }

    @Parameterized.Parameters(name = "mode={0}, window={1}, ddl={2}")
    public static Collection<Object[]> data() {
        final Object[][] modes = {
                {"sync", 0L},
                {"adaptive", 0L},
                {"adaptive", 50_000L}
        };
        final String[] ddls = {
                "ADD COLUMN c INT",
                "ADD COLUMN c SYMBOL",
                "DROP COLUMN d",
                "RENAME COLUMN d TO e",
                "ALTER COLUMN d TYPE LONG"
        };
        final List<Object[]> params = new ArrayList<>();
        for (Object[] mode : modes) {
            for (String ddl : ddls) {
                params.add(new Object[]{mode[0], mode[1], ddl});
            }
        }
        return params;
    }

    /**
     * The DDL itself must survive the crash once it returns, whether or not the apply job ran before the
     * crash, with a V1 and a V2 sequencer log. The sequencer must sync the header after it publishes the
     * record, and a V2 log the part file holding the record ahead of the header: otherwise the ALTER is lost,
     * the table keeps a column its sequencer has no txn for, or the table comes back suspended. Each case
     * crashes on its own, right after its DDL, so that no later flush can make it durable.
     */
    @Test
    public void testAcknowledgedDdlSurvivesCrash() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, groupWindowUs);
        node1.setProperty(PropertyKey.CAIRO_WAL_COMMIT_WRITEBACK_DRAIN, false);
        runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false;
            List<String> expected = null;
            // Applied before the crash or not, with a V1 or a V2 log. The first case gives the expected schema.
            for (String table : new String[]{"applied_v1", "applied_v2", "pending_v1", "pending_v2"}) {
                node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, table.endsWith("v2") ? 1000 : 0);
                execute("CREATE TABLE " + table + " (ts TIMESTAMP, v LONG, d INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO " + table + " VALUES (0, 1, 1)");
                drainWalQueue();
                final TableToken token = engine.verifyTableName(table);
                final List<String> before = columns(table);
                markDurableBaseline();
                execute("ALTER TABLE " + table + " " + ddl);
                Assert.assertEquals(2, engine.getTableSequencerAPI().lastTxn(token));
                if (table.startsWith("applied")) {
                    drainWalQueue();
                    if (expected == null) {
                        expected = columns(table);
                        Assert.assertNotEquals("the DDL must change the schema", before, expected);
                    }
                }

                recoverAfterCrash(new TableToken[]{token});

                final String what = "an acknowledged DDL must survive the crash [table=" + table + ']';
                Assert.assertFalse(
                        what + ": " + engine.getTableSequencerAPI().getTxnTracker(token).getErrorMessage(),
                        engine.getTableSequencerAPI().isSuspended(token)
                );
                Assert.assertEquals(what, 2, engine.getTableSequencerAPI().lastTxn(token));
                Assert.assertEquals(what, expected, columns(table));
                execute("INSERT INTO " + table + " (ts, v) VALUES (1, 2)");
                drainWalQueue();
                assertQuery("SELECT v FROM " + table + " ORDER BY v").expectSize().returns("v\n1\n2\n");
                releaseEngineHandles();
            }
        });
    }

    @Test
    public void testRolledCommitSurvivesCrash() throws Exception {
        assertRolledCommitSurvivesCrash(false);
    }

    /**
     * The same crash with the new segment's directory entry made durable by the test, so that only the
     * rolled files' content decides the outcome.
     */
    @Test
    public void testRolledEventRecordSurvivesCrash() throws Exception {
        assertRolledCommitSurvivesCrash(true);
    }

    private static void appendRow(WalWriter writer, long ts, long v) {
        final TableWriter.Row row = writer.newRow(ts);
        row.putLong(1, v);
        row.putInt(2, (int) v);
        row.append();
    }

    private void assertRolledCommitSurvivesCrash(boolean isWalDirSynced) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, groupWindowUs);
        node1.setProperty(PropertyKey.CAIRO_WAL_COMMIT_WRITEBACK_DRAIN, false);
        runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false;
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG, d INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("x");
            try (WalWriter writer = getWalWriter("x")) {
                appendRow(writer, 0, 1);
                writer.commit();
                markDurableBaseline();
                appendRow(writer, 1, 2);
                execute("ALTER TABLE x " + ddl);
                writer.commit();
                Assert.assertEquals("the pending rows must have rolled to a new segment", 1, writer.getSegmentId());
            }
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            Assert.assertEquals(3, tracker.getSeqTxn());
            if ("adaptive".equals(commitMode)) {
                Assert.assertEquals("the rolled txn must be acknowledged durable", 3, tracker.getLocalDurableSeqTxn());
            }
            if (isWalDirSynced) {
                try (Path path = new Path()) {
                    TableUtils.fsyncDirDurable(crashFf, path.of(root).concat(token.getDirName()).concat(WalUtils.WAL_NAME_BASE).put(1).$());
                }
            }

            recoverAfterCrash(new TableToken[]{token});

            Assert.assertFalse(
                    "table must not be suspended: " + engine.getTableSequencerAPI().getTxnTracker(token).getErrorMessage(),
                    engine.getTableSequencerAPI().isSuspended(token)
            );
            assertQuery("SELECT v FROM x ORDER BY v").expectSize().returns("v\n1\n2\n");
            releaseEngineHandles();
        });
    }

    private List<String> columns(String table) {
        return readColumn("table_columns('" + table + "')", "concat(\"column\", ' ', type)");
    }
}
