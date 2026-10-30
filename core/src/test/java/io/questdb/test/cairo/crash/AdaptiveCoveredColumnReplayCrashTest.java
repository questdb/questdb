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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A change that moves a covered column to a new name txn without rewriting its data (DROP INDEX, RENAME COLUMN)
 * or rewrites it (UPDATE) is applied to an adaptive WAL table after its last save point, and the process dies
 * before the next one. Recovery restores the save point, whose {@code _cv} names the column's old name txn, and
 * the replay applies the change again, at the same txn, so under the same new name txn.
 * <ul>
 *   <li>After a process kill every file the first apply wrote is still there under that name txn: the column
 *   file it linked or wrote, the covering values of the index that INCLUDEs the column linked under it, and the
 *   index files UPDATE rebuilt. The replay must take them for what they are, leftovers of an apply that never
 *   became durable, and not fail on a link that exists, write into a leftover, or adopt a leftover index.</li>
 *   <li>After a power loss the names the first apply added are gone, since ADAPTIVE takes no directory barrier
 *   for them, and the replay creates them anew.</li>
 * </ul>
 * Either way the table must not be suspended, and the index that INCLUDEs the column must read its values.
 */
public class AdaptiveCoveredColumnReplayCrashTest extends AbstractAdaptiveCrashTest {

    @Test
    public void testDropIndexOnCoveredColumnReplayedAfterKill() throws Exception {
        assertChangeReplayed(false, "ALTER TABLE x ALTER COLUMN k DROP INDEX", "k", this::assertDroppedIndexOnCoveredColumn);
    }

    @Test
    public void testDropIndexOnCoveredColumnReplayedAfterPowerLoss() throws Exception {
        assertChangeReplayed(true, "ALTER TABLE x ALTER COLUMN k DROP INDEX", "k", this::assertDroppedIndexOnCoveredColumn);
    }

    @Test
    public void testRenameCoveredColumnReplayedAfterKill() throws Exception {
        assertChangeReplayed(false, "ALTER TABLE x RENAME COLUMN v TO w", "w", this::assertRenamedCoveredColumn);
    }

    @Test
    public void testRenameCoveredColumnReplayedAfterPowerLoss() throws Exception {
        assertChangeReplayed(true, "ALTER TABLE x RENAME COLUMN v TO w", "w", this::assertRenamedCoveredColumn);
    }

    @Test
    public void testUpdateCoveredColumnReplayedAfterKill() throws Exception {
        assertChangeReplayed(false, "UPDATE x SET v = v + 10 WHERE ts < '2024-01-03'", "v", this::assertUpdatedCoveredColumn);
    }

    @Test
    public void testUpdateCoveredColumnReplayedAfterPowerLoss() throws Exception {
        assertChangeReplayed(true, "UPDATE x SET v = v + 10 WHERE ts < '2024-01-03'", "v", this::assertUpdatedCoveredColumn);
    }

    /**
     * @param powerLoss          whether the process dies by a power loss, which loses the names the change added,
     *                           or by a kill, which keeps every file it wrote
     * @param sql                the change
     * @param column             the column the change moves to a new name txn, by its name after the change
     * @param assertAfterReplay  reads the table through the index that INCLUDEs the column
     */
    private void assertChangeReplayed(boolean powerLoss, String sql, String column, TestUtils.LeakProneCode assertAfterReplay) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
        // No periodic save point inside a test, and none at writer close: the change stays uncovered.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1_000_000_000L);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, Long.MAX_VALUE);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        runWithCrashFacade(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (k, v), k SYMBOL INDEX, v DOUBLE) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO x VALUES
                        ('2024-01-01T00:00:00.000000Z', 'a', 'p', 1.0),
                        ('2024-01-02T00:00:00.000000Z', 'b', 'q', 2.0),
                        ('2024-01-03T00:00:00.000000Z', 'a', 'r', 3.0)
                    """);
            // The first apply takes the save point.
            drainWalQueue();
            final TableToken token = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            Assert.assertEquals("the first apply must take the save point", 1, tracker.getDurableEpochSeqTxn());
            if (powerLoss) {
                markDurableBaseline();
            }

            execute(sql);
            drainWalQueue();
            Assert.assertFalse("the change suspended the table", anyTableSuspended(token));
            Assert.assertEquals("the change must be applied", 2, tracker.getSeqTxn());
            Assert.assertEquals("no save point must cover the change", 1, tracker.getDurableEpochSeqTxn());
            final long nameTxn = firstPartitionColumnNameTxn(column);
            Assert.assertTrue("the change must move the column to a new name txn", nameTxn > 0);
            assertFirstPartitionColumnFileExists(column, nameTxn);

            if (powerLoss) {
                recoverAfterCrash(new TableToken[]{token});
            } else {
                kill(token);
                Assert.assertTrue("a kill keeps the column file the change wrote", Files.exists(firstPartitionColumnFile(column, nameTxn).$()));
                engine.notifyWalTxnRepublisher(token);
                drainWalQueue();
            }
            Assert.assertFalse("recovery suspended the table: " + tracker.getErrorMessage(), anyTableSuspended(token));
            Assert.assertEquals("the replay must apply the change again", 2, tracker.getSeqTxn());
            Assert.assertEquals("the replay must move the column to the same name txn", nameTxn, firstPartitionColumnNameTxn(column));
            assertAfterReplay.run();
            releaseEngineHandles();
        });
    }

    /**
     * Reads the dropped index's column through its new name txn, and through the covering values linked under it.
     */
    private void assertDroppedIndexOnCoveredColumn() throws Exception {
        assertQuery("SELECT \"column\", indexed FROM table_columns('x') WHERE \"column\" = 'k'").noRandomAccess().returns("""
                column\tindexed
                k\tfalse
                """);
        assertQuery("SELECT ts FROM x WHERE k = 'p'").timestamp("ts").returns("""
                ts
                2024-01-01T00:00:00.000000Z
                """);
        assertQuery("SELECT s, k, v, ts FROM x WHERE s = 'a'")
                .noRandomAccess()
                .expectSize()
                .timestamp("ts")
                .withPlanContaining("CoveringIndex on: s with: k, v, ts")
                .returns("""
                        s\tk\tv\tts
                        a\tp\t1.0\t2024-01-01T00:00:00.000000Z
                        a\tr\t3.0\t2024-01-03T00:00:00.000000Z
                        """);
    }

    private void assertFirstPartitionColumnFileExists(String column, long nameTxn) {
        final Path file = firstPartitionColumnFile(column, nameTxn);
        Assert.assertTrue("column file lost: " + file, Files.exists(file.$()));
    }

    /**
     * Reads the renamed column through its new name txn, and through the covering values linked under it.
     */
    private void assertRenamedCoveredColumn() throws Exception {
        assertQuery("SELECT sum(w) FROM x").noRandomAccess().expectSize().returns("""
                sum
                6.0
                """);
        assertQuery("SELECT s, k, w, ts FROM x WHERE s = 'a'")
                .noRandomAccess()
                .expectSize()
                .timestamp("ts")
                .withPlanContaining("CoveringIndex on: s with: k, w, ts")
                .returns("""
                        s\tk\tw\tts
                        a\tp\t1.0\t2024-01-01T00:00:00.000000Z
                        a\tr\t3.0\t2024-01-03T00:00:00.000000Z
                        """);
    }

    /**
     * Reads the updated column through its new name txn, and the index rebuilt with the covering values of the
     * updated column, both in the two partitions the update rewrote.
     */
    private void assertUpdatedCoveredColumn() throws Exception {
        assertQuery("SELECT sum(v) FROM x").noRandomAccess().expectSize().returns("""
                sum
                26.0
                """);
        assertQuery("SELECT s, k, v, ts FROM x WHERE s = 'a'")
                .noRandomAccess()
                .expectSize()
                .timestamp("ts")
                .withPlanContaining("CoveringIndex on: s with: k, v, ts")
                .returns("""
                        s\tk\tv\tts
                        a\tp\t11.0\t2024-01-01T00:00:00.000000Z
                        a\tr\t3.0\t2024-01-03T00:00:00.000000Z
                        """);
    }

    /**
     * The data file of the column in the first partition, under the given name txn. A thread-local path.
     */
    private Path firstPartitionColumnFile(String column, long nameTxn) {
        try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
            final Path path = Path.getThreadLocal(root).concat(reader.getTableToken().getDirName());
            TableUtils.setPathForNativePartition(
                    path,
                    reader.getMetadata().getTimestampType(),
                    reader.getPartitionedBy(),
                    reader.getPartitionTimestampByIndex(0),
                    reader.getPartitionNameTxnByIndex(0)
            );
            TableUtils.dFile(path, column, nameTxn);
            return path;
        }
    }

    private long firstPartitionColumnNameTxn(String column) {
        try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
            final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(column));
            return reader.getColumnVersionReader().getColumnNameTxn(reader.getPartitionTimestampByIndex(0), writerIndex);
        }
    }

    /**
     * A kill keeps every byte written but loses the process: its scoreboards and sequencer state. The restart
     * recovers every adaptive table before it builds any writer job.
     */
    private void kill(TableToken token) {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseCrashOrphanedWalWriters();
        engine.releaseAllWalWriters();
        engine.getTxnScoreboardPool().remove(token);
        engine.getTableSequencerAPI().resetForReboot(token);
        new RecoveryCoordinator(engine).recover();
    }
}
