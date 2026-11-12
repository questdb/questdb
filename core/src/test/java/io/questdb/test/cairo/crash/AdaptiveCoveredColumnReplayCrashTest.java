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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.IndexFactory;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

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
 * <p>
 * Under SYNC, the directory DROP INDEX moves an attached parquet partition to must survive a power loss right after
 * the DDL, and a failed sync of the move that DROP INDEX or RENAME COLUMN makes poisons the engine.
 */
public class AdaptiveCoveredColumnReplayCrashTest extends AbstractAdaptiveCrashTest {
    // the columns of the tables of createAttachedPartitionSource() and createAttachedParquetPartition()
    private static final String ATTACHED_TABLE_COLUMNS = "(ts TIMESTAMP, s SYMBOL INDEX, t SYMBOL INDEX TYPE POSTING INCLUDE (s, v), v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY ";

    @Test
    public void testDropIndexMovingAttachedParquetPartitionReplayedAfterKill() throws Exception {
        assertAttachedParquetPartitionMoveReplayed(false, true);
    }

    @Test
    public void testDropIndexMovingAttachedParquetPartitionReplayedAfterPowerLoss() throws Exception {
        assertAttachedParquetPartitionMoveReplayed(true, true);
    }

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
    public void testRenameMovingAttachedParquetPartitionReplayedAfterKill() throws Exception {
        assertAttachedParquetPartitionMoveReplayed(false, false);
    }

    @Test
    public void testRenameMovingAttachedParquetPartitionReplayedAfterPowerLoss() throws Exception {
        assertAttachedParquetPartitionMoveReplayed(true, false);
    }

    @Test
    public void testSyncDdlMovingAttachedParquetPartitionFailedSyncIsFailStop() throws Exception {
        Assume.assumeFalse("a directory takes no fsync on Windows", Os.isWindows());
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        // the barrier of the new directory
        assertSyncDdlMoveSyncFailureIsFailStop(new FsyncFailureFacade("fsyncConvertedPartitionDir", 0, false));
        // the sync of an index rebuilt in the new directory, which DROP INDEX would report as a plain error unless the
        // move raised the stop itself
        assertSyncDdlMoveSyncFailureIsFailStop(new FsyncFailureFacade("rebuildParquetRewriteIndexes", 0, true));
    }

    @Test
    public void testSyncDropIndexMovingAttachedParquetPartitionSurvivesPowerLoss() throws Exception {
        // Under SYNC, DROP INDEX on s moves the attached parquet partition, whose s carries the column version 31, to a
        // directory named after its txn, see createAttachedParquetPartition(), and the power fails right after the
        // DDL. The durable _txn names the new directory, which must survive with the links of data.parquet and _pm
        // and the index of t rebuilt there. RENAME COLUMN s TO s2 is left out: under SYNC it loses the links of the
        // symbol map files of s2 in the table directory on a power loss, a separate bug.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        runWithCrashFacade(() -> {
            createAttachedPartitionSource();
            for (boolean isWal : new boolean[]{true, false}) {
                final String context = isWal ? "WAL" : "non-WAL";
                execute("DROP TABLE IF EXISTS x");
                createAttachedParquetPartition(isWal);
                final TableToken token = engine.verifyTableName("x");
                final long ddlTxn = getTableTxn("x");
                Assert.assertTrue(context, partitionColumnNameTxn("x", "s") >= ddlTxn);
                markDurableBaseline();

                execute("ALTER TABLE x ALTER COLUMN s DROP INDEX");
                drainWalQueue();
                Assert.assertFalse(context + ": the DDL suspended the table", anyTableSuspended(token));
                Assert.assertEquals(context + ": the DDL must move the partition", ddlTxn, firstPartitionNameTxn());

                recoverAfterCrash(new TableToken[]{token});
                Assert.assertFalse(context + ": recovery suspended the table", anyTableSuspended(token));
                Assert.assertEquals(context + ": the DDL must survive", ddlTxn + 1, getTableTxn("x"));
                Assert.assertEquals(context + ": _txn must name the moved directory", ddlTxn, firstPartitionNameTxn());
                Assert.assertEquals(context, ddlTxn, partitionColumnNameTxn("x", "s"));
                Assert.assertEquals(context, 31, partitionColumnNameTxn("x", "t"));
                final List<String> files = partitionDirFiles(ddlTxn);
                Assert.assertTrue(context + ": " + files, files.contains("data.parquet") && files.contains("_pm"));
                Assert.assertTrue(context + ": " + files, files.stream().noneMatch(name -> name.startsWith("s.")));
                Assert.assertTrue(context + ": " + files, indexKeyFileExists(ddlTxn, IndexType.POSTING, "t", 31));
                assertQuery("SELECT t, s, v, ts FROM x WHERE t = 'a'")
                        .noRandomAccess()
                        .expectSize()
                        .timestamp("ts")
                        .withPlanContaining("CoveringIndex on: t")
                        .returns("""
                                t\ts\tv\tts
                                a\ta\t1.0\t2024-01-01T00:00:00.000000Z
                                a\ta\t3.0\t2024-01-01T02:00:00.000000Z
                                a\ta\t4.0\t2024-01-02T00:00:00.000000Z
                                """);
                assertQuery("SELECT s, v, ts FROM x WHERE s = 'a'")
                        .timestamp("ts")
                        .returns("""
                                s\tv\tts
                                a\t1.0\t2024-01-01T00:00:00.000000Z
                                a\t3.0\t2024-01-01T02:00:00.000000Z
                                a\t4.0\t2024-01-02T00:00:00.000000Z
                                """);
                releaseEngineHandles();
            }
        });
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
     * The parquet partition 2024-01-01 of x is attached from a copy of table src's, where s carries the column version
     * 31, at or above the txn of the DROP INDEX on s or RENAME COLUMN s TO s2 applied after the last save point. The
     * DDL moves the partition to a directory named after its txn and rebuilds there the indexes it keeps: the index of
     * s2, and t, which INCLUDEs s. After the process dies, recovery restores the save point, whose _txn names the old
     * directory, and the replay moves the partition again, to the same directory and name txns. The old directory
     * goes once a save point covers the DDL.
     *
     * @param powerLoss   whether the process dies by a power loss, which loses the names the DDL added, or by a
     *                    kill, which keeps every file it wrote
     * @param isDropIndex DROP INDEX on s, or RENAME COLUMN s TO s2
     */
    private void assertAttachedParquetPartitionMoveReplayed(boolean powerLoss, boolean isDropIndex) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
        // No periodic save point inside a test, and none at writer close: the DDL stays uncovered.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1_000_000_000L);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, Long.MAX_VALUE);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        final String columnName = isDropIndex ? "s" : "s2";
        runWithCrashFacade(() -> {
            createAttachedPartitionSource();
            createAttachedParquetPartition(true);
            final TableToken token = engine.verifyTableName("x");
            // a save point covers the conversion; the DDL after it is the change recovery replays
            releaseSavePointHolds("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            final long seqTxn = tracker.getSeqTxn();
            Assert.assertEquals("the conversion must be covered by a save point", seqTxn, tracker.getDurableEpochSeqTxn());
            final long ddlTxn = getTableTxn("x");
            final long oldPartitionNameTxn = firstPartitionNameTxn();
            Assert.assertTrue("the attached column version must be at or above the txn of the DDL", partitionColumnNameTxn("x", "s") >= ddlTxn);
            if (powerLoss) {
                markDurableBaseline();
            }

            execute(isDropIndex ? "ALTER TABLE x ALTER COLUMN s DROP INDEX" : "ALTER TABLE x RENAME COLUMN s TO s2");
            drainWalQueue();
            Assert.assertFalse("the DDL suspended the table", anyTableSuspended(token));
            Assert.assertEquals("the DDL must be applied", seqTxn + 1, tracker.getSeqTxn());
            Assert.assertEquals("no save point must cover the DDL", seqTxn, tracker.getDurableEpochSeqTxn());
            Assert.assertEquals("the DDL must move the partition to the directory of its txn", ddlTxn, firstPartitionNameTxn());
            Assert.assertEquals("the DDL must name the column after its txn", ddlTxn, partitionColumnNameTxn("x", columnName));

            if (powerLoss) {
                recoverAfterCrash(new TableToken[]{token});
            } else {
                kill(token);
                engine.notifyWalTxnRepublisher(token);
                drainWalQueue();
            }
            Assert.assertFalse("recovery suspended the table: " + tracker.getErrorMessage(), anyTableSuspended(token));
            Assert.assertEquals("the replay must apply the DDL again", seqTxn + 1, tracker.getSeqTxn());
            Assert.assertEquals("the replay must move the partition to the same directory", ddlTxn, firstPartitionNameTxn());
            Assert.assertEquals("the replay must name the column after the same txn", ddlTxn, partitionColumnNameTxn("x", columnName));
            // the save point holds the old directory until one covers the DDL
            Assert.assertTrue("the old directory went before a save point covered the DDL", partitionDirExists(oldPartitionNameTxn));
            assertQuery("SELECT t, " + columnName + ", v, ts FROM x WHERE t = 'a'")
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex on: t")
                    .returns("t\t" + columnName + """
                            \tv\tts
                            a\ta\t1.0\t2024-01-01T00:00:00.000000Z
                            a\ta\t3.0\t2024-01-01T02:00:00.000000Z
                            a\ta\t4.0\t2024-01-02T00:00:00.000000Z
                            """);
            if (isDropIndex) {
                execute("ALTER TABLE x ALTER COLUMN s ADD INDEX");
                drainWalQueue();
                Assert.assertFalse("ADD INDEX suspended the table", anyTableSuspended(token));
            }
            assertQuery("SELECT " + columnName + ", v, ts FROM x WHERE " + columnName + " = 'a'")
                    .timestamp("ts")
                    .withPlanContaining("Index forward scan on: " + columnName)
                    .returns(columnName + """
                            \tv\tts
                            a\t1.0\t2024-01-01T00:00:00.000000Z
                            a\t3.0\t2024-01-01T02:00:00.000000Z
                            a\t4.0\t2024-01-02T00:00:00.000000Z
                            """);
            releaseSavePointHolds("x");
            Assert.assertFalse("the old directory must go once a save point covers the DDL", partitionDirExists(oldPartitionNameTxn));
            releaseEngineHandles();
        });
    }

    /**
     * Under SYNC, a sync of the move that DROP INDEX on s or RENAME COLUMN s TO s2 makes of the attached parquet
     * partition fails, see {@link #createAttachedParquetPartition}: {@code ff} fails the barrier of the new
     * directory, or the sync of an index rebuilt there. The engine is poisoned and refuses the next write, the DDL
     * fails with the data-sync CairoError when it runs outside the WAL, and nothing is committed: the partition keeps
     * its directory, and s its column version 31.
     */
    private void assertSyncDdlMoveSyncFailureIsFailStop(FsyncFailureFacade ff) throws Exception {
        assertMemoryLeak(ff, () -> {
            execute("DROP TABLE IF EXISTS src");
            createAttachedPartitionSource();
            for (boolean isDropIndex : new boolean[]{true, false}) {
                for (boolean isWal : new boolean[]{true, false}) {
                    final String context = (isDropIndex ? "DROP INDEX" : "RENAME") + (isWal ? ", WAL" : ", non-WAL");
                    execute("DROP TABLE IF EXISTS x");
                    createAttachedParquetPartition(isWal);
                    final long ddlTxn = getTableTxn("x");
                    final long oldPartitionNameTxn = firstPartitionNameTxn();
                    Assert.assertTrue(context, partitionColumnNameTxn("x", "s") >= ddlTxn);

                    Throwable failure = null;
                    ff.isArmed = true;
                    try {
                        execute(isDropIndex ? "ALTER TABLE x ALTER COLUMN s DROP INDEX" : "ALTER TABLE x RENAME COLUMN s TO s2");
                        drainWalQueue();
                    } catch (Throwable th) {
                        failure = th;
                    } finally {
                        ff.isArmed = false;
                    }
                    final boolean isPoisoned = engine.isDurabilityFailed();
                    Throwable nextWriteFailure = null;
                    try {
                        execute("INSERT INTO x (ts) VALUES ('2024-01-03T00:00:00.000000Z')");
                    } catch (Throwable th) {
                        nextWriteFailure = th;
                    }
                    engine.resetDurabilityFailure();
                    releaseEngineHandles();
                    Assert.assertEquals(context + ": the injected failure must have been reached", 1, ff.failureCount);
                    ff.failureCount = 0;
                    Assert.assertTrue(context + ": a failed data sync must poison the engine, failure: " + failure, isPoisoned);
                    Assert.assertTrue(
                            context + ": a poisoned engine must refuse the next write, failure: " + nextWriteFailure,
                            nextWriteFailure instanceof CairoError
                                    && nextWriteFailure.getMessage().contains("engine is poisoned by a failed durability barrier")
                    );
                    if (!isWal) {
                        Assert.assertTrue(
                                context + ": the statement must fail with a data-sync CairoError, failure: " + failure,
                                failure instanceof CairoError && CairoException.isDataSyncFailure(failure)
                        );
                    }
                    // the next writer brings back the state of before the DDL, the name of s too
                    try (TableWriter writer = getWriter("x")) {
                        Assert.assertEquals(context, ddlTxn, writer.getTxn());
                    }
                    Assert.assertEquals(context + ": the DDL must not commit", ddlTxn, getTableTxn("x"));
                    Assert.assertEquals(context, oldPartitionNameTxn, firstPartitionNameTxn());
                    Assert.assertTrue(context, partitionDirExists(oldPartitionNameTxn));
                    Assert.assertFalse(context, partitionDirExists(ddlTxn));
                    Assert.assertEquals(context, 31, partitionColumnNameTxn("x", "s"));
                    assertQuery("SELECT s, v, ts FROM x WHERE s = 'a'")
                            .timestamp("ts")
                            .withPlanContaining("Index forward scan on: s")
                            .returns("""
                                    s\tv\tts
                                    a\t1.0\t2024-01-01T00:00:00.000000Z
                                    a\t3.0\t2024-01-01T02:00:00.000000Z
                                    a\t4.0\t2024-01-02T00:00:00.000000Z
                                    """);
                    releaseEngineHandles();
                }
            }
        });
    }

    /**
     * @param powerLoss         whether the process dies by a power loss, which loses the names the change added,
     *                          or by a kill, which keeps every file it wrote
     * @param sql               the change
     * @param column            the column the change moves to a new name txn, by its name after the change
     * @param assertAfterReplay reads the table through the index that INCLUDEs the column
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
     * Table x of {@link #ATTACHED_TABLE_COLUMNS}, a WAL table when {@code isWal}. Its parquet partition 2024-01-01 is
     * attached from a copy of the partition of table src, see {@link #createAttachedPartitionSource()}, where s
     * carries the column version 31. Partition 2024-01-02 holds ('a', 4.0) at 00:00 and ('b', 5.0) at 01:00. t takes
     * the symbol of s in every row.
     */
    private void createAttachedParquetPartition(boolean isWal) throws Exception {
        execute("CREATE TABLE x " + ATTACHED_TABLE_COLUMNS + (isWal ? "WAL" : "BYPASS WAL"));
        // the symbols in the order src uses them first, so that they take the keys of the copied column files
        execute("""
                INSERT INTO x VALUES
                    ('2024-01-02T00:00:00.000000Z', 'a', 'a', 4.0),
                    ('2024-01-02T01:00:00.000000Z', 'b', 'b', 5.0)
                """);
        drainWalQueue();
        final TableToken token = engine.verifyTableName("x");
        try (Path from = new Path(); Path to = new Path(); TableReader reader = engine.getReader(engine.verifyTableName("src"))) {
            from.of(root).concat(reader.getTableToken().getDirName());
            TableUtils.setPathForNativePartition(
                    from,
                    reader.getMetadata().getTimestampType(),
                    reader.getPartitionedBy(),
                    reader.getPartitionTimestampByIndex(0),
                    reader.getPartitionNameTxnByIndex(0)
            );
            from.slash$();
            to.of(root).concat(token.getDirName()).concat("2024-01-01").put(engine.getConfiguration().getAttachPartitionSuffix()).slash$();
            TestUtils.copyDirectory(from, to, engine.getConfiguration().getMkDirMode());
        }
        execute("ALTER TABLE x ATTACH PARTITION LIST '2024-01-01'");
        drainWalQueue();
        execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
    }

    /**
     * Table src of {@link #ATTACHED_TABLE_COLUMNS}, at txn 31. Its partition 2024-01-01 holds ('a', 1.0) at 00:00,
     * ('b', 2.0) at 01:00 and ('a', 3.0) at 02:00, t taking the symbol of s. s carries the column version 31 there,
     * and so does t, which INCLUDEs s: the UPDATE of s rebuilds its index.
     */
    private void createAttachedPartitionSource() throws Exception {
        execute("CREATE TABLE src " + ATTACHED_TABLE_COLUMNS + "BYPASS WAL");
        execute("""
                INSERT INTO src VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 'a', 1.0),
                    ('2024-01-01T01:00:00.000000Z', 'b', 'b', 2.0),
                    ('2024-01-01T02:00:00.000000Z', 'a', 'a', 3.0)
                """);
        // src moves to txn 31, then s of 2024-01-01 takes column version 31 under a pinned reader
        while (getTableTxn("src") < 31) {
            execute("INSERT INTO src (ts) VALUES ('2024-01-09T00:00:00.000000Z')");
        }
        try (TableReader ignore = engine.getReader(engine.verifyTableName("src"))) {
            execute("UPDATE src SET s = s WHERE ts IN '2024-01-01'");
        }
        Assert.assertEquals(31, partitionColumnNameTxn("src", "s"));
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
        return partitionColumnNameTxn("x", column);
    }

    private long firstPartitionNameTxn() {
        try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
            return reader.getPartitionNameTxnByIndex(0);
        }
    }

    private long getTableTxn(String tableName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            return reader.getTxn();
        }
    }

    // the name txn of the column in the first partition of the table
    private long partitionColumnNameTxn(String tableName, String column) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(column));
            return reader.getColumnVersionReader().getColumnNameTxn(reader.getPartitionTimestampByIndex(0), writerIndex);
        }
    }

    // whether the key file of the index on the column, under the column name txn, is in the directory of partition
    // 2024-01-01 of x under the name txn
    private boolean indexKeyFileExists(long partitionNameTxn, byte indexType, String columnName, long columnNameTxn) {
        try (Path path = new Path()) {
            path.of(root).concat(engine.verifyTableName("x").getDirName()).concat("2024-01-01").put('.').put(partitionNameTxn);
            return Files.exists(IndexFactory.keyFileName(indexType, path, columnName, columnNameTxn));
        }
    }

    // the names of the files in the directory of partition 2024-01-01 of x under the name txn, sorted
    private List<String> partitionDirFiles(long partitionNameTxn) {
        final String[] names = new File(new File(root, engine.verifyTableName("x").getDirName()), "2024-01-01." + partitionNameTxn).list();
        Assert.assertNotNull("the directory of the partition must exist", names);
        Arrays.sort(names);
        return Arrays.asList(names);
    }

    // whether the directory of partition 2024-01-01 of x under the name txn exists
    private boolean partitionDirExists(long partitionNameTxn) {
        try (Path path = new Path()) {
            path.of(root).concat(engine.verifyTableName("x").getDirName()).concat("2024-01-01").put('.').put(partitionNameTxn);
            return Files.exists(path.$());
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
