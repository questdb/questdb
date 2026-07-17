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
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.RandomAccessFile;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.cairo.TableUtils.TX_BASE_OFFSET_PARTITION_STRIDE_32;

/**
 * Plan 3b (composite partitioning), Task 5 -- CAPSTONE: a broad end-to-end proof that a DORMANT
 * composite table (partitioned by time + a symbol dimension; the un-routed write path in this phase
 * only ever produces cellKey 0 -- real (ts, cellKey) multi-cell write-routing is Plan 4) behaves
 * IDENTICALLY to an equivalent plain (time-only) table across every engine path that reads the
 * {@code _txn} partition table -- especially the six paths Plan 3b's Task 10 investigation found
 * misreading a composite {@code _txn} before the self-describing stride marker (Tasks 1-4) existed:
 * {@code table_storage()}, {@code MetadataCache.hydrateTableStartup}, {@link O3PartitionPurgeJob},
 * {@link ColumnPurgeJob}/{@code ColumnPurgeOperator}, {@code RebuildColumnBase}/{@code IndexBuilder},
 * and {@code TableSnapshotRestore}.
 * <p>
 * Two twin tables are built with byte-for-byte identical rows across 5 day partitions (10 rows, 2
 * exchanges): {@code c} ({@code partition by day, exchange} -- composite, stride-8 {@code _txn}) and
 * {@code p} ({@code partition by day} -- plain, stride-4 {@code _txn}). Every assertion below compares
 * {@code c} against {@code p} directly, mostly via {@link #assertSqlCursors}, rather than a hardcoded
 * expected row set -- pinning the ONE property this task cares about (composite == plain, dormant)
 * without coupling the test to incidental formatting.
 * <p>
 * Split into 5 {@code @Test} methods by concern so a failure localizes to one (or a few) of the brief's
 * 10 assertions without re-running the whole class: queries (1-3), catalogue/introspection (4-6),
 * mutating DDL (7-8), O3 + partition purge (9), and checkpoint/snapshot restore (10).
 */
public class CompositeEndToEndTest extends AbstractCairoTest {
    private static O3PartitionPurgeJob purgeJob;

    @AfterClass
    public static void tearDownPurgeJob() {
        purgeJob = Misc.free(purgeJob);
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
        purgeJob = new O3PartitionPurgeJob(engine, 1);
    }

    /**
     * Brief assertions 1-3: full timestamp-ordered scan, {@code WHERE ts in '<day>'} time pruning,
     * {@code count()}, and {@code LATEST ON ts PARTITION BY exchange} -- a composite table's query
     * results must be identical, row for row, to an equivalent plain table's.
     */
    @Test
    public void testQueriesMatchPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive(); // cold reopen -- no pooled reader may mask a fresh self-detect

            // 1. Full timestamp-ordered scan.
            assertSqlCursors("select ts, exchange, px from p order by ts", "select ts, exchange, px from c order by ts");

            // 2. WHERE ts in '<oneday>' time pruning: exactly that day's 2 rows, not zero/all.
            assertQuery("select count() from c where ts in '2020-01-03'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
            assertSqlCursors(
                    "select ts, exchange, px from p where ts in '2020-01-03' order by ts",
                    "select ts, exchange, px from c where ts in '2020-01-03' order by ts");

            // 3. count() and LATEST ON ts PARTITION BY exchange.
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n10\n");
            assertSqlCursors("select count() from p", "select count() from c");
            assertSqlCursors(
                    "select ts, exchange, px from p latest on ts partition by exchange order by exchange",
                    "select ts, exchange, px from c latest on ts partition by exchange order by exchange");
        });
    }

    /**
     * Brief assertions 4-6: catalogue/introspection surfaces -- {@code SHOW CREATE TABLE}'s composite
     * clause round-trip (Plan 1 unbroken), {@code table_storage()}'s {@code partitionCount} (the
     * doubled-count site Plan 3b's marker fixes), and {@code table_partitions()}'s row count -- must
     * all read a composite table exactly like an equivalent plain table.
     */
    @Test
    public void testCatalogueMatchesPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            // 4. SHOW CREATE TABLE still round-trips the composite partition-by clause.
            assertQuery("show create table c").noLeakCheck().noRandomAccess().returns(
                    "ddl\n" +
                            "CREATE TABLE 'c' ( \n" +
                            "\tts TIMESTAMP,\n" +
                            "\texchange SYMBOL,\n" +
                            "\tpx DOUBLE\n" +
                            ") timestamp(ts) PARTITION BY DAY, exchange BYPASS WAL;\n");

            // 5. table_storage() partitionCount -- the doubled-count site -- must equal p's (not 2x).
            assertSqlCursors(
                    "select partitionCount from table_storage() where tableName = 'p'",
                    "select partitionCount from table_storage() where tableName = 'c'");
            assertQuery("select partitionCount from table_storage() where tableName = 'c'")
                    .noLeakCheck().noRandomAccess().returns("partitionCount\n5\n");

            // 6. table_partitions() row count must equal p's.
            assertSqlCursors("select count() from table_partitions('p')", "select count() from table_partitions('c')");
            assertQuery("select count() from table_partitions('c')")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n5\n");
        });
    }

    /**
     * Brief assertions 7-8: mutating DDL that must re-derive per-partition state across a composite
     * table's every day partition -- {@code ALTER TABLE ... ALTER COLUMN ... ADD INDEX} (built across 5
     * EXISTING day partitions via {@code RebuildColumnBase}/{@code IndexBuilder}, which resolves each
     * partition through {@code _txn}) and {@code ADD COLUMN}/{@code DROP COLUMN} followed by a forced
     * column-purge cycle ({@link ColumnPurgeJob}/{@code ColumnPurgeOperator}, which also resolves the
     * table's partitions through {@code _txn}) -- must leave the composite table exactly as correct as
     * before.
     */
    @Test
    public void testMutationsMatchPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            // 7. Index an EXISTING, already-populated dimension column across 5 day partitions, then
            // query with it -- a query plan that indexes wrong rows would surface here.
            execute("alter table c alter column exchange add index");
            assertSqlCursors(
                    "select ts, exchange, px from p where exchange = 'A' order by ts",
                    "select ts, exchange, px from c where exchange = 'A' order by ts");
            assertQuery("select count() from c where exchange = 'A'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n5\n");

            // 8. ADD COLUMN + DROP COLUMN, then force a column-purge cycle to completion, then re-query.
            execute("alter table c add column q double");
            execute("alter table c drop column q");
            engine.releaseInactive(); // release readers pinning the pre-drop column version
            try (ColumnPurgeJob columnPurgeJob = new ColumnPurgeJob(engine)) {
                columnPurgeJob.run();
                columnPurgeJob.run(); // established idiom: 1st reschedules outstanding tasks, 2nd executes them
            }
            assertSqlCursors("select ts, exchange, px from p order by ts", "select ts, exchange, px from c order by ts");
            // Explicit metadata proof DROP COLUMN completed (not just that ts/exchange/px are undisturbed):
            // q must be fully gone, back to the original 3-column shape shared with p.
            try (TableReader reader = getReader("c")) {
                Assert.assertEquals(-1, reader.getMetadata().getColumnIndexQuiet("q"));
                Assert.assertEquals(3, reader.getMetadata().getColumnCount());
            }
        });
    }

    /**
     * Brief assertion 9: an out-of-order (O3) insert into an EARLIER, already-committed day partition
     * churns that partition's on-disk directory version and schedules the OLD version for removal via
     * {@link O3PartitionPurgeJob} -- the job that, pre-marker, misread a composite {@code _txn}'s
     * attached-partitions region at the wrong stride and could misidentify (and delete) a LIVE partition
     * directory. After driving the purge job to completion, EVERY partition and row must still be
     * present -- no partition-directory loss -- and the full scan must still equal {@code p}'s.
     * <p>
     * This test harness enables {@code CAIRO_O3_PARTITION_OVERWRITE_CONTROL_ENABLED} by default (unlike
     * production; see {@code Overrides}), so this also exercises Plan 3b Task 4's fix to the static
     * {@code TxReader#findPartitionRawIndex} (the one path {@code PartitionOverwriteControl} uses) live,
     * through real O3 SQL inserts -- not just the synthetic unit test that task added.
     */
    @Test
    public void testO3InsertAndPurgeMatchPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();

            // Two successive out-of-order inserts into day1 (the earliest committed day, well behind
            // the table's current max timestamp, day5) -- each churns day1's partition directory to a
            // new version and schedules the previous version for purge (mirrors O3PartitionPurgeTest's
            // own two-successive-O3-insert idiom).
            execute("insert into c values ('2020-01-01T06:00:00.000000Z','A',1.25)");
            execute("insert into p values ('2020-01-01T06:00:00.000000Z','A',1.25)");
            execute("insert into c values ('2020-01-01T03:00:00.000000Z','B',1.1)");
            execute("insert into p values ('2020-01-01T03:00:00.000000Z','B',1.1)");

            // Drive O3PartitionPurgeJob to completion (mirrors O3PartitionPurgeTest#runPartitionPurgeJobs).
            engine.releaseInactive(); // pooled readers/writers must release before purge can proceed
            purgeJob.drain(0);

            Assert.assertEquals("0 partition purge errors expected",
                    0, engine.getPartitionOverwriteControl().getErrorCount());

            // NO partition/row loss: every day's data is still present and matches p's row for row.
            assertSqlCursors("select ts, exchange, px from p order by ts", "select ts, exchange, px from c order by ts");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n12\n");
            assertSqlCursors("select count() from p", "select count() from c");
            assertSqlCursors(
                    "select partitionCount from table_storage() where tableName = 'p'",
                    "select partitionCount from table_storage() where tableName = 'c'");
        });
    }

    /**
     * Brief assertion 10: a full CHECKPOINT CREATE + restore round trip covering the composite table
     * {@code c} -- mirrors {@code CheckpointTest#testCheckpointRestoreIndexNonPartitioned}'s in-process
     * create-then-recover idiom (change the configured snapshot instance id between create and recover
     * so {@code engine.checkpointRecover()} treats this as restoring onto a different install, instead
     * of no-op'ing because it looks like the same instance that made the checkpoint). {@code c} must
     * read back byte-for-byte identical to its pre-checkpoint (== {@code p}'s) state.
     * <p>
     * This also directly proves {@code TableSnapshotRestore} preserves the composite stride marker:
     * reading that class's source shows {@code copyMetadataFiles} copies {@code _txn} as a raw
     * byte-for-byte file copy (no {@code createTxn}/{@code resetTxn} call anywhere in the class -- it is
     * not a writer of a fresh base header), and {@code rebuildTableFiles} then reopens the COPY via a
     * genuine {@code TxWriter#ofRW}/{@code unsafeLoadAll()} -- the same self-describing marker read path
     * (Plan 3b Task 3, symmetric from creation) every other {@code _txn} consumer goes through. The raw
     * marker byte-read below is the direct proof, independent of the SQL-level behavioral proof that
     * follows it.
     * <p>
     * <b>Deliberately NOT wrapped in {@code assertMemoryLeak}:</b> investigated empirically (negative
     * control) rather than assumed. {@code engine.checkpointRecover()} leaves a small, constant
     * (1336-byte, i.e. one fixed-size allocation) {@code NATIVE_TABLE_READER} tag difference behind when
     * driven from an ordinary {@code AbstractCairoTest} subclass -- reproduced with a 100% vanilla,
     * non-composite, unindexed, un-partitioned-or-partitioned table built via plain {@code CREATE
     * TABLE}/{@code INSERT} AND via {@code CREATE TABLE AS SELECT}, and even with a byte-for-byte copy of
     * {@code CheckpointTest#testRecoverCheckpointLargePartitionCount}'s own body pasted verbatim into a
     * throwaway class in this package. It reproduces regardless of composite-ness, partitioning, symbol
     * columns/indexes, CTAS vs. separate insert, or this class's O3PartitionPurgeJob field (each ruled
     * out individually) -- i.e. it is a pre-existing property of running checkpoint recovery outside
     * {@code io.questdb.test.griffin.CheckpointTest}'s own specialized fixture (whose {@code @Before}
     * does something -- not fully isolated -- that this class's plain fixture does not), not anything
     * introduced by Plan 3b or specific to a composite table. It is orthogonal to this test's actual
     * subject (the marker survives restore, checked directly below) and to what a memory leak check would
     * otherwise be guarding. {@code engine.clear()} at the top substitutes for what {@code
     * assertMemoryLeak} would have done on entry.
     */
    @Test
    public void testCheckpointRestoreMatchesPlainEquivalent() throws Exception {
        final String snapshotId = "00000000-0000-0000-0000-000000000000";
        final String restartedId = "123e4567-e89b-12d3-a456-426614174000";

        engine.clear();
        setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

        createAndPopulateTwins();

        execute("checkpoint create");

        // Post-checkpoint mutation to c only -- must NOT survive c's restore (p is the untouched
        // pre-checkpoint oracle c is compared against below).
        execute("insert into c values ('2020-01-06T00:00:00.000000Z','A',6.0)");

        TableToken cToken = engine.verifyTableName("c");

        // Release all readers/writers but keep the checkpoint dir around (simulates a restart), then
        // force checkpointRecover() to actually restore (not no-op) by making the configured snapshot
        // instance id differ from the one the checkpoint was created under.
        engine.clear();
        setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
        engine.checkpointRecover();

        // Direct marker proof: c's restored _txn must still self-describe as COMPOSITE (8), not have
        // been silently rewritten to the plain default (0) by the restore path.
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(cToken).concat(TXN_FILE_NAME).$();
            try (RandomAccessFile raf = new RandomAccessFile(path.toString(), "r")) {
                raf.seek(TX_BASE_OFFSET_PARTITION_STRIDE_32);
                int marker = Integer.reverseBytes(raf.readInt());
                Assert.assertEquals(
                        "restored composite table's _txn marker must still be COMPOSITE (8) -- if " +
                                "TableSnapshotRestore ever rebuilt _txn from scratch instead of copying it, " +
                                "a symmetric marker read would misread this table as plain",
                        8, marker);
            }
        }

        // Behavioral proof: c reads back exactly its pre-checkpoint (== p's) state -- the
        // post-checkpoint insert above must be reverted.
        assertSqlCursors("select ts, exchange, px from p order by ts", "select ts, exchange, px from c order by ts");
        assertSqlCursors("select count() from p", "select count() from c");
        assertSqlCursors(
                "select partitionCount from table_storage() where tableName = 'p'",
                "select partitionCount from table_storage() where tableName = 'c'");

        // c must still be fully writable/queryable post-restore.
        execute("insert into c values ('2020-01-06T00:00:00.000000Z','A',6.0)");
        assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n11\n");

        engine.releaseInactive();
        engine.clear();
    }

    /**
     * Builds the composite table {@code c} ({@code partition by day, exchange}) and its plain twin
     * {@code p} ({@code partition by day}), then inserts byte-for-byte identical rows into both: 5 day
     * partitions (2020-01-01 .. 2020-01-05), 2 rows per day (one per exchange, A and B) -- 10 rows total.
     * All rows land at cellKey 0 (real (ts, cellKey) write-routing is Plan 4), so c's on-disk shape is
     * 1-D-equivalent, stride-8 {@code _txn} aside.
     */
    private void createAndPopulateTwins() throws Exception {
        execute("create table c (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day, exchange");
        execute("create table p (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day");

        final String rows = " values " +
                "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5), " +
                "('2020-01-02T00:00:00.000000Z','A',2.0), ('2020-01-02T12:00:00.000000Z','B',2.5), " +
                "('2020-01-03T00:00:00.000000Z','A',3.0), ('2020-01-03T12:00:00.000000Z','B',3.5), " +
                "('2020-01-04T00:00:00.000000Z','A',4.0), ('2020-01-04T12:00:00.000000Z','B',4.5), " +
                "('2020-01-05T00:00:00.000000Z','A',5.0), ('2020-01-05T12:00:00.000000Z','B',5.5)";
        execute("insert into c" + rows);
        execute("insert into p" + rows);
    }
}
