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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.RowExpiryUtil;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.ExpiryValidationResult;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Hardening tests for the EXPIRE ROWS feature, covering the level-3-review fixes:
 * <ul>
 *     <li><b>Monotonicity gate</b> — physical cleanup is SKIPPED for a non-monotonic policy (e.g.
 *         {@code ts > now()}, which un-expires rows as time advances), so it can never physically delete a
 *         row a later read would show. The read filter stays authoritative for visibility. Clock-free and
 *         {@code ts < now()}-style (monotonic) policies still reclaim.</li>
 *     <li><b>No policied-view chains</b> — a materialized view cannot be created over a base that carries an
 *         EXPIRE ROWS policy, and {@code SET EXPIRE} is rejected on a view that already has dependents.</li>
 *     <li><b>DROP EXPIRE [ROWS]</b> — both spellings are accepted.</li>
 *     <li><b>Read vs cleanup boundary agreement</b> — for {@code ts < now()} the row exactly at the frozen
 *         {@code now()} boundary is kept by both the read filter and the cleanup classifier.</li>
 * </ul>
 */
public class MatViewExpireRowsHardeningTest extends AbstractCairoTest {

    private static final long FEB_10 = 1_707_523_200_000_000L; // 2024-02-10T00:00:00Z
    private static final long JAN_10 = 1_704_844_800_000_000L; // 2024-01-10T00:00:00Z
    private static final long JAN_25 = 1_706_140_800_000_000L; // 2024-01-25T00:00:00Z
    private static final long MAR_01 = 1_709_251_200_000_000L; // 2024-03-01T00:00:00Z

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testClockAndRandomThresholdCleanupSkipped() throws Exception {
        assertMemoryLeak(() -> {
            // A pre-epoch clock makes `now() - now()*2` a positive post-epoch threshold. Before the fix,
            // cleanup classified it monotonic and physically dropped the first two partitions.
            setCurrentMicros(-864_000_000_000L); // 1969-12-22, threshold of decreasing transform = 1970-01-11
            execute("CREATE TABLE base (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    (1.0, '1970-01-02T00:00:00.000000Z'),
                    (2.0, '1970-01-05T00:00:00.000000Z'),
                    (3.0, '1970-02-01T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();

            assertMixedClockCleanupSkipped("mv_direct", "ts < now() + rnd_long()");
            assertMixedClockCleanupSkipped("mv_nested", "ts < dateadd('u', rnd_int(), now())");
            assertMixedClockCleanupSkipped("mv_decreasing", "ts < now() - (now()::long * 2)");
            assertMixedClockCleanupSkipped("mv_nested_decreasing", "ts < (now() - (now()::long * 2)) - 1_000_000L");
        });
    }

    @Test
    public void testCleanupUsesPolicyDeadlinesAndAuthoritativeSnapshot() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("CREATE TABLE base (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES (1.0, '2024-01-01T00:00:00.000000Z'),
                    (2.0, '2024-01-02T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("""
                    CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base)
                    EXPIRE ROWS WHEN v < 0 CLEANUP EVERY 1h""");
            drainWalAndMatViewQueues();
            engine.getMetadataCache().hydrateAllTables();

            final TableToken token = engine.verifyTableName("mv");
            Assert.assertTrue(engine.getMetadataCache().mayTableHaveExpiryPolicy(token));
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.runNow());
                Assert.assertEquals(1, job.getPolicyDiscoveryCount());
                Assert.assertFalse(job.runNow());
                Assert.assertEquals("unchanged policy version and future deadline avoid discovery",
                        1, job.getPolicyDiscoveryCount());

                execute("ALTER MATERIALIZED VIEW mv DROP EXPIRE");
                drainWalAndMatViewQueues();
                Assert.assertFalse(engine.getMetadataCache().mayTableHaveExpiryPolicy(token));
                Assert.assertFalse(engine.getMetadataCache().mayHaveExpiryPolicy());
                Assert.assertFalse(job.runNow());
                Assert.assertEquals("policy publication invalidates the deadline immediately",
                        1, job.getPolicyDiscoveryCount());
            }
        });
    }

    @Test
    public void testClearCacheKeepsExpiryGateOpenUntilRehydration() throws Exception {
        // clearCache() empties the expiry-policy snapshot AND resets fullyHydrated, so the global and
        // per-table expiry gates fall back to their conservative "open while not fully hydrated" answer
        // until the cache re-hydrates. Regression guard: were the fullyHydrated reset dropped, an
        // already-hydrated cache would keep fullyHydrated == true while the snapshot is empty, so
        // mayHaveExpiryPolicy() would read !true || 0 > 0 == false -- every read would then skip the
        // expiry filter and expose expired rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 2.0, '2024-01-02T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) EXPIRE ROWS WHEN v < 2");
            drainWalAndMatViewQueues();

            // Fully hydrate so the policy is in the snapshot and fullyHydrated latches true.
            engine.getMetadataCache().hydrateAllTables();
            final TableToken token = engine.verifyTableName("mv");
            Assert.assertTrue(engine.getMetadataCache().mayHaveExpiryPolicy());
            Assert.assertTrue(engine.getMetadataCache().mayTableHaveExpiryPolicy(token));

            // Clear the cache WITHOUT re-hydrating: the snapshot is now empty, but the gates must stay open
            // because the cache is no longer fully hydrated.
            try (MetadataCacheWriter w = engine.getMetadataCache().writeLock()) {
                w.clearCache();
            }
            Assert.assertTrue("cleared cache must not close the global expiry gate",
                    engine.getMetadataCache().mayHaveExpiryPolicy());
            Assert.assertTrue("cleared cache must not close the per-table expiry gate",
                    engine.getMetadataCache().mayTableHaveExpiryPolicy(token));

            // The read filter still hides the expired row (v = 1 < 2 -> A gone; B kept).
            assertQuery("SELECT sym, v FROM mv ORDER BY sym").noLeakCheck().returns("sym\tv\nB\t2.0\n");
        });
    }

    @Test
    public void testClockFreeValuePredicateReclaims() throws Exception {
        // Positive control for the monotonicity gate: a CLOCK-FREE value predicate is monotonic (mat-view
        // rows are immutable, so the predicate's per-row value never changes), so cleanup reclaims a
        // wholly-expired non-active partition.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," + // v > 100 -> false -> kept? no: expire WHEN v>100, so v=1 kept
                    "('B', 500.0, '2024-01-02T00:00:00.000000Z')," + // v > 100 -> expired (whole 01-02 partition)
                    "('C', 3.0, '2024-01-03T00:00:00.000000Z')");    // active partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v > 100");
            drainWalAndMatViewQueues();
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            final boolean worked = runCleanup("mv");
            drainWalAndMatViewQueues();

            Assert.assertTrue("clock-free monotonic policy must reclaim", worked);
            // 01-02 wholly expired -> reclaimed; 01-01 (kept v=1) and 01-03 (active) remain.
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("sym\tv\nA\t1.0\nC\t3.0\n");
        });
    }

    @Test
    public void testExpiryFilterAcrossCteNestedAndUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 2.0, '2024-01-02T00:00:00.000000Z'),
                    ('C', null, '2024-01-03T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) EXPIRE ROWS WHEN v < 2");
            drainWalAndMatViewQueues();

            assertQuery("WITH x AS (SELECT sym FROM mv) SELECT * FROM x UNION ALL SELECT * FROM x ORDER BY sym")
                    .noLeakCheck().returns("sym\nB\nB\nC\nC\n");
            // Every reference to mv -- outer and inner -- must apply the read filter. Without it, the inner
            // subquery readmits the expired row A into the IN set and the result becomes A,B,C.
            assertQuery("SELECT sym FROM mv WHERE sym IN (SELECT sym FROM mv) ORDER BY sym")
                    .noLeakCheck().returns("sym\nB\nC\n");
            // Both UNION arms must filter: without the filter, v < 5 exposes A (v = 1) alongside B.
            assertQuery("SELECT sym FROM mv WHERE v < 5 UNION ALL SELECT sym FROM mv WHERE v IS NULL ORDER BY sym")
                    .noLeakCheck().returns("sym\nB\nC\n");

        });
    }

    @Test
    public void testExpiryTimestampThresholdMicrosAcceptsOnlyDropOldShapes() throws Exception {
        // expiryTimestampThresholdMicros feeds the partition-bounds fast path, which drops whole partitions
        // below T. It must return a threshold ONLY for "expire everything below T" shapes (ts < T, T > ts)
        // and LONG_NULL for the opposite "keep old, expire recent" shapes (ts > T, T < ts) -- otherwise the
        // fast path would drop the KEPT partitions (data loss). It also requires a typed TIMESTAMP threshold:
        // a bare string literal is not a timestamp constant, so it takes the (always-correct) survivor scan.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final TableToken token = engine.verifyTableName("x");
            final long day2 = 86_400_000_000L; // 1970-01-02T00:00:00.000000Z
            try (
                    TableMetadata metadata = engine.getTableMetadata(token);
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                // Drop-old shapes: threshold accepted (micros of T).
                Assert.assertEquals(day2, compiler.expiryTimestampThresholdMicros(
                        sqlExecutionContext, metadata, "ts < '1970-01-02T00:00:00.000000Z'::timestamp", "ts"));
                Assert.assertEquals(day2, compiler.expiryTimestampThresholdMicros(
                        sqlExecutionContext, metadata, "ts <= '1970-01-02T00:00:00.000000Z'::timestamp", "ts"));
                Assert.assertEquals(day2, compiler.expiryTimestampThresholdMicros(
                        sqlExecutionContext, metadata, "'1970-01-02T00:00:00.000000Z'::timestamp > ts", "ts"));
                Assert.assertEquals(day2, compiler.expiryTimestampThresholdMicros(
                        sqlExecutionContext, metadata, "'1970-01-02T00:00:00.000000Z'::timestamp >= ts", "ts"));

                // Keep-old shapes: MUST be rejected (LONG_NULL). Accepting these would make the bounds fast
                // path drop the partitions below T, which are exactly the rows the policy keeps.
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThresholdMicros(
                        sqlExecutionContext, metadata, "ts > '1970-01-02T00:00:00.000000Z'::timestamp", "ts"));
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThresholdMicros(
                        sqlExecutionContext, metadata, "ts >= '1970-01-02T00:00:00.000000Z'::timestamp", "ts"));
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThresholdMicros(
                        sqlExecutionContext, metadata, "'1970-01-02T00:00:00.000000Z'::timestamp < ts", "ts"));

                // A bare (untyped) string literal is not a TIMESTAMP constant: no fast path, survivor scan.
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThresholdMicros(
                        sqlExecutionContext, metadata, "ts < '1970-01-02T00:00:00.000000Z'", "ts"));

                // A threshold that references a column is not a constant bound.
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThresholdMicros(
                        sqlExecutionContext, metadata, "ts < ts", "ts"));
            }
        });
    }

    @Test
    public void testExpiryValidationReturnsReusableClassification() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final TableToken token = engine.verifyTableName("x");
            try (
                    TableMetadata metadata = engine.getTableMetadata(token);
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                final ExpiryValidationResult clockFree = compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "v < 0", 0);
                Assert.assertFalse(clockFree.hasClock());
                Assert.assertTrue(clockFree.isDeterministic());
                Assert.assertTrue(clockFree.isMonotonic());
                Assert.assertEquals(1, clockFree.getReferencedColumnIndexes().size());
                Assert.assertEquals(metadata.getColumnIndexQuiet("v"), clockFree.getReferencedColumnIndexes().getQuick(0));

                final ExpiryValidationResult advancingThreshold = compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < now()", 0);
                Assert.assertTrue(advancingThreshold.hasClock());
                Assert.assertFalse(advancingThreshold.isDeterministic());
                Assert.assertTrue(advancingThreshold.isMonotonic());
                Assert.assertEquals(metadata.getTimestampIndex(), advancingThreshold.getReferencedColumnIndexes().getQuick(0));

                final ExpiryValidationResult reversingThreshold = compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts > now()", 0);
                Assert.assertTrue(reversingThreshold.hasClock());
                Assert.assertFalse(reversingThreshold.isMonotonic());

                Assert.assertFalse(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < now() + rnd_long()", 0).isMonotonic());
                Assert.assertFalse(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < dateadd('u', rnd_int(), now())", 0).isMonotonic());
                Assert.assertFalse(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < now() - (now()::long * 2)", 0).isMonotonic());
                Assert.assertFalse(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < (now() - (now()::long * 2)) - 1_000_000L", 0).isMonotonic());

                // Look-back offsets on a bare clock are proven advancing: the canonical retention
                // predicates reclaim disk. Look-forward offsets, calendar units (a variable shift),
                // and non-constant offsets stay unproven.
                Assert.assertTrue(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < now() - 1_000_000L", 0).isMonotonic());
                Assert.assertTrue(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < dateadd('d', -1, now())", 0).isMonotonic());
                Assert.assertTrue(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < dateadd('h', -36, now())", 0).isMonotonic());
                Assert.assertFalse(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < dateadd('u', 1, now())", 0).isMonotonic());
                Assert.assertFalse(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < dateadd('M', -1, now())", 0).isMonotonic());
                Assert.assertFalse(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < now() + 1_000_000L", 0).isMonotonic());
            }

            // A subquery predicate is never treated as safe for physical cleanup: the expression parse
            // rejects it up front (it runs without a query model), and isExpiryCleanupMonotonic maps
            // the rejection to non-monotonic, so the cleanup job skips such a policy. The classifier
            // also checks for QUERY nodes itself, covering a subquery that ever got past the parse.
            execute("CREATE TABLE blk (s SYMBOL)");
            execute("CREATE TABLE y (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            try (
                    TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("y"));
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                try {
                    compiler.validateExpiryPredicateOnMetadata(
                            sqlExecutionContext, metadata, "s IN (SELECT s FROM blk)", 0);
                    Assert.fail("subquery predicate must be rejected");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "query is not allowed here");
                }
                Assert.assertFalse("subquery predicate must never classify monotonic", compiler.isExpiryCleanupMonotonic(
                        sqlExecutionContext, metadata, "y", "s IN (SELECT s FROM blk)", "ts"));
            }

            execute("CREATE TABLE xns (ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            try (
                    TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("xns"));
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                Assert.assertTrue(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "now() > ts", 0).isMonotonic());
                Assert.assertTrue(compiler.isExpiryCleanupMonotonic(
                        sqlExecutionContext, metadata, "xns", "now() > ts", "ts"));
            }
        });
    }

    @Test
    public void testDropExpireRowsAndDropExpireBothWork() throws Exception {
        // DROP EXPIRE and DROP EXPIRE ROWS are both accepted (symmetry with SET EXPIRE ROWS).
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            Assert.assertNotNull(expiryPredicate("mv"));

            execute("alter materialized view mv drop expire rows");
            drainWalAndMatViewQueues();
            Assert.assertNull("DROP EXPIRE ROWS must clear the policy", expiryPredicate("mv"));

            // Re-add, then DROP EXPIRE (no ROWS) must also clear it.
            execute("alter materialized view mv set expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            Assert.assertNotNull(expiryPredicate("mv"));
            execute("alter materialized view mv drop expire");
            drainWalAndMatViewQueues();
            Assert.assertNull("DROP EXPIRE must clear the policy", expiryPredicate("mv"));
        });
    }

    @Test
    public void testStructuralCleanupDefersWithoutWalMutation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base SELECT 'A', x, dateadd('d', x::int, '2024-01-01'::timestamp) FROM long_sequence(4)");
            drainWalAndMatViewQueues();
            execute("""
                    CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base)
                    EXPIRE ROWS KEEP HIGHEST v PARTITION BY k CLEANUP EVERY 1s""");
            drainWalAndMatViewQueues();
            final TableToken token = engine.verifyTableName("mv");
            final String predicate = RowExpiryUtil.encodeKeepBy(1, true, "v", "k");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.cleanupTable(token, predicate));
            }
            assertQuery("SELECT count() p, sum(numRows) r FROM table_partitions('mv')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck().returns("p\tr\n4\t4\n");

            // Repeated sweeps remain no-ops and never mutate WAL state.
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.cleanupTable(token, predicate));
            }
            assertQuery("SELECT count() p, sum(numRows) r FROM table_partitions('mv')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck().returns("p\tr\n4\t4\n");
        });
    }

    @Test
    public void testReplaceFailureMidCopyRollsBackWithoutLeakingRows() throws Exception {
        // The per-partition catch in cleanupTable0 frees the WAL writer when a REPLACE fails mid-append, so
        // the half-appended survivors roll back on close and cannot ride along in the NEXT partition's
        // REPLACE_RANGE commit (which would resurrect them outside the deleted range). The failure is
        // injected deterministically with the dev-mode test_fault() function as the predicate's FIRST
        // conjunct (AND short-circuits, so leading with it makes the per-row evaluation count exact):
        // armed to throw on the evaluation of d1's SECOND survivor, AFTER the first survivor was already
        // appended to the writer. The parallel filter is disabled so the predicate evaluates row by row
        // inside the copy loop; the async path reduces a whole page frame before the copier consumes it,
        // which would fire the fault before any row was appended. d1's sweep fails half-appended; d2 must
        // then compact cleanly on a fresh writer with no d1 row leaking into its commit, and a later sweep
        // compacts d1.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED, "false");
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', -1.0, '2024-01-01T00:00:00.000000Z')," +  // expired (d1)
                    "('B', 5.0, '2024-01-01T01:00:00.000000Z')," +   // kept (d1 survivor #1: appended, then rolled back)
                    "('C', 6.0, '2024-01-01T02:00:00.000000Z')," +   // kept (d1 survivor #2: the throwing evaluation)
                    "('D', -2.0, '2024-01-02T00:00:00.000000Z')," +  // expired (d2)
                    "('E', 7.0, '2024-01-02T01:00:00.000000Z')," +   // kept (d2 survivor)
                    "('F', 9.0, '2024-01-03T00:00:00.000000Z')");    // kept (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when test_fault() and v < 0");
            drainWalAndMatViewQueues();
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')")
                    .noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t6\n");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }

            // Per-row evaluations in the sweep, in order: d1 count scan (A, B, C = 3 calls), then d1's
            // survivor SELECT (A, B, C); arming past 5 calls lands the throw on C - d1's second survivor -
            // with B already appended to the REPLACE writer.
            // armToFailAfter resets the global trigger counter, so the absolute count below is exact.
            TestFaultFunctionFactory.armToFailAfter(5);
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.cleanupTable(token, predicate);
            } finally {
                TestFaultFunctionFactory.disarm();
            }
            Assert.assertEquals("the injected fault must have fired exactly once",
                    1, TestFaultFunctionFactory.faultsTriggered());
            drainWalAndMatViewQueues();

            // d1's REPLACE failed mid-append and rolled back (3 rows intact); d2 compacted to its survivor
            // on a fresh writer; no half-appended d1 row leaked into d2's commit (B appears exactly once).
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')")
                    .noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t5\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("""
                    sym\tv
                    B\t5.0
                    C\t6.0
                    E\t7.0
                    F\t9.0
                    """);

            // A later sweep (fault disarmed) compacts d1 as well; the visible set is unchanged.
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertTrue(job.cleanupTable(token, predicate));
            }
            drainWalAndMatViewQueues();
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')")
                    .noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("""
                    sym\tv
                    B\t5.0
                    C\t6.0
                    E\t7.0
                    F\t9.0
                    """);
        });
    }

    @Test
    public void testScalarCleanupMemoryLimitBreachDefersWithBackoffAndRecovers() throws Exception {
        // Real memory-tracker coverage on the scalar destructive path: cleanupTable0 binds the sweep's
        // execution context to a MAT_VIEW_REFRESH-workload tracker before compiling the survivor queries.
        // The survivor scan of a small native partition reaches no tracker-wired allocator on its own, so
        // the predicate carries the dev-mode alloc_tracked(l) function - the designated way to drive a
        // per-workload breach from SQL (see WorkloadMemoryTrackerTest). With the limit at 1 byte the
        // survivor-query compile charges the tracker and throws, the per-partition catch marks the sweep
        // failed, and nothing is reclaimed. The job's scheduler then applies the failure backoff (no
        // re-discovery until it elapses). Raising the limit and letting the backoff elapse resumes
        // reclamation: the fully-expired partition is wiped and the partial one compacts to its survivors.
        // The 1h cadence makes the recovery retry at +1s reachable only through the 1s failure backoff, so
        // the test fails if the backoff scheduling disappears.
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES " +
                    "('A', -1.0, '2024-01-01T00:00:00.000000Z')," +  // expired (v < 0); 01-01 partial -> REPLACE
                    "('B', 8.0, '2024-01-01T00:00:00.000000Z')," +   // kept survivor in 01-01
                    "('A', -5.0, '2024-01-02T00:00:00.000000Z')," +  // expired; 01-02 fully expired -> DROP
                    "('A', 9.0, '2024-01-03T00:00:00.000000Z')");    // kept (active partition)
            drainWalAndMatViewQueues();
            execute("""
                    CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base)
                    EXPIRE ROWS WHEN v < 0 AND alloc_tracked(1024) = 42 CLEANUP EVERY 1h""");
            drainWalAndMatViewQueues();
            engine.getMetadataCache().hydrateAllTables();

            setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 1L);
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                // The survivor query trips the 1-byte budget: the sweep fails, reclaims nothing.
                Assert.assertFalse("breached sweep must not reclaim", job.runNow());
                assertQuery("SELECT count() p, sum(numRows) r FROM table_partitions('mv')")
                        .noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");

                // Failure backoff: until it elapses, the scheduler does not even re-discover policies.
                final long discoveryCountAfterFailure = job.getPolicyDiscoveryCount();
                setCurrentMicros(JAN_10 + 500_000L); // inside the 1s backoff
                Assert.assertFalse(job.runNow());
                Assert.assertEquals(
                        "sweep must defer within the failure backoff window",
                        discoveryCountAfterFailure, job.getPolicyDiscoveryCount()
                );
                assertQuery("SELECT count() p, sum(numRows) r FROM table_partitions('mv')")
                        .noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");

                // Raised limit + elapsed backoff: reclamation resumes and completes.
                setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 256L * 1024 * 1024);
                setCurrentMicros(JAN_10 + 1_000_000L); // backoff elapsed
                Assert.assertTrue("recovered sweep must reclaim", job.runNow());
            }
            drainWalAndMatViewQueues();
            assertQuery("SELECT count() p, sum(numRows) r FROM table_partitions('mv')")
                    .noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n2\t2\n");
            assertQuery("SELECT k, v FROM mv ORDER BY k").noLeakCheck().returns("k\tv\nA\t9.0\nB\t8.0\n");
        });
    }

    @Test
    public void testStructuralCleanupRemainsDeferredAfterMemoryLimitChange() throws Exception {
        // A structural (KEEP HIGHEST) policy exits cleanupTable0 before the memory tracker is even
        // acquired, so the limit set here is inert by design: this test pins that structural cleanup
        // stays deferred REGARDLESS of the memory budget, not the tracker itself. The tracker's
        // breach/backoff/recovery behavior is covered by
        // testScalarCleanupMemoryLimitBreachDefersWithBackoffAndRecovers.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // expired (A max=9)
                    "('B', 8.0, '2024-01-01T00:00:00.000000Z')," +   // B max -> survives in d1
                    "('A', 5.0, '2024-01-02T00:00:00.000000Z')," +   // expired (A max=9)
                    "('A', 9.0, '2024-01-03T00:00:00.000000Z')");    // A max (active partition)
            drainWalAndMatViewQueues();
            execute("""
                    CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base)
                    EXPIRE ROWS KEEP HIGHEST v PARTITION BY k CLEANUP EVERY 1s""");
            drainWalAndMatViewQueues();
            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }

            setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 1L);
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.cleanupTable(token, predicate));
            }
            assertQuery("SELECT count() p, sum(numRows) r FROM table_partitions('mv')")
                    .noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");

            // Structural cleanup remains disabled independently of the available query-memory budget.
            setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 256L * 1024 * 1024);
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.cleanupTable(token, predicate));
            }
            drainWalAndMatViewQueues();
            assertQuery("SELECT count() p, sum(numRows) r FROM table_partitions('mv')")
                    .noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");
            assertQuery("SELECT k, v FROM mv ORDER BY k").noLeakCheck().returns("k\tv\nA\t9.0\nB\t8.0\n");
        });
    }

    @Test
    public void testNonMonotonicFuturePredicateCleanupSkippedAndRowsSurvive() throws Exception {
        // A non-monotonic policy "ts > now()" expires FUTURE rows; as now() advances past them they un-expire.
        // Cleanup must NOT physically delete them (the gate skips reclamation), so when now() advances they
        // reappear. The read filter stays authoritative throughout.
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-05T00:00:00.000000Z')," + // past  -> ts > now() false -> kept
                    "('B', 2.0, '2024-01-15T00:00:00.000000Z')," + // future-> ts > now() true  -> expired (non-active)
                    "('C', 3.0, '2024-01-20T00:00:00.000000Z')");  // future-> expired (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts > now()");
            drainWalAndMatViewQueues();

            // Three logical partitions; read filter shows only the non-expired past row.
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nA\n");

            // Cleanup must be a NO-OP: the policy is non-monotonic, so reclamation is skipped.
            final boolean worked = runCleanup("mv");
            drainWalAndMatViewQueues();
            Assert.assertFalse("non-monotonic policy must skip physical cleanup", worked);
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nA\n");

            // Advance the clock past all rows: every row now satisfies ts <= now() -> all kept and VISIBLE.
            // This only holds because cleanup did not delete the future rows while they were expired.
            setCurrentMicros(JAN_25);
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nA\nB\nC\n");
        });
    }

    @Test
    public void testSubqueryPredicateRejectedAtCreateAndAlter() throws Exception {
        // A subquery predicate (sym IN (SELECT ...)) reads another table whose contents can change, so
        // a row expired now could un-expire later; were such a policy ever stored, physical cleanup
        // could permanently delete rows the read filter must show again. The grammar rejects it at both
        // DDL entry points (the predicate parse runs without a query model).
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create table blk (s symbol)");
            drainWalAndMatViewQueues();

            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when sym in (select s from blk)",
                    25,
                    "query is not allowed here"
            );
            Assert.assertNull(engine.getTableTokenIfExists("mv"));

            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "alter materialized view mv set expire rows when sym in (select s from blk)",
                    48,
                    "query is not allowed here"
            );
            Assert.assertEquals("the existing policy must stay intact", "v < 2.0", expiryPredicate("mv"));
        });
    }

    @Test
    public void testCleanupIntervalMalformedValueRejected() throws Exception {
        // The CLEANUP EVERY stride must be <digits><unit s/m/h/d/w>, parsed by the same strict helper
        // as SAMPLE BY intervals. A lenient parse of "30ms" reads the trailing 's' as the unit and the
        // unparseable "30m" prefix as 1, silently storing a 1s cadence the user did not write.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when v < 2.0 cleanup every 30ms",
                    93,
                    "expected single letter qualifier"
            );
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when v < 2.0 cleanup every x5h",
                    91,
                    "expected single letter qualifier"
            );
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when v < 2.0 cleanup every 0h",
                    91,
                    "zero is not a valid cleanup value"
            );
            // a well-formed stride still parses and round-trips
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0 cleanup every 90m");
            drainWalAndMatViewQueues();
            assertQuery("select expire_cleanup_every from materialized_views()")
                    .noRandomAccess().noLeakCheck().returns("expire_cleanup_every\n90m\n");
        });
    }

    @Test
    public void testPredicateWithBindVariableRejected() throws Exception {
        // A stored predicate has no statement to supply bind values, so "v > $1" would fail on every
        // read with "undefined bind variable"; both DDL entry points reject it up front.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when v > $1",
                    25,
                    "invalid EXPIRE ROWS predicate: bind variables are not supported"
            );
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "alter materialized view mv set expire rows when v > $1",
                    48,
                    "invalid EXPIRE ROWS predicate: bind variables are not supported"
            );
            Assert.assertNull("no policy must have been stored", expiryPredicate("mv"));
        });
    }

    @Test
    public void testPredicateWithLineCommentRejected() throws Exception {
        // The captured clause text is stored verbatim and embedded into single-line generated SQL (the
        // read filter, the cleanup survivor queries, SHOW CREATE output), where a line comment swallows
        // the closing tokens and fails every read of the view. All capture sites reject line comments;
        // a terminated block comment embeds safely and stays legal.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when v < 2.0 -- note",
                    77,
                    "line comments are not supported in EXPIRE ROWS clauses"
            );
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when v < max(v) over (partition by sym) -- note",
                    104,
                    "line comments are not supported in EXPIRE ROWS clauses"
            );
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows keep latest partition by sym -- note",
                    93,
                    "line comments are not supported in EXPIRE ROWS clauses"
            );
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when v < 2.0 /* dangling",
                    77,
                    "unterminated block comment in EXPIRE ROWS clause"
            );
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "alter materialized view mv set expire rows when v < 2.0 -- note",
                    56,
                    "line comments are not supported in EXPIRE ROWS clauses"
            );

            // a terminated block comment is preserved in the stored text and every read still works
            execute("alter materialized view mv set expire rows when /* half */ v < 2.0");
            drainWalAndMatViewQueues();
            Assert.assertEquals("/* half */ v < 2.0", expiryPredicate("mv"));
            assertQuery("select count() c from mv").noRandomAccess().expectSize().noLeakCheck().returns("c\n0\n");
        });
    }

    @Test
    public void testPredicateWithTrailingTokenRejected() throws Exception {
        // The expression parser stops at the first token it cannot absorb, but the FULL captured text
        // is what gets stored and embedded into every read's generated SQL; a leftover token there
        // fails every read. Validation requires the whole predicate to be one expression.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when v < 2.0 oops",
                    25,
                    "invalid EXPIRE ROWS predicate: unexpected token after expression: oops"
            );
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "alter materialized view mv set expire rows when v < 2.0 oops",
                    48,
                    "invalid EXPIRE ROWS predicate: unexpected token after expression: oops"
            );
            Assert.assertNull("no policy must have been stored", expiryPredicate("mv"));
        });
    }

    @Test
    public void testRootLevelAggregatePredicateRejected() throws Exception {
        // The read filter embeds the predicate as a CASE argument, where an aggregate is illegal. The
        // function parser rejects an aggregate only when it is an argument of another function, so a
        // bare root-level aggregate binds fine at validation; the explicit check closes that gap.
        assertMemoryLeak(() -> {
            execute("create table base (flag boolean, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when bool_and(flag)",
                    25,
                    "invalid EXPIRE ROWS predicate: aggregate functions are not supported"
            );
            // an aggregate nested under an operator is rejected by the function parser
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when max(v) > 1.0",
                    25,
                    "invalid EXPIRE ROWS predicate"
            );
            Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testCreateViewJoiningPoliciedViewRejected() throws Exception {
        // The no-policied-chains rule covers JOINED tables, not only the base: refresh cannot
        // evaluate a now()-based policy at all, so the chain is rejected up front like a policied
        // base.
        assertMemoryLeak(() -> {
            execute("create table b (sym symbol, bv double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create table base2 (sym symbol, vv double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            execute("create materialized view v as (select * from base2) expire rows when vv < 2.0");
            drainWalAndMatViewQueues();

            assertExceptionNoLeakCheck(
                    "create materialized view m2 with base b as (select b.ts, b.sym, first(v.vv) vv from b join v on (sym) sample by 1d)",
                    25,
                    "cannot create a materialized view referencing 'v': it carries an EXPIRE ROWS policy"
            );
            Assert.assertNull(engine.getTableTokenIfExists("m2"));
        });
    }

    @Test
    public void testRefreshFiltersPoliciedViewJoinedAfterCreation() throws Exception {
        // A view can gain a policy AFTER another view was created referencing it as a JOIN table
        // (the dependency graph tracks base edges only, so ALTER SET EXPIRE cannot see the join
        // edge). The refresh then reads the policied view FILTERED - exactly as any query reads it -
        // instead of silently materializing its expired rows.
        assertMemoryLeak(() -> {
            execute("create table b (sym symbol, bv double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create table base2 (sym symbol, vv double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            execute("create materialized view v as (select * from base2)");
            drainWalAndMatViewQueues();
            execute("""
                    create materialized view m2 with base b as
                    (select b.ts, b.sym, first(v.vv) vv from b join v on (sym) sample by 1d)""");
            drainWalAndMatViewQueues();

            execute("alter materialized view v set expire rows when vv < 2.0");
            drainWalAndMatViewQueues();

            execute("""
                    insert into base2 values
                    ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 5.0, '2024-01-01T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("""
                    insert into b values
                    ('A', 10.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 20.0, '2024-01-01T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();

            // A's v-row (vv=1.0) is expired, so the refresh join drops A; only B materializes.
            assertQuery("select sym, vv from m2 order by sym")
                    .expectSize().noLeakCheck().returns("sym\tvv\nB\t5.0\n");
        });
    }

    @Test
    public void testCreateViewOverPoliciedBaseRejected() throws Exception {
        // A materialized view must not derive from a base that carries an EXPIRE ROWS policy: refresh reads
        // the RAW base, so it would copy the base's expired-but-not-yet-reclaimed rows.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            execute("create materialized view a as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();

            assertExceptionNoLeakCheck(
                    "create materialized view b as (select * from a)",
                    25,
                    "the base carries an EXPIRE ROWS policy"
            );
            Assert.assertNull(engine.getTableTokenIfExists("b"));
        });
    }

    @Test
    public void testSetExpireOnViewWithDependentsRejected() throws Exception {
        // The reverse direction: a view that other materialized views derive from must not GAIN a policy
        // (those dependents would copy its expired rows on refresh).
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            execute("create materialized view a as (select * from base)");
            drainWalAndMatViewQueues();
            execute("create materialized view b as (select * from a)");
            drainWalAndMatViewQueues();

            assertExceptionNoLeakCheck(
                    "alter materialized view a set expire rows when v < 2.0",
                    24,
                    "materialized view(s), which would copy expired rows on refresh"
            );
            Assert.assertNull("policy must not have been set on a", expiryPredicate("a"));
        });
    }

    @Test
    public void testSetExpireRaceWithDependentCreateSkipsAlterWithoutSuspending() throws Exception {
        // The CREATE-vs-ALTER race, reproduced deterministically via committed-but-not-applied sequencing.
        // ALTER a SET EXPIRE is sequenced while a has no dependents (so its statement-time dependents check
        // passes), THEN a dependent view b is created over a, THEN the ALTER is applied. At apply time the
        // stored ALTER SQL is recompiled and its dependents check now finds b. The rejection is
        // WAL-recoverable, so ApplyWal2TableJob SKIPS the ALTER instead of suspending a (a non-recoverable
        // rejection would suspend a, and the advanced watermark would make RESUME skip it anyway). The final
        // topology is consistent: a keeps no policy, b survives, and both stay queryable.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            execute("create materialized view a as (select * from base)");
            drainWalAndMatViewQueues();

            final TableToken aToken = engine.verifyTableName("a");
            final SeqTxnTracker aTracker = engine.getTableSequencerAPI().getTxnTracker(aToken);

            // Sequence ALTER a SET EXPIRE while a has no dependents (statement-time check passes), but do NOT
            // apply it: committed-but-not-applied, so a's _meta still carries no policy.
            execute("alter materialized view a set expire rows when v < 2.0");
            Assert.assertTrue(
                    "precondition: SET EXPIRE must be sequenced but not applied",
                    aTracker.getWriterTxn() < aTracker.getSeqTxn()
            );

            // Register a dependent over a. Both the create-time pre-check and the post-registration re-check
            // read a's still-policy-free metadata (the pending SET EXPIRE is not applied), so b is created.
            execute("create materialized view b as (select * from a)");
            Assert.assertNotNull("dependent view b must be created", engine.getTableTokenIfExists("b"));
            Assert.assertTrue(
                    "creating b must not apply a's pending SET EXPIRE",
                    aTracker.getWriterTxn() < aTracker.getSeqTxn()
            );

            // Apply the pending SET EXPIRE. The recompile's dependents check finds b and rejects; the
            // WAL-recoverable rejection makes the apply skip rather than suspend a.
            drainWalAndMatViewQueues();

            Assert.assertFalse(
                    "base must NOT be suspended by the apply-time dependents rejection",
                    engine.getTableSequencerAPI().isSuspended(aToken)
            );
            Assert.assertNull("the racing SET EXPIRE must be skipped, leaving a policy-free", expiryPredicate("a"));
            Assert.assertNotNull("dependent view b must survive the race", engine.getTableTokenIfExists("b"));

            // a still refreshes after the skipped ALTER: base -> a propagation works.
            execute("insert into base values ('A', 1.0, '2024-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQuery("select sym, v from a order by sym").expectSize().noLeakCheck().returns("sym\tv\nA\t1.0\n");

            // Once the dependent is dropped, a accepts an EXPIRE ROWS policy normally -- confirming the race
            // left a's ALTER path healthy (not suspended, not stuck) and the topology consistent.
            execute("drop materialized view b");
            drainWalAndMatViewQueues();
            execute("alter materialized view a set expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            Assert.assertFalse("a must stay unsuspended", engine.getTableSequencerAPI().isSuspended(aToken));
            Assert.assertNotNull("a must accept a policy once its dependent is gone", expiryPredicate("a"));
        });
    }

    @Test
    public void testFailedCleanupRetriesOnNextGlobalTick() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("CREATE TABLE base (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES (1.0, '2024-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("""
                    CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base)
                    EXPIRE ROWS WHEN v < 0 CLEANUP EVERY 1h""");
            drainWalAndMatViewQueues();
            engine.getMetadataCache().hydrateAllTables();

            final int[] attempts = {0};
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine) {
                @Override
                public boolean cleanupTable(TableToken tableToken, String predicate) {
                    if (attempts[0]++ == 0) {
                        throw new RuntimeException("injected cleanup failure");
                    }
                    return false;
                }
            }) {
                Assert.assertFalse(job.runNow());
                Assert.assertEquals(1, attempts[0]);
                setCurrentMicros(JAN_10 + 999_999L);
                Assert.assertFalse(job.runNow());
                Assert.assertEquals(1, attempts[0]);
                setCurrentMicros(JAN_10 + 1_000_000L);
                Assert.assertFalse(job.runNow());
                Assert.assertEquals(2, attempts[0]);
            }
        });
    }

    @Test
    public void testLookBackClockPredicateReclaimsPhysically() throws Exception {
        // The canonical retention predicate "ts < dateadd('d', -1, now())" is proven monotonic (a
        // look-back offset on a bare clock advances with the clock), so the cleanup job physically
        // reclaims under it: the fully-expired old partition is wiped, the partly-recent one and the
        // active one stay, and the visible set is unchanged.
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("""
                    insert into base values
                    ('OLD', 1.0, '2024-01-05T00:00:00.000000Z'),
                    ('MID', 2.0, '2024-01-09T12:00:00.000000Z'),
                    ('NEW', 3.0, '2024-01-10T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts < dateadd('d', -1, now())");
            drainWalAndMatViewQueues();

            // now()=Jan10, threshold=Jan09 00:00 -> OLD expired; MID and NEW visible.
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nMID\nNEW\n");
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')")
                    .noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t3\n");

            final boolean worked = runCleanup("mv");
            drainWalAndMatViewQueues();
            Assert.assertTrue("look-back clock policy must physically reclaim", worked);
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')")
                    .noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n2\t2\n");
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nMID\nNEW\n");
        });
    }

    @Test
    public void testCaseMismatchedClockThresholdReclaimsViaFastPath() throws Exception {
        // A micros designated timestamp whose EXPIRE predicate spells the column in a different case (TS vs
        // ts). The monotonicity classifier resolves the column case-insensitively and proves the policy
        // monotonic, so cleanup runs; the bounds fast path must resolve the operand the same way. If it does
        // not, the sweep falls to the survivor scan with the SKIP generation cache ON, and that content-keyed
        // cache remembers a still-live partition as SKIP and never reclaims it as now() advances past the
        // threshold. One job instance sweeps twice (the cache lives on the job) with the clock advanced
        // between, so a stalled second sweep is observable.
        assertMemoryLeak(() -> {
            setCurrentMicros(FEB_10);
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("""
                    insert into base values
                    ('OLD', 1.0, '2024-01-15T00:00:00.000000Z'),
                    ('NEW', 2.0, '2024-06-01T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            // The column is `ts`; the policy spells it `TS`.
            execute("create materialized view mv as (select * from base) expire rows when TS < dateadd('d', -30, now())");
            drainWalAndMatViewQueues();
            final TableToken token = engine.verifyTableName("mv");
            final String predicate = expiryPredicate("mv");
            Assert.assertEquals("TS < dateadd('d', -30, now())", predicate);
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                // Sweep 1 at 2024-02-10: 30-day threshold is 2024-01-11, so OLD (2024-01-15) is still fully
                // live -> classified SKIP by the bounds fast path.
                Assert.assertFalse("nothing expired at the first sweep", job.cleanupTable(token, predicate));
                drainWalAndMatViewQueues();
                assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");

                // Advance ~3 weeks: threshold 2024-01-31 now exceeds OLD's day, so the SECOND sweep of the
                // SAME job must reclaim OLD. A content-keyed SKIP cache would stall here.
                setCurrentMicros(MAR_01);
                Assert.assertTrue("OLD must reclaim once now() advances past the threshold",
                        job.cleanupTable(token, predicate));
                // The case-mismatched operand still takes the no-scan bounds fast path: a wholly-expired
                // partition is classified from its [floor, nextFloor) bounds, so no survivor scan runs.
                Assert.assertEquals("case-mismatched operand must take the no-scan bounds fast path",
                        0, job.getScalarPartitionScanCount());
            }
            drainWalAndMatViewQueues();
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n1\n");
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\n");
        });
    }

    @Test
    public void testNanosClockThresholdReclaimsAsClockAdvances() throws Exception {
        // A TIMESTAMP_NS designated column. expiryTimestampThresholdMicros returns LONG_NULL for any non-micros
        // timestamp, so a clock policy on an NS view always runs through the survivor scan with the SKIP
        // generation cache eligible. The cache is keyed only by partition content, so a still-live partition
        // cached as SKIP at one sweep must not suppress its reclamation once now() advances past the threshold.
        // One job instance sweeps twice (the cache lives on the job) with the clock advanced between.
        assertMemoryLeak(() -> {
            setCurrentMicros(FEB_10);
            execute("create table base (sym symbol, v double, ts timestamp_ns) timestamp(ts) partition by day wal");
            execute("""
                    insert into base values
                    ('OLD', 1.0, '2024-01-15T00:00:00.000000Z'),
                    ('NEW', 2.0, '2024-06-01T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts < dateadd('d', -30, now())");
            drainWalAndMatViewQueues();
            final TableToken token = engine.verifyTableName("mv");
            final String predicate = expiryPredicate("mv");
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
            // The NS-vs-micros clock comparison hides OLD only once now() advances: at 2024-02-10 both rows are
            // within the 30-day window and visible.
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\nOLD\n");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                // Sweep 1 at 2024-02-10: threshold 2024-01-11, so OLD (2024-01-15) is still fully live -> SKIP.
                Assert.assertFalse("nothing expired at the first sweep", job.cleanupTable(token, predicate));
                drainWalAndMatViewQueues();
                assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");

                // Advance ~3 weeks: threshold 2024-01-31 now exceeds OLD's day, so the SECOND sweep of the SAME
                // job must reclaim OLD. A content-keyed SKIP cache would stall here.
                setCurrentMicros(MAR_01);
                Assert.assertTrue("OLD must reclaim once now() advances past the threshold",
                        job.cleanupTable(token, predicate));
            }
            drainWalAndMatViewQueues();
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n1\n");
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\n");
        });
    }

    @Test
    public void testRepeatedCleanupFailuresBackOffExponentially() throws Exception {
        // A persistently failing cleanup must not re-run its full sweep on every 1-second global
        // tick: each failure doubles the per-table retry gap (1s, 2s, 4s, ... capped at 10 minutes),
        // and a successful or deferred sweep resets it.
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("CREATE TABLE base (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES (1.0, '2024-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("""
                    CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base)
                    EXPIRE ROWS WHEN v < 0 CLEANUP EVERY 1h""");
            drainWalAndMatViewQueues();
            engine.getMetadataCache().hydrateAllTables();

            final int[] attempts = {0};
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine) {
                @Override
                public boolean cleanupTable(TableToken tableToken, String predicate) {
                    attempts[0]++;
                    throw new RuntimeException("injected cleanup failure");
                }
            }) {
                job.runNow();                                // attempt 1 at T0 -> backoff 1s
                Assert.assertEquals(1, attempts[0]);

                setCurrentMicros(JAN_10 + 1_000_000L);       // T0+1s -> attempt 2 -> backoff 2s
                job.runNow();
                Assert.assertEquals(2, attempts[0]);

                setCurrentMicros(JAN_10 + 2_000_000L);       // 1s into the 2s gap -> no attempt
                job.runNow();
                Assert.assertEquals(2, attempts[0]);

                setCurrentMicros(JAN_10 + 3_000_000L);       // 2s gap elapsed -> attempt 3 -> backoff 4s
                job.runNow();
                Assert.assertEquals(3, attempts[0]);

                setCurrentMicros(JAN_10 + 6_999_999L);       // inside the 4s gap -> no attempt
                job.runNow();
                Assert.assertEquals(3, attempts[0]);

                setCurrentMicros(JAN_10 + 7_000_000L);       // 4s gap elapsed -> attempt 4
                job.runNow();
                Assert.assertEquals(4, attempts[0]);
            }
        });
    }

    @Test
    public void testOperationalKillSwitchControlsPoolOwnership() throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 1)) {
                setProperty(PropertyKey.CAIRO_ROW_EXPIRY_ENABLED, "false");
                Assert.assertFalse(RowExpiryCleanupJob.assignToPool(pool, engine));
                Assert.assertEquals(0, pool.getAssignedJobCount());
                Assert.assertEquals(0, pool.getFreeOnExitJobCount());

                setProperty(PropertyKey.CAIRO_ROW_EXPIRY_ENABLED, "true");
                Assert.assertTrue(RowExpiryCleanupJob.assignToPool(pool, engine));
                Assert.assertEquals(1, pool.getAssignedJobCount());
                Assert.assertEquals(1, pool.getFreeOnExitJobCount());
            }
        });
    }

    @Test
    public void testReadVsCleanupBoundaryAgreementForTsNowPredicate() throws Exception {
        // The read filter flips "ts < now()" to "ts >= now()" for pruning while cleanup classifies via the
        // bounds threshold (now() frozen per sweep). Both must agree at the boundary: a row at EXACTLY now()
        // is KEPT by the read filter (ts < now() is false) and must NOT be deleted by cleanup. The visible
        // set must be identical before and after a cleanup sweep.
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('OLD', 1.0, '2024-01-05T00:00:00.000000Z')," +   // ts < now()  -> expired (non-active, wiped)
                    "('EDGE', 2.0, '2024-01-10T00:00:00.000000Z')," +  // ts == now() -> kept (boundary, non-active)
                    "('NEW', 3.0, '2024-01-20T00:00:00.000000Z')");    // ts > now()  -> kept (active)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts < now()");
            drainWalAndMatViewQueues();

            final String visibleBefore = "sym\nEDGE\nNEW\n";
            assertQuery("select sym from mv order by sym").noLeakCheck().returns(visibleBefore);
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            final boolean worked = runCleanup("mv");
            drainWalAndMatViewQueues();
            Assert.assertTrue("ts < now() is monotonic and must reclaim the wholly-expired old partition", worked);

            // The expired OLD partition is reclaimed; the EDGE row exactly at now() and NEW row survive — the
            // post-cleanup visible set equals the pre-cleanup read-filtered set (no boundary divergence).
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
            assertQuery("select sym from mv order by sym").noLeakCheck().returns(visibleBefore);
        });
    }

    @Test
    public void testCleanupDefersWhileRefreshHoldsViewLock() throws Exception {
        // M5 serialization: cleanup and the mat-view refresh job are mutually exclusive per view via the
        // MatViewState lock — so a back-fill can never land between the survivor scan and the REPLACE_RANGE
        // commit. Holding that lock (as an in-progress refresh would) must make cleanup DEFER (no reclamation);
        // releasing it lets the next sweep reclaim normally.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," + // expired (non-active)
                    "('B', 2.0, '2024-01-02T00:00:00.000000Z')," + // expired (non-active)
                    "('C', 3.0, '2024-01-03T00:00:00.000000Z')");  // active partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts < '2024-01-02T00:00:00.000000Z'");
            drainWalAndMatViewQueues();
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            final TableToken token = engine.verifyTableName("mv");
            final io.questdb.cairo.mv.MatViewState state = engine.getMatViewStateStore().getViewState(token);
            Assert.assertNotNull("mat view must have a refresh state", state);

            // Simulate a refresh in progress by holding the per-view lock.
            Assert.assertTrue(state.tryLock());
            try {
                Assert.assertFalse("cleanup must defer while the view lock is held", runCleanup("mv"));
            } finally {
                state.unlock();
            }
            // Nothing reclaimed while deferred.
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            // Lock released: the next sweep reclaims the wholly-expired old partition.
            Assert.assertTrue("cleanup must reclaim once the lock is free", runCleanup("mv"));
            drainWalAndMatViewQueues();
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
        });
    }

    @Test
    public void testCleanupWithStalePredicateDuringUnappliedDropExpireSurvives() throws Exception {
        assertConcurrentPolicyChangeKeepsRows("alter materialized view mv drop expire", null);
    }

    @Test
    public void testCleanupWithStalePredicateDuringUnappliedSetExpireLooseningSurvives() throws Exception {
        // Loosen so every previously-expired row is kept by the new policy: the read filter keeps rows for
        // which the predicate is false, and ts < 2024-01-01 is false for all three rows (all >= 2024-01-01).
        assertConcurrentPolicyChangeKeepsRows(
                "alter materialized view mv set expire rows when ts < '2024-01-01T00:00:00.000000Z'",
                "ts < '2024-01-01T00:00:00.000000Z'"
        );
    }

    // M1 GATE deterministic pin (writer caught up + policy change lands MID-SWEEP). Since the seqTxn baseline
    // is now the reader's own applied txn (readerSeqTxn), even the committed-but-not-applied
    // testCleanupWithStalePredicate...Unapplied tests reach the bounds-DROP fast path (racyOpsAllowed is true
    // against that baseline) and defer at the per-commit gate -- so reverting the M1 one-liner already breaks
    // them. This test pins the same gate under the fully-applied mid-sweep race: the view is fully applied so
    // racyOpsAllowed == true and the bounds-DROP fast path IS entered, and an in-job barrier injects the
    // loosening ALTER exactly before the first destructive commit — advancing the sequencer past the sweep's
    // expectedSeqTxn while the writer was caught up. Only the M1 per-commit gate on the bounds-DROP fast path
    // can defer here; reverting that one-liner makes this test fail (the bounds-DROP would wipe OLD1/OLD2
    // against the stale strict predicate before the loosened policy applies).
    @Test
    public void testM1BoundsDropGateDefersOnMidSweepPolicyChange() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('OLD1', 1.0, '2024-01-05T00:00:00.000000Z')," + // ts < now() -> strict-expired, bounds-DROP eligible
                    "('OLD2', 2.0, '2024-01-08T00:00:00.000000Z')," + // ts < now() -> strict-expired, bounds-DROP eligible
                    "('NEW', 3.0, '2024-01-20T00:00:00.000000Z')");   // active partition
            drainWalAndMatViewQueues();
            // Strict "ts < now()" routes cleanup through the bounds-DROP fast path.
            execute("create materialized view mv as (select * from base) expire rows when ts < now()");
            drainWalAndMatViewQueues();

            // Under the strict policy both old partitions are expired; only NEW is visible.
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\n");

            final TableToken token = engine.verifyTableName("mv");
            final String stalePredicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                stalePredicate = m.getExpiryPredicate();
            }
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            // Writer caught up -> racyOpsAllowed == true at sweep start -> the bounds-DROP fast path is entered.
            Assert.assertEquals("precondition: view fully applied (writer caught up)",
                    tracker.getSeqTxn(), tracker.getWriterTxn());

            // Fire after cleanup has appended and synced its WAL event but before the conditional sequencer
            // allocation. The competing ALTER must take the next txn, reject cleanup's stale fence, and leave
            // no sequenced cleanup event behind.
            engine.getTableSequencerAPI().setTestNextTxnIfLastTxnBarrier(() -> {
                try {
                    execute("alter materialized view mv set expire rows when ts < '2024-01-01T00:00:00.000000Z'");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            final boolean reclaimed;
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                reclaimed = job.cleanupTable(token, stalePredicate);
            }
            Assert.assertFalse("conditional allocation must defer the bounds-DROP wipe", reclaimed);

            // Apply the loosened policy. Nothing was reclaimed; every row it keeps (all three -- ts < 2024-01-01 is
            // false for all) must survive. OLD1/OLD2 would be physically gone had the bounds-DROP committed.
            drainWalAndMatViewQueues();
            Assert.assertEquals("ts < '2024-01-01T00:00:00.000000Z'", expiryPredicate("mv"));
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\nOLD1\nOLD2\n");
        });
    }

    @Test
    public void testConditionalFenceDefersScanDropOnSetExpire() throws Exception {
        assertConditionalFenceDefersPolicyChange(
                false,
                "alter materialized view mv set expire rows when v < 0",
                "v < 0"
        );
    }

    @Test
    public void testConditionalFenceDefersSurvivorReplaceOnDropExpire() throws Exception {
        assertConditionalFenceDefersPolicyChange(true, "alter materialized view mv drop expire", null);
    }

    @Test
    public void testConditionalFenceRetriesAfterCompetingDataTxn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("""
                    insert into base values
                    ('OLD', 1.0, '2024-01-05T00:00:00.000000Z'),
                    ('NEW', 3.0, '2024-01-20T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("mv");
            final String predicate = expiryPredicate("mv");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            final long seqTxnBefore = tracker.getSeqTxn();
            engine.getTableSequencerAPI().setTestNextTxnIfLastTxnBarrier(() -> {
                try (WalWriter competingWriter = engine.getWalWriter(token)) {
                    final TableWriter.Row row = competingWriter.newRow(1_704_412_800_000_000L);
                    row.putSym(0, "KEEP");
                    row.putDouble(1, 3.0);
                    row.append();
                    competingWriter.commit();
                }
            });

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse("competing data txn must reject the cleanup fence", job.cleanupTable(token, predicate));
            }
            Assert.assertEquals("rejected cleanup must not consume a sequencer txn", seqTxnBefore + 1, tracker.getSeqTxn());

            drainWalAndMatViewQueues();
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertTrue("next sweep must retry successfully", job.cleanupTable(token, predicate));
            }
            drainWalAndMatViewQueues();
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nKEEP\nNEW\n");
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
        });
    }

    // C1 (mid-sweep predicate reconciliation, data-loss). A cleanup sweep snapshots (token, predicate) at
    // sweep-start discovery (runSerially) and opens its reader LATER. If an ALTER SET/DROP EXPIRE applied in
    // between and was FULLY applied (writer caught up), the seqTxn per-commit gate alone does NOT defer -- the
    // sequencer moved but the writer is caught up again, so racyOpsAllowed is true and getSeqTxn()==expectedSeqTxn
    // holds. cleanupTable0 must instead re-read the AUTHORITATIVE predicate from its reader and defer when it no
    // longer matches the stale discovery predicate. Reproduced deterministically: apply the loosening/DROP fully,
    // then drive cleanup with the OLD strict predicate exactly as a stale discovery snapshot would. Without the
    // reconciliation this wipes OLD1/OLD2 against the stale strict predicate.
    @Test
    public void testCleanupWithStalePredicateAfterAppliedSetExpireLooseningSurvives() throws Exception {
        assertCleanupWithStaleDiscoveryPredicateKeepsRows(
                "alter materialized view mv set expire rows when ts < '2024-01-01T00:00:00.000000Z'",
                "ts < '2024-01-01T00:00:00.000000Z'"
        );
    }

    @Test
    public void testCleanupWithStalePredicateAfterAppliedDropExpireSurvives() throws Exception {
        assertCleanupWithStaleDiscoveryPredicateKeepsRows("alter materialized view mv drop expire", null);
    }

    @Test
    public void testScalarCleanupSkipsUnchangedPartitionGenerations() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                        ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                        ('B', 2.0, '2024-01-02T00:00:00.000000Z'),
                        ('C', 3.0, '2024-01-03T00:00:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) EXPIRE ROWS WHEN v < 0");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("mv");
            final String predicate = expiryPredicate("mv");
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.cleanupTable(token, predicate));
                Assert.assertEquals(2, job.getScalarPartitionScanCount());

                Assert.assertFalse(job.cleanupTable(token, predicate));
                Assert.assertEquals("unchanged persisted partition generations skip rescans",
                        2, job.getScalarPartitionScanCount());

                execute("INSERT INTO base VALUES ('D', 4.0, '2024-01-01T12:00:00.000000Z')");
                drainWalAndMatViewQueues();
                Assert.assertFalse(job.cleanupTable(token, predicate));
                Assert.assertEquals("historical WAL-range replacement invalidates only the changed partition",
                        3, job.getScalarPartitionScanCount());
            }
        });
    }

    private void assertCleanupWithStaleDiscoveryPredicateKeepsRows(String policyChangeSql, String newPredicateOrNullForDrop) throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('OLD1', 1.0, '2024-01-05T00:00:00.000000Z')," + // ts < now() -> strict-expired under the STALE predicate
                    "('OLD2', 2.0, '2024-01-08T00:00:00.000000Z')," + // ts < now() -> strict-expired under the STALE predicate
                    "('NEW', 3.0, '2024-01-20T00:00:00.000000Z')");   // active partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts < now()");
            drainWalAndMatViewQueues();

            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\n");

            // The stale discovery predicate: the strict policy in force at (simulated) discovery time.
            final TableToken token = engine.verifyTableName("mv");
            final String stalePredicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                stalePredicate = m.getExpiryPredicate();
            }

            // Apply the policy change FULLY (writer caught up): a reader opened now reflects the NEW policy, so
            // the seqTxn gate alone would NOT defer -- only the authoritative-predicate re-read does.
            execute(policyChangeSql);
            drainWalAndMatViewQueues();
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            Assert.assertEquals("precondition: policy change fully applied (writer caught up)",
                    tracker.getSeqTxn(), tracker.getWriterTxn());

            // Drive cleanup with the STALE strict predicate, exactly as a discovery snapshot taken before the change.
            final boolean reclaimed;
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                reclaimed = job.cleanupTable(token, stalePredicate);
            }
            Assert.assertFalse("cleanup must defer when the authoritative predicate changed since the discovery snapshot", reclaimed);

            if (newPredicateOrNullForDrop == null) {
                Assert.assertNull("DROP EXPIRE cleared the policy", expiryPredicate("mv"));
            } else {
                Assert.assertEquals(newPredicateOrNullForDrop, expiryPredicate("mv"));
            }
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");
            if (newPredicateOrNullForDrop == null) {
                assertQuery("select sym from mv order by sym").expectSize().noLeakCheck().returns("sym\nNEW\nOLD1\nOLD2\n");
            } else {
                assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\nOLD1\nOLD2\n");
            }
        });
    }

    private void assertConditionalFenceDefersPolicyChange(
            boolean isReplace,
            String policyChangeSql,
            String expectedPredicate
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute(isReplace
                    ? "insert into base values " +
                      "('OLD', 1.0, '2024-01-05T00:00:00.000000Z')," +
                      "('KEEP', 3.0, '2024-01-05T01:00:00.000000Z')," +
                      "('NEW', 4.0, '2024-01-20T00:00:00.000000Z')"
                    : "insert into base values " +
                      "('OLD', 1.0, '2024-01-05T00:00:00.000000Z')," +
                      "('NEW', 4.0, '2024-01-20T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("mv");
            final String stalePredicate = expiryPredicate("mv");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            final long seqTxnBefore = tracker.getSeqTxn();
            engine.getTableSequencerAPI().setTestNextTxnIfLastTxnBarrier(() -> {
                try {
                    execute(policyChangeSql);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse("policy change must reject the cleanup fence", job.cleanupTable(token, stalePredicate));
            }
            Assert.assertEquals("rejected cleanup must not consume a sequencer txn", seqTxnBefore + 1, tracker.getSeqTxn());

            drainWalAndMatViewQueues();
            Assert.assertEquals(expectedPredicate, expiryPredicate("mv"));
            if (expectedPredicate == null) {
                assertQuery("select sym from mv order by sym").expectSize().noLeakCheck().returns("sym\nKEEP\nNEW\nOLD\n");
            } else {
                assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\nOLD\n");
            }
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
        });
    }

    // M1/M6 DETERMINISTIC data-loss guard (non-fuzz): a cleanup sweep that snapshotted a now-STALE (stricter)
    // predicate must not physically delete rows the CURRENT (loosened / dropped) policy keeps. TableWriter's
    // policy-change apply path does not take the MatViewState lock the sweep holds, so a policy change can
    // apply mid-sweep; the sequencer-txn gate on every destructive commit (incl. the bounds-DROP fast path,
    // see RowExpiryCleanupJob) is what prevents the wipe. As in testDeterministicBackfillBetweenScanAndCommit-
    // Survives, there is no in-job hook to pause between the survivor scan and the destructive commit, so we
    // reproduce the gate's precondition DETERMINISTICALLY: the policy change is left COMMITTED-BUT-NOT-APPLIED
    // (its sequencer txn is published but not applied, so writerTxn < seqTxn). With the reader-txn baseline the
    // bounds-DROP fast path is entered and the per-commit gate (getSeqTxn() != expectedSeqTxn) is what defers.
    // Cleanup with the stale strict predicate MUST defer (reclaim nothing); after the change is applied, every row the new policy keeps must
    // still be physically present. The instruction-level scan-vs-commit interleave with the writer caught up is
    // exercised probabilistically by MatViewRowExpiryFuzzTest.
    private void assertConcurrentPolicyChangeKeepsRows(String policyChangeSql, String loosenedPredicateOrNullForDrop) throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('OLD1', 1.0, '2024-01-05T00:00:00.000000Z')," + // ts < now() -> strict-expired (non-active, bounds-DROP eligible)
                    "('OLD2', 2.0, '2024-01-08T00:00:00.000000Z')," + // ts < now() -> strict-expired (non-active, bounds-DROP eligible)
                    "('NEW', 3.0, '2024-01-20T00:00:00.000000Z')");   // active partition
            drainWalAndMatViewQueues();
            // A scalar "ts < now()" strict policy routes cleanup through the bounds-DROP fast path.
            execute("create materialized view mv as (select * from base) expire rows when ts < now()");
            drainWalAndMatViewQueues();

            // Under the strict policy the two old partitions are expired; only NEW is visible.
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\n");
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            final TableToken token = engine.verifyTableName("mv");
            final String stalePredicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                stalePredicate = m.getExpiryPredicate();
            }
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            Assert.assertEquals("precondition: view fully applied", tracker.getSeqTxn(), tracker.getWriterTxn());

            // Apply the policy change to the view's sequencer but DO NOT apply it (no drain) -> writerTxn < seqTxn.
            execute(policyChangeSql);
            Assert.assertTrue(
                    "precondition: policy change must be committed-but-not-applied (writerTxn < seqTxn)",
                    tracker.getWriterTxn() < tracker.getSeqTxn()
            );

            // Cleanup with the STALE strict predicate must defer: the writer is not caught up, so no destructive
            // commit (bounds-DROP included) fires and nothing the loosened/dropped policy keeps can be wiped.
            final boolean reclaimed;
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                reclaimed = job.cleanupTable(token, stalePredicate);
            }
            Assert.assertFalse("cleanup must defer while a policy change is committed-but-not-applied", reclaimed);

            // Apply the policy change. No partition may have been reclaimed, and every row the new policy keeps
            // (all three under both the loosened threshold and DROP EXPIRE) must be present and visible.
            drainWalAndMatViewQueues();
            if (loosenedPredicateOrNullForDrop == null) {
                Assert.assertNull("DROP EXPIRE must clear the policy", expiryPredicate("mv"));
            } else {
                Assert.assertEquals(loosenedPredicateOrNullForDrop, expiryPredicate("mv"));
            }
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");
            if (loosenedPredicateOrNullForDrop == null) {
                assertQuery("select sym from mv order by sym").expectSize().noLeakCheck().returns("sym\nNEW\nOLD1\nOLD2\n");
            } else {
                assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\nOLD1\nOLD2\n");
            }
        });
    }

    private void assertMixedClockCleanupSkipped(String viewName, String predicate) throws Exception {
        execute("CREATE MATERIALIZED VIEW " + viewName + " AS (SELECT * FROM base) EXPIRE ROWS WHEN " + predicate);
        drainWalAndMatViewQueues();
        final TableToken token = engine.verifyTableName(viewName);
        final String storedPredicate = expiryPredicate(viewName);
        try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
            Assert.assertFalse("mixed clock/random threshold must skip physical cleanup", job.cleanupTable(token, storedPredicate));
        }
        drainWalAndMatViewQueues();
        assertQuery("SELECT count() p, sum(numRows) r FROM table_partitions('" + viewName + "')")
                .noRandomAccess()
                .expectSize()
                .noLeakCheck()
                .returns("p\tr\n3\t3\n");
    }

    private String expiryPredicate(String name) {
        final TableToken token = engine.verifyTableName(name);
        try (TableMetadata m = engine.getTableMetadata(token)) {
            return m.getExpiryPredicate();
        }
    }

    private boolean runCleanup(String name) {
        final TableToken token = engine.verifyTableName(name);
        final String predicate;
        try (TableMetadata m = engine.getTableMetadata(token)) {
            predicate = m.getExpiryPredicate();
        }
        try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
            return job.cleanupTable(token, predicate);
        }
    }
}
