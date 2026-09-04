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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Composite partitioning, Plan 4e Task 4 -- CAPSTONE: proves an EXPRESSION composite dimension
 * ({@code partition by day, (upper(region)) AS r}) behaves IDENTICALLY, across the SAME broad
 * lifecycle {@link CompositeEndToEndTest} (Plan 3b) and {@link CompositeRoutingEndToEndTest} (Plan 4a)
 * proved for IDENTITY/HASH/TRUNCATE, to an equivalent PRECOMPUTED-column plain table -- entirely via
 * SQL, never a white-box unit call into the eval bridge itself (that is {@link CompositeExpressionDimTest}'s
 * job; this class assumes Tasks 1-3 already proved the mechanism and instead proves the END-TO-END
 * SHAPE: multi-commit multi-cell routing, extend-existing-cell, checkpoint/snapshot restore, and the
 * composite feature gates -- all specifically for a KIND_EXPRESSION-dimensioned table).
 * <p>
 * Every test uses a single dimension, {@code (upper(region)) AS r}, deliberately mixing several raw
 * {@code region} spellings ({@code us}/{@code US}/{@code Us}) and ({@code eu}/{@code Eu}/{@code EU})
 * that all evaluate to the SAME two cells ({@code US}, {@code EU}) -- the "multi-value" collapse this
 * feature exists for, not just a single fixed spelling per cell. The twin plain table {@code p} always
 * carries a real, precomputed {@code r varchar} column populated with the identical {@code upper(region)}
 * value client-side (mirrors {@code CompositeExpressionDimTest}'s own established precomputed-twin
 * idiom) -- {@code c} has NO queryable {@code r} column of its own (the alias is a routing label, not a
 * materialized SQL column), so every {@code c}-side "per-r" query re-derives {@code upper(region)}.
 * <p>
 * <b>LATEST ON PARTITION BY r</b>: {@code LATEST ON}'s grammar only accepts a literal column reference
 * in {@code PARTITION BY} ({@code SqlParser#parseLatestByNew} calls {@code expectLiteral}), never a bare
 * expression -- confirmed by reading the parser directly, not assumed -- so {@code c} cannot write
 * {@code LATEST ON ts PARTITION BY upper(region)} at all. The equivalent proof against {@code c} instead
 * wraps it in a subquery that projects {@code upper(region)} as a real virtual column {@code r} first,
 * then applies {@code LATEST ON ts PARTITION BY r} to THAT subquery's result -- a documented, deliberate
 * proxy for the untypeable direct form, not a gap.
 * <p>
 * <b>Cross-cell ORDER BY (a real finding, NOT EXPRESSION-specific)</b>: this task's own
 * {@link #testExtendingExistingExpressionCellRoutesCorrectlyAndLeavesPriorCellsIntact()} first surfaced
 * that a BARE {@code order by ts} (no secondary sort key) on a composite table silently returns rows in
 * the WRONG order once two sibling cells on the SAME calendar day have genuinely interleaving timestamp
 * ranges (e.g. cell A has rows at 00:00 and 18:00, cell B has a row at 12:00 -- the correct global order
 * interleaves A, B, A, but a bare {@code order by ts} returned A, A, B instead, identical to the
 * unordered physical scan -- i.e. no real sort was applied at all). Verified via a throwaway negative
 * control (NOT part of this class, deleted before commit) that reproduced the IDENTICAL wrong order
 * against a plain {@code KIND_IDENTITY} composite dimension with no EXPRESSION involvement whatsoever --
 * this is a pre-existing, kind-agnostic composite read-path gap (most likely the query optimizer's
 * "designated timestamp column implies the cursor is already globally ts-ordered, skip adding an
 * explicit sort" fast-path, which holds for a plain table but not for a composite table's multi-cell
 * scan once cells interleave), not something Task 1-3 or this task introduced or should fix -- it is
 * out of scope for Plan 4e (an EXPRESSION-dimension feature task) to repair a generic composite
 * scan-ordering limitation that equally affects IDENTITY/HASH/TRUNCATE. Every affected assertion in
 * this class works around it the SAME way {@link CompositeRoutingEndToEndTest}/{@link
 * CompositeEndToEndTest} already do throughout (a real secondary sort key, {@code order by ts, region}
 * instead of bare {@code order by ts}) -- their consistent avoidance of a bare timestamp-only sort was,
 * in hindsight, very likely already an unwitting workaround for this exact gap, just never previously
 * confirmed or written down. Flagged prominently in this task's own report for maintainer follow-up;
 * NOT fixed here.
 * <p>
 * <b>ILP coverage</b>: not separately exercised here. Per Plan 4e's own design (option ii, see
 * {@code docs/superpowers/plans/2026-07-18-composite-partitioning-plan-4e-expression-dims.md}), the
 * {@code Function}-eval bridge lives inside {@code TableWriter}'s O3/WAL-apply commit path
 * ({@code resolveRowCellKey}/{@code resolveExpressionDimensionOrdinal}) -- the SAME apply-side path
 * every WAL commit funnels through regardless of which client protocol (PGWire SQL, ILP/line protocol,
 * HTTP) produced the WAL segment. SQL-INSERT is therefore not a special case; ILP coverage follows by
 * construction, not by a protocol-specific code path this class would need a separate test for.
 * <p>
 * <b>{@code TableReader#keyOfDimensionValue}</b>: its {@code KIND_EXPRESSION} case still throws
 * {@code UnsupportedOperationException} (a documented, deliberate Task 2/3 gap -- forward value-&gt;key
 * lookup, unreached by ordinary SQL since composite dimensions have no partition-pruning optimization
 * yet). The checkpoint round-trip test below therefore anchors its dense-key proof via
 * {@code TableReader#getCompositeDictionaries()}{@code .dictReaderFor(dimIndex)}{@code .keyOf(...)}
 * directly -- the real, already-working {@link io.questdb.cairo.SymbolMapReader} EXPRESSION shares with
 * TRUNCATE's dedicated-dict bucket (the exact same reader {@code valueOfDimensionKey}'s already-proven
 * reverse lookup uses) -- rather than routing through that still-gated wrapper method, which stays out
 * of scope for this task.
 */
public class CompositeExpressionEndToEndTest extends AbstractCairoTest {

    /**
     * Part A.1: several separate commits (each its own {@code insert} + {@code drainWalQueue}),
     * spanning three calendar days, where commit 1/2 are in-order (day2 then day3) and commit 3
     * backfills day1 -- a brand-new day EARLIER than every day committed so far, genuinely
     * out-of-order. Every cell touched by every commit is brand-new as of that commit (no extend --
     * that is {@link #testExtendingExistingExpressionCellRoutesCorrectlyAndLeavesPriorCellsIntact()}'s
     * job). Each day mixes a distinct raw {@code region} spelling into each of the two cells, so the
     * SAME two cells ({@code US}, {@code EU}) are reached via three different raw spellings apiece
     * across the whole test -- proving real per-row {@code upper()} evaluation collapses them, not
     * one physical cell per raw spelling (which would be 6 cells per day, 18 total, not 2/day, 6
     * total).
     */
    @Test
    public void testMultiDayMultiValueNewCellRoutingMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, region symbol, x double) timestamp(ts) " +
                    "partition by day, (upper(region)) AS r wal");
            execute("create table p (ts timestamp, region symbol, x double, r varchar) timestamp(ts) partition by day wal");

            // Commit 1: day2, two brand-new cells -- establishes a tail that is NOT the earliest day,
            // so commit 3's day1 backfill below is genuinely out-of-order.
            execute("insert into c values ('2020-01-02T00:00:00.000000Z','us',1.0), ('2020-01-02T12:00:00.000000Z','eu',1.5)");
            execute("insert into p values ('2020-01-02T00:00:00.000000Z','us',1.0,'US'), ('2020-01-02T12:00:00.000000Z','eu',1.5,'EU')");
            drainWalQueue();

            // Commit 2: day3, in-order (later than the current tail), two brand-new cells -- a
            // DIFFERENT raw casing than commit 1's, still collapsing to the SAME two cells.
            execute("insert into c values ('2020-01-03T00:00:00.000000Z','US',2.0), ('2020-01-03T12:00:00.000000Z','Eu',2.5)");
            execute("insert into p values ('2020-01-03T00:00:00.000000Z','US',2.0,'US'), ('2020-01-03T12:00:00.000000Z','Eu',2.5,'EU')");
            drainWalQueue();

            // Commit 3: day1, OUT-OF-ORDER -- a brand-new day earlier than every day committed so
            // far, two brand-new cells, a third raw casing.
            execute("insert into c values ('2020-01-01T00:00:00.000000Z','us',3.0), ('2020-01-01T12:00:00.000000Z','EU',3.5)");
            execute("insert into p values ('2020-01-01T00:00:00.000000Z','us',3.0,'US'), ('2020-01-01T12:00:00.000000Z','EU',3.5,'EU')");
            drainWalQueue();

            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            engine.releaseInactive(); // cold reopen -- no pooled reader/writer may mask a fresh self-detect

            // PHYSICAL: exactly 2 cell dirs (r=US, r=EU) per day -- not one per distinct raw spelling.
            TableToken tableToken = engine.verifyTableName("c");
            FilesFacade ff = configuration.getFilesFacade();
            for (String day : new String[]{"2020-01-01", "2020-01-02", "2020-01-03"}) {
                Assert.assertEquals(setOf("r=US", "r=EU"), listCellDirNames(ff, tableToken, day));
            }

            // LOGICAL: full ordered scan, table-wide count. Secondary sort key (region) alongside ts,
            // not bare "order by ts" -- see class javadoc's "cross-cell ORDER BY" note (a pre-existing,
            // kind-agnostic composite scan gap this task found). Not strictly triggered by THIS
            // dataset's per-day shape (each day contributes one row per cell here, so there is no
            // within-day interleaving to mis-order), but applied uniformly anyway: exact parity with
            // CompositeRoutingEndToEndTest/CompositeEndToEndTest's own established idiom (their
            // assertTablesMatch always sorts "ts, exch", never bare "ts"), and robust against a future
            // edit to this dataset that would introduce real interleaving.
            assertSqlCursors("select ts, region, x from p order by ts, region", "select ts, region, x from c order by ts, region");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n6\n");
            assertSqlCursors("select count() from p", "select count() from c");

            // Per-r filters (finer than the table-wide count(), catches a cross-cell swap): c
            // re-derives upper(region) over the real region column, p reads its real, precomputed r
            // column directly.
            assertSqlCursors(
                    "select ts, region, x from p where r = 'US' order by ts",
                    "select ts, region, x from c where upper(region) = 'US' order by ts");
            assertSqlCursors(
                    "select ts, region, x from p where r = 'EU' order by ts",
                    "select ts, region, x from c where upper(region) = 'EU' order by ts");
            assertQuery("select count() from c where upper(region) = 'US'").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            assertQuery("select count() from c where upper(region) = 'EU'").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");

            // LATEST ON ts PARTITION BY r: p reads its real r column directly; c derives an
            // equivalent virtual r column in a subquery first (see class javadoc -- LATEST ON's
            // PARTITION BY grammar only accepts a literal column, never a bare expression).
            assertSqlCursors(
                    "select ts, region, x from p latest on ts partition by r order by r",
                    "select ts, region, x from (select ts, region, x, upper(region) r from c) latest on ts partition by r order by r");

            // CATALOGUE: table_partitions() cell listing -- the upper-ed names -- across all 3 days.
            assertQuery("select count() from table_partitions('c')").noLeakCheck().noRandomAccess().expectSize().returns("count\n6\n");
            assertQuery("select name from table_partitions('c') order by name").noLeakCheck().expectSize().returns(
                    "name\n" +
                            "2020-01-01/r=EU\n" +
                            "2020-01-01/r=US\n" +
                            "2020-01-02/r=EU\n" +
                            "2020-01-02/r=US\n" +
                            "2020-01-03/r=EU\n" +
                            "2020-01-03/r=US\n");
        });
    }

    /**
     * Part A.2: commit 1 establishes two brand-new days with two brand-new cells each (all new,
     * succeeds). Commit 2 sends a single row, in order, that EXTENDS the already-populated day1/US
     * cell -- via a DIFFERENT raw {@code region} spelling ({@code US} uppercase, not commit 1's
     * {@code us}) that still evaluates to the SAME {@code r='US'} cell, forcing a fresh per-row eval +
     * an intern-HIT (not a fresh allocation) on the dedicated dict, followed by a real extend of the
     * already-populated physical partition. Reuses Plan 4b's general extend-existing-cell capability
     * (now unconditional for any dimension kind, per {@code deb53b12bd}'s commit message); this proves
     * it specifically for {@code KIND_EXPRESSION}, not just IDENTITY (which {@link
     * CompositeRoutingEndToEndTest#testExtendingExistingCellRoutesCorrectlyAndLeavesPriorCellsIntact()}
     * already covers).
     * <p>
     * {@code pBaseline} is a second plain table that only ever receives commit 1's rows -- a
     * historical record of commit 1's own exact content, used to prove {@code c}'s PRIOR cells (day2,
     * untouched by commit 2) remain intact alongside the newly-extended day1/US cell.
     */
    @Test
    public void testExtendingExistingExpressionCellRoutesCorrectlyAndLeavesPriorCellsIntact() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, region symbol, x double) timestamp(ts) " +
                    "partition by day, (upper(region)) AS r wal");
            execute("create table p (ts timestamp, region symbol, x double, r varchar) timestamp(ts) partition by day wal");
            execute("create table pBaseline (ts timestamp, region symbol, x double, r varchar) timestamp(ts) partition by day wal");

            execute("insert into c values " +
                    "('2020-01-01T00:00:00.000000Z','us',1.0), ('2020-01-01T12:00:00.000000Z','eu',1.5), " +
                    "('2020-01-02T00:00:00.000000Z','us',2.0), ('2020-01-02T12:00:00.000000Z','eu',2.5)");
            final String rowsP1 = " values " +
                    "('2020-01-01T00:00:00.000000Z','us',1.0,'US'), ('2020-01-01T12:00:00.000000Z','eu',1.5,'EU'), " +
                    "('2020-01-02T00:00:00.000000Z','us',2.0,'US'), ('2020-01-02T12:00:00.000000Z','eu',2.5,'EU')";
            execute("insert into p" + rowsP1);
            execute("insert into pBaseline" + rowsP1);
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // Commit 2: ONE row, in order, EXTENDING the already-populated day1/US cell.
            execute("insert into c values ('2020-01-01T18:00:00.000000Z','US',1.1)");
            execute("insert into p values ('2020-01-01T18:00:00.000000Z','US',1.1,'US')");
            drainWalQueue();

            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            engine.releaseInactive();

            assertQuery("select count() from p").noLeakCheck().noRandomAccess().expectSize().returns("count\n5\n");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n5\n");
            // A bare "order by ts" here is a REAL, verified gap, not a style choice -- see class
            // javadoc's "cross-cell ORDER BY" note. This exact test (day1 now has US=[00:00,18:00],
            // EU=[12:00] after the extend -- US's extended range genuinely straddles EU's single row)
            // is what surfaced it: a bare "order by ts" silently returned 00:00(US), 18:00(US),
            // 12:00(EU) instead of the correct 00:00(US), 12:00(EU), 18:00(US) -- confirmed NOT
            // EXPRESSION-specific via a negative control against a plain IDENTITY composite dimension
            // (same wrong order reproduces identically). The secondary sort key forces a real sort and
            // is the same proven workaround CompositeRoutingEndToEndTest/CompositeEndToEndTest already
            // use everywhere (their assertTablesMatch sorts "ts, exch", never bare "ts").
            assertSqlCursors("select ts, region, x from p order by ts, region", "select ts, region, x from c order by ts, region");

            // c's PRIOR, already-committed cells (commit 1) are unaffected by commit 2's extend --
            // day2 (untouched this commit) still matches pBaseline's own commit-1-only content exactly.
            assertSqlCursors(
                    "select ts, region, x from pBaseline where to_str(ts, 'yyyy-MM-dd') = '2020-01-02' order by ts, region",
                    "select ts, region, x from c where to_str(ts, 'yyyy-MM-dd') = '2020-01-02' order by ts, region");

            // Physical: still exactly 2 cell dirs per day -- the extend did NOT spawn a 3rd, duplicate
            // cell for day1.
            TableToken tableToken = engine.verifyTableName("c");
            FilesFacade ff = configuration.getFilesFacade();
            Assert.assertEquals(setOf("r=US", "r=EU"), listCellDirNames(ff, tableToken, "2020-01-01"));
            Assert.assertEquals(setOf("r=US", "r=EU"), listCellDirNames(ff, tableToken, "2020-01-02"));
        });
    }

    /**
     * Part A.3 (mirrors {@code CompositeEndToEndTest#testCheckpointRestoreRoutedCompositeTableRoundTrips}'s
     * own in-process create-then-recover idiom -- change the configured snapshot instance id between
     * create and recover so {@code engine.checkpointRecover()} treats this as restoring onto a
     * different install instead of no-op'ing). Two commits build a genuinely-routed EXPRESSION table
     * (multiple cells, spanning multiple days, one cell extended by the second commit) before the
     * checkpoint, so both the dedicated EXPRESSION dictionary and the per-cell column-versions are
     * non-trivial. After restore: every captured query surface must be byte-identical, the dedicated
     * dict's dense key for {@code 'US'} must be UNCHANGED (proof it was rebuilt from its preserved
     * {@code .c} file, not left torn or reallocated), and a fresh post-restore insert must route a
     * REUSED r-value into the SAME cell (not a duplicate) while a brand-new r-value gets a genuinely
     * new key -- Plan 4d's checkpoint/restore cell-awareness fix, and Task 2's dict rebuild, verified
     * together for {@code KIND_EXPRESSION} specifically (Plan 4d's own tests only covered
     * IDENTITY/TRUNCATE).
     * <p>
     * Deliberately NOT wrapped in {@code assertMemoryLeak} -- mirrors {@code CompositeEndToEndTest}'s
     * own checkpoint tests' documented reason (a small, constant, composite-unrelated allocation-tag
     * artifact of driving checkpoint recovery outside {@code io.questdb.test.griffin.CheckpointTest}'s
     * own fixture). {@code engine.clear()} at the top substitutes for what {@code assertMemoryLeak}
     * would have done on entry.
     */
    @Test
    public void testCheckpointRestoreExpressionDimensionRoundTrips() throws Exception {
        final String snapshotId = "00000000-0000-0000-0000-000000000000";
        final String restartedId = "123e4567-e89b-12d3-a456-426614174000";

        engine.clear();
        setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

        execute("create table c (ts timestamp, region symbol, x double) timestamp(ts) " +
                "partition by day, (upper(region)) AS r wal");

        // Commit 1: two brand-new days, two brand-new cells each, interleaved.
        execute("insert into c values " +
                "('2020-01-01T00:00:00.000000Z','us',1.0), ('2020-01-01T12:00:00.000000Z','eu',1.5), " +
                "('2020-01-02T00:00:00.000000Z','US',2.0), ('2020-01-02T12:00:00.000000Z','Eu',2.5)");
        // Commit 2: an in-order row extending the already-populated day2/US cell (non-trivial column
        // version), plus a brand-new day3/CA cell.
        execute("insert into c values " +
                "('2020-01-02T18:00:00.000000Z','us',2.75), ('2020-01-03T00:00:00.000000Z','ca',3.0)");
        drainWalQueue();
        Assert.assertFalse("c must not be suspended after routing setup",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));

        // Anchor: the dedicated dict's dense key for 'US' BEFORE the checkpoint (see class javadoc
        // for why this reads getCompositeDictionaries().dictReaderFor(0) directly instead of the
        // still-gated TableReader#keyOfDimensionValue wrapper).
        final int usKeyBefore;
        try (TableReader r = getReader("c")) {
            usKeyBefore = r.getCompositeDictionaries().dictReaderFor(0).keyOf("US");
            Assert.assertTrue("expected 'US' to already be interned pre-checkpoint", usKeyBefore >= 0);
        }

        // Capture every query surface, BEFORE the checkpoint. "order by ts, region" (not bare "order
        // by ts") -- day2 here has US=[00:00,18:00]/EU=[12:00], the same cross-cell-interleaving shape
        // that surfaces the pre-existing, kind-agnostic composite bare-ORDER-BY-ts scan gap documented
        // in the class javadoc; the secondary key is the same proven workaround used throughout.
        sink.clear();
        printSql("select ts, region, x from c order by ts, region");
        final String scanBefore = sink.toString();
        sink.clear();
        printSql("select upper(region) r, count() from c group by upper(region) order by r");
        final String countsBefore = sink.toString();
        sink.clear();
        printSql("select ts, region, x from (select ts, region, x, upper(region) r from c) latest on ts partition by r order by r");
        final String latestBefore = sink.toString();
        sink.clear();
        printSql("select name from table_partitions('c') order by name");
        final String partitionsBefore = sink.toString();

        execute("checkpoint create");

        // Insert MORE data after the checkpoint -- must NOT survive restore.
        execute("insert into c values ('2020-01-04T00:00:00.000000Z','de',4.0)");
        drainWalQueue();

        engine.clear();
        setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
        try {
            engine.checkpointRecover();

            sink.clear();
            printSql("select ts, region, x from c order by ts, region");
            TestUtils.assertEquals("full ordered scan", scanBefore, sink.toString());
            sink.clear();
            printSql("select upper(region) r, count() from c group by upper(region) order by r");
            TestUtils.assertEquals("per-r counts", countsBefore, sink.toString());
            sink.clear();
            printSql("select ts, region, x from (select ts, region, x, upper(region) r from c) latest on ts partition by r order by r");
            TestUtils.assertEquals("LATEST ON (via derived-column proxy)", latestBefore, sink.toString());
            sink.clear();
            printSql("select name from table_partitions('c') order by name");
            TestUtils.assertEquals("table_partitions() cell listing", partitionsBefore, sink.toString());

            // Dedicated dict intact: 'US' resolves to the SAME dense key post-restore.
            try (TableReader r = getReader("c")) {
                Assert.assertEquals("'US' must reuse its pre-checkpoint dedicated-dict key",
                        usKeyBefore, r.getCompositeDictionaries().dictReaderFor(0).keyOf("US"));
            }

            // A post-restore insert must route correctly: a REUSED r value (via a fresh raw casing,
            // 'Us') on a BRAND-NEW day reuses the existing cell (table_partitions() grows by exactly
            // 2 -- one new leaf partition per cell landing on the new day, not more/fewer), and a
            // brand-new r value gets the next, distinct key rather than colliding with 'US'.
            execute("insert into c values " +
                    "('2020-01-05T00:00:00.000000Z','Us',5.0), ('2020-01-05T06:00:00.000000Z','fr',5.5)");
            drainWalQueue();
            Assert.assertFalse("c must not be suspended after the post-restore insert",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));

            // Pre-checkpoint cells: (day1,US)(day1,EU)(day2,US)(day2,EU)(day3,CA) == 5. The
            // post-restore insert adds day5 for both US (reused cell) and FR (brand-new cell) == +2
            // leaf partitions. Row count: 6 pre-checkpoint (the post-checkpoint 'de' row must NOT
            // survive) + 2 post-restore == 8.
            assertQuery("select count() from table_partitions('c')")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n7\n");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n8\n");

            try (TableReader r = getReader("c")) {
                int usKeyAfterFreshInsert = r.getCompositeDictionaries().dictReaderFor(0).keyOf("US");
                Assert.assertEquals("repeated r='US' must still resolve to its original key after a fresh insert",
                        usKeyBefore, usKeyAfterFreshInsert);
                int frKey = r.getCompositeDictionaries().dictReaderFor(0).keyOf("FR");
                Assert.assertTrue("brand-new r='FR' must get a genuinely new key, distinct from 'US'",
                        frKey != usKeyBefore && frKey >= 0);
            }
        } finally {
            // checkpointRecover() does NOT itself clear DatabaseCheckpointAgent's in-progress flag --
            // even when it throws -- only checkpointRelease() does (mirrors CheckpointTest's own
            // class-wide @After net and CompositeEndToEndTest's identical checkpoint tests).
            engine.checkpointRelease();
            engine.releaseInactive();
            engine.clear();
        }
    }

    /**
     * Part A.4 (first half): the generic composite feature gates ({@code isRoutedComposite()} --
     * {@code dimCount > 0} and the {@code _cell} registry non-empty -- never inspects {@link
     * io.questdb.cairo.PartitionDimension#getKind()}) fire identically for a {@code KIND_EXPRESSION}-
     * dimensioned table as they do for IDENTITY (already covered by {@code
     * CompositeUnsupportedOpsTest#testDropColumnGated}) -- proven here directly for EXPRESSION, not
     * just assumed from the kind-agnostic predicate. DROP COLUMN is the representative op (mirrors
     * {@code CompositeUnsupportedOpsTest}'s own choice and {@code CompositeEndToEndTest}'s assertion
     * 8).
     */
    @Test
    public void testDropColumnGatedForExpressionTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, region symbol, x double) timestamp(ts) " +
                    "partition by day, (upper(region)) AS r wal");
            execute("insert into c values ('2020-01-01T00:00:00.000000Z','us',1.0), ('2020-01-01T12:00:00.000000Z','eu',1.5)");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // SP2 (2026-08-25): DROP COLUMN is supported on a composite table, including one whose
            // dimension is an EXPRESSION. x is not the expression's source column, so it drops cleanly.
            execute("alter table c drop column x");
            drainWalQueue();
            Assert.assertFalse(
                    "c must not be suspended after DROP COLUMN",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
            try (io.questdb.cairo.TableReader reader = getReader("c")) {
                Assert.assertEquals(-1, reader.getMetadata().getColumnIndexQuiet("x"));
            }
        });
    }

    /**
     * Part A.4 (second half): the remaining bounded gap Task 2 left (an EXPRESSION dimension
     * referencing a var-size VARCHAR/STRING/BINARY source column) must still fail CLEAN -- a
     * diagnosable {@code CairoException}, never an uncontrolled {@code ArrayIndexOutOfBoundsException}
     * -- for this capstone's own self-contained proof (duplicates {@code CompositeExpressionDimTest
     * #testInsertOnVarSizeSourceColumnThrowsCleanErrorNotAioobe} deliberately, per the brief's explicit
     * ask that the capstone itself assert this, not just rely on Task 2's own test class).
     */
    @Test
    public void testVarSizeSourceColumnExpressionStillRejectedCleanly() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, region symbol, note varchar, x double) timestamp(ts) " +
                    "partition by day, (upper(note)) AS r wal");
            execute("insert into c values ('2020-01-01T00:00:00.000000Z', 'us', 'hello', 1.0)");
            drainWalQueue();

            Assert.assertTrue(
                    "table must be suspended by the clean var-size-source-column guard, not crash unnoticed",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
            assertQuery(
                    "select suspended, " +
                            "errorMessage like '%composite partitioning does not yet support an EXPRESSION dimension referencing column%' clearMessage, " +
                            "errorMessage like '%ArrayIndexOutOfBoundsException%' isAioobe " +
                            "from wal_tables() where name = 'c'"
            )
                    .noLeakCheck().noRandomAccess()
                    .returns("suspended\tclearMessage\tisAioobe\ntrue\ttrue\tfalse\n");
        });
    }

    private void assertWalTableNotSuspended(String tableName) {
        Assert.assertFalse(
                tableName + " must not be suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
    }

    private static Set<String> setOf(String... values) {
        Set<String> set = new HashSet<>();
        for (String v : values) {
            set.add(v);
        }
        return set;
    }

    /**
     * Lists the immediate child directory names of {@code <dbRoot>/<tableToken>/<dayDirName>},
     * stripping each entry's trailing {@code .<nameTxn>} version suffix (e.g. {@code "r=US.3"} ->
     * {@code "r=US"}) so the result is comparable regardless of the exact nameTxn a real commit
     * happened to assign. Lifted from {@code CompositeExpressionDimTest}/{@code
     * CompositeRoutingEndToEndTest}'s identically-named helper (itself mirroring {@code
     * ShowPartitionsRecordCursorFactory#scanDetachedAndAttachablePartitions}'s own {@code
     * ff.findFirst/findName/findType/findNext/findClose} idiom) rather than widening those classes'
     * visibility, per this codebase's own established precedent for this need.
     */
    private static Set<String> listCellDirNames(FilesFacade ff, TableToken tableToken, String dayDirName) {
        Set<String> names = new HashSet<>();
        try (Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(dayDirName).$();
            long pFind = ff.findFirst(path.$());
            Assert.assertTrue("expected day directory to exist: " + path, pFind > 0L);
            try {
                StringSink nameSink = new StringSink();
                do {
                    nameSink.clear();
                    long name = ff.findName(pFind);
                    Utf8s.utf8ToUtf16Z(name, nameSink);
                    int type = ff.findType(pFind);
                    if (type == Files.DT_DIR && !Chars.equals(nameSink, ".") && !Chars.equals(nameSink, "..")) {
                        String entry = nameSink.toString();
                        int dot = entry.lastIndexOf('.');
                        names.add(dot > -1 ? entry.substring(0, dot) : entry);
                    }
                } while (ff.findNext(pFind) > 0);
            } finally {
                ff.findClose(pFind);
            }
        }
        return names;
    }
}
