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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Plan 4a Task 6 -- CAPSTONE: a consolidated end-to-end + crash-safety proof for the current
 * live-routing capability that Tasks 1-5 built (see those tasks' own reports, especially Task 5's
 * "Exactly which sequences route correctly vs. still throw" section). This class deliberately does
 * NOT assert beyond that established boundary:
 * <ul>
 *     <li>a commit routes correctly when every {@code (day, cellKey)} it touches is BRAND NEW as of
 *     that commit -- a new day, a new cell on an existing (current-tail or older, already-closed) day,
 *     or an out-of-order commit into a brand-new, chronologically-earlier day, in any mixture;</li>
 *     <li>a commit that EXTENDS an already-populated cell throws a loud, documented {@code
 *     CairoException} (the table suspends) rather than silently misrouting or corrupting -- a real,
 *     unresolved hazard Task 5 guarded rather than fixed (see its report §4/§6).</li>
 * </ul>
 * <p>
 * Distinct from {@link CompositeEndToEndTest} (Plan 3b's capstone for the DORMANT write path, where
 * every row lands at cellKey 0 -- no real per-cell routing existed yet): this class is the capstone
 * for Plan 4a's real {@code (ts, cellKey)} physical routing.
 * <p>
 * Reuses the exact helpers and idioms {@link CompositeRoutingTest} and {@link
 * CompositeDictPersistenceTest} already established: the composite-vs-plain-twin comparison
 * ({@link #assertTablesMatch}), the physical cell-directory listing ({@link #listCellDirNames}), the
 * multi-commit {@code insert}/{@code drainWalQueue} idiom, the suspended-table assertions, and the
 * "isolated interner call, closed without a row/commit" crash-safety idiom (mirrored, not literally
 * shared, since the originals are {@code private} to their own test classes -- lifted here rather than
 * widening those classes' visibility, per this task's own brief).
 */
public class CompositeRoutingEndToEndTest extends AbstractCairoTest {

    /**
     * Group 1 (multi-day, multi-exchange new-cell routing). Five separate commits (each its own
     * {@code insert} + {@code drainWalQueue}), spanning four calendar days and three exchanges, where
     * every commit's every cell is brand new as of that commit -- exactly the capability Task 5
     * proved, exercised here in combination rather than in isolation:
     * <ol>
     *     <li>commit 1: day2, two new cells (A, B) -- establishes a tail that is NOT the earliest day,
     *     so a later commit backfilling an earlier day is genuinely out-of-order;</li>
     *     <li>commit 2: day3, two new cells (A, B) -- in-order (later than the current tail);</li>
     *     <li>commit 3: day1, two new cells (A, B) -- OUT-OF-ORDER: a brand-new day earlier than every
     *     day committed so far;</li>
     *     <li>commit 4: day2 gains a THIRD, brand-new cell (C) -- a new cell on an existing,
     *     already-closed day that already has two other cells;</li>
     *     <li>commit 5: day0, one new cell (A) -- OUT-OF-ORDER again, a brand-new day earlier than
     *     every other day, with only a single cell (varying cell-count-per-day too).</li>
     * </ol>
     * Every row lands in a distinct cell (8 rows, 8 cells), so cell-count and row-count assertions
     * double-check each other without being redundant with the plain twin's own (day-only, 4-partition)
     * shape.
     */
    @Test
    public void testMultiCommitMultiDayMultiExchNewCellRoutingMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            insertIntoBothAndDrain("('2020-01-02T00:00:00.000000Z','A',1.0), ('2020-01-02T12:00:00.000000Z','B',1.5)");
            insertIntoBothAndDrain("('2020-01-03T00:00:00.000000Z','A',2.0), ('2020-01-03T12:00:00.000000Z','B',2.5)");
            insertIntoBothAndDrain("('2020-01-01T00:00:00.000000Z','A',3.0), ('2020-01-01T12:00:00.000000Z','B',3.5)");
            insertIntoBothAndDrain("('2020-01-02T18:00:00.000000Z','C',4.0)");
            insertIntoBothAndDrain("('2019-12-31T00:00:00.000000Z','A',5.0)");

            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            engine.releaseInactive(); // cold reopen -- no pooled reader/writer may mask a fresh self-detect

            // PHYSICAL: exact cell directories per day -- no dormant leftover, no cross-day bleed.
            TableToken tableToken = engine.verifyTableName("c");
            FilesFacade ff = configuration.getFilesFacade();
            Assert.assertEquals(setOf("exch=A"), listCellDirNames(ff, tableToken, "2019-12-31"));
            Assert.assertEquals(setOf("exch=A", "exch=B"), listCellDirNames(ff, tableToken, "2020-01-01"));
            Assert.assertEquals(setOf("exch=A", "exch=B", "exch=C"), listCellDirNames(ff, tableToken, "2020-01-02"));
            Assert.assertEquals(setOf("exch=A", "exch=B"), listCellDirNames(ff, tableToken, "2020-01-03"));

            // LOGICAL: full ordered scan, count(), per-exch group-by count(), and LATEST ON parity.
            assertTablesMatch("c", "p");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n8\n");

            // Per-exchange filters (finer than the table-wide count(), catches a same-exch cross-day swap).
            for (String exch : new String[]{"A", "B", "C"}) {
                assertSqlCursors(
                        "select ts, exch, px from p where exch = '" + exch + "' order by ts",
                        "select ts, exch, px from c where exch = '" + exch + "' order by ts");
            }
            assertQuery("select count() from c where exch = 'A'").noLeakCheck().noRandomAccess().expectSize().returns("count\n4\n");
            assertQuery("select count() from c where exch = 'B'").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            assertQuery("select count() from c where exch = 'C'").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");

            // Per-(day,exch) count parity -- finer than a table-wide count(), catches a same-count swap
            // between two cells.
            assertPerDayExchCountsMatch(
                    new String[]{"2019-12-31", "2020-01-01", "2020-01-02", "2020-01-03"},
                    new String[]{"A", "B", "C"});

            // CATALOGUE: table_partitions() lists one row per CELL (8, not the plain twin's 4 days),
            // rendered cell-aware (Plan 4a Task 5b) -- the direct proof every commit's cells are real,
            // distinct, on-disk (ts, cellKey) records.
            assertQuery("select count() from table_partitions('c')").noLeakCheck().noRandomAccess().expectSize().returns("count\n8\n");
            assertQuery("select name from table_partitions('c') order by name").noLeakCheck().expectSize().returns(
                    "name\n" +
                            "2019-12-31/exch=A\n" +
                            "2020-01-01/exch=A\n" +
                            "2020-01-01/exch=B\n" +
                            "2020-01-02/exch=A\n" +
                            "2020-01-02/exch=B\n" +
                            "2020-01-02/exch=C\n" +
                            "2020-01-03/exch=A\n" +
                            "2020-01-03/exch=B\n");
        });
    }

    /**
     * Group 2 (guarded extend is loud, not silent -- positive boundary assertion). Commit 1 establishes
     * two brand-new days with two brand-new cells each (all new, succeeds). Commit 2 sends a single row
     * that EXTENDS the already-populated day1/A cell -- the documented, guarded shape (Task 5 report
     * §4/§6: a real, unresolved native-heap-corruption hazard, guarded rather than fixed). This must
     * throw the clear {@code CairoException} (table suspends), and -- the assertion Task 6 adds beyond
     * Task 5's own regression tests -- both the plain twin AND {@code c}'s own PRIOR, already-committed
     * cells (from commit 1, before the guard fired) must be completely unaffected: no silent partial
     * application of the rejected commit, no corruption of what was already durably routed.
     * <p>
     * {@code pBaseline} is a second plain table that only ever receives commit 1's rows -- the oracle
     * for {@code c}'s expected POST-guard state, since {@code p} itself goes on to receive commit 2 too
     * (to prove commit 2 succeeds fully and normally on a plain table).
     */
    @Test
    public void testGuardedExtendThrowsLoudlyAndLeavesPriorCellsAndPlainTwinUnaffected() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");
            execute("create table pBaseline (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            final String rows1 = " values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5), " +
                    "('2020-01-02T00:00:00.000000Z','A',2.0), ('2020-01-02T12:00:00.000000Z','B',2.5)";
            execute("insert into c" + rows1);
            execute("insert into p" + rows1);
            execute("insert into pBaseline" + rows1);
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // Commit 2: ONE row that EXTENDS the already-populated day1/A cell -- the guarded shape.
            final String rows2 = " values ('2020-01-01T06:00:00.000000Z','A',1.1)";
            execute("insert into c" + rows2);
            execute("insert into p" + rows2);
            drainWalQueue();

            assertWalTableSuspendedWithMessage("c", "does not yet support a commit that extends an already-populated cell");
            engine.releaseInactive();

            // p (plain) is completely unaffected by c's suspension: commit 2 applied fully and normally.
            assertWalTableNotSuspended("p");
            assertQuery("select count() from p").noLeakCheck().noRandomAccess().expectSize().returns("count\n5\n");

            // c's PRIOR, already-committed cells (commit 1, before the guard fired) are unaffected: no
            // silent misroute, no partial/corrupt application of the rejected commit 2 -- exactly
            // pBaseline's rows (== commit 1's own), byte for byte, still visible.
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n4\n");
            assertSqlCursors("select ts, exch, px from pBaseline order by ts, exch", "select ts, exch, px from c order by ts, exch");
            assertSqlCursors("select exch, count() from pBaseline order by exch", "select exch, count() from c order by exch");

            // Physical cell directories for the prior, successfully-committed cells are also untouched.
            TableToken tableToken = engine.verifyTableName("c");
            FilesFacade ff = configuration.getFilesFacade();
            Assert.assertEquals(setOf("exch=A", "exch=B"), listCellDirNames(ff, tableToken, "2020-01-01"));
            Assert.assertEquals(setOf("exch=A", "exch=B"), listCellDirNames(ff, tableToken, "2020-01-02"));
        });
    }

    /**
     * Group 3 (crash-safety: uncommitted routing discarded). Establishes a REAL per-cell-routed
     * baseline via actual ingestion (two committed cells), then mirrors {@link
     * CompositeDictPersistenceTest#testUncommittedInternsDiscardedOnReopen}'s exact idiom on top of
     * that baseline: an ISOLATED {@code cellRegistry().internCell(...)} call for a brand-new,
     * never-before-seen ordinal tuple, with NO row appended, so {@link TableWriter#inTransaction()}
     * stays false and {@code commit()} (implicit on close) takes its no-op short-circuit -- the
     * registry's on-disk symbol-map files may already carry the interned tuple, but the durable
     * {@code _txn} count for that slot is never bumped.
     * <p>
     * This proves the crash-safety guarantee holds layered on top of REAL prior routing (not just a
     * bare freshly-created table, which is all the original test covers): a reopen must see only the
     * two committed cells, both in the registry count and in every query-visible surface (row count,
     * {@code table_partitions()}, and the physical cell directories) -- the uncommitted third cell must
     * be invisible everywhere, not just absent from one bookkeeping counter.
     */
    @Test
    public void testUncommittedRoutingDiscardedOnReopenOverRealBaseline() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");

            // Baseline: real per-cell routing via actual ingestion -- two committed cells (A, B).
            execute("insert into c values ('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            engine.releaseInactive();

            final int committedCellCount;
            try (TableReader r = getReader("c")) {
                committedCellCount = r.getCompositeDictionaries().cellRegistry().size();
            }
            Assert.assertEquals(2, committedCellCount);

            // Isolated intern of a brand-new, never-before-seen ordinal tuple -- NO row appended, so
            // inTransaction() stays false and the implicit commit()-on-close is a no-op. Mirrors
            // CompositeDictPersistenceTest#testUncommittedInternsDiscardedOnReopen, layered on the real
            // baseline established above instead of a bare freshly-created table.
            try (TableWriter w = getWriter("c")) {
                int uncommittedCellKey = w.getCompositeDictionaries().cellRegistry().internCell(new int[]{9999}, 1);
                Assert.assertEquals(
                        "the isolated intern must allocate a brand-new dense slot right after the baseline",
                        committedCellCount, uncommittedCellKey);
                // writer closed here WITHOUT appending a row and WITHOUT an explicit commit().
            }

            engine.releaseInactive(); // force a fresh reopen -- no pooled reader may mask stale state

            try (TableReader r = getReader("c")) {
                Assert.assertEquals(
                        "uncommitted cell intern must be discarded on reopen -- registry must reflect only the committed baseline",
                        committedCellCount, r.getCompositeDictionaries().cellRegistry().size());
            }

            // The VISIBLE cells (query-level) also reflect only the committed baseline -- no phantom 3rd
            // partition, no phantom row.
            assertQuery("select count() from table_partitions('c')").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");

            // Physical directories also unaffected -- exactly the 2 committed cell directories.
            TableToken tableToken = engine.verifyTableName("c");
            FilesFacade ff = configuration.getFilesFacade();
            Assert.assertEquals(setOf("exch=A", "exch=B"), listCellDirNames(ff, tableToken, "2020-01-01"));
        });
    }

    /**
     * Group 4 (reopen routing). Distinct from Group 3: this proves a genuinely FRESH writer instance
     * (forced open after {@code engine.releaseInactive()} released every pooled reader/writer) can go on
     * to route a brand-new cell correctly via the real WAL-apply/O3 dispatch path -- i.e. that the
     * registry, dimension dictionaries, and per-cell frontiers are correctly rehydrated from disk on
     * cold open, not merely that they persist (Task 5/{@link CompositeDictPersistenceTest} already prove
     * persistence in isolation). Uses the normal {@code insert}/{@code drainWalQueue} path (not a raw
     * {@code TableWriter} grabbed directly) specifically because that is what forces a real production
     * WAL-apply cycle to open a brand-new pooled {@code TableWriter} instance after the release, exactly
     * mirroring how a real restart would resume ingestion.
     */
    @Test
    public void testReopenedWriterRoutesNewCellCorrectlyAndReaderSeesAllCells() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            final String rows1 = " values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5), " +
                    "('2020-01-02T00:00:00.000000Z','A',2.0), ('2020-01-02T12:00:00.000000Z','B',2.5)";
            execute("insert into c" + rows1);
            execute("insert into p" + rows1);
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // Force a full cold release -- the NEXT writer must rehydrate entirely from disk (registry,
            // dictionaries, per-cell frontiers), not reuse any in-memory pooled state.
            engine.releaseInactive();

            // Reopen: insert rows for a brand-new cell (C) on a brand-new day (day3).
            final String rows2 = " values ('2020-01-03T00:00:00.000000Z','C',3.0)";
            execute("insert into c" + rows2);
            execute("insert into p" + rows2);
            drainWalQueue();

            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            engine.releaseInactive();

            // The reopened reader sees ALL prior cells + the new one -- full logical parity with the
            // plain twin (5 rows: the 4 pre-reopen + the 1 new one).
            assertTablesMatch("c", "p");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n5\n");

            // The new cell routed to its own, correct physical directory under the new day -- not merged
            // into / colliding with any prior cell.
            TableToken tableToken = engine.verifyTableName("c");
            FilesFacade ff = configuration.getFilesFacade();
            Assert.assertEquals(setOf("exch=A", "exch=B"), listCellDirNames(ff, tableToken, "2020-01-01"));
            Assert.assertEquals(setOf("exch=A", "exch=B"), listCellDirNames(ff, tableToken, "2020-01-02"));
            Assert.assertEquals(setOf("exch=C"), listCellDirNames(ff, tableToken, "2020-01-03"));

            // table_partitions() reflects all 5 cells (4 prior + 1 new), cell-aware, listed explicitly
            // (not just an aggregate count) so a prior-cell loss across the reopen would surface here.
            assertQuery("select name from table_partitions('c') order by name").noLeakCheck().expectSize().returns(
                    "name\n" +
                            "2020-01-01/exch=A\n" +
                            "2020-01-01/exch=B\n" +
                            "2020-01-02/exch=A\n" +
                            "2020-01-02/exch=B\n" +
                            "2020-01-03/exch=C\n");

            // A fresh reader (post cold-reopen) confirms the registry's total size. Note this counts
            // DISTINCT dimension-ordinal tuples (here, distinct exch VALUES: A, B, C -- 3), not
            // distinct (day, cellKey) attached-partition entries (5, asserted via table_partitions()
            // above) -- A and B are each reused across two days, so the registry does not grow per day.
            try (TableReader r = getReader("c")) {
                Assert.assertEquals(3, r.getCompositeDictionaries().cellRegistry().size());
            }
        });
    }

    private void assertWalTableNotSuspended(String tableName) {
        Assert.assertFalse(
                tableName + " must not be suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
    }

    private void assertWalTableSuspendedWithMessage(String tableName, String expectedMessageSubstring) throws Exception {
        Assert.assertTrue(
                tableName + " must be suspended after the not-yet-supported commit",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
        assertQuery("select suspended, errorMessage like '%" + expectedMessageSubstring + "%' clearMessage " +
                "from wal_tables() where name = '" + tableName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("suspended\tclearMessage\ntrue\ttrue\n");
    }

    /**
     * Per-(day, exch) count parity between {@code c} and {@code p}, for every combination of the given
     * ISO day strings crossed with the given exchange values -- finer than a table-wide {@code count()}
     * (which could still coincidentally match even if two cells' rows were swapped between each other).
     * A (day, exch) combination with zero rows on both sides trivially passes (0 == 0), so callers may
     * safely pass the full exchange set for every day without first checking which exchanges actually
     * landed on which day.
     * <p>
     * Deliberately filters on {@code to_str(ts, 'yyyy-MM-dd')} rather than a {@code ts >= day and ts <
     * day+1} range: the latter is recognized by the SQL optimiser as a prunable interval and hits a
     * pre-existing, out-of-scope bug in composite-table interval/partition-frame scanning (documented in
     * {@code CompositeRoutingTest}'s own identical helper and Task 5's report §6) that silently returns
     * zero rows for a day whose cell(s) were not the table's most-recently-appended partition. {@code
     * to_str(...)} is an opaque per-row function the optimiser cannot fold into an interval, so it falls
     * back to a plain filtered scan, which reads composite data correctly.
     */
    private void assertPerDayExchCountsMatch(String[] isoDays, String[] exchanges) throws SqlException {
        for (String day : isoDays) {
            for (String exch : exchanges) {
                String predicate = " where to_str(ts, 'yyyy-MM-dd') = '" + day + "' and exch = '" + exch + "'";
                assertSqlCursors("select count() from p" + predicate, "select count() from c" + predicate);
            }
        }
    }

    /**
     * Full-table parity between {@code c} and {@code p}: ordered scan, table-wide count, per-exchange
     * count, and {@code LATEST ON} -- mirrors {@code CompositeRoutingTest}'s identically-named helper.
     */
    private void assertTablesMatch(String composite, String plain) throws SqlException {
        assertSqlCursors("select ts, exch, px from " + plain + " order by ts, exch", "select ts, exch, px from " + composite + " order by ts, exch");
        assertSqlCursors("select count() from " + plain, "select count() from " + composite);
        assertSqlCursors("select exch, count() from " + plain + " order by exch", "select exch, count() from " + composite + " order by exch");
        assertSqlCursors(
                "select ts, exch, px from " + plain + " latest on ts partition by exch order by exch",
                "select ts, exch, px from " + composite + " latest on ts partition by exch order by exch");
    }

    private void insertIntoBothAndDrain(String valuesTuples) throws SqlException {
        execute("insert into c values " + valuesTuples);
        execute("insert into p values " + valuesTuples);
        drainWalQueue();
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
     * stripping each entry's trailing {@code .<nameTxn>} version suffix (e.g. {@code "exch=A.3"} ->
     * {@code "exch=A"}) so the result is comparable regardless of the exact nameTxn a real commit
     * happened to assign. Mirrors {@code CompositeRoutingTest}'s identically-named helper (itself
     * mirroring {@code ShowPartitionsRecordCursorFactory#scanDetachedAndAttachablePartitions}'s own
     * {@code ff.findFirst/findName/findType/findNext/findClose} idiom).
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
