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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.test.tools.TestUtils;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Sub-project 2 (column DDL) SURVEY — which of these already work on a composite table once their
 * gate is lifted, and which genuinely need cell-aware work?
 * <p>
 * This is the same measurement-first step that paid for itself three times today. In 1A it showed the
 * O3 leak was a WRITER defect and the purge job was innocent, sending the fix to a different file. In
 * 1B a probe showed an ungated cell-qualified DROP destroyed a whole day, turning "lift the gate" into
 * "narrow the gate". In 1D it FALSIFIED the plan's own hypothesis: TTL and FORCE DROP had NOT
 * inherited the shared fix, and lifting their gates would have made them silent no-ops.
 * <p>
 * The point is to learn which of the eight column-DDL gates are cheap and which are not, BEFORE
 * writing a plan that assumes either. Each test is a twin comparison: whatever the operation does to a
 * composite table it must do to its plain twin.
 * <p>
 * All {@code @Ignore}d — they are run by temporarily lifting the writer-side gates, exactly as 1D
 * Task 1 was run, and the findings recorded. They are NOT a claim that any of this works.
 * <p>
 * <b>This survey produced THREE successive false positives, each caught only by checking a different
 * observable than the last: rows, then structure flags, then on-disk files. Its first summary said
 * "3 of 5 pass". The true answer is 0 of 5.</b> A twin DATA comparison cannot verify a STRUCTURE
 * change, and neither can verify what is left on disk.
 * <p> {@code surveyAddIndex} passed — and a follow-up probe showed {@code isColumnIndexed} was
 * FALSE afterwards: {@code ADD INDEX} reported success and created no index. The twin comparison
 * could not see it, because an index changes no query RESULT; and the indexed-{@code WHERE} check was
 * accepted rather than refused by the indexed-predicate gate for the same reason — there was no index
 * to refuse over. A silent no-op passing as success is precisely the failure the cardinal rule
 * forbids, and lifting the gate on that evidence would have shipped it.
 * <p>
 * So every structure-changing survey below must assert the STRUCTURE ({@code isColumnIndexed}, the
 * column type, the on-disk files) and not only the rows.
 */
public class CompositeColumnDdlSurveyTest extends AbstractCompositeTwinTest {

    /**
     * ADD COLUMN of type SYMBOL. The gate is narrower than the others -- it guards the composite
     * INTERNERS, not the partition walk -- so what needs measuring is whether a new symbol column's
     * dictionary coexists with the dedicated dimension dictionaries and the _cell registry.
     * <p>
     * Asserts the VALUES read back, not merely that the DDL returned: a symbol column that resolves
     * every key to null would pass a bare "did it throw" check.
     */
    @Test(timeout = 60_000)
    public void surveyAddSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE",
                    "PARTITION BY DAY, exch LAYOUT PLAIN");
            seedTwoDays();

            execute("ALTER TABLE c ADD COLUMN tag SYMBOL");
            execute("ALTER TABLE p ADD COLUMN tag SYMBOL");
            drainWalQueue();

            // Force a full writer reopen. This is the path the defect ran through: configureColumnMemory()
            // rebuilds the dense symbol order from scratch, so an ALTER-time order that disagreed with it
            // only desyncs once the writer comes back. Without this the test can pass on the ALTER-time
            // ordering alone.
            engine.releaseInactive();

            // rows written AFTER the add, so the new column has real values in more than one cell
            insertIntoBoth("('2023-01-02T09:00:00.000000Z','E0','S0',99.0,'T0'),"
                    + "('2023-01-02T10:00:00.000000Z','E1','S1',98.0,'T1')");
            drainWalQueue();

            assertTwinEqual("", " ORDER BY ts, exch");
            // the symbol must resolve to its VALUE, not to null
            assertQuery("SELECT tag FROM c WHERE tag IS NOT NULL ORDER BY tag")
                    .noLeakCheck()
                    .returns("tag\nT0\nT1\n");
        });
    }

    /**
     * Indexed WHERE on a composite table. The gate lives in SqlCodeGenerator's row-cursor
     * (FilterOnValues) family, which is refused because that family's getScanDirection does not tell
     * the truth for a composite scan.
     * <p>
     * Prerequisite the survey's own history makes necessary: assert the index EXISTS first. Measured
     * 2026-08-18, ADD INDEX was a silent no-op, and the indexed-predicate gate then ACCEPTED the query
     * because there was no index for it to refuse over -- a green test proving nothing.
     */
    /**
     * Indexed WHERE on a composite table: the gate fires, and NO_INDEX gives correct answers.
     * <p>
     * This is the user-facing contract. The capability behind the gate is a PERFORMANCE one -- with
     * NO_INDEX the predicate stays a residual filter over the already-correct merged scan, so results
     * are right today; using the index would only make them faster. Per invariant 8, performance never
     * gates.
     * <p>
     * MEASURED 2026-08-25 with the gate lifted, on a table where one sym value spans two cells at
     * interleaved timestamps -- the plan being
     * {@code DeferredSingleSymbolFilterPageFrame / Index forward scan / Frame forward scan}, and a
     * page-frame scan walking CELLS sequentially:
     * <pre>
     * plain      01:00 E0, 02:00 E1, 03:00 E0, 04:00 E1
     * composite  01:00 E0, 03:00 E0, 02:00 E1, 04:00 E1
     * </pre>
     * Wrong ORDER, not wrong rows. Two earlier probe shapes passed against this same defect: seeding
     * one sym value per cell cannot expose it, and an outer {@code ORDER BY} sorts it away. Any future
     * attempt to lift this gate must use the interleaved shape below.
     */
    @Test(timeout = 60_000)
    public void surveyIndexedWhereReturnsCellMajorOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE",
                    "PARTITION BY DAY, exch LAYOUT PLAIN");
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0','X',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','E1','X',2.0),"
                    + "('2023-01-01T03:00:00.000000Z','E0','X',3.0),"
                    + "('2023-01-01T04:00:00.000000Z','E1','X',4.0)");
            drainWalQueue();

            execute("ALTER TABLE c ALTER COLUMN sym ADD INDEX");
            execute("ALTER TABLE p ALTER COLUMN sym ADD INDEX");
            drainWalQueue();
            assertIndexed("c", "sym", true);

            try {
                printSql("SELECT ts, exch FROM c WHERE sym = 'X'");
                org.junit.Assert.fail("the indexed WHERE gate must fire");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(),
                        "composite partitioning does not yet support an indexed WHERE predicate");
            }

            // the escape hatch must give the twin's answer, in the twin's ORDER, with no outer sort
            printSql("SELECT /*+ NO_INDEX(sym) */ ts, exch FROM c WHERE sym = 'X'");
            final String cRows = sink.toString();
            printSql("SELECT ts, exch FROM p WHERE sym = 'X'");
            final String pRows = sink.toString();
            org.junit.Assert.assertEquals("NO_INDEX must match the twin exactly", pRows, cRows);
        });
    }

    /**
     * ADD INDEX on a non-dimension symbol column. Indexes are per-partition, therefore already
     * per-cell, so this may need only the gate removed — but an index is built by scanning each
     * partition, and a composite partition is a cell.
     */
    @Test(timeout = 60_000)
    public void surveyAddIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE",
                    "PARTITION BY DAY, exch LAYOUT PLAIN");
            seedTwoDays();
            execute("ALTER TABLE c ALTER COLUMN sym ADD INDEX");
            execute("ALTER TABLE p ALTER COLUMN sym ADD INDEX");
            drainWalQueue();
            assertTwinEqual("");
            // THE ASSERTION THAT MATTERS: the index must actually exist. Without this the test passes
            // over a silent no-op -- measured 2026-08-18, isColumnIndexed was false here.
            assertIndexed("c", "sym", true);
            assertIndexed("p", "sym", true);
        });
    }

    /**
     * DROP COLUMN. Column files live per partition, so on a composite table they live per CELL. The
     * question is whether the removal walks cells or days.
     */
    @Test(timeout = 60_000)
    public void surveyDropColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE",
                    "PARTITION BY DAY, exch LAYOUT PLAIN");
            seedTwoDays();
            execute("ALTER TABLE c DROP COLUMN px");
            execute("ALTER TABLE p DROP COLUMN px");
            drainWalQueue();
            assertTwinEqual("", " ORDER BY ts, exch");
            // THE ASSERTION THAT MATTERS. The metadata drop succeeds either way, so the twin data
            // comparison above passes even when every cell's column file survives on disk. Measured
            // 2026-08-18: DROP COLUMN removed the DAY-LEVEL px.d and left E0/px.d, E1/px.d and
            // E2/px.d in place -- a per-cell disk leak invisible to any query.
            assertNoColumnFilesRemain("c", "px");
            // ANTI-VACUITY CONTROL, kept deliberately. This class's own history is three successive
            // false positives, each caught only by checking a different observable than the last. If
            // assertNoColumnFilesRemain ever stops finding files -- wrong walk depth, renamed layout --
            // it would pass for px while proving nothing, so it must still FAIL for a live column.
            boolean caught = false;
            try {
                assertNoColumnFilesRemain("c", "sym");
            } catch (AssertionError expected) {
                caught = true;
            }
            org.junit.Assert.assertTrue("assertNoColumnFilesRemain is VACUOUS -- it passed for a live column", caught);
        });
    }

    /**
     * DROP INDEX, the counterpart of {@link #surveyAddIndex()}.
     */
    @Test(timeout = 60_000)
    public void surveyDropIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("ts TIMESTAMP, exch SYMBOL, sym SYMBOL INDEX, px DOUBLE",
                    "PARTITION BY DAY, exch LAYOUT PLAIN");
            seedTwoDays();
            assertIndexed("c", "sym", true);  // precondition: the index exists before we drop it
            execute("ALTER TABLE c ALTER COLUMN sym DROP INDEX");
            execute("ALTER TABLE p ALTER COLUMN sym DROP INDEX");
            drainWalQueue();
            assertTwinEqual("");
            assertIndexed("c", "sym", false);
            assertIndexed("p", "sym", false);
        });
    }

    /**
     * RENAME COLUMN is the cheapest candidate in the whole sub-project: it is metadata-only and touches
     * no partition data at all. If ANY column DDL works unchanged on a composite table, it is this one
     * — which is exactly why it is worth measuring rather than assuming.
     */
    @Test(timeout = 60_000)
    public void surveyRenameColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE",
                    "PARTITION BY DAY, exch LAYOUT PLAIN");
            seedTwoDays();
            execute("ALTER TABLE c RENAME COLUMN px TO price");
            execute("ALTER TABLE p RENAME COLUMN px TO price");
            drainWalQueue();
            assertTwinEqual("", " ORDER BY ts, exch, price");
        });
    }

    /**
     * ALTER COLUMN TYPE rewrites every partition's column file, so on a composite table it must rewrite
     * every CELL's. Expected to be the most expensive of the four.
     */
    @Test(timeout = 60_000)
    public void surveyAlterColumnTypeToSymbol() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("ts TIMESTAMP, exch SYMBOL, note VARCHAR, px DOUBLE",
                    "PARTITION BY DAY, exch LAYOUT PLAIN");
            // seedTwoDays writes a SYMBOL in slot 3; this shape needs a VARCHAR there instead.
            // Each commit stays SINGLE-cell on purpose: a table with a var-size column has its own,
            // unrelated gate against an interleaved multi-cell commit, and writing E0 and E1 in one
            // batch trips that instead of exercising the conversion.
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0','N0',10.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-01T05:00:00.000000Z','E1','N1',11.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','E0','N2',20.0)");
            drainWalQueue();

            execute("ALTER TABLE c ALTER COLUMN note TYPE SYMBOL");
            execute("ALTER TABLE p ALTER COLUMN note TYPE SYMBOL");
            drainWalQueue();

            insertIntoBoth("('2023-01-02T09:00:00.000000Z','E1','N3',21.0)");
            drainWalQueue();

            assertTwinEqual("", " ORDER BY ts, exch");
            assertQuery("SELECT note FROM c ORDER BY note")
                    .noLeakCheck().expectSize()
                    .returns("note\nN0\nN1\nN2\nN3\n");
        });
    }

    @Test(timeout = 60_000)
    public void surveyAlterColumnType() throws Exception {
        assertMemoryLeak(() -> {
            createTwins("ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE",
                    "PARTITION BY DAY, exch LAYOUT PLAIN");
            seedTwoDays();
            execute("ALTER TABLE c ALTER COLUMN px TYPE FLOAT");
            execute("ALTER TABLE p ALTER COLUMN px TYPE FLOAT");
            drainWalQueue();
            assertTwinEqual("");
        });
    }

    /**
     * Asserts no file for {@code column} survives in ANY cell of the table. A dropped column stops
     * being read the moment the metadata changes, so queries -- and therefore twin comparisons -- are
     * blind to the files left behind.
     */
    private void assertNoColumnFilesRemain(String table, String column) throws Exception {
        engine.releaseInactive();
        final java.nio.file.Path root = java.nio.file.Paths.get(configuration.getDbRoot());
        final java.util.List<String> leaked = new java.util.ArrayList<>();
        try (java.util.stream.Stream<java.nio.file.Path> w = java.nio.file.Files.walk(root, 4)) {
            w.map(root::relativize).map(Object::toString)
                    .filter(x -> x.startsWith(table + '~'))
                    // WAL SEGMENT files are not partition column files -- they are the transient
                    // write-ahead copies and are reclaimed by WAL housekeeping, not by column purge.
                    // Including them made this assertion fail for a reason unrelated to what it tests.
                    .filter(x -> !x.contains("/wal"))
                    .filter(x -> x.endsWith('/' + column + ".d"))
                    .sorted()
                    .forEach(leaked::add);
        }
        org.junit.Assert.assertTrue("dropped column left files on disk: " + leaked, leaked.isEmpty());
    }

    /**
     * Asserts a column's INDEXED flag. A twin data comparison is blind to this, which is how
     * {@code ADD INDEX} passed while creating no index at all.
     */
    private void assertIndexed(String table, String column, boolean expected) throws Exception {
        engine.releaseInactive();
        try (io.questdb.cairo.TableReader reader = getReader(table)) {
            final int idx = reader.getMetadata().getColumnIndex(column);
            org.junit.Assert.assertEquals(table + '.' + column + " indexed flag",
                    expected, reader.getMetadata().isColumnIndexed(idx));
        }
    }

    /**
     * Two days, three cells each — enough that a day-blind operation is distinguishable from a
     * cell-aware one.
     */
    private void seedTwoDays() throws Exception {
        final StringBuilder sb = new StringBuilder();
        for (int day = 1; day <= 2; day++) {
            for (int cell = 0; cell <= 2; cell++) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append("('2023-01-0").append(day).append('T')
                        .append(String.format("%02d", 1 + cell * 4)).append(":00:00.000000Z','E")
                        .append(cell).append("','S").append(cell).append("',")
                        .append(day * 10 + cell).append(".0)");
            }
        }
        insertIntoBoth(sb.toString());
        drainWalQueue();
    }
}
