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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinDenseRecordCursorFactoryBase;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The ASOF algorithm auto-selection sizes the master by the rows it returns: an interval master
 * by the rows its intervals select, not by its whole table. On a two-key join, a master that may
 * be small gets a bounded Fast prelude in front of the Dense scan. Every test here checks that only
 * the algorithm changes - the scans below the join stay the same - and that every algorithm returns
 * the same rows where the choice flips.
 */
public class AsOfJoinMasterEstimateTest extends AbstractCairoTest {
    // 46 generated trades on day 3, plus five placed on the bounds and inside
    private static final String INTERVAL = "'2024-01-03T10:00:00.000000Z' AND '2024-01-03T10:02:00.000000Z'";
    private static final String MASTER_INTERVAL = "(SELECT * FROM trades WHERE ts BETWEEN " + INTERVAL + ")";
    private static final String[] SINGLE_KEY_HINTS = {"asof_dense", "asof_index", "asof_memoized", "asof_memoized_driveby", "asof_fast", "asof_linear"};
    private static final String[] TWO_KEY_HINTS = {"asof_dense", "asof_fast", "asof_linear"};

    @Test
    public void testBindVariableIntervalUnsetAtCompileKeepsWholeTableEstimate() throws Exception {
        assertMemoryLeak(() -> {
            createTables("index type posting", false);
            bindVariableService.clear();
            // No bind values: the intervals cannot be evaluated at compile time, so the estimate
            // falls back to the master's table, which is a third of the slave - Dense, no failure.
            final String sql = "SELECT t.ts, t.sym, q.bid FROM (SELECT * FROM trades WHERE ts BETWEEN $1 AND $2) t ASOF JOIN quotes q ON (sym)";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("AsOf Join Dense Single Symbol");
            assertQuery(sql).noLeakCheck().assertsPlanNotContaining("Indexed Scan");
        });
    }

    @Test
    public void testBitmapIndexKeepsItsWiderThreshold() throws Exception {
        assertMemoryLeak(() -> {
            createTables("index type bitmap", false);
            // ~0.5% of the slave: above the posting threshold (0.2%), below the bitmap one (2%)
            final String master = "(SELECT * FROM trades WHERE ts BETWEEN '2024-01-02T00:00:00.000000Z' AND '2024-01-02T01:00:00.000000Z')";
            final String sql = "SELECT t.ts, t.sym, q.bid FROM " + master + " t ASOF JOIN quotes q ON (sym)";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("AsOf Join Indexed Scan", "bp<=200");
            assertOnlyJoinDiffers(sql, "asof_dense");
            assertAllHintsAgree(sql, SINGLE_KEY_HINTS);
        });
    }

    @Test
    public void testFullTableMasterKeepsDense() throws Exception {
        assertMemoryLeak(() -> {
            createTables("index type posting", false);
            // no interval: the master is its whole table (a third of the slave), as before
            assertQuery("SELECT t.ts, t.sym, q.bid FROM trades t ASOF JOIN quotes q ON (sym)").noLeakCheck()
                    .assertsPlanContaining("AsOf Join Dense Single Symbol");
        });
    }

    @Test
    public void testIntervalMasterOnBitmapPicksIndexed() throws Exception {
        assertMemoryLeak(() -> {
            createTables("index type bitmap", false);
            assertIntervalMasterFlipsToIndexed();
        });
    }

    @Test
    public void testIntervalMasterOnParquetSlavePicksIndexed() throws Exception {
        assertMemoryLeak(() -> {
            createTables("index type posting", true);
            assertIntervalMasterFlipsToIndexed();
        });
    }

    @Test
    public void testIntervalMasterOnPostingPicksIndexed() throws Exception {
        assertMemoryLeak(() -> {
            createTables("index type posting", false);
            assertIntervalMasterFlipsToIndexed();
        });
    }

    @Test
    public void testIntervalFromConstantFunctionsIsCounted() throws Exception {
        assertMemoryLeak(() -> {
            createTables("index type posting", false);
            // the shape of TAQ idx 84: interval bounds from dateadd() over a literal
            final String master = "(SELECT * FROM trades WHERE ts BETWEEN dateadd('m', 3480, '2024-01-01') AND dateadd('m', 3482, '2024-01-01'))";
            final String sql = "SELECT t.ts, t.sym, q.bid FROM " + master + " t ASOF JOIN quotes q ON (sym)";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("AsOf Join Indexed Scan", "auto:master~51 slave~");
            assertAllHintsAgree(sql, SINGLE_KEY_HINTS);
        });
    }

    @Test
    public void testIntervalEstimateOpensOnlyCutPartitions() throws Exception {
        // Sizing an interval master must not map every partition the interval covers - not even
        // for a plain EXPLAIN: the partitions wholly inside count from the table's metadata, and
        // only the two the interval's ends cut are opened and searched.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE quotes (sym SYMBOL, ex SYMBOL, ts TIMESTAMP, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO quotes SELECT 's' || (x % 7), 'X' || (x % 3), '2024-01-01'::timestamp + x * 60_000_000L, x FROM long_sequence(20000)");
            execute("CREATE TABLE trades (sym SYMBOL, ex SYMBOL, ts TIMESTAMP, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO trades SELECT 's' || (x % 7), 'X' || (x % 3), '2024-01-01'::timestamp + x * 600_000_000L + 7, x FROM long_sequence(2000)");
            try (TableReader reader = engine.getReader("trades")) {
                Assert.assertEquals(14, reader.getPartitionCount());
            }
            engine.releaseAllReaders();
            final String sql = "SELECT t.ts, q.bid FROM (SELECT * FROM trades WHERE ts BETWEEN '2024-01-02T12:00' AND '2024-01-11T12:00') t "
                    + "ASOF JOIN quotes q ON (sym, ex)";
            // nine whole days and two halves: the master may be small next to the slave, so the
            // planner sized it
            assertQuery(sql).noLeakCheck().assertsPlanContaining("AsOf Join Dense Dual Symbol");
            try (TableReader reader = engine.getReader("trades")) {
                Assert.assertEquals(2, reader.getOpenPartitionCount());
            }
        });
    }

    @Test
    public void testIntervalMasterOnUnindexedSlaveKeepsDense() throws Exception {
        assertMemoryLeak(() -> {
            createTables("", false);
            // Memoized keeps the whole-table master estimate (a third of the slave): no flip.
            final String sql = "SELECT t.ts, t.sym, q.bid FROM " + MASTER_INTERVAL + " t ASOF JOIN quotes q ON (sym)";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("AsOf Join Dense Single Symbol");
            assertAllHintsAgree(sql, new String[]{"asof_dense", "asof_memoized", "asof_fast", "asof_linear"});
        });
    }

    @Test
    public void testPostingThresholdKeepsDenseForMidSizeMaster() throws Exception {
        assertMemoryLeak(() -> {
            createTables("index type posting", false);
            // ~0.5% of the slave: a posting lookup per master row does not repay here
            final String master = "(SELECT * FROM trades WHERE ts BETWEEN '2024-01-02T00:00:00.000000Z' AND '2024-01-02T01:00:00.000000Z')";
            final String sql = "SELECT t.ts, t.sym, q.bid FROM " + master + " t ASOF JOIN quotes q ON (sym)";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("AsOf Join Dense Single Symbol");
            assertQuery(sql).noLeakCheck().assertsPlanNotContaining("Indexed Scan");
        });
    }

    @Test
    public void testPostingThresholdIsConfigurable() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_ASOF_INDEX_POSTING_MAX_MASTER_BP, 200);
        assertMemoryLeak(() -> {
            createTables("index type posting", false);
            final String master = "(SELECT * FROM trades WHERE ts BETWEEN '2024-01-02T00:00:00.000000Z' AND '2024-01-02T01:00:00.000000Z')";
            assertQuery("SELECT t.ts, t.sym, q.bid FROM " + master + " t ASOF JOIN quotes q ON (sym)").noLeakCheck()
                    .assertsPlanContaining("AsOf Join Indexed Scan");
        });
    }

    @Test
    public void testProjectedSlaveIsSized() throws Exception {
        assertMemoryLeak(() -> {
            createTables("index type posting", false);
            // TAQ idx 84 and 83 select their slave columns in a subquery: a projection over the table
            final String slave = "(SELECT sym, ex, ts, bid FROM quotes)";
            final String sql = "SELECT t.ts, t.sym, q.bid FROM " + MASTER_INTERVAL + " t ASOF JOIN " + slave + " q ON (sym)";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("AsOf Join Indexed Scan", "auto:master~51 slave~300306 ");
            assertOnlyJoinDiffers(sql, "asof_dense");
            assertAllHintsAgree(sql, SINGLE_KEY_HINTS);
            final String sql2 = "SELECT t.ts, t.sym, t.ex, q.bid FROM (SELECT * FROM trades WHERE px > 99990) t ASOF JOIN " + slave + " q ON (sym, ex)";
            assertQuery(sql2).noLeakCheck().assertsPlanContaining("AsOf Join Dense Dual Symbol", "prelude: fast\n");
            assertAllHintsAgree(sql2, TWO_KEY_HINTS);
        });
    }

    @Test
    public void testTwoKeyFilteredMasterGetsPrelude() throws Exception {
        assertMemoryLeak(() -> {
            createTables("index type posting", false);
            // a filter the planner cannot size: the master may be small
            final String sql = "SELECT t.ts, t.sym, t.ex, q.bid, q.ts qts FROM (SELECT * FROM trades WHERE px > 99990) t ASOF JOIN quotes q ON (sym, ex)";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("AsOf Join Dense Dual Symbol", "prelude: fast\n");
            assertOnlyJoinDiffers(sql, "asof_dense");
            assertAllHintsAgree(sql, TWO_KEY_HINTS);
        });
    }

    @Test
    public void testTwoKeyIntervalMasterGetsPrelude() throws Exception {
        assertMemoryLeak(() -> {
            createTables("index type posting", false);
            final String sql = "SELECT t.ts, t.sym, t.ex, q.bid, q.ts qts FROM " + MASTER_INTERVAL + " t ASOF JOIN quotes q ON (sym, ex)";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("AsOf Join Dense Dual Symbol", "prelude: fast\n");
            assertOnlyJoinDiffers(sql, "asof_dense");
            assertAllHintsAgree(sql, TWO_KEY_HINTS);
        });
    }

    @Test
    public void testTwoKeyLargeMasterKeepsPlainDense() throws Exception {
        assertMemoryLeak(() -> {
            createTables("index type posting", false);
            final String sql = "SELECT t.ts, q.bid FROM trades t ASOF JOIN quotes q ON (sym, ex)";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("AsOf Join Dense Dual Symbol");
            assertQuery(sql).noLeakCheck().assertsPlanNotContaining("prelude");
            // a hint is a hint: no prelude
            assertQuery("SELECT /*+ asof_dense(t q) */ t.ts, q.bid FROM " + MASTER_INTERVAL + " t ASOF JOIN quotes q ON (sym, ex)").noLeakCheck()
                    .assertsPlanNotContaining("prelude");
        });
    }

    @Test
    public void testTwoKeyPreludeHandsOverMidStream() throws Exception {
        // Tiny budgets: the prelude serves one master row, or walks 1% of the slave, then hands
        // over to Dense mid-stream. The rows must not change at the switch.
        setProperty(PropertyKey.CAIRO_SQL_ASOF_MULTIKEY_FAST_MAX_MASTER_BP, 1);
        setProperty(PropertyKey.CAIRO_SQL_ASOF_MULTIKEY_FAST_MAX_BACKSCAN_PCT, 1);
        assertMemoryLeak(() -> {
            createTables("", false);
            for (String master : new String[]{
                    "(SELECT * FROM trades WHERE ts BETWEEN " + INTERVAL + " AND px > 0)",
                    "(SELECT * FROM trades WHERE px > 99000)",
                    "(SELECT * FROM trades WHERE px < 3000)"
            }) {
                final String sql = "SELECT t.ts, t.sym, t.ex, q.bid, q.ts qts FROM " + master + " t ASOF JOIN quotes q ON (sym, ex)";
                assertQuery(sql).noLeakCheck().assertsPlanContaining("prelude: fast\n");
                assertAllHintsAgree(sql, TWO_KEY_HINTS);
            }
        });
    }

    @Test
    public void testTwoKeyPreludeBackScanBudgetHoldsInsideOneBackScan() throws Exception {
        // The first master row's key pair never occurs in the slave, though both its symbols do:
        // its back-scan would walk to the slave's start. The budget (1% of the slave) must stop it
        // there and then, not after it, and the Dense scan must serve that row and the rest.
        setProperty(PropertyKey.CAIRO_SQL_ASOF_MULTIKEY_FAST_MAX_BACKSCAN_PCT, 1);
        assertMemoryLeak(() -> {
            createTables("", false);
            execute("INSERT INTO trades VALUES ('rare', 'X2', 0, '2024-01-03T09:00:00.000000Z', -1.0), "
                    + "('s5', 'X1', 1, '2024-01-03T09:30:00.000000Z', -2.0), ('rare', 'X0', 0, '2024-01-03T09:40:00.000000Z', -3.0)");
            final String sql = "SELECT t.ts, t.sym, t.ex, q.bid, q.ts qts FROM (SELECT * FROM trades WHERE px < 0) t ASOF JOIN quotes q ON (sym, ex)";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("AsOf Join Dense Dual Symbol", "prelude: fast\n");
            try (RecordCursorFactory factory = select(sql)) {
                RecordCursorFactory join = factory;
                while (!(join instanceof AsOfJoinDenseRecordCursorFactoryBase)) {
                    join = join.getBaseFactory();
                    Assert.assertNotNull("no Dense ASOF factory", join);
                }
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        // drain
                    }
                }
                final AsOfJoinDenseRecordCursorFactoryBase dense = (AsOfJoinDenseRecordCursorFactoryBase) join;
                final long budget = dense.getAdaptiveBackScanBudget();
                Assert.assertTrue("budget " + budget, budget > 1000);
                Assert.assertTrue("walked " + dense.getAdaptiveBackScanUsed() + " for a budget of " + budget,
                        dense.getAdaptiveBackScanUsed() <= budget + 1);
                Assert.assertTrue("the back-scan did not reach the budget", dense.getAdaptiveBackScanUsed() > budget);
            }
            printSql(hinted(sql, "asof_linear"));
            final String expected = sink.toString();
            TestUtils.assertContains(expected, "2024-01-03T09:00:00.000000Z\trare\tX2\tnull\t\n");
            assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().returns(expected);
        });
    }

    @Test
    public void testTwoKeyPreludeOffWhenDisabled() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_ASOF_MULTIKEY_FAST_MAX_MASTER_BP, 0);
        assertMemoryLeak(() -> {
            createTables("index type posting", false);
            final String sql = "SELECT t.ts, q.bid FROM " + MASTER_INTERVAL + " t ASOF JOIN quotes q ON (sym, ex)";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("AsOf Join Dense Dual Symbol");
            assertQuery(sql).noLeakCheck().assertsPlanNotContaining("prelude");
        });
    }

    @Test
    public void testTwoKeyThreeColumnKeyGetsPreludeOnGenericDense() throws Exception {
        assertMemoryLeak(() -> {
            createTables("", false);
            final String sql = "SELECT t.ts, t.sym, t.ex, q.bid FROM " + MASTER_INTERVAL + " t ASOF JOIN quotes q ON (sym, ex, k)";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("AsOf Join Dense", "prelude: fast\n");
            assertQuery(sql).noLeakCheck().assertsPlanNotContaining("Dual Symbol");
            assertAllHintsAgree(sql, TWO_KEY_HINTS);
        });
    }

    private static String hinted(String sql, String hint) {
        return sql.replaceFirst("SELECT ", "SELECT /*+ " + hint + "(t q) */ ");
    }

    // Plan lines below the join node: what the join reads, without how it joins.
    private static String scansBelowJoin(CharSequence plan) {
        final String[] lines = plan.toString().split("\n");
        int joinLine = -1;
        for (int i = 0; i < lines.length; i++) {
            if (lines[i].contains("AsOf Join")) {
                joinLine = i;
                break;
            }
        }
        Assert.assertTrue("no ASOF join in plan:\n" + plan, joinLine > -1);
        final int joinIndent = indentOf(lines[joinLine]);
        final StringBuilder b = new StringBuilder();
        for (int i = joinLine + 1; i < lines.length; i++) {
            final String trimmed = lines[i].trim();
            // the join's own attribute lines are indented by two and hold "name: value"
            if (indentOf(lines[i]) == joinIndent + 4 && !trimmed.isEmpty() && Character.isUpperCase(trimmed.charAt(0))) {
                b.append(lines[i]).append('\n');
            } else if (indentOf(lines[i]) > joinIndent + 4) {
                b.append(lines[i]).append('\n');
            }
        }
        return b.toString();
    }

    private static int indentOf(String line) {
        int n = 0;
        while (n < line.length() && line.charAt(n) == ' ') {
            n++;
        }
        return n;
    }

    private void assertAllHintsAgree(String sql, String[] hints) throws Exception {
        // the reference: Dense, the algorithm the planner chose before
        printSql(hinted(sql, "asof_dense"));
        final String expected = sink.toString();
        for (String hint : hints) {
            assertQuery(hinted(sql, hint)).noLeakCheck().inferRandomAccess().inferTimestamp().returns(expected);
        }
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().returns(expected);
        // the rows are not vacuous: some match, some have no prior slave row
        printSql("SELECT count(), count(bid) FROM (" + sql + ")");
        final String[] counts = sink.toString().split("\n")[1].split("\t");
        final long rows = Long.parseLong(counts[0]);
        final long matched = Long.parseLong(counts[1]);
        Assert.assertTrue("no rows: " + sql, rows > 0);
        Assert.assertTrue("no matches: " + sql, matched > 0);
        Assert.assertTrue("every row matched, the no-prior-row case is not exercised: " + sql, matched < rows);
    }

    private void assertIntervalMasterFlipsToIndexed() throws Exception {
        final String sql = "SELECT t.ts, t.sym, t.px, q.bid, q.ts qts FROM " + MASTER_INTERVAL + " t ASOF JOIN quotes q ON (sym)";
        // the master is sized by its interval: 46 generated trades plus the five placed there
        assertQuery(sql).noLeakCheck().assertsPlanContaining("AsOf Join Indexed Scan", "auto:master~51 slave~");
        assertOnlyJoinDiffers(sql, "asof_dense");
        assertAllHintsAgree(sql, SINGLE_KEY_HINTS);
        // the interval rows that exercise the edges are there
        // rare twice (lookback to day 1), late (no prior row), s48 (never quoted), NULL
        assertQuery("SELECT count() FROM " + MASTER_INTERVAL + " WHERE sym IN ('rare', 'late', 's48') OR sym IS NULL").noLeakCheck().noRandomAccess().expectSize()
                .returns("count\n5\n");
    }

    private void assertOnlyJoinDiffers(String sql, String hint) throws Exception {
        final StringSink autoPlan = new StringSink();
        printSql("EXPLAIN " + sql, autoPlan);
        printSql("EXPLAIN " + hinted(sql, hint));
        Assert.assertEquals(scansBelowJoin(sink), scansBelowJoin(autoPlan));
    }

    private void createTables(String quoteIndex, boolean parquetSlave) throws Exception {
        // quotes: 300k rows over three days, 47 symbols plus NULLs, two rows per timestamp
        execute("CREATE TABLE quotes (sym SYMBOL " + quoteIndex + ", ex SYMBOL, k INT, ts TIMESTAMP, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO quotes SELECT CASE WHEN x % 101 = 0 THEN NULL ELSE 's' || (x % 47) END, 'X' || (x % 3), (x % 2)::int, "
                + "'2024-01-01'::timestamp + (x / 2) * 1_728_000L, x::double FROM long_sequence(300000)");
        // same-symbol ties at one timestamp: copies of existing rows, appended out of order
        execute("INSERT INTO quotes SELECT sym, ex, k, ts, -bid FROM quotes WHERE bid % 997 = 0");
        // 'rare' is quoted on day 1 only, so day-3 trades look back across partitions;
        // 'late' is quoted only after the interval, so its trades have no prior row
        execute("INSERT INTO quotes VALUES "
                + "('rare', 'X0', 0, '2024-01-01T05:00:00.000000Z', 1.5), ('rare', 'X1', 1, '2024-01-01T05:00:00.000000Z', 2.5), "
                + "('rare', 'X0', 0, '2024-01-01T06:00:00.000000Z', 3.5), ('late', 'X0', 0, '2024-01-03T12:00:00.000000Z', 4.5)");
        // quotes exactly at a trade's timestamp, on the lower interval bound
        execute("INSERT INTO quotes VALUES ('s5', 'X1', 1, '2024-01-03T10:00:00.000000Z', 5.5), ('s5', 'X1', 1, '2024-01-03T10:00:00.000000Z', 6.5)");
        if (parquetSlave) {
            execute("ALTER TABLE quotes CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
        }
        // trades: 100k rows over the same three days, 50 symbols (s47..s49 never quoted) plus NULLs
        execute("CREATE TABLE trades (sym SYMBOL, ex SYMBOL, k INT, ts TIMESTAMP, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO trades SELECT CASE WHEN x % 89 = 0 THEN NULL ELSE 's' || (x % 50) END, 'X' || (x % 4), (x % 2)::int, "
                + "'2024-01-01'::timestamp + x * 2_592_000L + 7, x::double FROM long_sequence(100000)");
        // interval edges: trades exactly on both bounds, one at a slave tie
        execute("INSERT INTO trades VALUES "
                + "('s5', 'X1', 1, '2024-01-03T10:00:00.000000Z', 1.0), ('rare', 'X0', 0, '2024-01-03T10:02:00.000000Z', 2.0), "
                + "('late', 'X0', 0, '2024-01-03T10:01:00.000000Z', 3.0), ('rare', 'X1', 1, '2024-01-03T10:01:30.000000Z', 4.0), "
                + "(NULL, 'X0', 0, '2024-01-03T10:00:30.000000Z', 5.0)");
    }
}
