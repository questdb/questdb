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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.jit.JitUtil;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.CairoTestConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Differential test for the {@code LATEST ON} hoist ({@code SqlOptimiser.pushLatestByToTableModel}),
 * which rewrites a {@code LATEST ON} over a parser-generated scalar-expiry wrapper into a direct
 * table read. Plain tables, ordinary views and materialized views without expiry must keep their plans.
 * <p>
 * The rewrite drops the sub-query layers and carries a hand-picked set of attributes up from the
 * table model. Every attribute it does not carry - an alias, a {@code timestamp(...)} override, a
 * LIMIT, an ORDER BY, a join - it discards silently unless a guard rejects the shape first, and a
 * guard can only be written for an attribute somebody thought of. So this test checks the rewrite's
 * outcome rather than its preconditions: it runs each shape twice, once with the rewrite on and once
 * with it off, and compares the two. An attribute nobody guarded shows up as a difference whether or
 * not anybody predicted it.
 * <p>
 * The expiry cases use four lists:
 * <ul>
 *     <li>{@link #HOISTED} - the rewrite fires. Its plan must differ from the un-rewritten one, and
 *     the projection (column names, types and order), the rows and the designated timestamp must all
 *     survive it.</li>
 *     <li>{@link #NOT_HOISTED} - the rewrite must leave the shape alone, so both plans must be
 *     identical. This is what pins the guards: a guard that stops rejecting its shape changes that
 *     shape's plan and the two stop matching. Comparing rows alone would not catch it, because a
 *     wrongly hoisted query often returns the same rows on a small fixture.</li>
 *     <li>{@link #NEEDS_HOIST} - the shape need not compile without the rewrite. The direct read
 *     publishes a designated timestamp that the sub-query form does not, so the rewrite is what lets
 *     SAMPLE BY and the timestamp joins compile over it at all.</li>
 *     <li>{@link #ROWS_MAY_DIFFER} - the rewrite fires and the two forms return different rows. Only an
 *     unordered LIMIT does this, and both answers are correct; everything except the rows is still
 *     compared.</li>
 * </ul>
 * Row <b>order</b> is deliberately not invariant: the sub-query form emits one row per partition key
 * in map-insertion order and the direct read emits in timestamp order, so rows are compared as a
 * multiset. The ordering callers can rely on is asserted in {@code LatestByTest}.
 * <p>
 * The designated timestamp may appear where there was none, but must never move to another column or
 * disappear. That is the {@code timestamp(ts2)} defect, which changes no row and so is invisible to a
 * result comparison on its own. On the shapes where the un-hoisted form publishes no timestamp at all
 * there is nothing to compare against, so the check falls back to the column named by
 * {@code LATEST ON} - the only one the rewrite is allowed to publish.
 * <p>
 * Extending this is one line: a shape the rewrite must handle, or must leave alone, goes in the
 * matching list and is compared against the un-rewritten plan from then on.
 * <p>
 * The same query shapes without expiry must retain identical plans, rows and metadata with the
 * rewrite enabled or disabled. This checks the scope boundary independently of individual guards.
 */
public class LatestByHoistEquivalenceTest extends AbstractCairoTest {

    /**
     * Shapes the rewrite fires on. Plans must differ; projection, rows and timestamp must not.
     */
    private static final ObjList<String> HOISTED = new ObjList<>(
            "SELECT * FROM et LATEST ON ts PARTITION BY sym",
            "SELECT * FROM en LATEST ON ts PARTITION BY sym",
            "SELECT * FROM et WHERE v > 15 LATEST ON ts PARTITION BY sym",
            "SELECT * FROM et WHERE sym = 'BB' LATEST ON ts PARTITION BY sym",
            "SELECT * FROM et WHERE sym = 'BB' AND sym = 'CC' LATEST ON ts PARTITION BY sym",
            "SELECT * FROM et LATEST ON ts PARTITION BY sym, v",
            "SELECT * FROM et LATEST ON ts PARTITION BY v",
            "SELECT * FROM et WHERE sym = 'BB' LATEST ON ts PARTITION BY v",
            "SELECT * FROM et WHERE sym = 'ZZ' LATEST ON ts PARTITION BY v",
            "SELECT sym, v, ts, ts2 FROM et LATEST ON ts PARTITION BY sym",
            "SELECT ts, sym, v FROM et LATEST ON ts PARTITION BY sym",
            "SELECT * FROM et x WHERE x.v > 15 LATEST ON ts PARTITION BY sym",
            "SELECT * FROM et LATEST ON ts PARTITION BY sym UNION ALL SELECT * FROM et",
            "SELECT * FROM et LATEST ON ts PARTITION BY sym ORDER BY v LIMIT 1"
    );

    /**
     * Former general-purpose hoist candidates. Without expiry, every plan must now stay unchanged.
     */
    private static final String[] ORDINARY_SHAPES = {
            "SELECT * FROM (SELECT * FROM t) LATEST ON ts PARTITION BY sym",
            // Non-indexed keys must also keep their ordinary plans.
            "SELECT * FROM (SELECT * FROM n) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT * FROM t WHERE v > 15) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT * FROM t) WHERE v > 15 LATEST ON ts PARTITION BY sym",
            // A filter at each level.
            "SELECT * FROM (SELECT * FROM t WHERE v > 15) WHERE sym = 'BB' LATEST ON ts PARTITION BY sym",
            // filters that contradict each other collapse the table read to an empty factory
            "SELECT * FROM (SELECT * FROM t WHERE sym = 'BB') WHERE sym = 'CC' LATEST ON ts PARTITION BY sym",
            "WITH c AS (SELECT * FROM t) SELECT * FROM c LATEST ON ts PARTITION BY sym",
            // composite and non-SYMBOL partition keys
            "SELECT * FROM (SELECT * FROM t) LATEST ON ts PARTITION BY sym, v",
            "SELECT * FROM (SELECT * FROM t) LATEST ON ts PARTITION BY v",
            // an indexed-symbol filter under a non-SYMBOL partition key: the direct read has no
            // symbol key to seek, so the predicate has to stay in the filter
            "SELECT * FROM (SELECT * FROM t WHERE sym = 'BB') LATEST ON ts PARTITION BY v",
            "SELECT * FROM (SELECT * FROM t WHERE sym = 'ZZ') LATEST ON ts PARTITION BY v",
            // the dropped layer decides column order, so the table's storage order must not surface
            "SELECT * FROM (SELECT sym, v, ts, ts2 FROM t) LATEST ON ts PARTITION BY sym",
            "SELECT ts, sym, v FROM (SELECT v, sym, ts, ts2 FROM t) LATEST ON ts PARTITION BY sym",
            // an alias on the sub-query, qualifying the outer filter
            "SELECT * FROM (SELECT * FROM t) x WHERE x.v > 15 LATEST ON ts PARTITION BY sym",
            // SAMPLE BY at the same query level can compile through its own rewrites.
            "SELECT ts, count() FROM (SELECT * FROM t) LATEST ON ts PARTITION BY sym SAMPLE BY 1d",
            "SELECT ts, count() FROM (SELECT * FROM n) LATEST ON ts PARTITION BY sym SAMPLE BY 1d",
            // A CTE referenced twice; neither arm opts into scalar-expiry hoisting.
            "WITH c AS (SELECT * FROM t) SELECT * FROM c LATEST ON ts PARTITION BY sym UNION ALL SELECT * FROM c",
            // an ORDER BY above the LATEST ON decides the row a LIMIT keeps, so both forms agree
            "SELECT * FROM (SELECT * FROM t) LATEST ON ts PARTITION BY sym ORDER BY v LIMIT 1",
    };

    /**
     * Shapes the rewrite fires on where it also changes which rows come back. A LIMIT with no ORDER BY
     * keeps the first N rows in whatever order the factory emits them, and the two forms emit in
     * different orders: the sub-query form one row per partition key in map-insertion order, the direct
     * read in timestamp order. Both answers are correct - the query does not ask for an order - so the
     * rows are deliberately not compared here. Everything else still has to survive: the plan must
     * change, and the projection, the row count and the designated timestamp must not.
     * <p>
     * This is the one place where upgrading to the rewrite is visible to a caller as a different answer
     * rather than a different plan. A LIMIT lands on the model above the LATEST ON
     * ({@code SqlParser.parseDml0} moves it up out of the FROM clause), so no guard in
     * {@code findHoistableTableModel} can see it.
     */
    private static final String[] ROWS_MAY_DIFFER = {
            "SELECT * FROM et LATEST ON ts PARTITION BY sym LIMIT 1",
            "SELECT * FROM et LATEST ON ts PARTITION BY sym LIMIT 2",
            "SELECT * FROM en LATEST ON ts PARTITION BY sym LIMIT 1",
    };

    /**
     * Shapes the rewrite must leave alone. Both plans must be identical.
     */
    private static final String[] NOT_HOISTED = {
            // Parser provenance is necessary, not sufficient: these directly reach a marked read
            // but must still obey timestamp choice and join semantics.
            "SELECT * FROM et LATEST ON ts2 PARTITION BY sym",
            "SELECT * FROM et x TIMESTAMP(ts2) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM et JOIN j ON (v = w) LATEST ON ts PARTITION BY sym",
            // LATEST ON names a timestamp column that is not the table's designated one, so the
            // direct read - which always uses the designated one - would answer a different question
            "SELECT * FROM (SELECT * FROM t) LATEST ON ts2 PARTITION BY sym",
            // a sub-query timestamp override, in both arrangements
            "SELECT * FROM (SELECT * FROM t TIMESTAMP(ts2)) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT * FROM t TIMESTAMP(ts2)) LATEST ON ts2 PARTITION BY sym",
            // the override written on the LATEST ON model itself, in both arrangements. LATEST ON
            // overwrites the model's timestamp token, so only the explicit-timestamp flag still records
            // that the query declared ts2.
            "SELECT * FROM (SELECT * FROM t) x TIMESTAMP(ts2) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM ((SELECT * FROM t) x TIMESTAMP(ts2) LATEST ON ts PARTITION BY sym)",
            // clauses on the dropped layer that decide which rows reach the LATEST ON
            "SELECT * FROM (SELECT * FROM t LIMIT 3) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT * FROM t ORDER BY v) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT * FROM t UNION ALL SELECT * FROM t2) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT DISTINCT * FROM t) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT sym, max(v) v, max(ts) ts FROM t) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT sym, ts, max(v) v, max(ts2) ts2 FROM t SAMPLE BY 1d) LATEST ON ts PARTITION BY sym",
            // LATEST ON applies to the join output, not to the table under it
            "SELECT * FROM (SELECT * FROM t) JOIN j ON (v = w) LATEST ON ts PARTITION BY sym",
            // an alias on the table model qualifying that model's own filter: the rewrite would drop
            // the model the prefix resolves against
            "SELECT * FROM (SELECT * FROM t x WHERE x.v > 15) LATEST ON ts PARTITION BY sym",
            // projections that are not the table's full identity column list
            "SELECT * FROM (SELECT sym, ts FROM t) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT sym, sym s2, ts FROM t) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT v AS w, sym, ts FROM t) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT * FROM (SELECT * FROM t)) LATEST ON ts PARTITION BY sym",
            // the same clauses again, but on an intervening layer rather than on the table model,
            // and each with the table's full identity projection so only its own guard can reject it
            "SELECT * FROM (SELECT * FROM t JOIN j ON (v = w)) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM ((SELECT * FROM t LIMIT 3) LATEST ON ts PARTITION BY sym) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT v, sym, ts, ts2 FROM t GROUP BY v, sym, ts, ts2) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT DISTINCT v, sym, ts, ts2 FROM t) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT * FROM (SELECT * FROM t) WHERE v > 15) LATEST ON ts PARTITION BY sym",
    };

    /**
     * Shapes that need not compile without the rewrite, but must compile with it.
     */
    private static final String[] NEEDS_HOIST = {
            "SELECT * FROM (SELECT * FROM et LATEST ON ts PARTITION BY sym) ASOF JOIN u",
            "SELECT ts, count() FROM (SELECT * FROM et LATEST ON ts PARTITION BY sym) SAMPLE BY 1d",
            "SELECT ts, count() FROM (SELECT * FROM en LATEST ON ts PARTITION BY sym) SAMPLE BY 1d",
    };

    private static boolean isHoistEnabled = true;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // The hoist has no production property - it is always on in a running server. Override the
        // seam so this test can obtain the un-rewritten plan to compare against.
        configurationFactory = (root, telemetry, overrides) ->
                new CairoTestConfiguration(root, telemetry, overrides) {
                    @Override
                    public boolean isSqlLatestOnHoistEnabled() {
                        return isHoistEnabled;
                    }
                };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testOuterTimestampOverrideCompilesTheSameWithAndWithoutTheHoist() throws Exception {
        // A timestamp(...) on the LATEST ON model does not reach the sub-query form's factory, so a
        // SAMPLE BY above it has no timestamp to run on and the query does not compile. The hoist must
        // not turn that into a compiling query: the direct read publishes the table's designated
        // timestamp, so SAMPLE BY would silently run on ts instead of the declared ts2. The shape
        // compiles under neither setting, so it fits none of the lists above.
        assertMemoryLeak(() -> {
            createFixture();
            createExpiryFixture();
            final String sql = "SELECT ts, count() FROM (et x TIMESTAMP(ts2) "
                    + "LATEST ON ts PARTITION BY sym) SAMPLE BY 1d";
            final Outcome hoisted = run(sql, true);
            final Outcome plain = run(sql, false);
            Assert.assertNotNull("the hoist made this shape compile: " + sql, hoisted.error);
            Assert.assertEquals("the hoist changed how this shape is rejected: " + sql,
                    plain.error, hoisted.error);
        });
    }

    @Test
    public void testHoistPreservesProjectionRowsAndTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            createExpiryFixture();

            for (int i = 0, n = HOISTED.size(); i < n; i++) {
                final String sql = HOISTED.getQuick(i);
                final Outcome hoisted = run(sql, true);
                final Outcome plain = run(sql, false);
                Assert.assertNull("the hoist broke a query: " + sql + "\n  " + hoisted.error, hoisted.error);
                Assert.assertNull("does not compile un-hoisted, so it belongs in NEEDS_HOIST: " + sql
                        + "\n  " + plain.error, plain.error);
                Assert.assertNotEquals("no longer hoisted, so this shape now covers nothing: " + sql,
                        plain.plan, hoisted.plan);
                Assert.assertEquals("projection changed: " + sql, plain.columns, hoisted.columns);
                Assert.assertEquals("rows changed: " + sql, plain.rows.toString(), hoisted.rows.toString());
                assertTimestampSurvives(sql, plain, hoisted);
            }

            for (String query : ROWS_MAY_DIFFER) {
                final String sql = expirySql(query);
                final Outcome hoisted = run(sql, true);
                final Outcome plain = run(sql, false);
                Assert.assertNull("the hoist broke a query: " + sql + "\n  " + hoisted.error, hoisted.error);
                Assert.assertNull("does not compile un-hoisted, so it belongs in NEEDS_HOIST: " + sql
                        + "\n  " + plain.error, plain.error);
                Assert.assertNotEquals("no longer hoisted, so this shape now covers nothing: " + sql,
                        plain.plan, hoisted.plan);
                Assert.assertEquals("projection changed: " + sql, plain.columns, hoisted.columns);
                Assert.assertEquals("row count changed: " + sql, plain.rows.size(), hoisted.rows.size());
                assertTimestampSurvives(sql, plain, hoisted);
            }

            for (String query : NOT_HOISTED) {
                final String sql = expirySql(query);
                final Outcome hoisted = run(sql, true);
                final Outcome plain = run(sql, false);
                Assert.assertNull("does not compile, so it pins no guard: " + sql + "\n  " + plain.error, plain.error);
                Assert.assertNull("the hoist broke a query: " + sql + "\n  " + hoisted.error, hoisted.error);
                Assert.assertEquals("the hoist fired on a shape it must leave alone: " + sql,
                        plain.plan, hoisted.plan);
            }

            for (String query : NEEDS_HOIST) {
                final String sql = expirySql(query);
                final Outcome hoisted = run(sql, true);
                Assert.assertNull("does not compile even with the hoist: " + sql + "\n  " + hoisted.error, hoisted.error);
            }
        });
    }

    @Test
    public void testOrdinaryReadsKeepPlansWithAndWithoutHoist() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            createExpiryFixture();
            execute("CREATE VIEW plain_view AS (SELECT * FROM t)");
            execute("CREATE MATERIALIZED VIEW plain_mv AS (SELECT * FROM t), INDEX(sym)");
            drainWalAndMatViewQueues();

            // Compile expiry first to exercise model-pool reuse: its provenance must not leak into
            // the following ordinary reads. A query-level 'contains expiry' gate would also be wrong.
            Assert.assertNull(run("SELECT * FROM et LATEST ON ts PARTITION BY sym", true).error);
            for (String sql : ORDINARY_SHAPES) {
                assertUnchanged(sql);
            }
            for (String sql : NOT_HOISTED) {
                assertUnchanged(sql);
            }
            assertUnchanged("SELECT * FROM plain_view WHERE v > 15 LATEST ON ts PARTITION BY v");
            assertUnchanged("SELECT * FROM (SELECT * FROM plain_mv WHERE v > 15) LATEST ON ts PARTITION BY v");
            assertUnchanged("SELECT * FROM (SELECT * FROM t WHERE v > 15) LATEST ON ts PARTITION BY v "
                    + "UNION ALL SELECT * FROM et");
            assertUnchanged("SELECT * FROM (SELECT * FROM t) LATEST ON ts PARTITION BY sym LIMIT 1");
        });
    }

    @Test
    public void testScalarExpiryFiltersBeforeLatestAndKeepsTimestampOrder() throws Exception {
        assertMemoryLeak(() -> {
            for (TestTimestampType timestampType : TestTimestampType.values()) {
                final String base = "base_" + timestampType.name();
                final String view = "mv_" + timestampType.name();
                execute("CREATE TABLE " + base + " (k INT, v DOUBLE, ts " + timestampType.getTypeName()
                        + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO " + base + " VALUES "
                        + "(1, 1000, '2024-01-01'), (2, 1001, '2024-01-02'), (null, 1002, '2024-01-03'), "
                        + "(1, 900, '2024-01-04'), (2, null, '2024-01-05')");
                drainWalAndMatViewQueues();
                execute("CREATE MATERIALIZED VIEW " + view + " AS (SELECT * FROM " + base + ") EXPIRE ROWS WHEN v <= 990");
                drainWalAndMatViewQueues();
                final String suffix = getTimestampSuffix(timestampType.getTypeName());
                // The newest k=1 row expires, so choose its older row. A NULL predicate keeps the
                // newest k=2 row. The physical plan must return winners in timestamp order, not the
                // key insertion order (1, 2, null) that LatestBy light would return.
                assertQuery("SELECT * FROM " + view + " LATEST ON ts PARTITION BY k")
                        .noLeakCheck().timestamp("ts").expectSize()
                        .withPlanContaining("LatestByAllFiltered")
                        .returns("k\tv\tts\n"
                                + "1\t1000.0\t2024-01-01T00:00:00.000000" + suffix + "\n"
                                + "null\t1002.0\t2024-01-03T00:00:00.000000" + suffix + "\n"
                                + "2\tnull\t2024-01-05T00:00:00.000000" + suffix + "\n");
            }
        });
    }

    @Test
    public void testSelectiveOrdinaryReadsKeepAsyncFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE plain (k INT, s SYMBOL INDEX, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO plain VALUES
                    (1, 'A', 1000, '2024-01-01'),
                    (2, 'B', 1001, '2024-01-02'),
                    (null, null, 1002, '2024-01-03'),
                    (1, 'A', 900, '2024-01-04'),
                    (2, 'B', null, '2024-01-05')
                    """);
            execute("CREATE VIEW plain_view AS (SELECT * FROM plain WHERE v > 990.0)");
            final int previousJitMode = sqlExecutionContext.getJitMode();
            try {
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_ENABLED}) {
                    sqlExecutionContext.setJitMode(jitMode);
                    for (int keyIndex = 0; keyIndex < 3; keyIndex++) {
                        final String key = switch (keyIndex) {
                            case 0 -> "k";
                            case 1 -> "s";
                            default -> "s, k";
                        };
                        for (int sourceIndex = 0; sourceIndex < 2; sourceIndex++) {
                            final String source = sourceIndex == 0 ? "(SELECT * FROM plain WHERE v > 990.0)" : "plain_view";
                            final String sql = "SELECT k, v FROM " + source + " LATEST ON ts PARTITION BY " + key + " ORDER BY k";
                            assertUnchanged(sql);
                            assertQuery(sql).noLeakCheck().expectSize()
                                    .withPlanContaining("LatestBy light")
                                    .withPlanContaining(jitMode == SqlJitMode.JIT_MODE_ENABLED && JitUtil.isJitSupported()
                                            ? "Async JIT Filter" : "Async Filter")
                                    .returns("""
                                            k\tv
                                            null\t1002.0
                                            1\t1000.0
                                            2\t1001.0
                                            """);
                        }
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(previousJitMode);
            }
        });
    }

    @Test
    public void testTimestampExpiryStillHoistsAfterPolicyChanges() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            execute("CREATE MATERIALIZED VIEW et AS (SELECT * FROM t), INDEX(sym)");
            drainWalAndMatViewQueues();
            final String nested = "SELECT * FROM (SELECT * FROM et) LATEST ON ts PARTITION BY sym";
            assertUnchanged(nested);
            // This flippable policy deliberately does not carry isExpiryKeepFilter. Provenance must
            // survive independently, and the direct read must retain the extracted timestamp interval.
            execute("ALTER MATERIALIZED VIEW et SET EXPIRE ROWS WHEN ts < '1970-01-03'");
            drainWalAndMatViewQueues();
            final String sql = "SELECT sym, v, ts FROM et LATEST ON ts PARTITION BY sym";
            final Outcome enabled = run(sql, true);
            final Outcome disabled = run(sql, false);
            Assert.assertNull(enabled.error);
            Assert.assertNull(disabled.error);
            Assert.assertNotEquals(disabled.plan, enabled.plan);
            Assert.assertEquals("ts", enabled.tsColumn);
            assertQuery(sql).noLeakCheck().timestamp("ts").expectSize()
                    .withPlanContaining("Interval backward scan")
                    .returns("""
                            sym\tv\tts
                            BB\t30.0\t1970-01-03T00:00:00.000000Z
                            CC\t40.0\t1970-01-04T00:00:00.000000Z
                            AA\tnull\t1970-01-05T00:00:00.000000Z
                            """);
            execute("ALTER MATERIALIZED VIEW et DROP EXPIRE ROWS");
            drainWalAndMatViewQueues();
            assertUnchanged(nested);
            assertUnchanged("SELECT * FROM (SELECT * FROM t WHERE v > 15) LATEST ON ts PARTITION BY v");
        });
    }

    /**
     * The un-hoisted form publishes no designated timestamp for many of these shapes - that is what the
     * rewrite is for - so there is nothing to compare against on those. The rewrite fires only when
     * {@code LATEST ON} names the table's designated timestamp, so that column is the only one the
     * direct read may publish. Checking it that way covers the shapes a plain comparison says nothing
     * about, which is where a {@code timestamp(ts2)} defect would otherwise hide: it changes no row.
     */
    private static void assertTimestampSurvives(String sql, Outcome plain, Outcome hoisted) {
        if (plain.tsColumn != null) {
            Assert.assertEquals("designated timestamp changed: " + sql, plain.tsColumn, hoisted.tsColumn);
        } else if (hoisted.tsColumn != null) {
            Assert.assertEquals("the hoist published a timestamp on the wrong column: " + sql,
                    latestOnColumn(sql), hoisted.tsColumn);
        }
    }

    private static void assertUnchanged(String sql) {
        final Outcome enabled = run(sql, true);
        final Outcome disabled = run(sql, false);
        Assert.assertEquals("compilation changed: " + sql, disabled.error, enabled.error);
        Assert.assertNull("query must compile: " + sql, enabled.error);
        Assert.assertEquals("unexpected hoist: " + sql, disabled.plan, enabled.plan);
        Assert.assertEquals("projection changed: " + sql, disabled.columns, enabled.columns);
        Assert.assertEquals("timestamp changed: " + sql, disabled.tsColumn, enabled.tsColumn);
        Assert.assertEquals("rows changed: " + sql, disabled.rows.toString(), enabled.rows.toString());
    }

    private static void createExpiryFixture() throws Exception {
        execute("CREATE MATERIALIZED VIEW et AS (SELECT * FROM t), INDEX(sym) EXPIRE ROWS WHEN v < 0");
        execute("CREATE MATERIALIZED VIEW en AS (SELECT * FROM n) EXPIRE ROWS WHEN v < 0");
        drainWalAndMatViewQueues();
    }

    private static void createFixture() throws Exception {
        execute("CREATE TABLE t (v DOUBLE, sym SYMBOL INDEX, ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO t VALUES
                (10.0, 'CC', '1970-01-01T00:00:00.000000Z', '1970-01-04T00:00:00.000000Z'),
                (20.0, 'BB', '1970-01-02T00:00:00.000000Z', '1970-01-03T00:00:00.000000Z'),
                (30.0, 'BB', '1970-01-03T00:00:00.000000Z', '1970-01-02T00:00:00.000000Z'),
                (40.0, 'CC', '1970-01-04T00:00:00.000000Z', '1970-01-01T00:00:00.000000Z'),
                (null, 'AA', '1970-01-05T00:00:00.000000Z', null)""");
        // a union arm carrying a LATER row for an existing key, so hoisting past the union would
        // change the rows and not merely the plan
        execute("CREATE TABLE t2 (v DOUBLE, sym SYMBOL INDEX, ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t2 VALUES (99.0, 'BB', '1970-01-09T00:00:00.000000Z', null)");
        // the same keys with no index on them
        execute("CREATE TABLE n (v DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO n VALUES
                (10.0, 'CC', '1970-01-01T00:00:00.000000Z'),
                (20.0, 'BB', '1970-01-02T00:00:00.000000Z'),
                (30.0, 'BB', '1970-01-03T00:00:00.000000Z'),
                (40.0, 'CC', '1970-01-04T00:00:00.000000Z')""");
        // a join partner with a designated timestamp, for ASOF
        execute("CREATE TABLE u (sym SYMBOL INDEX, w DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO u VALUES
                ('BB', 1.0, '1970-01-01T00:00:00.000000Z'),
                ('CC', 2.0, '1970-01-02T00:00:00.000000Z')""");
        // a join partner sharing no column name with t, so `ts` and `sym` stay unambiguous
        execute("CREATE TABLE j (jsym SYMBOL, w DOUBLE)");
        execute("INSERT INTO j VALUES ('BB', 20.0), ('CC', 40.0)");
        drainWalAndMatViewQueues();
    }

    private static String expirySql(String sql) {
        return sql.replaceAll("\\bt\\b", "et").replaceAll("\\bn\\b", "en");
    }

    /**
     * The column named by the query's first {@code LATEST ON}.
     */
    private static String latestOnColumn(String sql) {
        final int lo = sql.indexOf("LATEST ON ") + "LATEST ON ".length();
        Assert.assertTrue("no LATEST ON in: " + sql, lo > "LATEST ON ".length() - 1);
        int hi = lo;
        while (hi < sql.length() && sql.charAt(hi) != ' ') {
            hi++;
        }
        return sql.substring(lo, hi);
    }

    private static Outcome run(String sql, boolean isHoist) {
        isHoistEnabled = isHoist;
        final Outcome outcome = new Outcome();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                final RecordMetadata metadata = factory.getMetadata();
                final StringSink columns = new StringSink();
                for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                    if (i > 0) {
                        columns.putAscii(',');
                    }
                    columns.put(metadata.getColumnName(i)).putAscii(':')
                            .put(ColumnType.nameOf(metadata.getColumnType(i)));
                }
                outcome.columns = columns.toString();
                final int tsIndex = metadata.getTimestampIndex();
                outcome.tsColumn = tsIndex < 0 ? null : metadata.getColumnName(tsIndex).toString();

                final StringSink rowSink = new StringSink();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        rowSink.clear();
                        TestUtils.println(record, metadata, rowSink);
                        outcome.rows.add(rowSink.toString());
                    }
                }
                outcome.rows.sort(CharSequence::compare);
            }
            final StringSink planSink = new StringSink();
            printSql("EXPLAIN " + sql, planSink);
            outcome.plan = planSink.toString();
        } catch (Throwable e) {
            outcome.error = e.getMessage() == null ? e.getClass().getName() : e.getMessage();
        } finally {
            isHoistEnabled = true;
        }
        return outcome;
    }

    private static final class Outcome {
        final ObjList<String> rows = new ObjList<>();
        String columns;
        String error;
        String plan = "";
        String tsColumn;
    }
}
