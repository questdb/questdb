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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.CairoTestConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Differential test for the {@code LATEST ON} hoist ({@code SqlOptimiser.pushLatestByToTableModel}),
 * which rewrites a {@code LATEST ON} sitting over a plain {@code SELECT * FROM t} sub-query into a
 * direct table read.
 * <p>
 * The rewrite drops the sub-query layers and carries a hand-picked set of attributes up from the
 * table model. Every attribute it does not carry - an alias, a {@code timestamp(...)} override, a
 * LIMIT, an ORDER BY, a join - it discards silently unless a guard rejects the shape first, and a
 * guard can only be written for an attribute somebody thought of. So this test checks the rewrite's
 * outcome rather than its preconditions: it runs each shape twice, once with the rewrite on and once
 * with it off, and compares the two. An attribute nobody guarded shows up as a difference whether or
 * not anybody predicted it.
 * <p>
 * A shape goes in exactly one of three lists:
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
 * </ul>
 * Row <b>order</b> is deliberately not invariant: the sub-query form emits one row per partition key
 * in map-insertion order and the direct read emits in timestamp order, so rows are compared as a
 * multiset. The ordering callers can rely on is asserted in {@code LatestByTest}.
 * <p>
 * The designated timestamp may appear where there was none, but must never move to another column or
 * disappear. That is the {@code timestamp(ts2)} defect, which changes no row and so is invisible to a
 * result comparison on its own.
 * <p>
 * Extending this is one line: a shape the rewrite must handle, or must leave alone, goes in the
 * matching list and is compared against the un-rewritten plan from then on.
 * <p>
 * Mutation-checked: deleting any one of the joins, LIMIT, ORDER BY, UNION and alias-prefix guards, of
 * the two timestamp conditions, of the identity-projection condition, or of the latest-by handover to
 * the empty factory in {@code SqlCodeGenerator} makes this test fail. The remaining guards in
 * {@code findHoistableTableModel} - joins, latest-by, group-by, sample-by, select-model-type and an
 * intervening WHERE on a layer above the table - are redundant: the identity-projection condition
 * rejects those shapes first, so deleting one changes no plan and no test can isolate it.
 */
public class LatestByHoistEquivalenceTest extends AbstractCairoTest {

    /**
     * Shapes the rewrite fires on. Plans must differ; projection, rows and timestamp must not.
     */
    private static final String[] HOISTED = {
            "SELECT * FROM (SELECT * FROM t) LATEST ON ts PARTITION BY sym",
            // a key with no index: the rewrite is what puts the designated timestamp back
            "SELECT * FROM (SELECT * FROM n) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT * FROM t WHERE v > 15) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT * FROM t) WHERE v > 15 LATEST ON ts PARTITION BY sym",
            // a filter at each level, ANDed by the rewrite
            "SELECT * FROM (SELECT * FROM t WHERE v > 15) WHERE sym = 'BB' LATEST ON ts PARTITION BY sym",
            // filters that contradict each other collapse the table read to an empty factory
            "SELECT * FROM (SELECT * FROM t WHERE sym = 'BB') WHERE sym = 'CC' LATEST ON ts PARTITION BY sym",
            "WITH c AS (SELECT * FROM t) SELECT * FROM c LATEST ON ts PARTITION BY sym",
            // composite and non-SYMBOL partition keys
            "SELECT * FROM (SELECT * FROM t) LATEST ON ts PARTITION BY sym, v",
            "SELECT * FROM (SELECT * FROM t) LATEST ON ts PARTITION BY v",
            // the dropped layer decides column order, so the table's storage order must not surface
            "SELECT * FROM (SELECT sym, v, ts, ts2 FROM t) LATEST ON ts PARTITION BY sym",
            "SELECT ts, sym, v FROM (SELECT v, sym, ts, ts2 FROM t) LATEST ON ts PARTITION BY sym",
            // an alias on the sub-query, qualifying the outer filter
            "SELECT * FROM (SELECT * FROM t) x WHERE x.v > 15 LATEST ON ts PARTITION BY sym",
            // SAMPLE BY above the LATEST ON reads the designated timestamp the rewrite keeps
            "SELECT ts, count() FROM (SELECT * FROM t) LATEST ON ts PARTITION BY sym SAMPLE BY 1d",
            "SELECT ts, count() FROM (SELECT * FROM n) LATEST ON ts PARTITION BY sym SAMPLE BY 1d",
    };

    /**
     * Shapes the rewrite must leave alone. Both plans must be identical.
     */
    private static final String[] NOT_HOISTED = {
            // LATEST ON names a timestamp column that is not the table's designated one, so the
            // direct read - which always uses the designated one - would answer a different question
            "SELECT * FROM (SELECT * FROM t) LATEST ON ts2 PARTITION BY sym",
            // a sub-query timestamp override, in both arrangements
            "SELECT * FROM (SELECT * FROM t TIMESTAMP(ts2)) LATEST ON ts PARTITION BY sym",
            "SELECT * FROM (SELECT * FROM t TIMESTAMP(ts2)) LATEST ON ts2 PARTITION BY sym",
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
            "SELECT * FROM ((SELECT * FROM t) LATEST ON ts PARTITION BY sym) ASOF JOIN u",
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
    public void testHoistPreservesProjectionRowsAndTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();

            for (String sql : HOISTED) {
                final Outcome hoisted = run(sql, true);
                final Outcome plain = run(sql, false);
                Assert.assertNull("the hoist broke a query: " + sql + "\n  " + hoisted.error, hoisted.error);
                Assert.assertNull("does not compile un-hoisted, so it belongs in NEEDS_HOIST: " + sql
                        + "\n  " + plain.error, plain.error);
                Assert.assertNotEquals("no longer hoisted, so this shape now covers nothing: " + sql,
                        plain.plan, hoisted.plan);
                Assert.assertEquals("projection changed: " + sql, plain.columns, hoisted.columns);
                Assert.assertEquals("rows changed: " + sql, plain.rows.toString(), hoisted.rows.toString());
                if (plain.tsColumn != null) {
                    Assert.assertEquals("designated timestamp changed: " + sql, plain.tsColumn, hoisted.tsColumn);
                }
            }

            for (String sql : NOT_HOISTED) {
                final Outcome hoisted = run(sql, true);
                final Outcome plain = run(sql, false);
                Assert.assertNull("does not compile, so it pins no guard: " + sql + "\n  " + plain.error, plain.error);
                Assert.assertNull("the hoist broke a query: " + sql + "\n  " + hoisted.error, hoisted.error);
                Assert.assertEquals("the hoist fired on a shape it must leave alone: " + sql,
                        plain.plan, hoisted.plan);
            }

            for (String sql : NEEDS_HOIST) {
                final Outcome hoisted = run(sql, true);
                Assert.assertNull("does not compile even with the hoist: " + sql + "\n  " + hoisted.error, hoisted.error);
            }
        });
    }

    private static void createFixture() throws Exception {
        execute("CREATE TABLE t (v DOUBLE, sym SYMBOL INDEX, ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
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
        execute("CREATE TABLE n (v DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
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
