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
import io.questdb.cairo.TableReader;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reject-side regression corpus for factory-tree shapes that used to hide {@code read_parquet()}
 * from the mat-view external-source guard ({@code usesExternalDataSource()} walked from the
 * sub-query's top factory). Window joins are two-child shapes outside the
 * {@code AbstractJoinRecordCursorFactory} hierarchy, and unnest and select-cursor wrappers hold
 * their input without exposing it through {@code getBaseFactory()}, so each propagates the
 * property explicitly; this corpus is what proves it. Without the explicit overrides, every
 * CREATE below was accepted and produced a view refreshing against an untracked external file.
 *
 * @see MatViewExternalSourceRejectionTest for the join/set-op/wrapper shapes pinned with the
 * original guard
 */
public class MatViewExternalSourceRejectionGapTest extends AbstractCairoTest {
    private static final String EXPECTED = "non-deterministic function cannot be used in materialized view";
    private static final AtomicInteger VIEW_SEQ = new AtomicInteger();

    @Before
    public void setUp() {
        super.setUp();
        inputRoot = root;
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testRejectsExternalSourceAsCursorColumn() throws Exception {
        // read_parquet() in the select list is rewritten (emitCursors) into a cross join over a
        // select-cursor model compiled to RecordAsAFieldRecordCursorFactory, which hides the leaf.
        assertRejected(
                null,
                "v > (SELECT count() FROM (SELECT read_parquet('x.parquet') r FROM cfg))",
                "v > (SELECT count() FROM (SELECT read_parquet('x.parquet') r FROM long_sequence(1)))"
        );
    }

    @Test
    public void testRejectsExternalSourceUnderSyncWindowJoin() throws Exception {
        // cairo.sql.parallel.window.join.enabled=false routes to the serial
        // WindowJoin(Fast)RecordCursorFactory pair, which propagates neither join side.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_JOIN_ENABLED, "false");
        try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine)) {
            assertRejected(
                    ctx,
                    // keyless -> WindowJoinRecordCursorFactory
                    "v > (SELECT max(w) FROM (SELECT sum(p.price) w FROM ((SELECT * FROM read_parquet('x.parquet')) TIMESTAMP(ts)) "
                            + "WINDOW JOIN prices p RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING))",
                    // symbol-keyed -> WindowJoinFastRecordCursorFactory selection path
                    "v > (SELECT max(w) FROM (SELECT sum(p.price) w FROM ((SELECT * FROM read_parquet('xs.parquet')) TIMESTAMP(ts)) t "
                            + "WINDOW JOIN prices p ON t.sym = p.sym RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING))"
            );
        }
    }

    @Test
    public void testRejectsExternalSourceUnderUnnest() throws Exception {
        // UnnestRecordCursorFactory wraps the parquet-reading master without forwarding it.
        assertRejected(
                null,
                "v > (SELECT max(u.val) FROM (SELECT * FROM read_parquet('x.parquet')) p, UNNEST(ARRAY[1.0,2.0]) u(val))"
        );
    }

    @Test
    public void testRejectsExternalSourceUnderWindowJoinAtDefaultConfig() throws Exception {
        // Default configuration: a master without page-frame support (LIMIT wrapper) falls off the
        // parallel window-join path onto the serial factories - no config change needed.
        assertRejected(
                null,
                "v > (SELECT max(w) FROM (SELECT sum(p.price) w FROM ((SELECT * FROM read_parquet('x.parquet') LIMIT 10) TIMESTAMP(ts)) "
                        + "WINDOW JOIN prices p RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING))"
        );
    }

    private static void encodeTable(CharSequence tableName, CharSequence fileName) {
        try (
                Path path = new Path();
                PartitionDescriptor descriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader(tableName)
        ) {
            path.of(root).concat(fileName);
            engine.getConfiguration().getFilesFacade().remove(path.$());
            PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
            PartitionEncoder.encode(descriptor, path);
            Assert.assertTrue(Files.exists(path.$()));
        }
    }

    private void assertRejected(SqlExecutionContext ctx, String... predicates) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select x::int v, (x * 1_000_000)::timestamp ts from long_sequence(10))");
            encodeTable("x", "x.parquet");
            execute("create table xs as (select rnd_symbol('a','b') sym, x::double v2, (x * 1_000_000)::timestamp ts from long_sequence(10))");
            encodeTable("xs", "xs.parquet");
            execute("CREATE TABLE prices (pts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(pts) PARTITION BY DAY");
            execute("CREATE TABLE cfg (ts TIMESTAMP, k SYMBOL, lim TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (String predicate : predicates) {
                final String view = "mv_gap_" + VIEW_SEQ.incrementAndGet();
                final String sql = "CREATE MATERIALIZED VIEW " + view + " AS SELECT ts, sum(v) AS s FROM base WHERE "
                        + predicate + " SAMPLE BY 1h";
                boolean accepted = false;
                try {
                    if (ctx != null) {
                        execute(sql, ctx);
                    } else {
                        execute(sql);
                    }
                    accepted = true;
                } catch (Throwable e) {
                    final String message = String.valueOf(e.getMessage());
                    if (!message.contains(EXPECTED)) {
                        throw new AssertionError(
                                "expected the materialized-view guard to reject this predicate, but it "
                                        + "failed for an unrelated reason.\n  predicate: " + predicate
                                        + "\n  error: " + message, e);
                    }
                }
                if (accepted) {
                    // drop before failing so the leak checker reports the real problem, not the view
                    execute("DROP MATERIALIZED VIEW " + view);
                    throw new AssertionError(
                            "materialized view must reject a sub-query reading an external source; "
                                    + "the external-source property failed to propagate "
                                    + "through a wrapping factory.\n  predicate: " + predicate);
                }
            }
        });
    }
}
