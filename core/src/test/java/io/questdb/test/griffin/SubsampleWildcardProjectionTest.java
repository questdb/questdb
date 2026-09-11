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

import io.questdb.PropertyKey;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.rnd.LongSequenceFunctionFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModelWrapper;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.CairoTestConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.IdentityHashMap;

public class SubsampleWildcardProjectionTest extends AbstractCairoTest {
    private static final String ALL_ROWS = """
            ts\tx\tts1\ty
            1970-01-01T00:00:00.000010Z\t1\t1970-01-01T00:00:00.000005Z\t10
            1970-01-01T00:00:00.000020Z\t2\t1970-01-01T00:00:00.000015Z\t90
            1970-01-01T00:00:00.000030Z\t3\t1970-01-01T00:00:00.000025Z\t20
            1970-01-01T00:00:00.000040Z\t4\t1970-01-01T00:00:00.000035Z\t30
            """;
    private static final String JOIN = " FROM ca a ASOF JOIN cb b";
    private static final String MINMAX_ROWS = """
            ts\tx\tts1\ty
            1970-01-01T00:00:00.000010Z\t1\t1970-01-01T00:00:00.000005Z\t10
            1970-01-01T00:00:00.000020Z\t2\t1970-01-01T00:00:00.000015Z\t90
            """;

    private static boolean isDistinctRewriteEnabled = true;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        configurationFactory = (root, telemetry, overrides) -> new CairoTestConfiguration(root, telemetry, overrides) {
            @Override
            public boolean isSqlDistinctGroupByRewriteEnabled() {
                return isDistinctRewriteEnabled;
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testDistinctTransparentVisibleGeneratedColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertDistinctGeneratedColumnProperties(
                    "SELECT a.ts, b.y FROM ca a JOIN LATERAL (SELECT * FROM cb WHERE cb.ts <= a.ts SUBSAMPLE minmax(y, 2)) b ON true",
                    true
            );
        });
    }

    @Test
    public void testDistinctTransparentHiddenGeneratedColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertDistinctGeneratedColumnProperties(
                    "SELECT a.ts, b.value FROM ca a JOIN LATERAL (SELECT DISTINCT y AS value FROM cb WHERE cb.ts <= a.ts) b ON true",
                    false
            );
        });
    }

    @Test
    public void testDistinctTransparentHiddenAliasedGeneratedColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertDistinctGeneratedColumnProperties(
                    "SELECT a.ts, b.value FROM ca a JOIN LATERAL (SELECT DISTINCT y AS value, y AS __qdb_outer_ref__0_ts FROM cb WHERE cb.ts <= a.ts) b ON true",
                    false
            );
        });
    }

    @Test
    public void testDistinctTransparentOrdinaryColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try {
                for (int rewrite = 0; rewrite < 2; rewrite++) {
                    isDistinctRewriteEnabled = rewrite == 1;
                    assertQuery("SELECT DISTINCT x FROM ca ORDER BY x").expectSize().returns("x\n1\n2\n3\n4\n");
                    assertQuery("SELECT DISTINCT abs(x) AS value, x AS renamed FROM ca ORDER BY renamed")
                            .expectSize().returns("value\trenamed\n1\t1\n2\t2\n3\t3\n4\t4\n");
                    assertQuery("SELECT DISTINCT ARRAY[x::DOUBLE, x::DOUBLE + 1][1] AS value FROM ca ORDER BY value")
                            .expectSize().returns("value\n1.0\n2.0\n3.0\n4.0\n");
                    assertQuery("SELECT DISTINCT x, ARRAY[x::DOUBLE] AS value FROM ca ORDER BY x")
                            .expectSize().returns("x\tvalue\n1\t[1.0]\n2\t[2.0]\n3\t[3.0]\n4\t[4.0]\n");
                    final var lateral = assertQuery("SELECT a.ts, b.value FROM ca a JOIN LATERAL (SELECT DISTINCT y AS value FROM cb WHERE cb.ts <= a.ts) b ON true ORDER BY a.ts, b.value")
                            .timestamp("ts");
                    if (!isDistinctRewriteEnabled) {
                        lateral.expectSize();
                    }
                    lateral.returns("ts\tvalue\n1970-01-01T00:00:00.000010Z\t10\n1970-01-01T00:00:00.000020Z\t10\n1970-01-01T00:00:00.000020Z\t90\n1970-01-01T00:00:00.000030Z\t10\n1970-01-01T00:00:00.000030Z\t20\n1970-01-01T00:00:00.000030Z\t90\n1970-01-01T00:00:00.000040Z\t10\n1970-01-01T00:00:00.000040Z\t20\n1970-01-01T00:00:00.000040Z\t30\n1970-01-01T00:00:00.000040Z\t90\n");
                }
            } finally {
                isDistinctRewriteEnabled = true;
            }
        });
    }

    @Test
    public void testDistinctTransparentOrdinaryColumnsAliasExpressionsDisabled() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "false");
        testDistinctTransparentOrdinaryColumns();
    }

    @Test
    public void testComputedProjectionUsesCompletedValueType() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String source = " FROM (SELECT ts, concat('v', x) AS value FROM ca)";
            assertQuery("SELECT typeOf(value) AS value_type" + source)
                    .expectSize().returns("value_type\nSTRING\nSTRING\nSTRING\nSTRING\n");
            assertMarkedError("SELECT *" + source + " SUBSAMPLE minmax(^value, 2)",
                    "numeric column expected, got: STRING");
            assertMarkedError("SELECT * FROM (SELECT ts, x::STRING AS value FROM ca) SUBSAMPLE minmax(^value, 0)",
                    "numeric column expected, got: STRING");
            assertQuery("SELECT ts, abs(x) AS value FROM ca SUBSAMPLE minmax(value, 2)")
                    .timestamp("ts").withPlanContaining("CachedWindow").returns("""
                            ts\tvalue
                            1970-01-01T00:00:00.000010Z\t1
                            1970-01-01T00:00:00.000040Z\t4
                            """);
            assertMarkedError("SELECT *" + source + " SUBSAMPLE minmax(^value, 0)",
                    "numeric column expected, got: STRING");
        });
    }

    @Test
    public void testCompetingInputColumnError() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertMarkedError("SELECT ^absent AS value, ts FROM ca", "Invalid column: absent");
            assertMarkedError("SELECT ^absent AS value, ts FROM ca SUBSAMPLE minmax(missing, 0)",
                    "Invalid column: absent");
        });
    }

    @Test
    public void testCompetingInputFunctionError() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertMarkedError("SELECT ^no_such_function(x) AS value, ts FROM ca", "unknown function name: no_such_function(INT)");
            assertMarkedError("SELECT ^no_such_function(x) AS value, ts FROM ca SUBSAMPLE minmax(value, 0)",
                    "unknown function name: no_such_function(INT)");
        });
    }

    @Test
    public void testCompetingInputHiddenTimestampControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertMarkedError("SELECT no_such_function(x) AS value FROM ca ^SUBSAMPLE minmax(value, 0)",
                    "SUBSAMPLE requires a designated timestamp column; the SELECT list must include it unchanged");
            assertQuery("SELECT a.ts, a.x, b.ts, b.y" + JOIN + " SUBSAMPLE minmax(y, 2)")
                    .timestamp("ts").returns(MINMAX_ROWS);
        });
    }

    @Test
    public void testCompetingInputNamedWindowError() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertMarkedError("SELECT ts, x, row_number() OVER ^missing AS rn FROM ca",
                    "window 'missing' is not defined");
            assertMarkedError("SELECT ts, x, row_number() OVER ^missing AS rn FROM ca SUBSAMPLE minmax(x, 0)",
                    "window 'missing' is not defined");
        });
    }

    @Test
    public void testCompetingInputWhereWindowError() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertMarkedError("SELECT ts, x FROM ca WHERE ^row_number() OVER () > 1",
                    "window function is not allowed in WHERE clause");
            assertMarkedError("SELECT ts, x FROM ca WHERE ^row_number() OVER () > 1 SUBSAMPLE minmax(x, 0)",
                    "window function is not allowed in WHERE clause");
        });
    }

    @Test
    public void testCompetingInputWindowNeighborControls() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, x, row_number() OVER w AS rn FROM ca WINDOW w AS (ORDER BY ts) SUBSAMPLE minmax(x, 2)")
                    .timestamp("ts").withPlanContaining("CachedWindow").returns("""
                            ts\tx\trn
                            1970-01-01T00:00:00.000010Z\t1\t1
                            1970-01-01T00:00:00.000040Z\t4\t4
                            """);
            assertQuery("SELECT ts, x FROM ca WHERE x > 1 SUBSAMPLE minmax(x, 2)")
                    .timestamp("ts").withPlanContaining("CachedWindow").returns("""
                            ts\tx
                            1970-01-01T00:00:00.000020Z\t2
                            1970-01-01T00:00:00.000040Z\t4
                            """);
        });
    }

    @Test
    public void testDuplicateSlaveValue() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("ALTER TABLE cb RENAME COLUMN y TO x");
            final String rows = MINMAX_ROWS.replace("\ty\n", "\tx1\n");
            assertQuery("SELECT a.ts, a.x, b.ts, b.x" + JOIN + " SUBSAMPLE minmax(x1, 2)")
                    .timestamp("ts").returns(rows);
            assertQuery("SELECT *" + JOIN).timestamp("ts").noRandomAccess().expectSize()
                    .returns(ALL_ROWS.replace("\ty\n", "\tx1\n"));
            assertQuery("SELECT *" + JOIN + " SUBSAMPLE minmax(x1, 2)")
                    .timestamp("ts").withPlanContaining("CachedWindow").returns(rows);
        });
    }

    @Test
    public void testExplicitAliasControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.ts, a.x, b.ts, b.y AS value" + JOIN + " SUBSAMPLE minmax(value, 2)")
                    .timestamp("ts").withPlanContaining("CachedWindow")
                    .returns(MINMAX_ROWS.replace("\ty\n", "\tvalue\n"));
            final String sql = "SELECT a.ts, b.y AS value" + JOIN + " SUBSAMPLE minmax(y, 2)";
            assertException(sql, sql.lastIndexOf("y,"), "column not found in SELECT list: y");
        });
    }

    @Test
    public void testExplicitProjectionControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.ts, a.x, b.ts, b.y" + JOIN + " SUBSAMPLE minmax(y, 2)")
                    .timestamp("ts").withPlanContaining("CachedWindow").returns(MINMAX_ROWS);
        });
    }

    @Test
    public void testLateralWildcardPartitionRejectionControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertMarkedError("""
                    SELECT a.ts, b.y FROM ca a JOIN LATERAL (
                        SELECT * FROM cb WHERE cb.ts <= a.ts SUBSAMPLE ^minmax(y, 2)
                    ) b ON true""", "minmax() does not support PARTITION BY");
            assertMarkedError("""
                    SELECT a.ts, b.y FROM ca a JOIN LATERAL (
                        SELECT * FROM cb c ASOF JOIN ca d WHERE c.ts <= a.ts SUBSAMPLE ^minmax(y, 2)
                    ) b ON true""", "minmax() does not support PARTITION BY");
            assertQuery("SELECT a.ts, a.x, b.ts, b.y" + JOIN + " SUBSAMPLE minmax(y, 2)")
                    .timestamp("ts").returns(MINMAX_ROWS);
        });
    }

    @Test
    public void testMixedProjectionAliasCollision() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String rows = """
                    y\tts\tx\tts1\ty1
                    1\t1970-01-01T00:00:00.000010Z\t1\t1970-01-01T00:00:00.000005Z\t10
                    2\t1970-01-01T00:00:00.000020Z\t2\t1970-01-01T00:00:00.000015Z\t90
                    """;
            assertQuery("SELECT b.y * 0 + a.x AS y, *" + JOIN + " LIMIT 2")
                    .timestamp("ts").noRandomAccess().expectSize().returns(rows);
            assertQuery("SELECT b.y * 0 + a.x AS y, *" + JOIN + " SUBSAMPLE minmax(y1, 2)")
                    .timestamp("ts").withPlanContaining("CachedWindow").returns(rows);
        });
    }

    @Test
    public void testNestedWildcardSlaveValue() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM (SELECT *" + JOIN + ") SUBSAMPLE minmax(y, 2)")
                    .timestamp("ts").withPlanContaining("CachedWindow").returns(MINMAX_ROWS);
        });
    }

    @Test
    public void testNestedWrapperUnaliasedQualifiedWildcard() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // oracle: the wrapper without SUBSAMPLE exposes the designated timestamp unchanged
            assertQuery("SELECT * FROM (SELECT ca.* FROM ca) q").timestamp("ts").expectSize().returns(allPrimaryRows());
            // oracle: the same projection with enumerated columns, and with an aliased table, already compile
            assertQuery("SELECT * FROM (SELECT ts, x FROM ca) q SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
            assertQuery("SELECT * FROM (SELECT a.* FROM ca a) q SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
            // an unaliased table.* inside the wrapper must resolve exactly like the aliased form
            assertQuery("SELECT * FROM (SELECT ca.* FROM ca) q SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
            assertQuery("SELECT q.* FROM (SELECT ca.* FROM ca) q SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
            assertQuery("SELECT * FROM (SELECT ca.* FROM ca) q SUBSAMPLE minmax(x, 2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
            assertQuery("SELECT * FROM (SELECT ca.* FROM ca) q SUBSAMPLE lttb(x, 2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
            // two wrapper levels: the inner wrapper's names are re-derived once per level
            assertQuery("SELECT * FROM (SELECT * FROM (SELECT ca.* FROM ca) q) r SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
        });
    }

    @Test
    public void testNestedCteQualifiedWildcard() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("WITH q AS (SELECT ca.* FROM ca) SELECT * FROM q").timestamp("ts").expectSize().returns(allPrimaryRows());
            assertQuery("WITH q AS (SELECT ts, x FROM ca) SELECT * FROM q SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
            assertQuery("WITH q AS (SELECT ca.* FROM ca) SELECT * FROM q SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
            assertQuery("WITH q AS (SELECT ca.* FROM ca) SELECT * FROM q SUBSAMPLE lttb(x, 2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
        });
    }

    @Test
    public void testNestedWrapperRenamedDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // b.ts claims "ts" first, so the designated a.ts leaves the wrapper as ts1
            final String renamedSource = " FROM (SELECT b.ts, a.*" + JOIN + ") q";
            assertQuery("SELECT *" + renamedSource).timestamp("ts1").noRandomAccess().expectSize().returns("""
                    ts\tts1\tx
                    1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000010Z\t1
                    1970-01-01T00:00:00.000015Z\t1970-01-01T00:00:00.000020Z\t2
                    1970-01-01T00:00:00.000025Z\t1970-01-01T00:00:00.000030Z\t3
                    1970-01-01T00:00:00.000035Z\t1970-01-01T00:00:00.000040Z\t4
                    """);
            final String renamedRows = """
                    ts\tts1\tx
                    1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000010Z\t1
                    1970-01-01T00:00:00.000035Z\t1970-01-01T00:00:00.000040Z\t4
                    """;
            // oracle: the explicit projection samples by ts1
            assertQuery("SELECT ts, ts1, x" + renamedSource + " SUBSAMPLE uniform(2)")
                    .timestamp("ts1").withPlanContaining("over (order by [ts1])").returns(renamedRows);
            assertQuery("SELECT *" + renamedSource + " SUBSAMPLE uniform(2)")
                    .timestamp("ts1").withPlanContaining("over (order by [ts1])").returns(renamedRows);
            assertQuery("SELECT q.*" + renamedSource + " SUBSAMPLE uniform(2)")
                    .timestamp("ts1").withPlanContaining("over (order by [ts1])").returns(renamedRows);
            assertQuery("SELECT *" + renamedSource + " SUBSAMPLE minmax(x, 2)")
                    .timestamp("ts1").withPlanContaining("over (order by [ts1])").returns(renamedRows);

            // both branches through wildcards: b.* then a.*
            final String reversedSource = " FROM (SELECT b.*, a.*" + JOIN + ") q";
            final String reversedRows = """
                    ts\ty\tts1\tx
                    1970-01-01T00:00:00.000005Z\t10\t1970-01-01T00:00:00.000010Z\t1
                    1970-01-01T00:00:00.000035Z\t30\t1970-01-01T00:00:00.000040Z\t4
                    """;
            assertQuery("SELECT ts, y, ts1, x" + reversedSource + " SUBSAMPLE uniform(2)")
                    .timestamp("ts1").withPlanContaining("over (order by [ts1])").returns(reversedRows);
            assertQuery("SELECT *" + reversedSource + " SUBSAMPLE uniform(2)")
                    .timestamp("ts1").withPlanContaining("over (order by [ts1])").returns(reversedRows);

            // an outer collision on top of the inner rename: ts (constant), ts1 (b.ts), ts11 (a.ts)
            assertQuery("SELECT 1 AS ts, *" + renamedSource).timestamp("ts11").noRandomAccess().expectSize().returns("""
                    ts\tts1\tts11\tx
                    1\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000010Z\t1
                    1\t1970-01-01T00:00:00.000015Z\t1970-01-01T00:00:00.000020Z\t2
                    1\t1970-01-01T00:00:00.000025Z\t1970-01-01T00:00:00.000030Z\t3
                    1\t1970-01-01T00:00:00.000035Z\t1970-01-01T00:00:00.000040Z\t4
                    """);
            assertQuery("SELECT 1 AS ts, *" + renamedSource + " SUBSAMPLE uniform(2)")
                    .timestamp("ts11").withPlanContaining("over (order by [ts11])").returns("""
                            ts\tts1\tts11\tx
                            1\t1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000010Z\t1
                            1\t1970-01-01T00:00:00.000035Z\t1970-01-01T00:00:00.000040Z\t4
                            """);
        });
    }

    @Test
    public void testNestedWrapperConstantTimestampCollision() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String rows = """
                    ts\tts1\tx
                    1\t1970-01-01T00:00:00.000010Z\t1
                    1\t1970-01-01T00:00:00.000040Z\t4
                    """;
            // oracles: the explicit projection over the wrapper, and the collision placed on the outer projection
            assertQuery("SELECT ts, ts1, x FROM (SELECT 1 AS ts, * FROM ca) q SUBSAMPLE uniform(2)")
                    .timestamp("ts1").withPlanContaining("over (order by [ts1])").returns(rows);
            assertQuery("SELECT 1 AS ts, * FROM (SELECT * FROM ca) q SUBSAMPLE uniform(2)")
                    .timestamp("ts1").withPlanContaining("over (order by [ts1])").returns(rows);
            assertQuery("SELECT * FROM (SELECT 1 AS ts, * FROM ca) q SUBSAMPLE uniform(2)")
                    .timestamp("ts1").withPlanContaining("over (order by [ts1])").returns(rows);
            assertQuery("SELECT * FROM (SELECT 1 AS ts, ca.* FROM ca) q SUBSAMPLE uniform(2)")
                    .timestamp("ts1").withPlanContaining("over (order by [ts1])").returns(rows);
        });
    }

    @Test
    public void testNestedWrapperTrailingRenameControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // a.* first: the designated a.ts keeps "ts" and the trailing b.ts becomes ts1
            final String rows = """
                    ts\tx\tts1
                    1970-01-01T00:00:00.000010Z\t1\t1970-01-01T00:00:00.000005Z
                    1970-01-01T00:00:00.000040Z\t4\t1970-01-01T00:00:00.000035Z
                    """;
            assertQuery("SELECT * FROM (SELECT a.*, b.ts" + JOIN + ") q SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(rows);
            assertQuery("SELECT * FROM (SELECT ca.*, cb.ts FROM ca ASOF JOIN cb) q SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(rows);
        });
    }

    @Test
    public void testNestedWrapperHiddenTimestampControls() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String hidden = "SUBSAMPLE requires a designated timestamp column; the SELECT list must include it unchanged";
            // the primary branch's timestamp never leaves the wrapper
            assertMarkedError("SELECT * FROM (SELECT b.*" + JOIN + ") q ^SUBSAMPLE uniform(2)", hidden);
            assertMarkedError("WITH q AS (SELECT b.*" + JOIN + ") SELECT * FROM q ^SUBSAMPLE uniform(2)", hidden);
            // an explicit outer projection that drops ts1 hides it, one that keeps it samples by it
            assertMarkedError("SELECT ts, x FROM (SELECT b.*, a.*" + JOIN + ") q ^SUBSAMPLE uniform(2)", hidden);
            assertQuery("SELECT ts, ts1, x FROM (SELECT b.*, a.*" + JOIN + ") q SUBSAMPLE uniform(2)")
                    .timestamp("ts1").withPlanContaining("over (order by [ts1])").returns("""
                            ts\tts1\tx
                            1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000010Z\t1
                            1970-01-01T00:00:00.000035Z\t1970-01-01T00:00:00.000040Z\t4
                            """);
            // an unresolvable prefix reserves nothing; the mirror reports the hidden timestamp for both forms.
            // These two pins document PRE-EXISTING error precedence (the mirror runs before the expansion
            // that would report "invalid table alias"), not a designed contract; a later change may
            // legitimately switch them to the expansion's message.
            assertMarkedError("SELECT zz.* FROM ca ^SUBSAMPLE uniform(2)", hidden);
            assertMarkedError("SELECT * FROM (SELECT zz.* FROM ca) q ^SUBSAMPLE uniform(2)", hidden);
            assertMarkedError("SELECT * FROM (SELECT ^zz.* FROM ca) q", "invalid table alias");
        });
    }

    @Test
    public void testWildcardBeforeExplicitDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // b.* claims "ts" first, so the explicit designated a.ts leaves the projection as ts1
            assertQuery("SELECT b.*, a.ts" + JOIN).timestamp("ts1").noRandomAccess().expectSize().returns("""
                    ts\ty\tts1
                    1970-01-01T00:00:00.000005Z\t10\t1970-01-01T00:00:00.000010Z
                    1970-01-01T00:00:00.000015Z\t90\t1970-01-01T00:00:00.000020Z
                    1970-01-01T00:00:00.000025Z\t20\t1970-01-01T00:00:00.000030Z
                    1970-01-01T00:00:00.000035Z\t30\t1970-01-01T00:00:00.000040Z
                    """);
            final String rows = """
                    ts\ty\tts1
                    1970-01-01T00:00:00.000005Z\t10\t1970-01-01T00:00:00.000010Z
                    1970-01-01T00:00:00.000035Z\t30\t1970-01-01T00:00:00.000040Z
                    """;
            // oracle: the fully explicit projection (the parser dedups a.ts to ts1) samples by ts1
            assertQuery("SELECT b.ts, b.y, a.ts" + JOIN + " SUBSAMPLE uniform(2)")
                    .timestamp("ts1").withPlanContaining("over (order by [ts1])").returns(rows);
            assertQuery("SELECT b.*, a.ts" + JOIN + " SUBSAMPLE uniform(2)")
                    .timestamp("ts1").withPlanContaining("over (order by [ts1])").returns(rows);
            assertQuery("SELECT * FROM (SELECT b.*, a.ts" + JOIN + ") q SUBSAMPLE uniform(2)")
                    .timestamp("ts1").withPlanContaining("over (order by [ts1])").returns(rows);
        });
    }

    @Test
    public void testNestedWrapperShapes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // inner SUBSAMPLE already desugared under the wrapper (three synthetic wrapper levels)
            assertQuery("SELECT * FROM (SELECT ts, x FROM ca SUBSAMPLE uniform(4)) q SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
            assertQuery("SELECT * FROM (SELECT ca.* FROM ca SUBSAMPLE uniform(4)) q SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
            // UNION ALL wrapper with an explicit TIMESTAMP(ts): the first branch's projection names it
            assertQuery("SELECT * FROM (SELECT ts, x FROM ca UNION ALL SELECT ts, x FROM ca WHERE x < 0) TIMESTAMP(ts) SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
            assertQuery("SELECT * FROM (SELECT ca.* FROM ca UNION ALL SELECT ca.* FROM ca WHERE x < 0) TIMESTAMP(ts) SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
            // wrapper over a JOIN whose primary branch is an unaliased-star subquery
            final String joinRows = """
                    ts\tx\tts1\ty
                    1970-01-01T00:00:00.000010Z\t1\t1970-01-01T00:00:00.000005Z\t10
                    1970-01-01T00:00:00.000040Z\t4\t1970-01-01T00:00:00.000035Z\t30
                    """;
            assertQuery("SELECT * FROM (SELECT * FROM (SELECT ts, x FROM ca) a ASOF JOIN cb b) q SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(joinRows);
            assertQuery("SELECT * FROM (SELECT * FROM (SELECT ca.* FROM ca) a ASOF JOIN cb b) q SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(joinRows);
            // the wrapper is the primary join model and SUBSAMPLE sits on the join level
            assertQuery("SELECT * FROM (SELECT ts, x FROM ca) a ASOF JOIN cb b SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(joinRows);
            assertQuery("SELECT * FROM (SELECT ca.* FROM ca) a ASOF JOIN cb b SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(joinRows);
            // a user column named after the keep helper inside the wrapper: the helper escapes to __keep_subsample1
            final String keepRows = """
                    ts\tx\t__keep_subsample
                    1970-01-01T00:00:00.000010Z\t1\ttrue
                    1970-01-01T00:00:00.000040Z\t4\ttrue
                    """;
            assertQuery("SELECT * FROM (SELECT ts, x, true AS __keep_subsample FROM ca) q SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(keepRows);
            assertQuery("SELECT * FROM (SELECT ca.*, true AS __keep_subsample FROM ca) q SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(keepRows);
        });
    }

    @Test
    public void testNestedViewQualifiedWildcard() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v AS (SELECT ca.* FROM ca)");
            drainViewQueue();
            // noLeakCheck: the per-query leak battery clears the engine, which empties the view graph
            assertQuery("SELECT * FROM v").noLeakCheck().timestamp("ts").expectSize().returns(allPrimaryRows());
            assertQuery("SELECT ts, x FROM v SUBSAMPLE uniform(2)")
                    .noLeakCheck().timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
            assertQuery("SELECT * FROM v SUBSAMPLE uniform(2)")
                    .noLeakCheck().timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
            assertQuery("SELECT * FROM v SUBSAMPLE lttb(x, 2)")
                    .noLeakCheck().timestamp("ts").withPlanContaining("over (order by [ts])").returns(primaryRows());
        });
    }

    @Test
    public void testNestedSampleByWrapper() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String allRows = """
                    ts\ta
                    1970-01-01T00:00:00.000010Z\t1.0
                    1970-01-01T00:00:00.000020Z\t2.0
                    1970-01-01T00:00:00.000030Z\t3.0
                    1970-01-01T00:00:00.000040Z\t4.0
                    """;
            final String rows = """
                    ts\ta
                    1970-01-01T00:00:00.000010Z\t1.0
                    1970-01-01T00:00:00.000040Z\t4.0
                    """;
            assertQuery("SELECT * FROM (SELECT ts, avg(x) a FROM ca SAMPLE BY 10U) TIMESTAMP(ts)")
                    .timestamp("ts").expectSize().returns(allRows);
            assertQuery("SELECT * FROM (SELECT ts, avg(x) a FROM ca SAMPLE BY 10U) TIMESTAMP(ts) SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(rows);
            // the time zone forces rewriteSampleBy to wrap the aggregation in an explicit projection;
            // 10U buckets keep the same UTC boundaries, so the no-SUBSAMPLE twin reports the same rows
            assertQuery("SELECT * FROM (SELECT ts, avg(x) a FROM ca SAMPLE BY 10U ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin') TIMESTAMP(ts)")
                    .timestamp("ts").expectSize().returns(allRows);
            assertQuery("SELECT * FROM (SELECT ts, avg(x) a FROM ca SAMPLE BY 10U ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin') TIMESTAMP(ts) SUBSAMPLE uniform(2)")
                    .timestamp("ts").withPlanContaining("over (order by [ts])").returns(rows);
        });
    }

    @Test
    public void testNestedWrapperFailureThenReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the value argument fails after the mirror already ran for the wrapper shape; the same
            // compiler must then resolve a wrapper query with a clean reservation namespace
            assertCompileErrorThenReuse(
                    "SELECT * FROM (SELECT ca.* FROM ca) q SUBSAMPLE minmax(^missing, 2)",
                    "column not found in SELECT list: missing",
                    "SELECT * FROM (SELECT ca.* FROM ca) q SUBSAMPLE minmax(x, 2)"
            );
            assertCompileErrorThenReuse(
                    "SELECT * FROM (SELECT b.ts, a.*" + JOIN + ") q SUBSAMPLE minmax(^missing, 2)",
                    "column not found in SELECT list: missing",
                    "SELECT * FROM (SELECT ca.* FROM ca) q SUBSAMPLE minmax(x, 2)"
            );
        });
    }

    @Test
    public void testNullComputedValueControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String source = " FROM (SELECT ts, NULL AS value FROM ca)";
            assertQuery("SELECT typeOf(value) AS value_type" + source)
                    .expectSize().returns("value_type\nNULL\nNULL\nNULL\nNULL\n");
            assertMarkedError("SELECT *" + source + " SUBSAMPLE minmax(^value, 2)",
                    "numeric column expected, got: NULL");
            assertQuery("SELECT ts, NULL::DOUBLE AS value FROM ca SUBSAMPLE minmax(value, 2)")
                    .timestamp("ts").withPlanContaining("CachedWindow").returns("ts\tvalue\n");
            assertMarkedError("SELECT ts, NULL::STRING AS value FROM ca SUBSAMPLE minmax(^value, 0)",
                    "numeric column expected, got: STRING");
        });
    }

    @Test
    public void testNullProjectionUsesCompletedValueType() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertMarkedError("SELECT ts, NULL AS value FROM ca SUBSAMPLE minmax(^value, 0)",
                    "numeric column expected, got: NULL");
        });
    }

    @Test
    public void testOrdinaryWildcardControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT *" + JOIN).timestamp("ts").noRandomAccess().expectSize().returns(ALL_ROWS);
        });
    }

    @Test
    public void testPrimaryWildcardControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String rows = """
                    ts\tx\tts1\ty
                    1970-01-01T00:00:00.000010Z\t1\t1970-01-01T00:00:00.000005Z\t10
                    1970-01-01T00:00:00.000040Z\t4\t1970-01-01T00:00:00.000035Z\t30
                    """;
            assertQuery("SELECT *" + JOIN + " SUBSAMPLE minmax(x, 2)")
                    .timestamp("ts").withPlanContaining("CachedWindow").returns(rows);
        });
    }

    @Test
    public void testQualifiedArgumentAndMissingColumnControls() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String qualified = "SELECT *" + JOIN + " SUBSAMPLE minmax(b.y, 2)";
            assertException(qualified, qualified.lastIndexOf("b.y"),
                    "qualified column names are not supported in SUBSAMPLE arguments; use the unqualified SELECT list name");
            final String missing = "SELECT *" + JOIN + " SUBSAMPLE minmax(missing, 2)";
            assertException(missing, missing.lastIndexOf("missing"), "column not found in SELECT list: missing");
            assertQuery("SELECT a.ts, a.x, b.ts, b.y" + JOIN + " SUBSAMPLE minmax(y, 2)")
                    .timestamp("ts").returns(MINMAX_ROWS);
        });
    }

    @Test
    public void testQualifiedWildcardSlaveValue() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.*, b.*" + JOIN).timestamp("ts").noRandomAccess().expectSize().returns(ALL_ROWS);
            assertQuery("SELECT a.*, b.*" + JOIN + " SUBSAMPLE minmax(y, 2)")
                    .timestamp("ts").withPlanContaining("CachedWindow").returns(MINMAX_ROWS);
        });
    }

    @Test
    public void testReversedWildcardUsesCompletedValueType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ca (ts TIMESTAMP, x STRING) TIMESTAMP(ts)");
            execute("CREATE TABLE cb (ts TIMESTAMP, x INT) TIMESTAMP(ts)");
            execute("INSERT INTO ca VALUES (10, 'a'), (20, 'b'), (30, 'c'), (40, 'd')");
            execute("INSERT INTO cb VALUES (5, 10), (15, 90), (25, 20), (35, 30)");
            final String rows = """
                    ts\tx\tts1\tx1
                    1970-01-01T00:00:00.000005Z\t10\t1970-01-01T00:00:00.000010Z\ta
                    1970-01-01T00:00:00.000015Z\t90\t1970-01-01T00:00:00.000020Z\tb
                    """;
            assertQuery("SELECT b.ts, b.x, a.ts, a.x" + JOIN + " SUBSAMPLE minmax(x, 2)")
                    .timestamp("ts1").returns(rows);
            assertQuery("SELECT b.*, a.*" + JOIN + " LIMIT 2")
                    .timestamp("ts1").noRandomAccess().expectSize().returns(rows);
            assertQuery("SELECT b.*, a.*" + JOIN + " SUBSAMPLE minmax(x, 2)")
                    .timestamp("ts1").withPlanContaining("CachedWindow").returns(rows);
        });
    }

    @Test
    public void testTrailingDuplicateExpressionControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Actual expansion rejects this order; do not manufacture a suffixed output alias.
            assertException("SELECT *, -b.y AS y" + JOIN, 0, "Duplicate column [name=y]");
        });
    }

    @Test
    public void testUnionBoundaryUsesCompletedValueType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ua (ts TIMESTAMP, x LONG) TIMESTAMP(ts)");
            execute("CREATE TABLE ub (ts TIMESTAMP, x STRING) TIMESTAMP(ts)");
            execute("INSERT INTO ua VALUES (10, 10)");
            execute("INSERT INTO ub VALUES (20, '20')");
            final String source = " FROM (SELECT ts, x FROM ua UNION ALL SELECT ts, x FROM ub) TIMESTAMP(ts)";
            assertQuery("SELECT typeOf(x) AS value_type" + source)
                    .noRandomAccess().expectSize().returns("value_type\nSTRING\nSTRING\n");
            assertMarkedError("SELECT *" + source + " SUBSAMPLE minmax(^x, 0)",
                    "numeric column expected, got: STRING");
            assertMarkedError("SELECT ts, x" + source + " SUBSAMPLE minmax(^x, 0)",
                    "numeric column expected, got: STRING");
        });
    }

    @Test
    public void testUnionNumericWideningControl() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ua (ts TIMESTAMP, x LONG) TIMESTAMP(ts)");
            execute("CREATE TABLE ub (ts TIMESTAMP, y DOUBLE) TIMESTAMP(ts)");
            execute("INSERT INTO ua VALUES (10, 10), (20, 90)");
            execute("INSERT INTO ub VALUES (30, 20.5), (40, 30.5)");
            final String source = " FROM (SELECT ts, x FROM ua UNION ALL SELECT ts, y FROM ub) TIMESTAMP(ts)";
            assertQuery("SELECT typeOf(x) AS value_type" + source)
                    .noRandomAccess().expectSize().returns("value_type\nDOUBLE\nDOUBLE\nDOUBLE\nDOUBLE\n");
            final String rows = """
                    ts\tx
                    1970-01-01T00:00:00.000010Z\t10.0
                    1970-01-01T00:00:00.000020Z\t90.0
                    """;
            assertQuery("SELECT ts, x" + source + " SUBSAMPLE minmax(x, 2)")
                    .timestamp("ts").withPlanContaining("CachedWindow").returns(rows);
            assertQuery("SELECT *" + source + " SUBSAMPLE minmax(x, 2)")
                    .timestamp("ts").withPlanContaining("CachedWindow").returns(rows);
        });
    }

    @Test
    public void testSetOperationSymbolTypeControls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE symbols (x SYMBOL, k LONG)");
            execute("INSERT INTO symbols VALUES ('a', 1)");
            assertQuery("SELECT typeOf(x) AS value_type FROM (SELECT x FROM symbols UNION ALL SELECT x AS y FROM symbols)")
                    .noRandomAccess().expectSize().returns("value_type\nSYMBOL\nSYMBOL\n");
            assertQuery("SELECT typeOf(x) AS value_type FROM (SELECT x FROM symbols UNION ALL SELECT NULL AS y FROM symbols)")
                    .noRandomAccess().expectSize().returns("value_type\nSTRING\nSTRING\n");
            assertQuery("SELECT typeOf(x) AS value_type FROM (SELECT x FROM symbols UNION ALL (SELECT x AS y FROM symbols UNION ALL SELECT x AS z FROM symbols))")
                    .noRandomAccess().expectSize().returns("value_type\nSYMBOL\nSYMBOL\nSYMBOL\n");
            // These are metadata oracles for different set factories. Infer their access mode,
            // but retain the deterministic second pass and calculateSize checks.
            assertQuery("SELECT typeOf(x) AS value_type FROM (SELECT x, k FROM symbols INTERSECT SELECT x AS y, k FROM symbols)")
                    .inferRandomAccess().sizeMayVary().returns("value_type\nSYMBOL\n");
            assertQuery("SELECT typeOf(x) AS value_type FROM (SELECT x, k FROM symbols INTERSECT SELECT x AS y, k::DOUBLE FROM symbols)")
                    .inferRandomAccess().sizeMayVary().returns("value_type\nSTRING\n");
            assertQuery("SELECT typeOf(x) AS value_type FROM (SELECT x, k FROM symbols EXCEPT SELECT x AS y, k FROM symbols WHERE false)")
                    .inferRandomAccess().sizeMayVary().returns("value_type\nSYMBOL\n");
            assertQuery("SELECT typeOf(x) AS value_type FROM (SELECT x, k FROM symbols EXCEPT SELECT x AS y, k::DOUBLE FROM symbols WHERE false)")
                    .inferRandomAccess().sizeMayVary().returns("value_type\nSTRING\n");
        });
    }

    @Test
    public void testWildcardErrorPrecedenceControls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE nots (ts TIMESTAMP, x STRING)");
            execute("CREATE TABLE typed (ts TIMESTAMP, x STRING) TIMESTAMP(ts)");
            ObjList<String> methods = new ObjList<>();
            methods.add("m4");
            methods.add("minmax");
            methods.add("lttb");
            for (int i = 0; i < methods.size(); i++) {
                final String method = methods.getQuick(i);
                assertMarkedError("SELECT * FROM nots SUBSAMPLE " + method + "(^missing, 0)",
                        "column not found in SELECT list: missing");
                assertMarkedError("SELECT * FROM typed SUBSAMPLE " + method + "(^missing, 0)",
                        "column not found in SELECT list: missing");
                assertMarkedError("SELECT * FROM nots ^SUBSAMPLE " + method + "(x, 0)",
                        "SUBSAMPLE requires a designated timestamp column; the query source has no designated timestamp");
                assertMarkedError("SELECT * FROM typed SUBSAMPLE " + method + "(^x, 0)",
                        "numeric column expected, got: STRING");
                assertMarkedError("SELECT * FROM typed SUBSAMPLE " + method + "(^typed.x, 0)",
                        "qualified column names are not supported in SUBSAMPLE arguments; use the unqualified SELECT list name");
            }
            createTables();
            assertMarkedError("SELECT b.*" + JOIN + " SUBSAMPLE minmax(^missing, 0)",
                    "column not found in SELECT list: missing");
            assertQuery("SELECT a.ts, a.x, b.ts, b.y" + JOIN + " SUBSAMPLE minmax(y, 2)")
                    .timestamp("ts").returns(MINMAX_ROWS);
        });
    }

    @Test
    public void testWildcardSdtErrorPrecedenceControls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE nots (ts TIMESTAMP, x INT)");
            execute("CREATE TABLE typed (ts TIMESTAMP, x STRING) TIMESTAMP(ts)");
            assertMarkedError("SELECT * FROM nots SUBSAMPLE ^sdt(missing, -1)",
                    "SUBSAMPLE requires a designated timestamp column; the query source has no designated timestamp");
            assertMarkedError("SELECT * FROM typed SUBSAMPLE sdt(^missing, -1)",
                    "column not found in SELECT list: missing");
            assertMarkedError("SELECT * FROM typed SUBSAMPLE sdt(x, ^-1)",
                    "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
        });
    }

    @Test
    public void testWildcardSingleInvalidControls() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertMarkedError("SELECT * FROM ca SUBSAMPLE minmax(x, ^0)", "target points must be at least 2");
            assertMarkedError("SELECT * FROM ca SUBSAMPLE minmax(^missing, 2)", "column not found in SELECT list: missing");
            assertMarkedError("SELECT * FROM ca SUBSAMPLE minmax(^ts, 2)", "numeric column expected, got: TIMESTAMP");
            assertMarkedError("SELECT b.*" + JOIN + " ^SUBSAMPLE uniform(2)",
                    "SUBSAMPLE requires a designated timestamp column; the SELECT list must include it unchanged");
            assertQuery("SELECT a.ts, a.x, b.ts, b.y" + JOIN + " SUBSAMPLE minmax(y, 2)")
                    .timestamp("ts").returns(MINMAX_ROWS);
        });
    }

    @Test
    public void testWildcardSlaveValue() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT *" + JOIN + " SUBSAMPLE minmax(y, 2)")
                    .timestamp("ts").withPlanContaining("CachedWindow").returns(MINMAX_ROWS);
        });
    }

    @Test
    public void testPendingSubsampleRecipeUnchangedAfterValidation() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertRecipeGeneration("SELECT * FROM ca SUBSAMPLE minmax(x, 2)", null, -1, 1);
            assertRecipeGeneration("SELECT * FROM ca SUBSAMPLE minmax(x, 1 + 1)", null, -1, 1);
            assertQuery("SELECT * FROM ca SUBSAMPLE minmax(x, 1 + 1)").timestamp("ts").returns(primaryRows());
        });
    }

    @Test
    public void testPendingSubsampleRecipeUnchangedAfterTargetFailure() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertMarkedRecipeError("SELECT * FROM ca SUBSAMPLE minmax(x, ^0)", "target points must be at least 2");
            assertMarkedRecipeError("SELECT * FROM ca SUBSAMPLE minmax(x, (^absent AND true) AND false)", "Invalid column: absent");
        });
    }

    @Test
    public void testPendingSubsampleRawGapOrderAndRecipe() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertRecipeGeneration("SELECT * FROM ca SUBSAMPLE lttb(x, 3, '1h')", null, -1, 1);
            assertMarkedRecipeError("SELECT * FROM ca SUBSAMPLE lttb(x, 3, '1h' ^|| '')",
                    "gap threshold must be a string constant such as '1h'");
            assertMarkedRecipeError("SELECT * FROM ca SUBSAMPLE lttb(x, 3, '1h'^::STRING)",
                    "gap threshold must be a string constant such as '1h'");
            assertMarkedRecipeError("SELECT ts, x::STRING AS value FROM ca SUBSAMPLE lttb(^value, 0, 3)",
                    "numeric column expected, got: STRING");
            assertMarkedRecipeError("SELECT * FROM ca SUBSAMPLE lttb(x, ^0, 3)", "target points must be at least 2");
            assertMarkedRecipeError("SELECT * FROM ca SUBSAMPLE lttb(x, 3, ^3)",
                    "gap threshold must be a string constant such as '1h'");
        });
    }

    @Test
    public void testPendingSubsampleRecipeSnapshotSensitivity() throws Exception {
        final ObjectPool<ExpressionNode> pool = new ObjectPool<>(ExpressionNode.FACTORY, 16);
        final ExpressionNode constant = pool.next().of(ExpressionNode.CONSTANT, "2", 0, 3);
        final ExpressionNode unchanged = ExpressionNode.deepClone(pool, constant);
        final NodeSnapshot constantSnapshot = new NodeSnapshot(constant);
        Assert.assertEquals(0, constantSnapshot.foldValue);
        constant.reassociateConstants(false);
        Assert.assertTrue(ExpressionNode.compareNodesExact(unchanged, constant));
        Assert.assertEquals(2, readConstFoldLongValue(constant));
        Assert.assertThrows(AssertionError.class, constantSnapshot::assertUnchanged);
        final ExpressionNode lhs = pool.next().of(ExpressionNode.OPERATION, "and", 1, 10);
        lhs.paramCount = 2;
        lhs.lhs = pool.next().of(ExpressionNode.LITERAL, "absent", 0, 1);
        lhs.rhs = pool.next().of(ExpressionNode.CONSTANT, "true", 0, 14);
        final ExpressionNode root = pool.next().of(ExpressionNode.OPERATION, "and", 1, 20);
        root.paramCount = 2;
        root.lhs = lhs;
        root.rhs = pool.next().of(ExpressionNode.CONSTANT, "false", 0, 24);
        final NodeSnapshot links = new NodeSnapshot(root);
        root.reassociateConstants(false);
        Assert.assertNotSame(lhs, root.lhs);
        Assert.assertThrows(AssertionError.class, links::assertUnchanged);
    }

    @Test
    public void testPendingSubsampleBothParserPaths() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final int[] parses = new int[2];
            final String name = "subsample_counted_target";
            registerFactory(name, new FunctionFactory() {
                @Override
                public String getSignature() {
                    return name + "()";
                }

                @Override
                public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext context) {
                    parses[context.getWindowContext().isEmpty() ? 0 : 1]++;
                    return new IntFunction() {
                        @Override
                        public int getInt(Record rec) {
                            return 2;
                        }

                        @Override
                        public boolean isConstant() {
                            return true;
                        }
                    };
                }
            });
            try {
                for (int light = 0; light < 2; light++) {
                    setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 1 ? "true" : "false");
                    final String sql = "SELECT * FROM ca SUBSAMPLE minmax(x, " + name + "())";
                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        final IQueryModel model = (IQueryModel) compiler.generateExecutionModel(sql, sqlExecutionContext);
                        final ObjList<RecipeSnapshot> snapshots = snapshotRecipes(model, 1);
                        parses[0] = parses[1] = 0;
                        try (RecordCursorFactory factory = compiler.generateSelectWithRetries(model, null, sqlExecutionContext, false)) {
                            Assert.assertEquals(1, parses[0]);
                            Assert.assertEquals(2, parses[1]);
                            assertRecipesUnchanged(snapshots);
                            assertFactory(factory).withContext(sqlExecutionContext).timestamp("ts").returns(primaryRows());
                        }
                        assertRecipesUnchanged(snapshots);
                    }
                    assertQuery(sql).assertsPlanContaining(light == 1 ? "CachedWindowLightSelect" : "Filter filter: __keep_subsample\n        CachedWindow\n          unorderedFunctions: [minmax(ts,x,2) over (order by [ts])]");
                }
            } finally {
                engine.getFunctionFactoryCache().getFactories().remove(name);
            }
        });
    }

    @Test
    public void testPendingSubsampleFactoryRegeneration() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 1 ? "true" : "false");
                bindVariableService.setInt("target", 2);
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    final IQueryModel model = (IQueryModel) compiler.generateExecutionModel(
                            "SELECT * FROM ca SUBSAMPLE minmax(x, :target)", sqlExecutionContext);
                    final ObjList<RecipeSnapshot> snapshots = snapshotRecipes(model, 1);
                    final ObjectPool<ExpressionNode> filters = new ObjectPool<>(ExpressionNode.FACTORY, 32);
                    final IQueryModel filter = findKeepFilter(model);
                    Assert.assertNotNull(filter);
                    IQueryModel.backupWhereClause(filters, model);
                    // Codegen consumes ordinary WHERE nodes. Follow its existing fallback lifecycle,
                    // without restoring or replacing any part of the private SUBSAMPLE recipe.
                    for (int pass = 0; pass < 4; pass++) {
                        final int target = pass == 2 ? 4 : 2;
                        bindVariableService.setInt("target", target);
                        assertRecipesUnchanged(snapshots);
                        IQueryModel.restoreWhereClause(filters, model);
                        assertRecipesUnchanged(snapshots);
                        Assert.assertEquals("__keep_subsample", filter.getWhereClause().token);
                        try (RecordCursorFactory factory = compiler.generateSelectWithRetries(model, null, sqlExecutionContext, false)) {
                            assertFactory(factory).withContext(sqlExecutionContext).timestamp("ts").returns(target == 2 ? primaryRows() : allPrimaryRows());
                        }
                        Assert.assertNull(filter.getWhereClause());
                        assertRecipesUnchanged(snapshots);
                    }
                }
            }
            assertRecipeGeneration("WITH selected AS (SELECT * FROM ca SUBSAMPLE minmax(x, 2)) "
                    + "SELECT * FROM selected UNION ALL SELECT * FROM selected", null, -1, 2);
            assertQuery("WITH selected AS (SELECT * FROM ca SUBSAMPLE minmax(x, 2)) "
                    + "SELECT * FROM selected UNION ALL SELECT * FROM selected")
                    .noRandomAccess().returns(primaryRows() + primaryRows().substring(5));
        });
    }

    @Test
    public void testPendingSubsampleInputGenerationCount() throws Exception {
        assertMemoryLeak(() -> {
            final int[] counts = new int[5];
            final String name = "subsample_counted_input";
            registerFactory(name, new FunctionFactory() {
                @Override
                public String getSignature() {
                    return name + "(v)";
                }

                @Override
                public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext context) throws SqlException {
                    counts[0]++;
                    final Function delegate = new LongSequenceFunctionFactory().newInstance(position, args, argPositions, configuration, context);
                    final RecordCursorFactory base = delegate.getRecordCursorFactory();
                    return new CursorFunction(new AbstractRecordCursorFactory(base.getMetadata()) {
                        @Override
                        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
                            counts[1]++;
                            final RecordCursor cursor = base.getCursor(executionContext);
                            return new RecordCursor() {
                                @Override
                                public void close() {
                                    counts[4]++;
                                    cursor.close();
                                }

                                @Override
                                public Record getRecord() {
                                    return cursor.getRecord();
                                }

                                @Override
                                public Record getRecordB() {
                                    return cursor.getRecordB();
                                }

                                @Override
                                public boolean hasNext() {
                                    counts[3]++;
                                    return cursor.hasNext();
                                }

                                @Override
                                public long preComputedStateSize() {
                                    return cursor.preComputedStateSize();
                                }

                                @Override
                                public void recordAt(Record record, long rowId) {
                                    cursor.recordAt(record, rowId);
                                }

                                @Override
                                public long size() {
                                    return cursor.size();
                                }

                                @Override
                                public void toTop() {
                                    cursor.toTop();
                                }
                            };
                        }

                        @Override
                        public boolean recordCursorSupportsRandomAccess() {
                            return base.recordCursorSupportsRandomAccess();
                        }

                        @Override
                        public void toPlan(PlanSink sink) {
                            sink.type(name).child(base);
                        }

                        @Override
                        protected void _close() {
                            counts[2]++;
                            delegate.close();
                        }
                    });
                }
            });
            try {
                final String sql = "SELECT * FROM (SELECT x::TIMESTAMP AS ts, x FROM " + name + "(4)) TIMESTAMP(ts) SUBSAMPLE minmax(x, 2)";
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    final IQueryModel model = (IQueryModel) compiler.generateExecutionModel(sql, sqlExecutionContext);
                    final int enumerations = counts[0];
                    final int cursorOpens = counts[1];
                    try (RecordCursorFactory factory = compiler.generateSelectWithRetries(model, null, sqlExecutionContext, false)) {
                        // Enumeration already creates this table function; generation consumes it.
                        Assert.assertEquals(1, enumerations);
                        Assert.assertEquals(enumerations, counts[0]);
                        Assert.assertEquals(0, cursorOpens);
                        Assert.assertEquals(cursorOpens, counts[1]);
                        Assert.assertEquals(0, counts[3]);
                        assertFactory(factory).withContext(sqlExecutionContext).timestamp("ts").returns("ts\tx\n1970-01-01T00:00:00.000001Z\t1\n1970-01-01T00:00:00.000004Z\t4\n");
                        Assert.assertTrue("ordinary iteration must exercise the counter", counts[3] > 0);
                    }
                }
                final int opens = counts[1];
                final int iterations = counts[3];
                final int factories = counts[0];
                assertQuery(sql).assertsPlanContaining(name);
                // ExplainPlanFactory initializes its base cursor once, without iterating it.
                Assert.assertEquals(opens + 1, counts[1]);
                Assert.assertEquals(iterations, counts[3]);
                Assert.assertEquals(factories + 1, counts[0]);
                Assert.assertEquals(counts[0], counts[2]);
                Assert.assertEquals(counts[1], counts[4]);
            } finally {
                engine.getFunctionFactoryCache().getFactories().remove(name);
            }
        });
    }

    @Test
    public void testQuotedDotOrdinaryWildcardProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM (SELECT ts, renamed AS \"v.dot\" FROM (SELECT ts, x AS renamed FROM ca))")
                    .timestamp("ts").expectSize().returns(allPrimaryRows().replace("\tx\n", "\tv.dot\n"));
        });
    }

    @Test
    public void testQuotedDotDirectExplicitProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, x AS \"v.dot\" FROM ca")
                    .timestamp("ts").expectSize().returns(allPrimaryRows().replace("\tx\n", "\tv.dot\n"));
            assertQuery("SELECT ts, x AS \"v.dot\" FROM ca SUBSAMPLE minmax(\"v.dot\", 2)")
                    .timestamp("ts").returns(primaryRows().replace("\tx\n", "\tv.dot\n"));
        });
    }

    @Test
    public void testBoundValueAliasThroughWindowTranslation() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM (SELECT a.ts AS time, b.y AS renamed" + JOIN + ") SUBSAMPLE minmax(renamed, 2)")
                    .timestamp("time").returns("time\trenamed\n1970-01-01T00:00:00.000010Z\t10\n1970-01-01T00:00:00.000020Z\t90\n");
            assertQuery("SELECT * FROM (SELECT ts, renamed AS \"v.dot\" FROM (SELECT ts, x AS renamed FROM ca)) SUBSAMPLE minmax(\"v.dot\", 2)")
                    .timestamp("ts").returns(primaryRows().replace("\tx\n", "\tv.dot\n"));
            assertQuery("SELECT ts, abs(x)::DOUBLE AS value FROM ca SUBSAMPLE minmax(value, 2)")
                    .timestamp("ts").returns("ts\tvalue\n1970-01-01T00:00:00.000010Z\t1.0\n1970-01-01T00:00:00.000040Z\t4.0\n");
        });
    }

    @Test
    public void testWildcardValueMethodDispatch() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final ObjList<String> clauses = new ObjList<>();
            clauses.add("m4(y, 4)");
            clauses.add("minmax(y, 2)");
            clauses.add("lttb(y, 2)");
            clauses.add("lttb(y, 2, '1h')");
            final String endpoints = MINMAX_ROWS.substring(0, MINMAX_ROWS.indexOf("1970-01-01T00:00:00.000020Z"))
                    + ALL_ROWS.substring(ALL_ROWS.indexOf("1970-01-01T00:00:00.000040Z"));
            for (int i = 0; i < clauses.size(); i++) {
                final String rows = i == 0 ? ALL_ROWS : i == 1 ? MINMAX_ROWS : endpoints;
                assertQuery("SELECT a.ts, a.x, b.ts, b.y" + JOIN + " SUBSAMPLE " + clauses.getQuick(i)).timestamp("ts").returns(rows);
                assertQuery("SELECT *" + JOIN + " SUBSAMPLE " + clauses.getQuick(i)).timestamp("ts").withPlanContaining("CachedWindow").returns(rows);
            }
            assertQuery("SELECT * FROM (SELECT ts, x::DOUBLE AS value FROM ca) SUBSAMPLE sdt(value, 0.5)")
                    .timestamp("ts").returns("ts\tvalue\n1970-01-01T00:00:00.000010Z\t1.0\n1970-01-01T00:00:00.000040Z\t4.0\n");
        });
    }

    @Test
    public void testPendingMissingTimestampPrecedenceAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE nots (ts TIMESTAMP, x INT)");
            final ObjList<String> methods = new ObjList<>();
            methods.add("m4");
            methods.add("minmax");
            methods.add("lttb");
            for (int i = 0; i < methods.size(); i++) {
                final String method = methods.getQuick(i);
                assertCompileErrorThenReuse("SELECT * FROM nots SUBSAMPLE " + method + "(^missing, 0)", "column not found in SELECT list: missing");
                assertCompileErrorThenReuse("SELECT x FROM ca SUBSAMPLE " + method + "(^ca.x, 0)", "qualified column names are not supported in SUBSAMPLE arguments");
                assertCompileErrorThenReuse("SELECT * FROM nots SUBSAMPLE " + method + "(^abs(x), 0)", "SUBSAMPLE value argument must be a column name; alias the expression");
                assertCompileErrorThenReuse("SELECT * FROM nots ^SUBSAMPLE " + method + "(x, 0)", "the query source has no designated timestamp");
                assertCompileErrorThenReuse("SELECT x FROM ca ^SUBSAMPLE " + method + "(x, 0)", "the SELECT list must include it unchanged");
            }
            assertCompileErrorThenReuse("SELECT * FROM nots SUBSAMPLE ^sdt(missing, -1)", "the query source has no designated timestamp");
            assertCompileErrorThenReuse("SELECT x FROM ca SUBSAMPLE ^sdt(missing, -1)", "the SELECT list must include it unchanged");
        });
    }

    @Test
    public void testPendingSubsampleFailureThenReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertCompileErrorThenReuse("SELECT * FROM ca SUBSAMPLE minmax(^missing, 2)", "column not found in SELECT list: missing");
            assertCompileErrorThenReuse("SELECT ts, x::STRING AS value FROM ca SUBSAMPLE minmax(^value, 0)", "numeric column expected, got: STRING");
            assertCompileErrorThenReuse("SELECT * FROM ca SUBSAMPLE minmax(x, ^0)", "target points must be at least 2");
            assertCompileErrorThenReuse("SELECT * FROM ca SUBSAMPLE lttb(x, 3, ^3)", "gap threshold must be a string constant");
            assertCompileErrorThenReuse("SELECT ^no_such_function(x) AS value, ts FROM ca SUBSAMPLE minmax(value, 0)", "unknown function name: no_such_function(INT)");
            assertCompileErrorThenReuse("SELECT a.ts, b.y FROM ca a JOIN LATERAL (SELECT * FROM cb WHERE cb.ts <= a.ts SUBSAMPLE ^minmax(y, 2)) b ON true", "minmax() does not support PARTITION BY");
        });
    }

    @Test
    public void testPendingLateralWindowVisibility() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertCompileErrorThenReuse("SELECT a.ts, b.y FROM ca a JOIN LATERAL (SELECT * FROM cb WHERE cb.ts <= a.ts SUBSAMPLE ^minmax(y, 2) LIMIT 2) b ON true", "minmax() does not support PARTITION BY");
            assertQuery("SELECT * FROM (SELECT * FROM ca SUBSAMPLE minmax(x, 2)) a JOIN LATERAL (SELECT 7 AS k) b ON true")
                    .timestamp("ts").noRandomAccess().returns("ts\tx\tk\n1970-01-01T00:00:00.000010Z\t1\t7\n1970-01-01T00:00:00.000040Z\t4\t7\n");
            assertQuery("WITH selected AS (SELECT * FROM ca SUBSAMPLE minmax(x, 2)) SELECT * FROM selected WHERE x = 1 UNION ALL SELECT * FROM selected WHERE x = 4")
                    .noRandomAccess().returns(primaryRows());
        });
    }

    @Test
    public void testWildcardCompletedAggregationBoundary() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT DISTINCT *" + JOIN + " SUBSAMPLE minmax(y, 2)").returns(MINMAX_ROWS);
            final String rows = "ts\tvalue\n1970-01-01T00:00:00.000010Z\t10\n1970-01-01T00:00:00.000020Z\t90\n";
            assertQuery("SELECT a.ts, sum(b.y) AS value" + JOIN + " GROUP BY a.ts SUBSAMPLE minmax(value, 2)")
                    .withPlanContaining("CachedWindow").returns(rows);
            assertQuery("SELECT * FROM (SELECT a.ts, sum(b.y) AS value" + JOIN + " GROUP BY a.ts ORDER BY ts) TIMESTAMP(ts) SUBSAMPLE minmax(value, 2)")
                    .timestamp("ts").returns(rows);
            assertQuery("SELECT a.ts, sum(b.y) AS value" + JOIN + " SAMPLE BY 10U SUBSAMPLE minmax(value, 2)")
                    .timestamp("ts").withPlanContaining("CachedWindow").returns(rows);
        });
    }

    @Test
    public void testWildcardOrderingLimitAndUnionBranches() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String last = "1970-01-01T00:00:00.000040Z\t4\n";
            assertQuery("SELECT * FROM ca SUBSAMPLE minmax(x, 2) ORDER BY ts DESC").timestampDesc("ts")
                    .returns("ts\tx\n" + last + "1970-01-01T00:00:00.000010Z\t1\n");
            assertQuery("SELECT * FROM ca SUBSAMPLE minmax(x, 2) LIMIT 1").timestamp("ts")
                    .returns("ts\tx\n1970-01-01T00:00:00.000010Z\t1\n");
            assertQuery("SELECT * FROM (SELECT * FROM ca ORDER BY ts DESC) SUBSAMPLE minmax(x, 2)").timestampDesc("ts")
                    .returns("ts\tx\n" + last + "1970-01-01T00:00:00.000010Z\t1\n");
            assertQuery("SELECT * FROM (SELECT * FROM ca SUBSAMPLE minmax(x, 4)) SUBSAMPLE minmax(x, 2)")
                    .timestamp("ts").returns(primaryRows());
            assertQuery("(SELECT * FROM ca SUBSAMPLE minmax(x, 2) LIMIT 1) UNION ALL (SELECT * FROM ca SUBSAMPLE minmax(x, 2) LIMIT -1)")
                    .noRandomAccess().returns(primaryRows());
        });
    }

    @Test
    public void testWildcardDerivedSourceProducers() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM (SELECT x::TIMESTAMP AS ts, x FROM long_sequence(4)) TIMESTAMP(ts) SUBSAMPLE minmax(x, 2)")
                    .timestamp("ts").returns("ts\tx\n1970-01-01T00:00:00.000001Z\t1\n1970-01-01T00:00:00.000004Z\t4\n");
            execute("CREATE TABLE arrays (ts TIMESTAMP, a DOUBLE[]) TIMESTAMP(ts)");
            execute("INSERT INTO arrays VALUES (10, ARRAY[1.0, 9.0]), (20, ARRAY[2.0, 3.0])");
            assertQuery("SELECT * FROM (SELECT ts, u.value FROM arrays, UNNEST(a) u(value)) TIMESTAMP(ts) SUBSAMPLE minmax(value, 2)")
                    .timestamp("ts").returns("ts\tvalue\n1970-01-01T00:00:00.000010Z\t1.0\n1970-01-01T00:00:00.000010Z\t9.0\n");
            execute("CREATE TABLE pivoted (ts TIMESTAMP, c SYMBOL, v INT) TIMESTAMP(ts)");
            execute("INSERT INTO pivoted VALUES (10, 'a', 1), (20, 'a', 9), (30, 'a', 2), (40, 'a', 3)");
            assertQuery("SELECT * FROM pivoted PIVOT (sum(v) FOR c IN ('a') GROUP BY ts) SUBSAMPLE minmax(a, 2)")
                    .timestamp("ts").returns("ts\ta\n1970-01-01T00:00:00.000010Z\t1\n1970-01-01T00:00:00.000020Z\t9\n");
        });
    }

    @Test
    public void testWildcardSelectionBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT *" + JOIN + " WHERE a.x < 0 SUBSAMPLE minmax(y, 2)").timestamp("ts").returns("ts\tx\tts1\ty\n");
            assertQuery("SELECT *" + JOIN + " WHERE a.x = 1 SUBSAMPLE minmax(y, 2)").timestamp("ts")
                    .returns(MINMAX_ROWS.substring(0, MINMAX_ROWS.indexOf("1970-01-01T00:00:00.000020Z")));
            for (int target = 4; target <= 6; target += 2) {
                assertQuery("SELECT *" + JOIN + " SUBSAMPLE minmax(y, " + target + ")").timestamp("ts").returns(ALL_ROWS);
            }
            execute("CREATE TABLE empty_slave (ts TIMESTAMP, y INT) TIMESTAMP(ts)");
            assertQuery("SELECT * FROM ca a ASOF JOIN empty_slave b SUBSAMPLE minmax(y, 2)").timestamp("ts").returns("ts\tx\tts1\ty\n");
            execute("CREATE TABLE ties (ts TIMESTAMP, v INT) TIMESTAMP(ts)");
            execute("INSERT INTO ties VALUES (10, NULL), (10, 1), (10, 9), (20, 2)");
            assertQuery("SELECT * FROM ties SUBSAMPLE minmax(v, 2)").timestamp("ts")
                    .returns("ts\tv\n1970-01-01T00:00:00.000010Z\t1\n1970-01-01T00:00:00.000010Z\t9\n");
        });
    }

    @Test
    public void testPendingSubsamplePublicKeepAndHelperCollision() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 1 ? "true" : "false");
                final String sql = "SELECT * FROM (SELECT ts, x, true AS __keep_subsample, false AS __keep_subsample1 FROM ca) SUBSAMPLE minmax(x, 2)";
                assertQuery(sql).timestamp("ts").withPlanContaining(light == 1 ? "CachedWindowLightSelect" : "CachedWindow\n")
                        .returns("ts\tx\t__keep_subsample\t__keep_subsample1\n1970-01-01T00:00:00.000010Z\t1\ttrue\tfalse\n1970-01-01T00:00:00.000040Z\t4\ttrue\tfalse\n");
                assertQuery("SELECT * FROM (SELECT ts, x, minmax(ts, x, 2) OVER (ORDER BY ts) AS keep FROM ca) WHERE keep")
                        .timestamp("ts").withPlanContaining("Filter filter: keep")
                        .returns("ts\tx\tkeep\n1970-01-01T00:00:00.000010Z\t1\ttrue\n1970-01-01T00:00:00.000040Z\t4\ttrue\n");
            }
        });
    }

    @Test
    public void testSetOperationSymbolSegments() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE symbols (x SYMBOL, k LONG)");
            execute("INSERT INTO symbols VALUES ('a', 1)");
            assertQuery("SELECT typeOf(x) AS t FROM (SELECT x FROM symbols UNION SELECT x FROM symbols)")
                    .noRandomAccess().returns("t\nSYMBOL\n");
            assertQuery("SELECT typeOf(x) AS t FROM (SELECT x FROM symbols UNION ALL SELECT x FROM symbols UNION SELECT x FROM symbols)")
                    .noRandomAccess().returns("t\nSYMBOL\n");
            assertQuery("SELECT typeOf(x) AS t FROM (SELECT x FROM symbols UNION ALL (SELECT x FROM symbols UNION SELECT x FROM symbols))")
                    .noRandomAccess().returns("t\nSYMBOL\nSYMBOL\n");
            assertQuery("SELECT typeOf(x) AS t FROM ((SELECT x FROM symbols UNION SELECT x FROM symbols) UNION ALL SELECT x FROM symbols)")
                    .noRandomAccess().returns("t\nSYMBOL\nSYMBOL\n");
            assertQuery("SELECT typeOf(x) AS t FROM (SELECT x, k FROM symbols INTERSECT ALL SELECT x, k FROM symbols)")
                    .returns("t\nSYMBOL\n");
            assertQuery("SELECT typeOf(x) AS t FROM (SELECT x, k FROM symbols EXCEPT ALL SELECT x, k::DOUBLE FROM symbols WHERE false)")
                    .returns("t\nSTRING\n");
        });
    }

    @Test
    public void testProtectedSubsampleAliasNamespace() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 1 ? "true" : "false");
                final String plan = light == 1 ? "CachedWindowLightSelect" : "CachedWindow\n";
                final String rows = primaryRows().replace("\tx\n", "\tV.Dot\n");
                assertQuery("SELECT ts, x AS \"V.Dot\" FROM ca SUBSAMPLE minmax(v.dot, 2)")
                        .timestamp("ts").withPlanContaining(plan).returns(rows);
                assertQuery("SELECT ts, x AS \"V.Dot\" FROM ca SUBSAMPLE minmax(\"v.DOT\", 2)")
                        .timestamp("ts").returns(rows);
                assertQuery("SELECT ts, x AS \"in\" FROM ca").timestamp("ts").expectSize()
                        .returns(allPrimaryRows().replace("\tx\n", "\tin\n"));
                assertQuery("SELECT ts, x AS \"in\" FROM ca SUBSAMPLE minmax(\"in\", 2)")
                        .timestamp("ts").withPlanContaining(plan).returns(primaryRows().replace("\tx\n", "\tin\n"));
                final String collision = "SELECT a.ts, a.x, b.y AS \"a.x\"" + JOIN;
                final String selected = "ts\tx\ta.x\n1970-01-01T00:00:00.000010Z\t1\t10\n1970-01-01T00:00:00.000020Z\t2\t90\n";
                assertQuery(collision + " LIMIT 2").timestamp("ts").noRandomAccess().expectSize().returns(selected);
                // The whole exposed output name wins, not the input qualifier a.x.
                assertQuery(collision + " SUBSAMPLE minmax(a.x, 2)").timestamp("ts").returns(selected);
                assertQuery(collision + " SUBSAMPLE minmax(\"a.x\", 2)").timestamp("ts").returns(selected);
                assertQuery("SELECT a.ts, b.y AS \"a.x\", a.x" + JOIN + " SUBSAMPLE minmax(a.x, 2)")
                        .timestamp("ts").returns("ts\ta.x\tx\n1970-01-01T00:00:00.000010Z\t10\t1\n1970-01-01T00:00:00.000020Z\t90\t2\n");
                assertCompileErrorThenReuse("SELECT * FROM ca SUBSAMPLE minmax(^ca.x, 2)", "qualified column names are not supported");
                assertCompileErrorThenReuse("SELECT ts, x AS renamed FROM ca SUBSAMPLE minmax(^v.dot, 2)", "qualified column names are not supported");
                assertCompileErrorThenReuse("SELECT * FROM ca SUBSAMPLE minmax(^__keep_subsample, 2)", "column not found in SELECT list: __keep_subsample");
                assertQuery("SELECT ts, x AS \"__keep_subsample.dot\" FROM ca SUBSAMPLE minmax(\"__keep_subsample.dot\", 2)")
                        .timestamp("ts").returns(primaryRows().replace("\tx\n", "\t__keep_subsample.dot\n"));
            }
        });
    }

    @Test
    public void testProtectedSubsampleAliasMethodsAndTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final ObjList<String> clauses = new ObjList<>();
            clauses.add("m4(\"v.dot\", 4)");
            clauses.add("minmax(\"v.dot\", 2)");
            clauses.add("lttb(\"v.dot\", 2)");
            clauses.add("lttb(\"v.dot\", 2, '1h')");
            clauses.add("sdt(\"v.dot\", 0.5)");
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 1 ? "true" : "false");
                for (int i = 0; i < clauses.size(); i++) {
                    final String sql = "SELECT * FROM (SELECT ts, x::DOUBLE AS \"v.dot\" FROM ca) SUBSAMPLE " + clauses.getQuick(i);
                    final String rows = i == 0
                            ? "ts\tv.dot\n1970-01-01T00:00:00.000010Z\t1.0\n1970-01-01T00:00:00.000020Z\t2.0\n1970-01-01T00:00:00.000030Z\t3.0\n1970-01-01T00:00:00.000040Z\t4.0\n"
                            : "ts\tv.dot\n1970-01-01T00:00:00.000010Z\t1.0\n1970-01-01T00:00:00.000040Z\t4.0\n";
                    assertQuery(sql).timestamp("ts").withPlanContaining(light == 1 ? "CachedWindowLightSelect" : "CachedWindow\n").returns(rows);
                }
                assertCompileErrorThenReuse("SELECT ts, concat('v', x) AS \"v.dot\" FROM ca SUBSAMPLE minmax(^\"v.dot\", 0)", "numeric column expected, got: STRING");
                assertCompileErrorThenReuse("SELECT ts, NULL AS \"v.dot\" FROM ca SUBSAMPLE minmax(^\"v.dot\", 0)", "numeric column expected, got: NULL");
                assertQuery("SELECT ts, NULL::DOUBLE AS \"v.dot\" FROM ca SUBSAMPLE minmax(\"v.dot\", 2)")
                        .timestamp("ts").returns("ts\tv.dot\n");
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    final IQueryModel model = (IQueryModel) compiler.generateExecutionModel("SELECT ts, x AS \"v.dot\" FROM ca SUBSAMPLE minmax(\"v.dot\", 2)", sqlExecutionContext);
                    final ObjList<RecipeSnapshot> snapshots = snapshotRecipes(model, 1);
                    Assert.assertEquals("v.dot", snapshots.getQuick(0).raw.args.getQuick(0).token.toString());
                    Assert.assertEquals("\"v.dot\"", snapshots.getQuick(0).root.rhs.token.toString());
                    try (RecordCursorFactory factory = compiler.generateSelectWithRetries(model, null, sqlExecutionContext, false)) {
                        assertRecipesUnchanged(snapshots);
                        assertFactory(factory).withContext(sqlExecutionContext).timestamp("ts").returns(primaryRows().replace("\tx\n", "\tv.dot\n"));
                    }
                    assertRecipesUnchanged(snapshots);
                }
            }
        });
    }

    @Test
    public void testPendingSubsampleCarrierOrdinaryLateralControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.ts, b.y FROM ca a JOIN LATERAL (SELECT * FROM cb WHERE cb.ts <= a.ts) b ON true ORDER BY a.ts, b.y")
                    .timestamp("ts").expectSize().returns("ts\ty\n1970-01-01T00:00:00.000010Z\t10\n1970-01-01T00:00:00.000020Z\t10\n1970-01-01T00:00:00.000020Z\t90\n1970-01-01T00:00:00.000030Z\t10\n1970-01-01T00:00:00.000030Z\t20\n1970-01-01T00:00:00.000030Z\t90\n1970-01-01T00:00:00.000040Z\t10\n1970-01-01T00:00:00.000040Z\t20\n1970-01-01T00:00:00.000040Z\t30\n1970-01-01T00:00:00.000040Z\t90\n");
        });
    }

    @Test
    public void testPendingSubsampleGeneratedCarrierExclusionWithoutDistinctRewrite() throws Exception {
        try {
            isDistinctRewriteEnabled = false;
            testPendingSubsampleGeneratedCarrierExclusion();
        } finally {
            isDistinctRewriteEnabled = true;
        }
    }

    @Test
    public void testPendingSubsampleUserCarrierNameWithoutDistinctRewrite() throws Exception {
        try {
            isDistinctRewriteEnabled = false;
            testPendingSubsampleUserCarrierName();
        } finally {
            isDistinctRewriteEnabled = true;
        }
    }

    @Test
    public void testPendingSubsampleUserCarrierName() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 1 ? "true" : "false");
                assertQuery("SELECT ts, x AS __qdb_outer_ref__0_ts FROM ca SUBSAMPLE minmax(__qdb_outer_ref__0_ts, 2)")
                        .timestamp("ts").returns(primaryRows().replace("\tx\n", "\t__qdb_outer_ref__0_ts\n"));
            }
        });
    }

    @Test
    public void testPendingSubsampleGeneratedCarrierExclusion() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT a.ts, b.y FROM ca a JOIN LATERAL (SELECT * FROM cb WHERE cb.ts <= a.ts SUBSAMPLE minmax(y, 2)) b ON true";
            final ObjList<String> generatedAliases = new ObjList<>();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final IQueryModel model = (IQueryModel) compiler.generateExecutionModel(sql, sqlExecutionContext);
                final IdentityHashMap<IQueryModel, Boolean> visited = new IdentityHashMap<>();
                final ObjList<IQueryModel> todo = new ObjList<>();
                todo.add(model);
                while (todo.size() > 0) {
                    final IQueryModel current = todo.popLast();
                    if (current == null || visited.put(current, true) != null) {
                        continue;
                    }
                    final ObjList<QueryColumn> columns = current.getColumns();
                    for (int i = 0; i < columns.size(); i++) {
                        final QueryColumn column = columns.getQuick(i);
                        if (column.isGenerated()) {
                            Assert.assertSame(column, current.getAliasToColumnMap().get(column.getAlias()));
                            generatedAliases.add(column.getAlias().toString());
                        }
                    }
                    todo.add(current.getNestedModel());
                    todo.add(current.getUnionModel());
                    for (int i = 1; i < current.getJoinModels().size(); i++) {
                        todo.add(current.getJoinModels().getQuick(i));
                    }
                }
            }
            Assert.assertTrue("must reach real lateral-generated carriers", generatedAliases.size() > 0);
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 1 ? "true" : "false");
                for (int i = 0; i < generatedAliases.size(); i++) {
                    final String alias = generatedAliases.getQuick(i);
                    assertCompileErrorThenReuse(sql.replace("minmax(y, 2)", "minmax(^" + alias + ", 2)"), "column not found in SELECT list: " + alias);
                }
            }
        });
    }

    @Test
    public void testPendingSubsampleSharedWrapper() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT o.ts, o.x, b.y FROM (SELECT ts, max(x) AS x FROM (SELECT * FROM ca SUBSAMPLE minmax(x, 2)) GROUP BY ts) o "
                    + "JOIN LATERAL (SELECT y FROM cb WHERE y >= o.x * 10) b ON true ORDER BY o.ts, b.y";
            final String rows = "ts\tx\ty\n1970-01-01T00:00:00.000010Z\t1\t10\n1970-01-01T00:00:00.000010Z\t1\t20\n1970-01-01T00:00:00.000010Z\t1\t30\n1970-01-01T00:00:00.000010Z\t1\t90\n1970-01-01T00:00:00.000040Z\t4\t90\n";
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 1 ? "true" : "false");
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    final IQueryModel model = (IQueryModel) compiler.generateExecutionModel(sql, sqlExecutionContext);
                    final ObjList<RecipeSnapshot> snapshots = snapshotRecipes(model, 1);
                    final IdentityHashMap<IQueryModel, Boolean> visited = new IdentityHashMap<>();
                    final ObjList<IQueryModel> todo = new ObjList<>();
                    todo.add(model);
                    int wrapperCount = 0;
                    while (todo.size() > 0) {
                        final IQueryModel current = todo.popLast();
                        if (current == null || visited.put(current, true) != null) {
                            continue;
                        }
                        if (current instanceof QueryModelWrapper wrapper) {
                            wrapperCount++;
                            Assert.assertTrue(wrapper.getDelegate().hasSharedRefs());
                            final ObjList<RecipeSnapshot> sharedRecipes = snapshotRecipes(wrapper.getDelegate(), 1);
                            Assert.assertSame(snapshots.getQuick(0).owner, sharedRecipes.getQuick(0).owner);
                        }
                        todo.add(current.getNestedModel());
                        todo.add(current.getUnionModel());
                        for (int i = 1; i < current.getJoinModels().size(); i++) {
                            todo.add(current.getJoinModels().getQuick(i));
                        }
                    }
                    Assert.assertTrue("must reach a real QueryModelWrapper", wrapperCount > 0);
                    try (RecordCursorFactory factory = compiler.generateSelectWithRetries(model, null, sqlExecutionContext, false)) {
                        assertRecipesUnchanged(snapshots);
                        assertFactory(factory).withContext(sqlExecutionContext).timestamp("ts").expectSize().returns(rows);
                    }
                    assertRecipesUnchanged(snapshots);
                }
                assertQuery(sql).withPlanContaining("(Shared)").timestamp("ts").expectSize().returns(rows);
            }
            assertCompileErrorThenReuse("SELECT a.ts, b.y FROM ca a JOIN LATERAL ((SELECT * FROM cb WHERE cb.ts <= a.ts SUBSAMPLE ^minmax(y, 2)) UNION ALL (SELECT * FROM cb WHERE cb.ts <= a.ts)) b ON true", "minmax() does not support PARTITION BY");
        });
    }

    @Test
    public void testPendingSubsampleDistinctSharedWrapper() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT o.ts, o.x, b.y FROM (SELECT ts, max(x) AS x FROM (SELECT * FROM ca SUBSAMPLE minmax(x, 2)) GROUP BY ts) o "
                    + "JOIN LATERAL (SELECT DISTINCT y FROM cb WHERE y >= o.x * 10) b ON true ORDER BY o.ts, b.y";
            final String rows = "ts\tx\ty\n1970-01-01T00:00:00.000010Z\t1\t10\n1970-01-01T00:00:00.000010Z\t1\t20\n1970-01-01T00:00:00.000010Z\t1\t30\n1970-01-01T00:00:00.000010Z\t1\t90\n1970-01-01T00:00:00.000040Z\t4\t90\n";
            for (int light = 0; light < 2; light++) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, light == 1 ? "true" : "false");
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    for (int pass = 0; pass < 2; pass++) {
                        final IQueryModel model = (IQueryModel) compiler.generateExecutionModel(sql, sqlExecutionContext);
                        final ObjList<RecipeSnapshot> snapshots = snapshotRecipes(model, 1);
                        final IdentityHashMap<IQueryModel, Boolean> visited = new IdentityHashMap<>();
                        final ObjList<IQueryModel> todo = new ObjList<>();
                        todo.add(model);
                        int wrapperCount = 0;
                        while (todo.size() > 0) {
                            final IQueryModel current = todo.popLast();
                            if (current == null || visited.put(current, true) != null) {
                                continue;
                            }
                            if (current instanceof QueryModelWrapper wrapper) {
                                wrapperCount++;
                                Assert.assertTrue(wrapper.getDelegate().hasSharedRefs());
                                final ObjList<RecipeSnapshot> sharedRecipes = snapshotRecipes(wrapper.getDelegate(), 1);
                                Assert.assertSame(snapshots.getQuick(0).owner, sharedRecipes.getQuick(0).owner);
                            }
                            todo.add(current.getNestedModel());
                            todo.add(current.getUnionModel());
                            for (int i = 1; i < current.getJoinModels().size(); i++) {
                                todo.add(current.getJoinModels().getQuick(i));
                            }
                        }
                        Assert.assertTrue("DISTINCT must retain a real QueryModelWrapper", wrapperCount > 0);
                        try (RecordCursorFactory factory = compiler.generateSelectWithRetries(model, null, sqlExecutionContext, false)) {
                            assertRecipesUnchanged(snapshots);
                            assertFactory(factory).withContext(sqlExecutionContext).timestamp("ts").returns(rows);
                        }
                        // Compare before the next optimisation resets the compiler pools.
                        assertRecipesUnchanged(snapshots);
                    }
                }
                assertQuery(sql).withPlanContaining("(Shared)").timestamp("ts").returns(rows);
            }
        });
    }

    @Test
    public void testDistinctRewriteDispatch() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final ObjList<String> queries = new ObjList<>();
            queries.add("SELECT DISTINCT x FROM ca");
            queries.add("SELECT DISTINCT x AS renamed FROM ca");
            queries.add("SELECT DISTINCT abs(x) AS value FROM ca");
            queries.add("SELECT DISTINCT ARRAY[x::DOUBLE][1] AS value FROM ca");
            queries.add("SELECT DISTINCT ARRAY[x::DOUBLE] AS value FROM ca");
            try {
                for (int aliases = 0; aliases < 2; aliases++) {
                    setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, aliases == 1 ? "true" : "false");
                    for (int rewrite = 0; rewrite < 2; rewrite++) {
                        isDistinctRewriteEnabled = rewrite == 1;
                        try (SqlCompiler compiler = engine.getSqlCompiler()) {
                            for (int i = 0; i < queries.size(); i++) {
                                assertDistinctRewriteShape(compiler, queries.getQuick(i), false);
                            }
                        }
                    }
                }
            } finally {
                isDistinctRewriteEnabled = true;
            }
        });
    }

    @Test
    public void testDistinctHiddenGeneratedPublicStars() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try {
                for (int rewrite = 0; rewrite < 2; rewrite++) {
                    isDistinctRewriteEnabled = rewrite == 1;
                    for (int collision = 0; collision < 2; collision++) {
                        final String suffix = collision == 1 ? ", y AS __qdb_outer_ref__0_ts" : "";
                        final String sql = "SELECT a.ts, b.* FROM ca a JOIN LATERAL (SELECT DISTINCT y AS value" + suffix
                                + " FROM cb WHERE cb.ts <= a.ts) b ON true";
                        final String header = collision == 1 ? "ts\tvalue\t__qdb_outer_ref__0_ts\n" : "ts\tvalue\n";
                        final String rows = collision == 1
                                ? "1970-01-01T00:00:00.000010Z\t10\t10\n1970-01-01T00:00:00.000020Z\t10\t10\n1970-01-01T00:00:00.000020Z\t90\t90\n1970-01-01T00:00:00.000030Z\t10\t10\n1970-01-01T00:00:00.000030Z\t20\t20\n1970-01-01T00:00:00.000030Z\t90\t90\n1970-01-01T00:00:00.000040Z\t10\t10\n1970-01-01T00:00:00.000040Z\t20\t20\n1970-01-01T00:00:00.000040Z\t30\t30\n1970-01-01T00:00:00.000040Z\t90\t90\n"
                                : "1970-01-01T00:00:00.000010Z\t10\n1970-01-01T00:00:00.000020Z\t10\n1970-01-01T00:00:00.000020Z\t90\n1970-01-01T00:00:00.000030Z\t10\n1970-01-01T00:00:00.000030Z\t20\n1970-01-01T00:00:00.000030Z\t90\n1970-01-01T00:00:00.000040Z\t10\n1970-01-01T00:00:00.000040Z\t20\n1970-01-01T00:00:00.000040Z\t30\n1970-01-01T00:00:00.000040Z\t90\n";
                        final var all = assertQuery(sql + " ORDER BY a.ts, b.value").timestamp("ts");
                        final var empty = assertQuery(sql + " WHERE a.x < 0 ORDER BY a.ts, b.value").timestamp("ts");
                        final var singleton = assertQuery(sql + " WHERE a.x = 1 ORDER BY a.ts, b.value").timestamp("ts");
                        if (!isDistinctRewriteEnabled) {
                            all.expectSize();
                            empty.expectSize();
                            singleton.expectSize();
                        }
                        all.returns(header + rows);
                        empty.returns(header);
                        singleton.returns(header + rows.substring(0, rows.indexOf('\n') + 1));
                    }
                }
            } finally {
                isDistinctRewriteEnabled = true;
            }
        });
    }

    @Test
    public void testDistinctCountHelperVisibility() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try {
                for (int rewrite = 0; rewrite < 2; rewrite++) {
                    isDistinctRewriteEnabled = rewrite == 1;
                    assertQuery("SELECT * FROM (SELECT DISTINCT x % 2 AS key FROM ca) ORDER BY key")
                            .expectSize().returns("key\n0\n1\n");
                    assertQuery("SELECT * FROM (SELECT * FROM (SELECT DISTINCT x % 2 AS key FROM ca WHERE x < 0)) ORDER BY key")
                            .expectSize().returns("key\n");
                    assertQuery("SELECT * FROM (SELECT DISTINCT NULL::INT AS key FROM ca) ORDER BY key")
                            .expectSize().returns("key\nnull\n");
                    assertQuery("SELECT * FROM (SELECT DISTINCT ts, x AS count FROM ca ORDER BY ts) TIMESTAMP(ts) SUBSAMPLE minmax(count, 2)")
                            .timestamp("ts").returns(primaryRows().replace("\tx\n", "\tcount\n"));
                    assertCompileErrorThenReuse("SELECT * FROM (SELECT DISTINCT ts, x FROM ca ORDER BY ts) TIMESTAMP(ts) SUBSAMPLE minmax(^count, 2)",
                            "column not found in SELECT list: count");
                }
            } finally {
                isDistinctRewriteEnabled = true;
            }
        });
    }

    @Test
    public void testDistinctAbandonedRewriteThenReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String window = "SELECT DISTINCT x AS renamed, row_number() OVER () AS rn FROM ca ORDER BY renamed";
            final String aggregate = "SELECT DISTINCT x AS renamed, count() AS n FROM ca GROUP BY x ORDER BY renamed";
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertDistinctRewriteShape(compiler, window, true);
                assertDistinctRewriteShape(compiler, aggregate, true);
                try (RecordCursorFactory factory = compiler.compile(window, sqlExecutionContext).getRecordCursorFactory()) {
                    assertFactory(factory).withContext(sqlExecutionContext).expectSize().returns("renamed\trn\n1\t1\n2\t2\n3\t3\n4\t4\n");
                }
                try (RecordCursorFactory factory = compiler.compile(aggregate, sqlExecutionContext).getRecordCursorFactory()) {
                    assertFactory(factory).withContext(sqlExecutionContext).expectSize().returns("renamed\tn\n1\t1\n2\t1\n3\t1\n4\t1\n");
                }
                assertCompilerError(compiler, "SELECT DISTINCT x AS renamed, ^no_such_function(x) FROM ca", "unknown function name: no_such_function(INT)");
                try (RecordCursorFactory factory = compiler.compile("SELECT DISTINCT x AS renamed FROM ca ORDER BY renamed", sqlExecutionContext).getRecordCursorFactory()) {
                    assertFactory(factory).withContext(sqlExecutionContext).expectSize().returns("renamed\n1\n2\n3\n4\n");
                }
                assertCompilerError(compiler, "SELECT a.ts, b.y FROM ca a JOIN LATERAL (SELECT * FROM cb WHERE cb.ts <= a.ts SUBSAMPLE minmax(^__qdb_outer_ref__0_ts, 2)) b ON true",
                        "column not found in SELECT list: __qdb_outer_ref__0_ts");
                try (RecordCursorFactory factory = compiler.compile("SELECT ts, x AS __qdb_outer_ref__0_ts FROM ca SUBSAMPLE minmax(__qdb_outer_ref__0_ts, 2)", sqlExecutionContext).getRecordCursorFactory()) {
                    assertFactory(factory).withContext(sqlExecutionContext).timestamp("ts").returns(primaryRows().replace("\tx\n", "\t__qdb_outer_ref__0_ts\n"));
                }
            }
            assertQuery(window).withPlanContaining("Distinct").expectSize().returns("renamed\trn\n1\t1\n2\t2\n3\t3\n4\t4\n");
            assertQuery(aggregate).withPlanContaining("Distinct").expectSize().returns("renamed\tn\n1\t1\n2\t1\n3\t1\n4\t1\n");
        });
    }

    private void assertDistinctRewriteShape(SqlCompiler compiler, String sql, boolean isAbandoned) throws Exception {
        final IQueryModel model = (IQueryModel) compiler.generateExecutionModel(sql, sqlExecutionContext);
        boolean hasDistinct = false;
        boolean hasGroupBy = false;
        for (IQueryModel current = model; current != null; current = current.getNestedModel()) {
            hasDistinct |= current.getSelectModelType() == IQueryModel.SELECT_MODEL_DISTINCT;
            hasGroupBy |= current.getSelectModelType() == IQueryModel.SELECT_MODEL_GROUP_BY;
        }
        Assert.assertEquals(sql, !isDistinctRewriteEnabled || isAbandoned, hasDistinct);
        if (isDistinctRewriteEnabled && !isAbandoned) {
            Assert.assertTrue(sql, hasGroupBy);
        }
    }

    private void assertCompilerError(SqlCompiler compiler, String markedSql, String message) throws Exception {
        try (RecordCursorFactory ignored = compiler.compile(markedSql.replace("^", ""), sqlExecutionContext).getRecordCursorFactory()) {
            Assert.fail("expected: " + message);
        } catch (SqlException e) {
            Assert.assertEquals(markedSql.indexOf('^'), e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
    }

    private void assertDistinctGeneratedColumnProperties(String sql, boolean isVisible) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final IQueryModel model = (IQueryModel) compiler.generateExecutionModel(sql, sqlExecutionContext);
            final IdentityHashMap<IQueryModel, Boolean> visited = new IdentityHashMap<>();
            final ObjList<IQueryModel> todo = new ObjList<>();
            int checked = 0;
            todo.add(model);
            while (todo.size() > 0) {
                final IQueryModel current = todo.popLast();
                if (current == null || visited.put(current, true) != null) {
                    continue;
                }
                final IQueryModel nested = current.getNestedModel();
                if (current.getSelectModelType() == IQueryModel.SELECT_MODEL_CHOOSE && nested != null
                        && nested.getSelectModelType() == IQueryModel.SELECT_MODEL_GROUP_BY) {
                    final ObjList<CharSequence> aliases = nested.getAliasToColumnMap().keys();
                    for (int i = 0; i < aliases.size(); i++) {
                        final QueryColumn source = nested.getAliasToColumnMap().get(aliases.getQuick(i));
                        if (source.isGenerated() && source.isIncludeIntoWildcard() == isVisible) {
                            final QueryColumn reference = current.getAliasToColumnMap().get(source.getAlias());
                            if (reference != null) {
                                Assert.assertEquals(ExpressionNode.LITERAL, reference.getAst().type);
                                Assert.assertEquals(source.getAlias().toString(), reference.getAst().token.toString());
                                if (isVisible) {
                                    Assert.assertNotSame(source, reference);
                                }
                                Assert.assertEquals("transparent DISTINCT visibility: " + source.getAlias(), source.isIncludeIntoWildcard(), reference.isIncludeIntoWildcard());
                                Assert.assertEquals("transparent DISTINCT provenance: " + source.getAlias(), source.isGenerated(), reference.isGenerated());
                                checked++;
                            }
                        }
                    }
                }
                if (current instanceof QueryModelWrapper wrapper) {
                    todo.add(wrapper.getDelegate());
                }
                todo.add(nested);
                todo.add(current.getUnionModel());
                for (int i = 1; i < current.getJoinModels().size(); i++) {
                    todo.add(current.getJoinModels().getQuick(i));
                }
            }
            Assert.assertTrue("must reach a real DISTINCT reference with source visibility=" + isVisible, checked > 0);
        }
    }

    private void assertCompileErrorThenReuse(String markedSql, String message) throws Exception {
        assertCompileErrorThenReuse(markedSql, message, "SELECT * FROM ca SUBSAMPLE minmax(x, 2)");
    }

    private void assertCompileErrorThenReuse(String markedSql, String message, String reuseSql) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try {
                final RecordCursorFactory factory = compiler.compile(markedSql.replace("^", ""), sqlExecutionContext).getRecordCursorFactory();
                if (factory != null) {
                    factory.close();
                }
                Assert.fail("expected: " + message);
            } catch (SqlException e) {
                Assert.assertEquals(markedSql.indexOf('^'), e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }
            try (RecordCursorFactory factory = compiler.compile(reuseSql, sqlExecutionContext).getRecordCursorFactory()) {
                assertFactory(factory).withContext(sqlExecutionContext).timestamp("ts").returns(primaryRows());
            }
        }
    }

    private static IQueryModel findKeepFilter(IQueryModel model) {
        for (IQueryModel current = model; current != null; current = current.getNestedModel()) {
            if (current.getWhereClause() != null && "__keep_subsample".contentEquals(current.getWhereClause().token)) {
                return current;
            }
        }
        return null;
    }

    private static String allPrimaryRows() {
        return "ts\tx\n1970-01-01T00:00:00.000010Z\t1\n1970-01-01T00:00:00.000020Z\t2\n1970-01-01T00:00:00.000030Z\t3\n1970-01-01T00:00:00.000040Z\t4\n";
    }

    private void assertMarkedRecipeError(String markedSql, String message) throws Exception {
        assertRecipeGeneration(markedSql.replace("^", ""), message, markedSql.indexOf('^'), 1);
    }

    private void assertRecipeGeneration(String sql, String message, int position, int ownerCount) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            final IQueryModel model = (IQueryModel) compiler.generateExecutionModel(sql, sqlExecutionContext);
            final ObjList<RecipeSnapshot> snapshots = snapshotRecipes(model, ownerCount);
            final ObjectPool<ExpressionNode> filters = new ObjectPool<>(ExpressionNode.FACTORY, 32);
            IQueryModel.backupWhereClause(filters, model);
            for (int pass = 0; pass < 2; pass++) {
                assertRecipesUnchanged(snapshots);
                IQueryModel.restoreWhereClause(filters, model);
                assertRecipesUnchanged(snapshots);
                try (RecordCursorFactory factory = compiler.generateSelectWithRetries(model, null, sqlExecutionContext, false)) {
                    Assert.assertNull("expected generation failure", message);
                    // Row assertions use the fluent factory/query battery separately; this checks the
                    // real same-model regeneration before any compiler reset can hide AST mutation.
                    Assert.assertNotNull(factory.getMetadata());
                } catch (SqlException e) {
                    Assert.assertNotNull(e.getMessage(), message);
                    TestUtils.assertContains(e.getFlyweightMessage(), message);
                    Assert.assertEquals(position, e.getPosition());
                } finally {
                    assertRecipesUnchanged(snapshots);
                }
            }
            try (RecordCursorFactory factory = compiler.compile("SELECT * FROM ca SUBSAMPLE minmax(x, 2)", sqlExecutionContext).getRecordCursorFactory()) {
                assertFactory(factory).withContext(sqlExecutionContext).timestamp("ts").returns(primaryRows());
            }
        }
    }

    private static void assertRecipesUnchanged(ObjList<RecipeSnapshot> snapshots) throws Exception {
        for (int i = 0; i < snapshots.size(); i++) {
            snapshots.getQuick(i).assertUnchanged();
        }
    }

    private static String primaryRows() {
        return "ts\tx\n1970-01-01T00:00:00.000010Z\t1\n1970-01-01T00:00:00.000040Z\t4\n";
    }

    private static long readConstFoldLongValue(ExpressionNode node) throws Exception {
        final Field field = ExpressionNode.class.getDeclaredField("constFoldLongValue");
        field.setAccessible(true);
        return field.getLong(node);
    }

    private static void registerFactory(String name, FunctionFactory factory) throws SqlException {
        final ObjList<FunctionFactoryDescriptor> descriptors = new ObjList<>();
        descriptors.add(new FunctionFactoryDescriptor(factory));
        Assert.assertNull(engine.getFunctionFactoryCache().getFactories().get(name));
        engine.getFunctionFactoryCache().getFactories().put(name, descriptors);
    }

    private static ObjList<RecipeSnapshot> snapshotRecipes(IQueryModel model, int expectedCount) throws Exception {
        final IdentityHashMap<IQueryModel, Boolean> visited = new IdentityHashMap<>();
        final IdentityHashMap<WindowExpression, Boolean> owners = new IdentityHashMap<>();
        final ObjList<IQueryModel> todo = new ObjList<>();
        final ObjList<RecipeSnapshot> snapshots = new ObjList<>();
        todo.add(model);
        while (todo.size() > 0) {
            final IQueryModel current = todo.popLast();
            if (current == null || visited.put(current, true) != null) {
                continue;
            }
            final ObjList<QueryColumn> columns = current.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if (columns.getQuick(i) instanceof WindowExpression window && window.getPendingSubsample() != null
                        && owners.put(window, true) == null) {
                    Assert.assertFalse(window.isSubsampleProjectionPending());
                    Assert.assertTrue(window.isSubsampleKeepFlag());
                    Assert.assertEquals(2, window.getAst().paramCount);
                    Assert.assertSame(window, window.getAst().windowExpression);
                    snapshots.add(new RecipeSnapshot(window));
                }
            }
            todo.add(current.getNestedModel());
            todo.add(current.getUnionModel());
            for (int i = 1; i < current.getJoinModels().size(); i++) {
                todo.add(current.getJoinModels().getQuick(i));
            }
        }
        Assert.assertEquals(expectedCount, snapshots.size());
        return snapshots;
    }

    private static final class RecipeSnapshot {
        private final WindowExpression owner;
        private final ExpressionNode raw;
        private final ExpressionNode root;
        private final int position;
        private final boolean hasSourceTimestamp;
        private final boolean isVisible;
        private final ObjList<NodeSnapshot> nodes = new ObjList<>();

        private RecipeSnapshot(WindowExpression owner) throws Exception {
            this.owner = owner;
            raw = owner.getPendingSubsample();
            root = owner.getAst();
            position = owner.getSubsamplePosition();
            hasSourceTimestamp = owner.hasSubsampleSourceTimestamp();
            isVisible = owner.isIncludeIntoWildcard();
            final IdentityHashMap<ExpressionNode, Boolean> visited = new IdentityHashMap<>();
            final ObjList<ExpressionNode> todo = new ObjList<>();
            todo.add(raw);
            todo.add(root);
            while (todo.size() > 0) {
                final ExpressionNode node = todo.popLast();
                if (node == null || visited.put(node, true) != null) {
                    continue;
                }
                nodes.add(new NodeSnapshot(node));
                todo.add(node.lhs);
                todo.add(node.rhs);
                todo.addAll(node.args);
            }
        }

        private void assertUnchanged() throws Exception {
            Assert.assertSame(raw, owner.getPendingSubsample());
            Assert.assertSame(root, owner.getAst());
            Assert.assertSame(owner, root.windowExpression);
            Assert.assertFalse(owner.isSubsampleProjectionPending());
            Assert.assertTrue(owner.isSubsampleKeepFlag());
            Assert.assertEquals(position, owner.getSubsamplePosition());
            Assert.assertEquals(hasSourceTimestamp, owner.hasSubsampleSourceTimestamp());
            Assert.assertEquals(isVisible, owner.isIncludeIntoWildcard());
            for (int i = 0; i < nodes.size(); i++) {
                nodes.getQuick(i).assertUnchanged();
            }
        }
    }

    private static final class NodeSnapshot {
        private final ExpressionNode node;
        private final ExpressionNode lhs;
        private final ExpressionNode rhs;
        private final ObjList<ExpressionNode> args;
        private final ObjList<ExpressionNode> elements = new ObjList<>();
        private final CharSequence token;
        private final String tokenText;
        private final int type;
        private final int paramCount;
        private final int precedence;
        private final int position;
        private final int intrinsicValue;
        private final int lateralDepth;
        private final boolean isConstant;
        private final boolean isImplemented;
        private final boolean isInnerPredicate;
        private final boolean isFoldValid;
        private final boolean isFoldWidening;
        private final long foldValue;
        private final IQueryModel query;
        private final WindowExpression window;
        private final Object scalarHolder;
        private final Object scalarCache;

        private NodeSnapshot(ExpressionNode node) throws Exception {
            this.node = node;
            lhs = node.lhs;
            rhs = node.rhs;
            args = node.args;
            elements.addAll(args);
            token = node.token;
            tokenText = token == null ? null : token.toString();
            type = node.type;
            paramCount = node.paramCount;
            precedence = node.precedence;
            position = node.position;
            intrinsicValue = node.intrinsicValue;
            lateralDepth = node.lateralDepth;
            isConstant = node.isConstantExpression;
            isImplemented = node.implemented;
            isInnerPredicate = node.innerPredicate;
            isFoldValid = node.isConstFoldLongValid();
            isFoldWidening = node.isConstFoldWidening();
            foldValue = readConstFoldLongValue(node);
            query = node.queryModel;
            window = node.windowExpression;
            scalarHolder = node.scalarBoundHolder;
            scalarCache = node.scalarBoundCompileCache;
        }

        private void assertUnchanged() throws Exception {
            Assert.assertSame(lhs, node.lhs);
            Assert.assertSame(rhs, node.rhs);
            Assert.assertSame(args, node.args);
            Assert.assertEquals(elements.size(), node.args.size());
            for (int i = 0; i < elements.size(); i++) {
                Assert.assertSame(elements.getQuick(i), node.args.getQuick(i));
            }
            Assert.assertSame(token, node.token);
            Assert.assertEquals(tokenText, node.token == null ? null : node.token.toString());
            Assert.assertEquals(type, node.type);
            Assert.assertEquals(paramCount, node.paramCount);
            Assert.assertEquals(precedence, node.precedence);
            Assert.assertEquals(position, node.position);
            Assert.assertEquals(intrinsicValue, node.intrinsicValue);
            Assert.assertEquals(lateralDepth, node.lateralDepth);
            Assert.assertEquals(isConstant, node.isConstantExpression);
            Assert.assertEquals(isImplemented, node.implemented);
            Assert.assertEquals(isInnerPredicate, node.innerPredicate);
            Assert.assertEquals(isFoldValid, node.isConstFoldLongValid());
            Assert.assertEquals(isFoldWidening, node.isConstFoldWidening());
            Assert.assertEquals(foldValue, readConstFoldLongValue(node));
            Assert.assertSame(query, node.queryModel);
            Assert.assertSame(window, node.windowExpression);
            Assert.assertSame(scalarHolder, node.scalarBoundHolder);
            Assert.assertSame(scalarCache, node.scalarBoundCompileCache);
        }
    }

    private static void assertMarkedError(String sql, String message) throws Exception {
        final int position = sql.indexOf('^');
        assertException(sql.replace("^", ""), position, message);
    }

    private static void createTables() throws Exception {
        execute("CREATE TABLE ca (ts TIMESTAMP, x INT) TIMESTAMP(ts)");
        execute("CREATE TABLE cb (ts TIMESTAMP, y INT) TIMESTAMP(ts)");
        execute("INSERT INTO ca VALUES (10, 1), (20, 2), (30, 3), (40, 4)");
        execute("INSERT INTO cb VALUES (5, 10), (15, 90), (25, 20), (35, 30)");
    }
}
