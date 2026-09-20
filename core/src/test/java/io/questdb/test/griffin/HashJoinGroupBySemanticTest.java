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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.assertAgainstBaseline;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.assertDifferential;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.context;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.fused;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.plan;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.result;

public class HashJoinGroupBySemanticTest extends AbstractCairoTest {
    // The optimiser pushes the interval below dateadd() as and_offset, which the fused analysis cannot
    // parse, so the analysis falls back to the ordinary plan. The placeholder takes a join.
    private static final String INTERVAL_FROM = " FROM (SELECT id, d, s, dateadd('d', -1, t) ts FROM a) r%sb p"
            + " ON r.id = p.id WHERE r.ts IN '2020-01-01'";
    // Aliases denote SQL sides, including RIGHT: r is always the SQL LHS.
    private static final String[] JOINS = {" join ", " left join ", " right join "};
    // SYMBOL keys match by text. The tables give equal text different symbol keys, and each
    // table has one key text that the other table's dictionary lacks.
    private static final String[] KEYS = {"r.id=p.id", "r.s=p.s"};
    private static final String PROJECTED_R = "(select s2, d, id, s, l, i, t, f from a) r";
    private static final String PROJECTED_P = "(select f, i, s, t, id, l, d, s2 from b) p";

    @Test
    public void testColumnRolesAndEveryAggregateType() throws Exception {
        assertMemoryLeak(() -> {
            createTables(false);
            for (int workers : new int[]{1, 4}) {
                try (SqlExecutionContextImpl context = context(engine, workers)) {
                    context.changePageFrameSizes(1, 2);
                    for (int threshold : new int[]{Integer.MAX_VALUE, 1}) {
                        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                        for (String join : JOINS) {
                            assertDifferential("select r.s lhs, r.s repeated, p.s rhs, r.i+p.i combined, "
                                    + "sum(r.d+p.d),avg(r.d+p.d),count(r.i+p.i),count(r.l+p.l)"
                                    + from(join) + " order by lhs,repeated,rhs,combined", context, true);
                            for (int keys = 0; keys < 4; keys++) {
                                for (int args = 1; args < 4; args++) {
                                    String group = columns(keys, false);
                                    String sql = "select " + (group.isEmpty() ? "" : group + ", ")
                                            + aggregates(args, false) + from(join)
                                            + (group.isEmpty() ? "" : " order by " + group);
                                    assertDifferential(sql, context, true);
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testGroupingKeysWithEveryRecordSinkType() throws Exception {
        assertMemoryLeak(() -> {
            createTables(false);
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 2);
                // RecordSinkFactory returns no sink class for the looping sink, so the fused
                // plan must instantiate the sink without branching on the class.
                for (int sinkType : new int[]{0, RecordSinkFactory.SINK_TYPE_SINGLE_METHOD,
                        RecordSinkFactory.SINK_TYPE_CHUNKED, RecordSinkFactory.SINK_TYPE_LOOPING}) {
                    setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, sinkType);
                    for (int threshold : new int[]{Integer.MAX_VALUE, 1}) {
                        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                        for (String join : JOINS) {
                            for (String on : KEYS) {
                                assertDifferential("SELECT r.s, p.s2, r.i + p.i combined, count(*) pairs, "
                                        + "sum(p.d) psum, avg(r.d) ravg" + from(join, on)
                                        + " ORDER BY r.s, p.s2, combined", context, true);
                                assertDifferential("SELECT count(*) pairs, sum(p.d) psum" + from(join, on),
                                        context, true);
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testInnerJoinOrientationsAgree() throws Exception {
        assertMemoryLeak(() -> {
            createTables(false);
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 2);
                // a and b start with 8 rows each. Step 1 makes b the larger table, step 2 makes a the larger one.
                for (int step = 0; step < 3; step++) {
                    if (step == 1) {
                        insertRows("b", true);
                    } else if (step == 2) {
                        insertRows("a", false);
                        insertRows("a", false);
                    }
                    for (String on : KEYS) {
                        for (boolean isKeyed : new boolean[]{false, true}) {
                            String select = isKeyed
                                    ? "SELECT r.s, p.s2, count(*) pairs, sum(r.d) rsum, sum(p.d) psum, count(p.i) pi"
                                    : "SELECT count(*) pairs, sum(r.d) rsum, avg(p.d) pavg, count(r.l) rl";
                            String orderBy = isKeyed ? " ORDER BY r.s, p.s2" : "";
                            String rp = select + " FROM " + PROJECTED_R + " JOIN " + PROJECTED_P + " ON " + on + orderBy;
                            String pr = select + " FROM " + PROJECTED_P + " JOIN " + PROJECTED_R + " ON " + on + orderBy;
                            assertDifferential(rp, context, true);
                            assertDifferential(pr, context, true);
                            try (
                                    RecordCursorFactory rpFactory = engine.select(rp, context);
                                    RecordCursorFactory prFactory = engine.select(pr, context)
                            ) {
                                // INNER builds the smaller table and keeps the join order on a tie,
                                // so r JOIN p swaps only when a is smaller and p JOIN r only when b is.
                                String rpPlan = plan(rpFactory, context);
                                String prPlan = plan(prFactory, context);
                                Assert.assertTrue(rpPlan, rpPlan.contains("inputSwapped: " + (step == 1)));
                                Assert.assertTrue(prPlan, prPlan.contains("inputSwapped: " + (step == 2)));
                                Assert.assertEquals(rp, result(rpFactory, context), result(prFactory, context));
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSymbolRolesAcrossAllStoragePairs() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 2);
            createTables(false);
            // Equal text has different IDs on both sources and in their second SYMBOL columns.
            try (TableReader a = getReader("a"); TableReader b = getReader("b")) {
                Assert.assertNotEquals(a.getSymbolMapReader(4).keyOf("shared"), b.getSymbolMapReader(4).keyOf("shared"));
                Assert.assertNotEquals(a.getSymbolMapReader(4).keyOf("shared"), a.getSymbolMapReader(5).keyOf("shared"));
                // Each build drops the key text that the other table's dictionary lacks.
                Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, a.getSymbolMapReader(4).keyOf("miss7"));
                Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, b.getSymbolMapReader(4).keyOf("miss9"));
            }
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 2);
                for (int left = 0; left < 3; left++) {
                    storage("a", left);
                    for (int right = 0; right < 3; right++) {
                        storage("b", right);
                        for (int threshold : new int[]{Integer.MAX_VALUE, 1}) {
                            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                            for (String join : JOINS) {
                                for (String on : KEYS) {
                                    for (int keys = 0; keys < 4; keys++) {
                                        for (int args = 1; args < 4; args++) {
                                            String group = columns(keys, true);
                                            assertDifferential("select " + (group.isEmpty() ? "" : group + ", ")
                                                    + aggregates(args, true) + from(join, on)
                                                    + (group.isEmpty() ? "" : " order by " + group), context, true);
                                        }
                                    }
                                    // f is needed only by WHERE; s2 is needed only inside an argument.
                                    String build = join.equals(" right join ") ? "r" : "p";
                                    String probe = build.equals("r") ? "p" : "r";
                                    // The narrower interval leaves probe dictionary entries without scanned rows.
                                    for (String end : new String[]{"2020-01-04", "2020-01-02"}) {
                                        String sql = "select r.s, p.s, count(r.s), count(p.s), "
                                                + "sum(length(r.s2)::double), avg(length(p.s2)::double)" + from(join, on)
                                                + " where (" + build + ".f='keep' or " + build + ".f is null)"
                                                + " and " + probe + ".t >= '2020-01-01' and " + probe + ".t < '" + end + "'"
                                                + " order by r.s,p.s";
                                        assertDifferential(sql, context, true);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSymbolClonesRebindingAndDictionaryGrowth() throws Exception {
        assertMemoryLeak(() -> {
            createTables(false);
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null);
                context.changePageFrameSizes(1, 2);
                for (int format = 0; format < 3; format++) {
                    for (int threshold : new int[]{Integer.MAX_VALUE, 1}) {
                        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                        for (String join : JOINS) {
                            for (String on : KEYS) {
                                for (boolean keyed : new boolean[]{false, true}) {
                                    storage("a", format);
                                    storage("b", format);
                                    bindVariableService.setStr(0, "shared");
                                    String sql = "select " + (keyed ? "r.s, p.s, r.s2, p.s2, " : "")
                                            + aggregates(3, true) + ",sum(case when p.s=$1 then 1.0 else 0.0 end) selected" + from(join, on)
                                            + (keyed ? " order by r.s,p.s,r.s2,p.s2" : "");
                                    try (RecordCursorFactory factory = engine.select(sql, context)) {
                                        fused(factory);
                                        // SYMBOL keys translate through each execution's dictionaries: new text on
                                        // both sides, dictionaries with only nulls, empty tables, and reassigned keys.
                                        for (int state = 0; state < 5; state++) {
                                            if (state == 1) {
                                                execute("insert into a values (1,4,40,4,'new','shared','keep','2020-01-04')");
                                                execute("insert into b values (1,8,80,8,'new','different','keep','2020-01-04')");
                                                bindVariableService.setStr(0, "new");
                                            } else if (state == 2) {
                                                execute("truncate table a");
                                                execute("truncate table b");
                                                execute("insert into a values (1,1,1,1,null,null,null,'2020-01-01')");
                                                execute("insert into b values (1,1,1,1,null,null,null,'2020-01-01')");
                                            } else if (state == 3) {
                                                execute("truncate table a");
                                                execute("truncate table b");
                                            } else if (state == 4) {
                                                insertRows("a", false);
                                                insertRows("b", true);
                                                bindVariableService.setStr(0, "shared");
                                            }
                                            if (state != 3) {
                                                storage("a", format);
                                                storage("b", format);
                                            }
                                            assertAgainstBaseline(sql, factory, context);
                                            if (keyed) {
                                                assertSymbolClones(factory, context);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testJoinKeyAndPayloadTopsAcrossStoragePairs() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 2);
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 2);
                // Bit 1 gives a column tops, bit 2 gives b column tops. Rows above a top read null keys,
                // which match null keys on the other side, including translated SYMBOL keys.
                for (int tops = 1; tops < 4; tops++) {
                    createTables((tops & 1) != 0, (tops & 2) != 0);
                    for (int left = 0; left < 3; left++) {
                        storage("a", left);
                        for (int right = 0; right < 3; right++) {
                            storage("b", right);
                            // The merge path does not depend on which side has tops.
                            for (int threshold : tops == 3 ? new int[]{Integer.MAX_VALUE, 1} : new int[]{Integer.MAX_VALUE}) {
                                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                                for (String join : JOINS) {
                                    String build = join.equals(" right join ") ? "r" : "p";
                                    for (String on : KEYS) {
                                        for (String filter : new String[]{"", " where " + build + ".f is null",
                                                " and " + build + ".f='keep'", " where " + build + ".d>1000"}) {
                                            for (boolean keyed : new boolean[]{false, true}) {
                                                String group = "r.id,p.id,r.s,p.s,r.i,p.l";
                                                assertDifferential("select " + (keyed ? group + ", " : "") + aggregates(3, false)
                                                        + from(join, on) + filter + (keyed ? " order by " + group : ""), context, true);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    execute("drop table a");
                    execute("drop table b");
                }
            }
        });
    }

    @Test
    public void testExtremeKeysNullsAndRejectedRealMatches() throws Exception {
        assertMemoryLeak(() -> {
            createTables(false);
            for (int state = 0; state < 12; state++) {
                execute("truncate table a");
                execute("truncate table b");
                if (state != 1 && state != 3 && state != 9) {
                    insertRows("a", false);
                }
                if (state != 2 && state != 3 && state != 10) {
                    insertRows("b", true);
                }
                switch (state) {
                    case 4 -> {
                        execute("update a set i=null,l=null,d=null,s=null,s2=null");
                        execute("update b set i=null,l=null,d=null,s=null,s2=null");
                    }
                    case 5 -> execute("update b set id=999");
                    case 6 -> {
                        execute("update a set id=1");
                        execute("update b set id=1");
                    }
                    // The SYMBOL key counterparts of states 5 and 6: text that a's dictionary lacks, so
                    // the build drops every non-null row, and one key text for every row.
                    case 7 -> execute("update b set s='b-only' where s is not null");
                    case 8 -> {
                        execute("update a set s='shared'");
                        execute("update b set s='shared'");
                    }
                    // TRUNCATE resets the dictionary, so the key column of the table without rows from
                    // insertRows() never holds text: every non-null key on the other side misses.
                    case 9 -> execute("insert into a (id,i,l,d,t) values (1,1,10,0.5,'2020-01-01'),(null,2,20,2,'2020-01-02')");
                    case 10 -> execute("insert into b (id,i,l,d,t) values (1,1,10,0.5,'2020-01-01'),(null,2,20,2,'2020-01-02')");
                    // The dictionary keeps every key text, but only null keys have rows.
                    case 11 -> {
                        execute("truncate table a keep symbol maps");
                        execute("insert into a (id,i,l,d,t) values (1,1,10,0.5,'2020-01-01'),(null,2,20,2,'2020-01-02')");
                    }
                    default -> {
                    }
                }
                try (SqlExecutionContextImpl context = context(engine, 4)) {
                    context.changePageFrameSizes(1, 2);
                    for (String join : JOINS) {
                        String build = join.equals(" right join ") ? "r" : "p";
                        for (String on : KEYS) {
                            for (String filter : new String[]{"", " where " + build + ".d is null",
                                    " where " + build + ".d>1000", " and " + build + ".d>1000",
                                    " where " + build + ".s is null or " + build + ".s='absent'"}) {
                                assertDifferential("select " + aggregates(3, false) + from(join, on) + filter, context, true);
                                assertDifferential("select r.id,p.id,r.s,p.s," + aggregates(3, false) + from(join, on)
                                        + filter + " order by r.id,p.id,r.s,p.s", context, true);
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSeededSymbolRoleStorageMatrix() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 3);
            createTables(false);
            Rnd rnd = new Rnd(130, 95);
            for (int scenario = 0; scenario < 18; scenario++) {
                for (String table : new String[]{"a", "b"}) {
                    execute("truncate table " + table);
                    int rows = 8 + rnd.nextInt(24);
                    for (int row = 0; row < rows; row++) {
                        String key = switch (rnd.nextInt(7)) {
                            case 0 -> "null";
                            case 1 -> "2147483647";
                            case 2 -> "-2147483647";
                            default -> Integer.toString(rnd.nextInt(5) - 2);
                        };
                        String s = rnd.nextInt(4) == 0 ? "null" : "'s" + rnd.nextInt(8) + "'";
                        String s2 = rnd.nextInt(4) == 0 ? "null" : "'s" + rnd.nextInt(8) + "'";
                        execute("insert into " + table + " values (" + key + "," + row + "," + row + ","
                                + (rnd.nextInt(4) == 0 ? "null" : Double.toString(row * 0.25)) + "," + s + "," + s2
                                + ",'keep','2020-01-0" + (1 + row / 12) + "')");
                    }
                }
                storage("a", scenario % 3);
                storage("b", scenario / 3 % 3);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, scenario < 9 ? 1 : Integer.MAX_VALUE);
                for (int workers : new int[]{1, 2, 4}) {
                    try (SqlExecutionContextImpl context = context(engine, workers)) {
                        context.changePageFrameSizes(1, 1 + rnd.nextInt(5));
                        for (String join : JOINS) {
                            // Random texts leave some SYMBOL keys in one table only.
                            for (String on : KEYS) {
                                for (int keys = 0; keys < 4; keys++) {
                                    String group = columns(keys, true);
                                    assertDifferential("select " + (group.isEmpty() ? "" : group + ", ")
                                            + aggregates(1 + rnd.nextInt(3), true) + from(join, on)
                                            + (group.isEmpty() ? "" : " order by " + group), context, true);
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSymbolKeySelfJoinsAndBuildSymbolPredicates() throws Exception {
        assertMemoryLeak(() -> {
            createTables(false);
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 2);
                for (String join : JOINS) {
                    // Both inputs read one dictionary through separate cursors, and the build still translates.
                    for (String table : new String[]{"a", "b"}) {
                        String from = " from " + table + " r" + join + table + " p on r.s=p.s";
                        assertDifferential("select " + aggregates(3, true) + from, context, true);
                        assertDifferential("select r.s,p.s2," + aggregates(3, true) + from + " order by r.s,p.s2", context, true);
                        assertDifferential("select r.s,p.s2,count(*) from " + table + " r" + join + table
                                + " p on r.s=p.s2 order by r.s,p.s2", context, true);
                    }
                    // Joined metadata keeps the build input's static symbol tables, so predicates on build
                    // SYMBOL columns resolve their constants through the build dictionary at init.
                    String build = join.equals(" right join ") ? "r" : "p";
                    for (String on : KEYS) {
                        for (String predicate : new String[]{
                                build + ".s='shared'", build + ".s in ('shared','other')", build + ".s is null",
                                build + ".s is not null", build + ".s!='shared'", build + ".s='absent'",
                                build + ".s not in ('shared','absent')", build + ".s2='shared'"}) {
                            String from = from(join, on) + " where " + predicate;
                            // An INNER join pushes the predicate into the build input; outer joins filter joined pairs.
                            try (RecordCursorFactory factory = engine.select("select count(*)" + from, context)) {
                                String plan = plan(factory, context);
                                Assert.assertEquals(plan, !join.equals(JOINS[0]), plan.contains("postJoinFilter:"));
                            }
                            assertDifferential("select " + aggregates(3, true) + from, context, true);
                            assertDifferential("select r.s,p.s," + aggregates(3, true) + from + " order by r.s,p.s", context, true);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testExcludedFunctionsKeysAndBarriers() throws Exception {
        assertMemoryLeak(() -> {
            createTables(false);
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                for (String join : JOINS) {
                    for (String side : new String[]{"r", "p"}) {
                        for (String aggregate : new String[]{"count(" + side + ".t)", "first(" + side + ".d)",
                                "last(" + side + ".d)", "count_distinct(" + side + ".s)", "count(" + side + ".i::short)"}) {
                            assertDifferential("select " + aggregate + from(join), context, false);
                            assertDifferential("select r.s,p.s," + aggregate + from(join) + " order by r.s,p.s", context, false);
                        }
                        // HashJoinGroupByAggregatesTest covers every admitted class; these flipped from excluded.
                        for (String aggregate : new String[]{"sum(" + side + ".i)", "sum(" + side + ".l)",
                                "avg(" + side + ".i)", "avg(" + side + ".l)", "min(" + side + ".d)", "max(" + side + ".d)",
                                "ksum(" + side + ".d)"}) {
                            assertDifferential("select " + aggregate + from(join), context, true);
                            assertDifferential("select r.s,p.s," + aggregate + from(join) + " order by r.s,p.s", context, true);
                        }
                    }
                    // PostgreSQL-style ::float means DOUBLE (SqlParser.rewritePgCast),
                    // unlike a FLOAT table column. Check the compiled allowlist boundary.
                    assertDifferential("select sum(r.d::float)" + from(join), context, true);
                    for (String on : new String[]{"r.id=p.id and r.i=p.i", "r.id+1=p.id", "r.l=p.l", "r.d=p.d", "r.t=p.t",
                            "r.s=p.s and r.id=p.id", "r.s=p.s and r.s2=p.s2"}) {
                        assertDifferential("select count(*) from " + PROJECTED_R + join + PROJECTED_P + " on " + on, context, false);
                    }
                    // SYMBOL keys match by text through reordered projections, also across different
                    // SYMBOL columns, whose dictionaries assign their own keys.
                    for (String on : new String[]{"r.s=p.s", "r.s=p.s2", "r.s2=p.s"}) {
                        assertDifferential("select count(*) from " + PROJECTED_R + join + PROJECTED_P + " on " + on, context, true);
                        assertDifferential("select r.s,p.s,r.s2,p.s2,count(*)" + from(join, on) + " order by r.s,p.s,r.s2,p.s2", context, true);
                    }
                    for (String barrier : new String[]{"select distinct r.s from ", "select r.s from "}) {
                        String inner = barrier + PROJECTED_R + join + PROJECTED_P + " on r.id=p.id"
                                + (barrier.contains("distinct") ? "" : " limit 2");
                        assertDifferential("select s,count(*) from (" + inner + ") order by s", context, false);
                    }
                }
                for (String table : new String[]{"a", "b"}) {
                    execute("alter table " + table + " add column v float");
                    execute("alter table " + table + " add column str string");
                    execute("alter table " + table + " add column vc varchar");
                    execute("update " + table + " set v=" + (table.equals("a") ? 1 : 2) + ", str=s, vc=s");
                }
                for (String join : JOINS) {
                    for (String side : new String[]{"r", "p"}) {
                        // sum(FLOAT) is SumFloat; avg(FLOAT) passes the FLOAT argument to AvgDouble.
                        for (String aggregate : new String[]{"sum(" + side + ".v)", "avg(" + side + ".v)"}) {
                            boolean isFused = aggregate.startsWith("sum");
                            assertDifferential("select " + aggregate + " from a r" + join + "b p on r.id=p.id", context, isFused);
                            assertDifferential("select r.s,p.s," + aggregate + " from a r" + join
                                    + "b p on r.id=p.id order by r.s,p.s", context, isFused);
                        }
                    }
                    // SYMBOL keys against text keys with the same values keep the ordinary plan.
                    for (String on : new String[]{"r.s=p.str", "r.str=p.s", "r.s=p.vc", "r.vc=p.s"}) {
                        assertDifferential("select r.s,p.s,count(*),sum(r.d),sum(p.d) from a r" + join + "b p on " + on
                                + " order by r.s,p.s", context, false);
                    }
                }
                assertDifferential("select count(*) from a r full join b p on r.id=p.id", context, false);
                assertDifferential("select count(*) from a r left join b p on r.id=p.id and r.d>0", context, false);
                assertDifferential("select count(*) from a r right join b p on r.id=p.id and p.d>0", context, false);
                assertDifferential("select count(*) from a r left join b p on r.id=p.id where r.d>p.d", context, false);
                assertDifferential("select count(*) from a r join b p on r.id=p.id join b q on r.id=q.id", context, false);
                // Random arguments cannot be compared across executions; assert plan equality and
                // deterministic row counts while still consuming the unsupported expression.
                String sql = "select count(*) n, sum(rnd_double()) d" + from(JOINS[0]);
                String ordinaryPlan;
                long pairs;
                context.setParallelHashJoinGroupByEnabled(false);
                try (RecordCursorFactory factory = engine.select(sql, context); RecordCursor cursor = factory.getCursor(context)) {
                    ordinaryPlan = plan(factory, context);
                    Assert.assertTrue(cursor.hasNext());
                    pairs = cursor.getRecord().getLong(0);
                    Assert.assertTrue(cursor.getRecord().getDouble(1) >= 0);
                }
                context.setParallelHashJoinGroupByEnabled(true);
                try (RecordCursorFactory factory = engine.select(sql, context); RecordCursor cursor = factory.getCursor(context)) {
                    Assert.assertEquals(ordinaryPlan, plan(factory, context));
                    Assert.assertFalse(ordinaryPlan.contains("Hash Join Group By"));
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(pairs, cursor.getRecord().getLong(0));
                    Assert.assertTrue(cursor.getRecord().getDouble(1) >= 0);
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testConstantWhereKeepsOrdinaryResults() throws Exception {
        assertMemoryLeak(() -> {
            createTables(false);
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 2);
                for (String join : JOINS) {
                    for (boolean isKeyed : new boolean[]{false, true}) {
                        String select = "SELECT " + (isKeyed ? "r.s, p.s, " : "") + aggregates(3, false);
                        String order = isKeyed ? " ORDER BY r.s, p.s" : "";
                        // generateJoins() replaces the whole join with an empty table for a constant-false WHERE.
                        for (String where : new String[]{" WHERE 1 = 0", " WHERE false", " WHERE 1 = 0 AND r.d > 0"}) {
                            assertOutcome(select + from(join) + where + order, context, false);
                        }
                        assertOutcome("DECLARE @x := 0 " + select + from(join) + " WHERE @x = 1" + order, context, false);
                        // Only an INNER JOIN merges an ON constant into WHERE; an outer join filters its build input.
                        assertOutcome(select + from(join) + " AND 1 = 0" + order, context, !join.equals(JOINS[0]));
                        // The analysis does not evaluate functions, so a constant-true WHERE also keeps the ordinary plan.
                        assertOutcome(select + from(join) + " WHERE 1 = 1" + order, context, false);
                    }
                    assertOutcome("SELECT count(*) n, sum(d) d FROM (SELECT p.d" + from(join) + " WHERE 1 = 0)", context, false);
                }
            }
        });
    }

    @Test
    public void testSymbolEqualityOnNullExtendedRows() throws Exception {
        // For a constant-false ON conjunct the ordinary plan reads the null-extended input from an
        // empty table, while the fused plan filters its build input and keeps the table's symbols.
        // a stores a NULL symbol and b stores none. SYMBOL equality on a null-extended row must
        // depend on neither, so both plans return the same rows with and without the conjunct.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (s SYMBOL, k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE b (s SYMBOL, k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO a VALUES
                        ('x', 1, '2024-01-01T00:00'),
                        (NULL, 2, '2024-01-01T01:00'),
                        ('y', 3, '2024-01-01T02:00')
                    """);
            execute("""
                    INSERT INTO b VALUES
                        ('x', 1, '2024-01-01T00:00'),
                        ('z', 4, '2024-01-01T01:00')
                    """);
            final String[] joins = {" LEFT JOIN ", " RIGHT JOIN "};
            final String[] ons = {"l.k = r.k", "l.k = r.k AND 1 = 2"};
            final String[][] results = {
                    {
                            """
                            ls\trs\tc\tlk\trk
                            \t\t1\t1\t0
                            x\tx\t1\t1\t1
                            y\t\t1\t1\t0
                            """,
                            """
                            ls\trs\tc\tlk\trk
                            \t\t1\t1\t0
                            x\t\t1\t1\t0
                            y\t\t1\t1\t0
                            """
                    },
                    {
                            """
                            ls\trs\tc\tlk\trk
                            \tz\t1\t0\t1
                            x\tx\t1\t1\t1
                            """,
                            """
                            ls\trs\tc\tlk\trk
                            \tx\t1\t0\t1
                            \tz\t1\t0\t1
                            """
                    }
            };
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                for (int i = 0; i < joins.length; i++) {
                    for (int j = 0; j < ons.length; j++) {
                        // The fuzzer found the divergence over projections; plain tables diverge too.
                        for (String input : new String[]{"%s", "(SELECT s, k, ts FROM %s)"}) {
                            final String sql = "SELECT l.s ls, r.s rs, count() c, count(l.k) lk, count(r.k) rk"
                                    + " FROM " + input.formatted("a") + " l" + joins[i] + input.formatted("b") + " r"
                                    + " ON " + ons[j] + " WHERE l.s = l.s AND r.s = r.s ORDER BY ls, rs";
                            for (boolean isFused : new boolean[]{true, false}) {
                                context.setParallelHashJoinGroupByEnabled(isFused);
                                try {
                                    final QueryAssertion assertion = assertQuery(sql)
                                            .withEngine(engine)
                                            .withContext(context)
                                            .expectSize();
                                    if (isFused) {
                                        assertion.withPlanContaining("Hash Join Group By");
                                    } else {
                                        assertion.withPlanNotContaining("Hash Join Group By");
                                    }
                                    assertion.returns(results[i][j]);
                                } finally {
                                    context.setParallelHashJoinGroupByEnabled(true);
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testLatestOnAboveJoinKeepsOrdinaryResults() throws Exception {
        assertMemoryLeak(() -> {
            createTables(false);
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 2);
                for (String join : JOINS) {
                    String rows = "SELECT r.t, r.s, p.s ps, p.d FROM a r" + join + "b p ON r.id = p.id";
                    for (boolean isKeyed : new boolean[]{false, true}) {
                        String select = "SELECT " + (isKeyed ? "s, " : "") + "count(*) n, sum(d) d, count(ps) ps FROM (";
                        // generateLatestBy() filters the joined rows before the ordinary GROUP BY reads them.
                        for (String latest : new String[]{
                                "(" + rows + ") LATEST ON t PARTITION BY s",
                                "(" + rows + " WHERE p.d > 0) LATEST ON t PARTITION BY s",
                                "(" + rows + " ORDER BY r.t DESC) LATEST ON t PARTITION BY s, ps",
                                "SELECT t, s, ps, d FROM (" + rows + ") LATEST ON t PARTITION BY s"
                        }) {
                            assertOutcome(select + latest + (isKeyed ? ") ORDER BY s" : ")"), context, false);
                        }
                    }
                }
                // Child compilation applies LATEST ON to a join input, so that shape keeps the fused plan.
                assertOutcome("SELECT count(*) n, sum(p.d) d FROM a r JOIN ((SELECT id, d, t FROM b) LATEST ON t PARTITION BY id) p"
                        + " ON r.id = p.id", context, true);
            }
        });
    }

    @Test
    public void testSampleByFillKeepsOrdinaryResults() throws Exception {
        assertMemoryLeak(() -> {
            createTables(false);
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 2);
                for (String join : JOINS) {
                    for (boolean isKeyed : new boolean[]{false, true}) {
                        // The ordinary plan fails to compile FROM-TO and TIME ZONE over aliased inputs, so use table names.
                        String select = "SELECT " + (isKeyed ? "a.s, " : "") + "count(*) n, sum(b.d) d, avg(a.d) a"
                                + " FROM a" + join + "b ON a.id = b.id";
                        // generateFill() wraps the ordinary GROUP BY in a fill cursor.
                        for (String fill : new String[]{"FILL(NULL)", "FILL(PREV)", "FILL(0)", "FILL(NULL, 0, PREV)"}) {
                            assertOutcome(select + " SAMPLE BY 1h " + fill + " ALIGN TO CALENDAR", context, false);
                            assertOutcome(select + " SAMPLE BY 1h " + fill + " ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'", context, false);
                            assertOutcome(select + " SAMPLE BY 1h FROM '2019-12-31T22:00:00' TO '2020-01-03T06:00:00' " + fill, context, false);
                        }
                        // Without fill values generateFill() returns the GROUP BY unchanged.
                        for (String fill : new String[]{"", " FILL(NONE)"}) {
                            assertOutcome(select + " SAMPLE BY 1h" + fill + " ALIGN TO CALENDAR", context, true);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testInvalidSqlKeepsCompilationErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables(false);
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                for (String sql : new String[]{"select sum(r.d,p.d)" + from(JOINS[0]), "select avg()" + from(JOINS[2]),
                        "select count(r.missing)" + from(JOINS[1]), "select missing_function(r.d)" + from(JOINS[0]),
                        "select sum(r.d) from a r join b p on r.missing=p.id",
                        // The ordinary compile reaches the WHERE clause before the aggregate.
                        "SELECT sum(missing_aggregate_arg(r.d)) FROM a r JOIN b p ON r.id = p.id WHERE missing_filter(r.d) > 0"}) {
                    String message = null;
                    int position = -1;
                    for (boolean enabled : new boolean[]{false, true}) {
                        context.setParallelHashJoinGroupByEnabled(enabled);
                        try (RecordCursorFactory ignored = engine.select(sql, context)) {
                            Assert.fail(sql);
                        } catch (SqlException ex) {
                            if (enabled) {
                                Assert.assertEquals(sql, message, ex.getFlyweightMessage().toString());
                                Assert.assertEquals(sql, position, ex.getPosition());
                            } else {
                                message = ex.getFlyweightMessage().toString();
                                position = ex.getPosition();
                            }
                        }
                        Assert.assertNull(context.getMemoryTracker());
                    }
                }
            }
        });
    }

    @Test
    public void testIntervalFilterThroughProjectionKeepsOrdinaryResults() throws Exception {
        assertMemoryLeak(() -> {
            createTables(false);
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 2);
                for (String join : JOINS) {
                    // The optimiser pushes the interval below dateadd() as an and_offset pseudo-function,
                    // which only interval extraction and generateFilter() can compile.
                    String from = " FROM (SELECT id, d, s, dateadd('d', -1, t) ts FROM a) r" + join + "b p ON r.id = p.id"
                            + " WHERE r.ts IN '2020-01-01'";
                    assertOutcome("SELECT r.s, count(*) n, sum(p.d) d" + from + " ORDER BY 1", context, false);
                    assertOutcome("SELECT count(*) n, sum(p.d) d" + from, context, false);
                }
            }
        });
    }

    @Test
    public void testScalarSubqueryOnOrLhsKeepsOuterOperand() throws Exception {
        assertSubqueryOperand("SELECT id, d FROM a", "d > (SELECT sum(p.d) d" + INTERVAL_FROM + ")", "OR", false);
    }

    @Test
    public void testInSubqueryOnOrLhsKeepsOuterOperand() throws Exception {
        assertSubqueryOperand("SELECT id, s FROM a",
                "s IN (SELECT s FROM (SELECT r.s, count(*) n, sum(p.d) d" + INTERVAL_FROM + ") WHERE n > 0)", "OR", false);
    }

    @Test
    public void testScalarSubqueryOnAndLhsKeepsOuterOperand() throws Exception {
        assertSubqueryOperand("SELECT id, d FROM a", "d > (SELECT sum(p.d) d" + INTERVAL_FROM + ")", "AND", false);
    }

    @Test
    public void testFusedSubqueryOnOrLhsKeepsOuterOperand() throws Exception {
        // The analysis accepts this sub-query while the outer operand is pending.
        assertSubqueryOperand("SELECT id, d FROM a", "d > (SELECT sum(p.d) d FROM a r%sb p ON r.id = p.id)", "OR", true);
    }

    @Test
    public void testUndefinedBindVariablesKeepOrdinaryTypesAndErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables(false);
            for (String table : new String[]{"a", "b"}) {
                execute("ALTER TABLE " + table + " ADD COLUMN n SYMBOL");
                execute("ALTER TABLE " + table + " ADD COLUMN str STRING");
                execute("UPDATE " + table + " SET n = CASE WHEN s = 'shared' THEN '1' ELSE s END,"
                        + " str = CASE WHEN s = 'shared' THEN '1' ELSE s END");
            }
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null);
                context.changePageFrameSizes(1, 2);
                // Collect every shape before comparing, so a regression reports all divergent outcomes.
                StringBuilder expected = new StringBuilder();
                StringBuilder actual = new StringBuilder();
                for (String join : JOINS) {
                    String from = " FROM a r" + join + "b p ON r.id = p.id";
                    // The ordinary compile infers $1 from each WHERE or ON clause before it parses the aggregate.
                    for (String sql : new String[]{
                            "SELECT sum(r.d + $1)" + from + " WHERE r.n = $1",
                            "SELECT sum(r.d + $1)" + from + " WHERE r.str = $1",
                            "SELECT sum(r.d * length($1))" + from + " WHERE r.d > $1",
                            "SELECT sum(r.d * length($1))" + from + " WHERE p.d > $1",
                            "SELECT sum(r.d * length($1))" + from + " AND p.d > $1",
                            // The ordinary compile generates the SQL LHS input first, the fused plan its probe input.
                            "SELECT sum(p.d) FROM (SELECT * FROM a WHERE d > $1) r" + join
                                    + "(SELECT * FROM b WHERE length($1) > 0) p ON r.id = p.id",
                            "SELECT sum(p.d) FROM (SELECT * FROM a WHERE length($1) > 0) r" + join
                                    + "(SELECT * FROM b WHERE d > $1) p ON r.id = p.id"
                    }) {
                        expected.append(sql).append('\n').append(bindOutcome(sql, context, false, ColumnType.UNDEFINED, "1")).append('\n');
                        actual.append(sql).append('\n').append(bindOutcome(sql, context, true, ColumnType.UNDEFINED, "1")).append('\n');
                    }
                }
                Assert.assertEquals(expected.toString(), actual.toString());
                // A client-typed bind variable needs no inference, so it keeps the fused plan.
                bindVariableService.clear();
                bindVariableService.setStr(0, "1");
                assertOutcome("SELECT sum(r.d + $1) FROM a r JOIN b p ON r.id = p.id WHERE r.n = $1", context, true);
            }
        });
    }

    @Test
    public void testWeakDimensionArrayBindVariablesKeepOrdinaryTypesAndErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables(false);
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null);
                context.changePageFrameSizes(1, 2);
                // PG Parse defines a float8[] parameter with weak dimensions, and the first cast that
                // FunctionParser compiles gives it concrete dimensions.
                int weakDims = ColumnType.encodeArrayTypeWithWeakDims(ColumnType.DOUBLE, true);
                StringBuilder expected = new StringBuilder();
                StringBuilder actual = new StringBuilder();
                for (String join : JOINS) {
                    String from = " FROM a r" + join + "b p ON r.id = p.id";
                    for (String[] casts : new String[][]{{"DOUBLE[]", "DOUBLE[][]"}, {"DOUBLE[][]", "DOUBLE[]"}}) {
                        // A later cast to fewer dimensions than the first one fails to compile.
                        String first = "array_sum($1::" + casts[0] + ")";
                        String second = "array_sum($1::" + casts[1] + ")";
                        for (String sql : new String[]{
                                "SELECT sum(r.d + " + second + ")" + from + " WHERE r.d > " + first,
                                "SELECT sum(r.d + " + second + ")" + from + " WHERE p.d > " + first,
                                "SELECT sum(r.d + " + second + ")" + from + " AND p.d > " + first,
                                "SELECT sum(p.d) FROM (SELECT * FROM a WHERE d > " + first + ") r" + join
                                        + "(SELECT * FROM b WHERE d > " + second + ") p ON r.id = p.id"
                        }) {
                            expected.append(sql).append('\n').append(bindOutcome(sql, context, false, weakDims, "0.5")).append('\n');
                            actual.append(sql).append('\n').append(bindOutcome(sql, context, true, weakDims, "0.5")).append('\n');
                        }
                    }
                }
                Assert.assertEquals(expected.toString(), actual.toString());
                // Casts leave an array with concrete dimensions unchanged, so it keeps the fused plan.
                bindVariableService.clear();
                bindVariableService.define(0, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1), 0);
                bindVariableService.setStr(0, "{0.5}");
                assertOutcome("SELECT sum(r.d + array_sum($1::DOUBLE[][])) FROM a r JOIN b p ON r.id = p.id"
                        + " WHERE r.d > array_sum($1::DOUBLE[])", context, true);
            }
        });
    }

    private static String aggregates(int roles, boolean symbols) {
        String sql = "count(*) pairs, count() pairs_again";
        for (int side = 1; side <= 2; side++) {
            if ((roles & side) != 0) {
                String s = side == 1 ? "r" : "p";
                if (symbols) {
                    sql += ",count(" + s + ".s) " + s + "s,count(" + s + ".s) " + s + "again,count(" + s + ".s2) " + s + "s2"
                            + ",count(length(" + s + ".s)) " + s + "len,sum(length(" + s + ".s2)::double) " + s + "sum"
                            + ",avg(length(" + s + ".s)::double) " + s + "avg";
                } else {
                    sql += ",count(" + s + ".id) " + s + "key,count(" + s + ".i) " + s + "i,count(" + s + ".l) " + s + "l"
                            + ",count(" + s + ".d) " + s + "d,count(" + s + ".s) " + s + "s,sum(" + s + ".d) " + s + "sum"
                            + ",avg(" + s + ".d) " + s + "avg,sum(" + s + ".d) " + s + "again"
                            + ",sum(coalesce(" + s + ".d,0.0)) " + s + "expr,avg(" + s + ".id::double) " + s + "keyavg";
                }
            }
        }
        return sql;
    }

    // Compares outcomes before plans, so a regression reports the wrong answer rather than only the plan.
    private static void assertOutcome(String sql, SqlExecutionContextImpl context, boolean isFused) throws Exception {
        String expected;
        context.setParallelHashJoinGroupByEnabled(false);
        try (RecordCursorFactory baseline = context.getCairoEngine().select(sql, context)) {
            Assert.assertFalse(plan(baseline, context).contains("Hash Join Group By"));
            expected = outcome(baseline, context);
        } finally {
            context.setParallelHashJoinGroupByEnabled(true);
        }
        try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context)) {
            Assert.assertEquals(sql, expected, outcome(factory, context));
            String actualPlan = plan(factory, context);
            Assert.assertEquals(sql + "\n" + actualPlan, isFused, actualPlan.contains("Hash Join Group By"));
        }
        Assert.assertNull(context.getMemoryTracker());
    }

    // FunctionParser visits a binary operator's right operand first, so a sub-query on the left compiles,
    // fused analysis included, while the right operand's function is pending in the same parser. The
    // mirrored placement, which compiles the sub-query before any outer operand, runs first as a control.
    private void assertSubqueryOperand(String select, String operand, String operator, boolean isFused) throws Exception {
        assertMemoryLeak(() -> {
            createTables(false);
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 2);
                for (String join : isFused ? new String[]{JOINS[0]} : JOINS) {
                    String subquery = String.format(operand, join);
                    assertOutcome(select + " WHERE i = 1 " + operator + " " + subquery, context, isFused);
                    assertOutcome(select + " WHERE " + subquery + " " + operator + " i = 1", context, isFused);
                }
            }
        });
    }

    private static void assertSymbolClones(RecordCursorFactory factory, SqlExecutionContextImpl context) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            SymbolTable[] clones = new SymbolTable[4];
            try {
                // Parent initialization may request these before the first row builds/merges.
                for (int col = 0; col < clones.length; col++) {
                    Assert.assertEquals(ColumnType.SYMBOL, factory.getMetadata().getColumnType(col));
                    clones[col] = cursor.newSymbolTable(col);
                    Assert.assertNotNull(clones[col]);
                    Assert.assertNotSame(cursor.getSymbolTable(col), clones[col]);
                }
                for (int pass = 0; pass < 2; pass++) {
                    while (cursor.hasNext()) {
                        Record record = cursor.getRecord();
                        for (int col = 0; col < clones.length; col++) {
                            CharSequence value = record.getSymA(col);
                            String expected = value == null ? null : value.toString();
                            int key = record.getInt(col);
                            Assert.assertEquals(expected, string(clones[col].valueOf(key)));
                            Assert.assertEquals(expected, string(cursor.getSymbolTable(col).valueBOf(key)));
                            Assert.assertEquals(expected, string(record.getSymB(col)));
                            // Access another dictionary while retaining the clone's A flyweight.
                            CharSequence saved = clones[col].valueOf(key);
                            clones[(col + 1) % clones.length].valueBOf(SymbolTable.VALUE_IS_NULL);
                            Assert.assertEquals(expected, string(saved));
                        }
                    }
                    cursor.toTop();
                }
            } finally {
                for (SymbolTable clone : clones) {
                    Misc.freeIfCloseable(clone);
                }
            }
        }
        Assert.assertEquals(0, fused(factory).getAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertNull(context.getMemoryTracker());
    }

    // Compiles with $1 left to the parser, as a client that leaves parameter types unspecified or
    // weakly typed does: an UNDEFINED type leaves $1 undefined. Binds the value, nested to the
    // dimensionality compilation gave $1 when it is an array, and executes.
    private static String bindOutcome(
            String sql,
            SqlExecutionContextImpl context,
            boolean isEnabled,
            int type,
            String value
    ) throws Exception {
        BindVariableService bindVariables = context.getBindVariableService();
        bindVariables.clear();
        if (type != ColumnType.UNDEFINED) {
            bindVariables.define(0, type, 0);
        }
        context.setParallelHashJoinGroupByEnabled(isEnabled);
        try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context)) {
            // PG Describe reports these inferred types for parameters the client left unspecified.
            StringBuilder types = new StringBuilder("types:");
            for (int i = 0, n = bindVariables.getIndexedVariableCount(); i < n; i++) {
                Function variable = bindVariables.getFunction(i);
                if (variable == null) {
                    types.append(" null");
                } else {
                    int variableType = variable.getType();
                    types.append(' ').append(ColumnType.nameOf(variableType));
                    if (variableType != ColumnType.UNDEFINED && ColumnType.isUndefined(variableType)) {
                        types.append(" (undefined)");
                    }
                }
            }
            bindVariables.setStr(0, nestedValue(bindVariables.getFunction(0), value));
            try {
                return types + "\n" + outcome(factory, context);
            } catch (ImplicitCastException e) {
                return types + "\nexecution error: " + e.getFlyweightMessage();
            }
        } catch (SqlException e) {
            return "compile error: [" + e.getPosition() + "] " + e.getFlyweightMessage();
        } finally {
            context.setParallelHashJoinGroupByEnabled(true);
        }
    }

    private static String columns(int roles, boolean symbols) {
        String r = symbols ? "r.s,r.s2,length(r.s)" : "r.id,r.i,r.l,r.d,r.s";
        String p = symbols ? "p.s,p.s2,length(p.s)" : "p.id,p.i,p.l,p.d,p.s";
        return switch (roles) {
            case 1 -> r;
            case 2 -> p;
            case 3 -> r + "," + p;
            default -> "";
        };
    }

    private void createTables(boolean tops) throws Exception {
        createTables(tops, tops);
    }

    // A table with tops gets two rows before every other column exists.
    private void createTables(boolean isTopsA, boolean isTopsB) throws Exception {
        for (String table : new String[]{"a", "b"}) {
            if (table.equals("a") ? isTopsA : isTopsB) {
                execute("create table " + table + " (t timestamp) timestamp(t) partition by day");
                execute("insert into " + table + " values ('2019-12-31'),('2020-01-01')");
                String[] names = {"id", "i", "l", "d", "s", "s2", "f"};
                String[] types = {"int", "int", "long", "double", "symbol", "symbol", "symbol"};
                for (int i = 0; i < names.length; i++) {
                    execute("alter table " + table + " add column " + names[i] + " " + types[i]);
                }
            } else {
                execute("create table " + table + " (id int,i int,l long,d double,s symbol,s2 symbol,f symbol,t timestamp) timestamp(t) partition by day");
            }
            insertRows(table, table.equals("b"));
        }
    }

    private static String from(String join) {
        return from(join, KEYS[0]);
    }

    private static String from(String join, String on) {
        return " from " + PROJECTED_R + join + PROJECTED_P + " on " + on;
    }

    private void insertRows(String table, boolean reverse) throws Exception {
        String first = reverse ? "'other','shared'" : "'shared','other'";
        String second = reverse ? "'shared','other'" : "'other','shared'";
        execute("insert into " + table + " (id,i,l,d,s,s2,f,t) values "
                + "(1,1,10,0.5," + first + ",'keep','2020-01-01T01'),"
                + "(1,null,null,null,null,null,'drop','2020-01-01T02'),"
                + "(1,2,20,2," + second + ",'drop','2020-01-02'),"
                + "(0,0,0,0,'zero','shared','keep','2020-01-02T01'),"
                + "(-2147483647,-1,-10,-0.5,'negative','other','keep','2020-01-02T02'),"
                + "(2147483647,4,40,4,'maximum',null,'drop','2020-01-03'),"
                + "(null,null,null,null,null,'null-key',null,'2020-01-03T01'),"
                + "(" + (reverse ? 7 : 9) + ",8,80,8,'miss" + (reverse ? 7 : 9) + "',null,'keep','2020-01-03T02')");
    }

    private static String nestedValue(@Nullable Function variable, String value) {
        if (variable == null || !ColumnType.isArray(variable.getType())) {
            return value;
        }
        int dims = Math.max(1, ColumnType.decodeWeakArrayDimensionality(variable.getType()));
        return "{".repeat(dims) + value + "}".repeat(dims);
    }

    private static String outcome(RecordCursorFactory factory, SqlExecutionContextImpl context) throws Exception {
        try {
            // The second read catches cursor reuse defects as well as wrong values.
            return result(factory, context) + result(factory, context);
        } catch (CairoException e) {
            return "error: " + e.getFlyweightMessage();
        }
    }

    private void storage(String table, int format) throws Exception {
        execute("alter table " + table + " convert partition to native where t >= 0");
        if (format != 0) {
            execute("alter table " + table + " convert partition to parquet where "
                    + (format == 1 ? "t < '2020-01-02'" : "t >= 0"));
        }
    }

    private static String string(CharSequence value) {
        return value == null ? null : value.toString();
    }
}
