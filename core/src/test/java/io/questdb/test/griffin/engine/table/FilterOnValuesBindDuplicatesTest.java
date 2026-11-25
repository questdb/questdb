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

package io.questdb.test.griffin.engine.table;

import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVarTuple;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.Objects;

/**
 * An IN list of bind variables may hold the same value more than once. The indexed scan
 * ({@code FilterOnValuesRecordCursorFactory}) sorts the values and must scan each distinct
 * value once; a value it scans twice returns its rows twice. Literal IN lists are de-duplicated
 * by the parser, so only bind variables, alone or mixed with literals, reach this.
 * <p>
 * Every query runs a list of bind tuples through one compiled factory, so each execution also
 * starts from the order the previous one left the value list in. The expected rows come from a
 * model of the table: the rows whose symbol is one of the distinct values.
 */
public class FilterOnValuesBindDuplicatesTest extends AbstractCairoTest {
    // row i has ts = i * 30 minutes and v = i + 1; partitioned by hour, so the rows span 4 partitions
    private static final String[] SYMS = {"A", "B", "C", "A", null, "B", "C", "A"};
    // the tuples bound to $1..$4, as the symbol values; null is a NULL bind, Z and Y are absent from the table
    private static final String[][] TUPLES = {
            {"A", "B", "C", null},
            {"A", "A", "B", "C"},
            {"A", "B", "B", "C"},
            {"A", "B", "C", "C"},
            {"A", "A", "B", "B"},
            {"C", "C", "A", "A"},
            {"A", "A", "A", "A"},
            {"C", "C", "C", "C"},
            {"A", "B", "A", "B"},
            {"B", "A", "C", "A"},
            {"C", "A", "C", "C"},
            {"A", "C", "B", "A"},
            {null, "A", null, "B"},
            {null, null, null, null},
            {"A", null, "A", null},
            {"Z", "Z", "A", "Y"},
            {"Z", "A", "Z", "A"},
            {"Z", "Z", "Z", "Z"},
            {"A", "B", "Z", "Y"},
    };

    @Test
    public void testCount() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", true);
            assertQuery("select count() from t where sym in ($1, $2, $3, $4)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .assertBinds(cases(Shape.COUNT, false));
        });
    }

    @Test
    public void testFilteredInRowOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", true);
            assertQuery("select sym, v from t where sym in ($1, $2, $3, $4) and v > 0 order by ts")
                    .noLeakCheck()
                    .assertBinds(cases(Shape.ROWS, false));
        });
    }

    @Test
    public void testFilteredInValueOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", true);
            assertQuery("select sym, v from t where sym in ($1, $2, $3, $4) and v > 0 order by v")
                    .noLeakCheck()
                    .assertBinds(cases(Shape.ROWS, false));
        });
    }

    @Test
    public void testInRowOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", true);
            assertQuery("select sym, v from t where sym in ($1, $2, $3, $4) order by ts")
                    .noLeakCheck()
                    .assertBinds(cases(Shape.ROWS, false));
        });
    }

    @Test
    public void testInSymbolOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", true);
            assertQuery("select sym, v from t where sym in ($1, $2, $3, $4) order by sym, v")
                    .noLeakCheck()
                    .assertBinds(cases(Shape.ROWS_BY_SYMBOL, false));
        });
    }

    @Test
    public void testInValueOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", true);
            assertQuery("select sym, v from t where sym in ($1, $2, $3, $4) order by v")
                    .noLeakCheck()
                    .assertBinds(cases(Shape.ROWS, false));
        });
    }

    @Test
    public void testLatestOn() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t", true);
            assertQuery("select sym, v from (t where sym in ($1, $2, $3, $4) latest on ts partition by sym) order by v")
                    .noLeakCheck()
                    .expectSize()
                    .assertBinds(cases(Shape.LATEST, false));
        });
    }

    @Test
    public void testMixedWithLiterals() throws Exception {
        // the literals A and C are distinct, so the parser keeps both; a bind equal to one of them is a duplicate
        assertMemoryLeak(() -> {
            createTable("t", true);
            assertQuery("select sym, v from t where sym in ($1, 'A', $2, $3, 'C', $4) order by v")
                    .noLeakCheck()
                    .assertBinds(cases(Shape.ROWS, true));
            assertQuery("select count() from t where sym in ('C', $1, $2, $3, $4, 'A')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .assertBinds(cases(Shape.COUNT, true));
        });
    }

    @Test
    public void testNotIndexed() throws Exception {
        // without an index the IN list is a row filter, which matches a row once whatever the duplicates
        assertMemoryLeak(() -> {
            createTable("tn", false);
            assertQuery("select sym, v from tn where sym in ($1, $2, $3, $4) order by v")
                    .noLeakCheck()
                    .assertBinds(cases(Shape.ROWS, false));
            assertQuery("select count() from tn where sym in ($1, $2, $3, $4)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .assertBinds(cases(Shape.COUNT, false));
        });
    }

    @Test
    public void testNotIn() throws Exception {
        // the excluded values are a key set, which a duplicate cannot enlarge
        assertMemoryLeak(() -> {
            createTable("t", true);
            assertQuery("select sym, v from t where sym not in ($1, $2, $3, $4) order by v")
                    .noLeakCheck()
                    .assertBinds(cases(Shape.ROWS_EXCLUDED, false));
        });
    }

    @Test
    public void testPlanIsTheIndexedScan() throws Exception {
        // pins that the queries above reach the indexed IN-list scan, not a row filter
        assertMemoryLeak(() -> {
            createTable("t", true);
            bindVariableService.clear();
            bindVariableService.setStr(0, "A");
            bindVariableService.setStr(1, "A");
            bindVariableService.setStr(2, "B");
            bindVariableService.setStr(3, "B");
            final StringSink plan = new StringSink();
            for (String sql : new String[]{
                    "select sym, v from t where sym in ($1, $2, $3, $4) order by v",
                    "select sym, v from t where sym in ($1, $2, $3, $4) order by ts",
                    "select sym, v from t where sym in ($1, $2, $3, $4) and v > 0 order by v",
                    "select count() from t where sym in ($1, $2, $3, $4)",
            }) {
                plan.clear();
                printSql("explain " + sql);
                plan.put(sink);
                TestUtils.assertContains(plan, "FilterOnValues");
            }
        });
    }

    private static boolean contains(String[] values, String sym) {
        for (String value : values) {
            if (Objects.equals(value, sym)) {
                return true;
            }
        }
        return false;
    }

    private static String expected(Shape shape, String[] values) {
        final StringSink sink = new StringSink();
        if (shape == Shape.COUNT) {
            int count = 0;
            for (String sym : SYMS) {
                if (contains(values, sym)) {
                    count++;
                }
            }
            return "count\n" + count + "\n";
        }
        sink.put("sym\tv\n");
        if (shape == Shape.ROWS_BY_SYMBOL) {
            // NULL sorts first, then the symbols in ascending order
            for (String key : new String[]{null, "A", "B", "C"}) {
                for (int i = 0; i < SYMS.length; i++) {
                    if (Objects.equals(SYMS[i], key) && contains(values, key)) {
                        row(sink, i);
                    }
                }
            }
            return sink.toString();
        }
        for (int i = 0; i < SYMS.length; i++) {
            if (contains(values, SYMS[i]) == (shape == Shape.ROWS_EXCLUDED)) {
                continue;
            }
            if (shape == Shape.LATEST) {
                boolean later = false;
                for (int j = i + 1; j < SYMS.length; j++) {
                    if (Objects.equals(SYMS[j], SYMS[i])) {
                        later = true;
                        break;
                    }
                }
                if (later) {
                    continue;
                }
            }
            row(sink, i);
        }
        return sink.toString();
    }

    private static void row(StringSink sink, int i) {
        sink.put(SYMS[i] == null ? "" : SYMS[i]).put('\t').put(i + 1).put('\n');
    }

    private ObjList<BindVarTuple> cases(Shape shape, boolean withLiteralsAC) {
        final ObjList<BindVarTuple> cases = new ObjList<>();
        for (String[] tuple : TUPLES) {
            final String[] values;
            if (withLiteralsAC) {
                values = new String[tuple.length + 2];
                System.arraycopy(tuple, 0, values, 0, tuple.length);
                values[tuple.length] = "A";
                values[tuple.length + 1] = "C";
            } else {
                values = tuple;
            }
            cases.add(BindVarTuple.ok(
                    String.join(",", java.util.Arrays.stream(tuple).map(s -> s == null ? "NULL" : s).toArray(String[]::new)),
                    expected(shape, values),
                    bindVariableService -> {
                        for (int i = 0; i < tuple.length; i++) {
                            bindVariableService.setStr(i, tuple[i]);
                        }
                    }
            ));
        }
        return cases;
    }

    private void createTable(String name, boolean indexed) throws Exception {
        execute("create table " + name + " (ts timestamp, sym symbol" + (indexed ? " index" : "") + ", v long) timestamp(ts) partition by hour");
        final StringSink insert = new StringSink();
        insert.put("insert into ").put(name).put(" values ");
        for (int i = 0; i < SYMS.length; i++) {
            if (i > 0) {
                insert.put(", ");
            }
            insert.put("(").put(i * 30L * 60 * 1_000_000).put("::timestamp, ")
                    .put(SYMS[i] == null ? "null" : "'" + SYMS[i] + "'")
                    .put(", ").put(i + 1).put(")");
        }
        execute(insert);
    }

    private enum Shape {
        COUNT, LATEST, ROWS, ROWS_BY_SYMBOL, ROWS_EXCLUDED
    }
}
