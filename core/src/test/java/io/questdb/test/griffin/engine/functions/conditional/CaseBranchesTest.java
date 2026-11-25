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

package io.questdb.test.griffin.engine.functions.conditional;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.conditional.CaseBranches;
import io.questdb.griffin.engine.functions.conditional.CaseFunction;
import io.questdb.griffin.engine.functions.memoization.MemoizerFunction;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link CaseBranches} names the values a CASE returns whatever the layout of its arguments: a
 * searched CASE keeps {@code [cond, value, ..., else]}, a switch {@code [value, ..., else, key]},
 * and the parser makes a switch of a searched CASE whose every condition compares one column to a
 * constant. Each expression's THEN values and ELSE are rendered as constants ({@code NULL} for a
 * NULL one, {@code f} for one that is not constant).
 */
public class CaseBranchesTest extends AbstractCairoTest {

    @Test
    public void testBranches() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (i int, l long, d double, s symbol, str string, b boolean, ts timestamp) timestamp(ts) partition by DAY");
            final String[][] cases = {
                    // searched CASE, rewritten to a switch on i
                    {"case when i = 1 then -1 else 1 end", "[-1] else 1", "true"},
                    {"case when i = 1 then -1 when i = 2 then 5 else 1 end", "[-1, 5] else 1", "true"},
                    // simple CASE, a switch, with and without ELSE
                    {"case i when 1 then -1 when 2 then 5 end", "[-1, 5] else NULL", "true"},
                    {"case i when 1 then 7 else -3 end", "[7] else -3", "true"},
                    // searched CASE that stays one: not every condition is an equality
                    {"case when i > 1 then -1 else 2 end", "[-1] else 2", "true"},
                    {"case when i > 1 then 3 end", "[3] else NULL", "true"},
                    {"case when i > 1 then 3 when i = 0 then -4 end", "[3, -4] else NULL", "true"},
                    // SYMBOL keys: one branch of INT constants is its own function; more, a picker
                    {"case s when 'a' then -1 else 1 end", "[-1] else 1", "false"},
                    {"case s when 'a' then -1 end", "[-1] else NULL", "false"},
                    {"case s when 'a' then -1 when 'b' then 2 else 1 end", "[-1, 2] else 1", "true"},
                    {"case s when 'a' then -1 when null then 7 else 1 end", "[-1, 7] else 1", "true"},
                    // STRING, DOUBLE, LONG, BOOLEAN and TIMESTAMP keys
                    {"case str when 'x' then -1 else 1 end", "[-1] else 1", "true"},
                    {"case str when 'x' then -1 when null then 4 end", "[-1, 4] else NULL", "true"},
                    {"case d when 1.5 then 2 else 3 end", "[2] else 3", "true"},
                    {"case l when 10 then 1 else 1000000000000 end", "[1] else 1000000000000", "true"},
                    {"case b when true then 1 when false then 2 end", "[1, 2] else NULL", "true"},
                    {"case b when true then 1 else 2 end", "[1] else 2", "true"},
                    {"case ts when '2020-01-01' then -1 else 0 end", "[-1] else 0", "true"},
                    // nested: the value that is a CASE is not a constant
                    {"case when i = 1 then case when l > 0 then -1 else 2 end else 0 end", "[f] else 0", "true"},
                    {"case when i > 1 then 1 else case i when 0 then -1 end end", "[1] else f", "true"},
                    // casts to the CASE's type, and NULL values
                    {"case when i > 0 then 1 else 2.5 end", "[1] else 2.5", "true"},
                    {"case when i = 1 then null else 2 end", "[NULL] else 2", "true"},
                    {"case i when 1 then 2 else null end", "[2] else NULL", "true"},
                    {"case when i > 0 then (-1)::long else 2 end", "[-1] else 2", "true"},
                    {"case when i > 0 then i else l end", "[f] else f", "true"},
            };
            for (String[] c : cases) {
                try (RecordCursorFactory factory = select("select " + c[0] + " v from x")) {
                    final Function function = projected(factory);
                    Assert.assertTrue(c[0] + ": " + function.getClass(), function instanceof CaseBranches);
                    Assert.assertEquals(c[0], Boolean.parseBoolean(c[2]), function instanceof CaseFunction);
                    Assert.assertEquals(c[0], c[1], render((CaseBranches) function));
                }
            }
            // the nested CASE's own values
            try (RecordCursorFactory factory = select("select case when i = 1 then case when l > 0 then -1 else 2 end else 0 end v from x")) {
                final Function function = projected(factory);
                final Function inner = ((CaseBranches) function).getThenValues().getQuick(0);
                Assert.assertTrue(inner instanceof CaseBranches);
                Assert.assertEquals("[-1] else 2", render((CaseBranches) inner));
            }
        });
    }

    // the query's one projected function
    private static Function projected(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof VirtualRecordCursorFactory virtual) {
                final Function function = virtual.getFunctions().getQuick(0);
                return function instanceof MemoizerFunction memoizer ? memoizer.getArg() : function;
            }
        }
        throw new AssertionError("no projection");
    }

    private static String render(CaseBranches branches) {
        final StringSink sink = new StringSink();
        final ObjList<Function> thenValues = branches.getThenValues();
        sink.put('[');
        for (int i = 0, n = thenValues.size(); i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            render(thenValues.getQuick(i), sink);
        }
        sink.put("] else ");
        render(branches.getElseValue(), sink);
        return sink.toString();
    }

    private static void render(Function value, StringSink sink) {
        if (!value.isConstant()) {
            sink.put('f');
            return;
        }
        switch (ColumnType.tagOf(value.getType())) {
            case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG -> {
                final long v = value.getLong(null);
                if (v == Numbers.LONG_NULL) {
                    sink.put("NULL");
                } else {
                    sink.put(v);
                }
            }
            case ColumnType.DOUBLE -> {
                final double v = value.getDouble(null);
                if (Double.isNaN(v)) {
                    sink.put("NULL");
                } else {
                    sink.put(v);
                }
            }
            case ColumnType.NULL -> sink.put("NULL");
            default -> sink.put("c:").put(ColumnType.nameOf(value.getType()));
        }
    }
}
