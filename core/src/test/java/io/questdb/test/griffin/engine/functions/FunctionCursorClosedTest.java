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

package io.questdb.test.griffin.engine.functions;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.AbstractCharSequenceHashSet;
import io.questdb.std.AbstractIntHashSet;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;

/**
 * A cached factory (SELECT) or operation (INSERT ... SELECT) outlives its cursor. A function that
 * builds state which grows with the data it reads - a cast-to-symbol dictionary, the value set of
 * {@code sym IN (SELECT ...)} - must drop that state when the cursor closes, instead of keeping it
 * until the next execution re-initializes the function.
 */
public class FunctionCursorClosedTest extends AbstractCairoTest {
    private static final String CAST_TO_SYMBOL_PREFIX = "io.questdb.griffin.engine.functions.cast.Cast";
    private static final String IN_SYMBOL_CURSOR_PREFIX = "io.questdb.griffin.engine.functions.bool.InSymbolCursorFunctionFactory$";
    private static final String ISSUE_CTES = """
            WITH ref AS (
                SELECT cast(k AS SYMBOL) k, v1
                FROM ref_table
                LATEST ON ts PARTITION BY k
            ),
            a AS (
                (SELECT t1.c1, cast(t1.k AS SYMBOL) k, t1.ts, ref.v1
                 FROM t1 INNER JOIN ref ON t1.k = ref.k) timestamp(ts)
            ),
            b AS (
                (SELECT t2.c1, cast(t2.k AS SYMBOL) k, t2.ts
                 FROM t2 INNER JOIN ref ON t2.k = ref.k) timestamp(ts)
            )
            """;
    private static final String ISSUE_PROJECTION = """
            SELECT a.c1, a.k, a.ts, b.c1 c2
            FROM a ASOF JOIN b ON (a.k = b.k)""";

    @Test
    public void testCastToSymbolInsertAsSelectReleasesDictionaries() throws Exception {
        assertMemoryLeak(() -> {
            createIssueTables();
            // the fixed symbol capacity keeps the writer's metadata version stable, so that the
            // compiled operation stays valid for a second execution
            execute("create table target (c1 long, k symbol capacity 2048, ts timestamp, c2 long) timestamp(ts) partition by day");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(ISSUE_CTES + "INSERT INTO target " + ISSUE_PROJECTION, sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT_AS_SELECT, cq.getType());
                try (InsertOperation op = cq.popInsertOperation()) {
                    final ObjList<Function> casts = collectFunctions(op, CAST_TO_SYMBOL_PREFIX);
                    Assert.assertTrue("expected cast-to-symbol functions in the operation tree", casts.size() > 0);
                    for (int run = 0; run < 2; run++) {
                        op.execute(sqlExecutionContext).await();
                        assertStateReleased(casts);
                    }
                }
            }
            // both executions of the cached operation inserted every row of the ASOF JOIN
            assertQuery("select count() from target").noRandomAccess().expectSize().returns("""
                    count
                    8000
                    """);
        });
    }

    @Test
    public void testCastToSymbolSelectReleasesDictionaries() throws Exception {
        assertMemoryLeak(() -> {
            createIssueTables();
            assertSelectReleasesState(ISSUE_CTES + ISSUE_PROJECTION, CAST_TO_SYMBOL_PREFIX);
        });
    }

    @Test
    public void testCastToSymbolUnderMultiArgFunctionReleasesDictionary() throws Exception {
        // concat() is a MultiArgFunction, so cursorClosed() only reaches the cast if it forwards
        assertMemoryLeak(() -> {
            execute("create table t as (select ('k' || x)::varchar v from long_sequence(1000))");
            assertSelectReleasesState("select concat(cast(v as symbol), '-') c from t", CAST_TO_SYMBOL_PREFIX);
        });
    }

    @Test
    public void testInSymbolCursorReleasesValueSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select ('s' || (x % 100))::symbol s, ('s' || x)::varchar v from long_sequence(1000))");
            execute("create table r as (select ('s' || x)::varchar s from long_sequence(50))");
            // a static symbol column takes the key-set variant, a cast to symbol the value-set one
            assertSelectReleasesState(
                    "select s in (select s from r) a, cast(v as symbol) in (select s from r) b from t",
                    IN_SYMBOL_CURSOR_PREFIX
            );
        });
    }

    @Test
    public void testNumericCastsToSymbolReleaseDictionaries() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table t as (
                        select x l, x::int i, x::short s, x::byte b, x::double d, x::float f,
                               cast(x as char) c, x::date dt, x::timestamp ts
                        from long_sequence(100)
                    )""");
            assertSelectReleasesState(
                    """
                            select cast(l as symbol) l, cast(i as symbol) i, cast(s as symbol) s, cast(b as symbol) b,
                                   cast(d as symbol) d, cast(f as symbol) f, cast(c as symbol) c,
                                   cast(dt as symbol) dt, cast(ts as symbol) ts
                            from t""",
                    CAST_TO_SYMBOL_PREFIX
            );
        });
    }

    private static void assertSelectReleasesState(String sql, String functionClassPrefix) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            final ObjList<Function> functions = collectFunctions(factory, functionClassPrefix);
            Assert.assertTrue("expected " + functionClassPrefix + "* functions in the factory tree", functions.size() > 0);
            String firstRun = null;
            for (int run = 0; run < 2; run++) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    for (int i = 0, n = functions.size(); i < n; i++) {
                        final Function function = functions.getQuick(i);
                        Assert.assertTrue(
                                "an open cursor must have populated " + function.getClass().getName(),
                                stateSize(function) > 0
                        );
                    }
                }
                assertStateReleased(functions);
                // the cached factory re-executes after the release and returns the same rows
                if (firstRun == null) {
                    firstRun = sink.toString();
                } else {
                    Assert.assertEquals(firstRun, sink.toString());
                }
            }
        }
    }

    private static void assertStateReleased(ObjList<Function> functions) throws IllegalAccessException {
        for (int i = 0, n = functions.size(); i < n; i++) {
            final Function function = functions.getQuick(i);
            Assert.assertEquals(
                    "cursor close must release the state of " + function.getClass().getName(),
                    0,
                    stateSize(function)
            );
        }
    }

    // Walks the factory/operation object graph, staying within the SQL engine's own classes, and
    // collects the functions whose class name starts with the given prefix. Factories keep their
    // functions in private fields with no common accessor, so reflection is the only uniform way
    // to reach them.
    private static ObjList<Function> collectFunctions(Object root, String classNamePrefix) throws IllegalAccessException {
        final ObjList<Function> result = new ObjList<>();
        final IdentityHashMap<Object, Boolean> visited = new IdentityHashMap<>();
        final ArrayDeque<Object> stack = new ArrayDeque<>();
        stack.push(root);
        while (!stack.isEmpty()) {
            final Object o = stack.pop();
            if (visited.put(o, Boolean.TRUE) != null) {
                continue;
            }
            if (o instanceof Function f && o.getClass().getName().startsWith(classNamePrefix)) {
                result.add(f);
            }
            if (o instanceof ObjList<?> list) {
                for (int i = 0, n = list.size(); i < n; i++) {
                    pushIfPresent(stack, list.getQuick(i));
                }
                continue;
            }
            final Class<?> clazz = o.getClass();
            if (clazz.isArray()) {
                if (!clazz.getComponentType().isPrimitive()) {
                    for (Object e : (Object[]) o) {
                        pushIfPresent(stack, e);
                    }
                }
                continue;
            }
            if (!clazz.getName().startsWith("io.questdb.griffin.") || o instanceof SqlExecutionContext || o instanceof SqlCompiler) {
                continue;
            }
            for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
                for (Field field : c.getDeclaredFields()) {
                    if (Modifier.isStatic(field.getModifiers()) || field.getType().isPrimitive()) {
                        continue;
                    }
                    field.setAccessible(true);
                    pushIfPresent(stack, field.get(o));
                }
            }
        }
        return result;
    }

    private static void createIssueTables() throws Exception {
        execute("create table ref_table (k varchar, v1 long, ts timestamp) timestamp(ts) partition by day");
        execute("insert into ref_table select ('k' || x)::varchar, x, (x * 1000000)::timestamp from long_sequence(1000)");
        execute("insert into ref_table select ('k' || x)::varchar, x + 1, (x * 1000000 + 2000000000)::timestamp from long_sequence(1000)");
        execute("create table t1 (c1 long, k varchar, ts timestamp) timestamp(ts) partition by day");
        execute("insert into t1 select x, ('k' || (1 + x % 1000))::varchar, (x * 1000000)::timestamp from long_sequence(4000)");
        execute("create table t2 (c1 long, k varchar, ts timestamp) timestamp(ts) partition by day");
        execute("insert into t2 select x, ('k' || (1 + x % 1000))::varchar, (x * 1000000)::timestamp from long_sequence(4000)");
    }

    private static void pushIfPresent(ArrayDeque<Object> stack, Object o) {
        if (o != null) {
            stack.push(o);
        }
    }

    // None of these functions exposes its per-query state, so the test reads it reflectively:
    // a cast to symbol counts the keys it has minted in "next" (starting at 1), and the
    // IN (SELECT ...) functions keep the subquery's values in a hash set.
    private static int stateSize(Function function) throws IllegalAccessException {
        for (Class<?> c = function.getClass(); c != null; c = c.getSuperclass()) {
            try {
                final Field next = c.getDeclaredField("next");
                next.setAccessible(true);
                return next.getInt(function) - 1;
            } catch (NoSuchFieldException ignore) {
                // not a cast to symbol, or the counter lives in a superclass
            }
        }
        for (Field field : function.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            final Object value = field.get(function);
            if (value instanceof AbstractCharSequenceHashSet set) {
                return set.size();
            }
            if (value instanceof AbstractIntHashSet set) {
                return set.size();
            }
        }
        throw new AssertionError("no per-query state found in " + function.getClass().getName());
    }
}
