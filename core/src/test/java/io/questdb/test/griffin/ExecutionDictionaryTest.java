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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.cast.CastLongToSymbolFunctionFactory;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ExecutionDictionaryTest extends AbstractCairoTest {
    @Test
    public void testAllCastTypesReleasedWithoutCursorClosed() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong(0, 42);
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            for (String type : List.of("byte", "short", "int", "long", "float", "double", "char", "date", "timestamp", "timestamp_ns", "string", "varchar")) {
                final String expected = switch (type) {
                    case "float" -> Float.toString(42);
                    case "double" -> Double.toString(42);
                    case "char" -> Character.toString((char) 42);
                    default -> Long.toString(42);
                };
                try (RecordCursorFactory factory = select("SELECT ($1::" + type + ")::symbol s")) {
                    RecordCursorFactory base = factory;
                    while (!(base instanceof VirtualRecordCursorFactory)) {
                        base = base.getBaseFactory();
                        Assert.assertNotNull(base);
                    }
                    final SymbolFunction function = (SymbolFunction) ((VirtualRecordCursorFactory) base).getFunctions().getQuick(0);
                    final QueryRegistry registry = engine.getQueryRegistry();
                    for (int run = 0; run < 2; run++) {
                        final long id = registry.register("cast " + type, sqlExecutionContext);
                        final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                        try {
                            function.init(null, sqlExecutionContext);
                            Assert.assertEquals(type, 0, function.getInt(null));
                            TestUtils.assertEquals(expected, function.valueOf(0));
                            Assert.assertTrue(type, tracker.getUsed() > 0);
                        } finally {
                            registry.unregister(id, sqlExecutionContext);
                        }
                        Assert.assertEquals(type, 0, tracker.getUsed());
                        Assert.assertEquals(type, baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                        Assert.assertNull(type, function.valueOf(0));
                    }
                }
            }
        });
    }

    @Test
    public void testCompileFailureReleasesDictionary() throws Exception {
        assertMemoryLeak(() -> {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            final String sql = "SELECT missing FROM long_sequence(length(now()::symbol))";
            assertException(sql, 7, "Invalid column: missing");
            Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
            assertQuery("SELECT 42").noLeakCheck().expectSize().returns("42\n42\n");
        });
    }

    @Test
    public void testCompileTimeDictionaryReleased() throws Exception {
        assertMemoryLeak(() -> {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            // long_sequence evaluates this argument while compiling, without calling init/close.
            sqlExecutionContext.setNowAndFixClock(0, ColumnType.TIMESTAMP_MICRO);
            final String sql = "SELECT x FROM long_sequence(length(now()::symbol))";
            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // The TIMESTAMP-to-SYMBOL cast renders the raw ticks: zero is "0".
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(1, cursor.getRecord().getLong(0));
                    Assert.assertFalse(cursor.hasNext());
                }
            }
            Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
        });
    }

    @Test
    public void testCreateTableAsSelectReleasesCompileTimeDictionary() throws Exception {
        assertMemoryLeak(() -> {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            sqlExecutionContext.setNowAndFixClock(0, ColumnType.TIMESTAMP_MICRO);
            execute("CREATE TABLE compiled AS (SELECT x FROM long_sequence(length(now()::symbol)))");
            Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
            assertQuery("compiled").expectSize().returns("x\n1\n");
        });
    }

    @Test
    public void testGenerateExecutionModelFailureReleasesCompileTimeDictionary() throws Exception {
        assertMemoryLeak(() -> {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (boolean isView : new boolean[]{false, true}) {
                    final SqlException error = Assert.assertThrows(SqlException.class, () -> generateModel(
                            compiler, "SELECT missing FROM long_sequence(length(now()::symbol))", isView
                    ));
                    Assert.assertEquals(7, error.getPosition());
                    TestUtils.assertEquals("Invalid column: missing", error.getFlyweightMessage());
                    Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                    Assert.assertNull(sqlExecutionContext.getExecutionResources());
                }
            }
        });
    }

    @Test
    public void testGenerateExecutionModelReleasesCompileTimeDictionary() throws Exception {
        assertMemoryLeak(() -> {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (boolean isView : new boolean[]{false, true}) {
                    final ExecutionModel model = generateModel(
                            compiler, "SELECT x FROM long_sequence(length(now()::symbol))", isView
                    );
                    Assert.assertEquals(ExecutionModel.QUERY, model.getModelType());
                    Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                }
            }
        });
    }

    @Test
    public void testGenerateSeriesReleasesWithoutFunctionNotification() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong(0, 3);
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            // The generate_series cursor does not forward cursorClosed to its arguments.
            try (RecordCursorFactory factory = select("SELECT * FROM generate_series(1, ($1::symbol)::long)")) {
                for (int run = 0; run < 3; run++) {
                    final MemoryTracker tracker;
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        tracker = sqlExecutionContext.getMemoryTracker();
                        Assert.assertTrue(tracker.getUsed() > 0);
                        for (long x = 1; x <= 3; x++) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(x, cursor.getRecord().getLong(0));
                        }
                        Assert.assertFalse(cursor.hasNext());
                    }
                    Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testImplicitCharCastRegistersDuringConstruction() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final QueryRegistry registry = engine.getQueryRegistry();
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            bindVariableService.setChar(0, 'A');
            final long id = registry.register("implicit CHAR cast", sqlExecutionContext);
            try (Function argument = parser.createBindVariable(sqlExecutionContext, 0, "$1", ExpressionNode.BIND_VARIABLE)) {
                argument.init(null, sqlExecutionContext);
                try (Function function = parser.createImplicitCast(0, argument, ColumnType.SYMBOL)) {
                    Assert.assertEquals(0, function.getInt(null));
                    TestUtils.assertEquals("A", ((SymbolFunction) function).valueOf(0));
                    Assert.assertTrue(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS) > baseline);
                    registry.unregister(id, sqlExecutionContext);
                    Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                    Assert.assertNull(((SymbolFunction) function).valueOf(0));
                }
            } finally {
                if (registry.getEntry(id) != null) {
                    registry.unregister(id, sqlExecutionContext);
                }
            }
        });
    }

    @Test
    public void testInitFailureReleasesDictionary() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setLong(0, 3);
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            final String sql = "SELECT * FROM generate_series(1, ($1::symbol)::long, $2)";
            bindVariableService.setLong(1, 0);
            try (RecordCursorFactory factory = select(sql)) {
                try {
                    factory.getCursor(sqlExecutionContext).close();
                    Assert.fail("zero step must fail");
                } catch (SqlException e) {
                    Assert.assertEquals(sql.indexOf("$2"), e.getPosition());
                    TestUtils.assertContains(e.getFlyweightMessage(), "step cannot be zero");
                }
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                bindVariableService.setLong(1, 1);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(1, cursor.getRecord().getLong(0));
                    // Abandon the cursor before EOF, as a disconnect does.
                }
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
            }
        });
    }

    @Test
    public void testMemoryLimitReleasesDictionary() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 128L);
            bindVariableService.setLong(0, 3);
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            try (RecordCursorFactory factory = select("SELECT * FROM generate_series(1, ($1::symbol)::long)")) {
                try {
                    factory.getCursor(sqlExecutionContext).close();
                    Assert.fail("the dictionary must exceed 128 bytes");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                }
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    for (long x = 1; x <= 3; x++) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(x, cursor.getRecord().getLong(0));
                    }
                    Assert.assertFalse(cursor.hasNext());
                }
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
            }
        });
    }

    @Test
    public void testOwnerRetainsDictionaryUntilLastReference() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            final long id = registry.registerOwner("SELECT 42::symbol", sqlExecutionContext);
            final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
            try (Function function = newCast(42)) {
                function.init(null, sqlExecutionContext);
                Assert.assertEquals(0, function.getInt(null));
                Assert.assertEquals(id, registry.register("nested cursor", sqlExecutionContext));
                registry.unregister(id, sqlExecutionContext);
                Assert.assertTrue(tracker.getUsed() > 0);
                TestUtils.assertEquals("42", ((SymbolFunction) function).valueOf(0));
                registry.unregister(id, sqlExecutionContext);
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                Assert.assertNull(((SymbolFunction) function).valueOf(0));
            } finally {
                if (registry.getEntry(id) != null) {
                    registry.unregister(id, sqlExecutionContext);
                }
            }
        });
    }

    @Test
    public void testSuspendedOwnersKeepSeparateDictionaries() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            final long first = registry.registerOwner("first portal", sqlExecutionContext);
            final MemoryTracker firstTracker = sqlExecutionContext.getMemoryTracker();
            long second = -1;
            try (Function firstFunction = newCast(42); Function secondFunction = newCast(84)) {
                firstFunction.init(null, sqlExecutionContext);
                Assert.assertEquals(0, firstFunction.getInt(null));
                registry.unmountOwner(first, sqlExecutionContext);
                second = registry.registerOwner("second portal", sqlExecutionContext);
                final MemoryTracker secondTracker = sqlExecutionContext.getMemoryTracker();
                secondFunction.init(null, sqlExecutionContext);
                Assert.assertEquals(0, secondFunction.getInt(null));
                registry.unmountOwner(second, sqlExecutionContext);

                registry.mountOwner(first, sqlExecutionContext);
                TestUtils.assertEquals("42", ((SymbolFunction) firstFunction).valueOf(0));
                registry.unmountOwner(first, sqlExecutionContext);
                registry.unregister(first, sqlExecutionContext);
                Assert.assertEquals(0, firstTracker.getUsed());
                Assert.assertTrue(secondTracker.getUsed() > 0);
                registry.mountOwner(second, sqlExecutionContext);
                TestUtils.assertEquals("84", ((SymbolFunction) secondFunction).valueOf(0));
                registry.unregister(second, sqlExecutionContext);
                Assert.assertEquals(0, secondTracker.getUsed());
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
            } finally {
                if (registry.getEntry(first) != null) {
                    registry.unregister(first, sqlExecutionContext);
                }
                if (registry.getEntry(second) != null) {
                    registry.unregister(second, sqlExecutionContext);
                }
            }
        });
    }

    @Test
    public void testUnregisterReleasesDictionaryWithoutCursorClosed() throws Exception {
        assertMemoryLeak(() -> {
            final QueryRegistry registry = engine.getQueryRegistry();
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            try (Function function = newCast(42)) {
                for (int run = 0; run < 3; run++) {
                    final long id = registry.register("SELECT 42::symbol", sqlExecutionContext);
                    final MemoryTracker tracker = sqlExecutionContext.getMemoryTracker();
                    try {
                        function.init(null, sqlExecutionContext);
                        Assert.assertEquals(0, function.getInt(null));
                        Assert.assertTrue(tracker.getUsed() > 0);
                        TestUtils.assertEquals("42", ((SymbolFunction) function).valueOf(0));
                    } finally {
                        registry.unregister(id, sqlExecutionContext);
                    }
                    // Keep the function alive, exactly as a cached factory does, and send no callback.
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                    Assert.assertNull(((SymbolFunction) function).valueOf(0));
                }
            }
        });
    }

    private static ExecutionModel generateModel(SqlCompiler compiler, String sql, boolean isView) throws SqlException {
        return isView
                ? compiler.generateViewExecutionModel(sql, sqlExecutionContext, new LowerCaseCharSequenceObjHashMap<>())
                : compiler.generateExecutionModel(sql, sqlExecutionContext);
    }

    private static Function newCast(long value) {
        final ObjList<Function> args = new ObjList<>();
        args.add(new LongFunction() {
            @Override
            public long getLong(Record rec) {
                return value;
            }
        });
        final IntList positions = new IntList();
        positions.add(0);
        return new CastLongToSymbolFunctionFactory().newInstance(0, args, positions, configuration, sqlExecutionContext);
    }
}
