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
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.async.UnorderedPageFrameReduceJob;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.HashJoinGroupByCandidate;
import io.questdb.griffin.HashJoinGroupByFunctions;
import io.questdb.griffin.HashJoinGroupByMetadata;
import io.questdb.griffin.PostOrderTreeTraversalAlgo;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.table.AsyncFilterContext;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.mp.Job;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.sql.async.SlotGatedWorkStealingStrategy;
import io.questdb.test.tools.LimitedMemoryTracker;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class AsyncHashJoinGroupByTest extends AbstractCairoTest {
    private static final String AGGREGATES = "select p.country, year(r.reading_ts) yr, month(r.reading_ts) mo, "
            + "sum(r.energy_kwh) energy, avg(r.irradiance_wm2) irradiance, sum(p.installed_kwp) capacity";
    private static final String INNER = " from r join p on r.plant_id=p.plant_id";
    private static final String OUTER = " from r left join p on r.plant_id=p.plant_id";
    private static final int WORKERS = 3;

    @Before
    public void setUp() {
        factoryProvider = SlotGatedWorkStealingStrategy.newFactoryProvider();
        super.setUp();
    }

    @Test
    public void testInnerOuterAndNormalizedRight() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String join : new String[]{INNER, OUTER, " from p right join r on r.plant_id=p.plant_id"}) {
                String sql = AGGREGATES + join;
                try (Fixture f = new Fixture(sql)) {
                    f.assertResults(sql);
                    f.assertResults(sql);
                }
            }
        });
    }

    @Test
    public void testEmptyBuildProbeAndAllMisses() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (Fixture inner = new Fixture(AGGREGATES + INNER);
                 Fixture outer = new Fixture(AGGREGATES + OUTER)) {
                execute("truncate table p");
                inner.assertResults(AGGREGATES + INNER);
                outer.assertResults(AGGREGATES + OUTER);
                execute("insert into p values (9, 'IT', 17)");
                inner.assertResults(AGGREGATES + INNER);
                outer.assertResults(AGGREGATES + OUTER);
                execute("truncate table r");
                inner.assertResults(AGGREGATES + INNER);
                outer.assertResults(AGGREGATES + OUTER);
                execute("truncate table p");
                inner.assertResults(AGGREGATES + INNER);
                outer.assertResults(AGGREGATES + OUTER);
            }
        });
    }

    @Test
    public void testPostJoinFiltersDoNotManufactureMisses() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String predicate : new String[]{"p.country is null", "p.installed_kwp is null", "p.installed_kwp > 6", "p.country = 'ES'"}) {
                String sql = "select year(r.reading_ts) yr, count(*) pairs, count(p.plant_id) ids, "
                        + "sum(coalesce(p.installed_kwp, 0.0)) capacity" + OUTER + " where " + predicate;
                try (Fixture f = new Fixture(sql)) {
                    f.assertResults(sql);
                }
            }
        });
    }

    @Test
    public void testInputFiltersAndIntervals() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String sql = AGGREGATES + " from r left join p on r.plant_id=p.plant_id and p.country in ('ES','IT')"
                    + " where r.reading_ts >= '2020-01-01' and r.reading_ts < '2021-01-01' and r.energy_kwh >= 20";
            try (Fixture f = new Fixture(sql,
                    "r where reading_ts >= '2020-01-01' and reading_ts < '2021-01-01'", ints(0, 1, 2, 3),
                    "p where country in ('ES','IT')", ints(0, 1, 2), "energy_kwh >= 20", null)) {
                f.assertResults(sql);
            }
        });
    }

    @Test
    public void testNormalizedRightWithPrunedReorderedProjections() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String probe = "select energy_kwh e, reading_ts ts, plant_id id, irradiance_wm2 irr from r";
            String build = "select installed_kwp cap, plant_id id, country c from p";
            String sql = "select p.c country, year(r.ts) yr, month(r.ts) mo, sum(r.e) energy, avg(r.irr) irradiance, sum(p.cap) capacity"
                    + " from (" + build + ") p right join (" + probe + ") r on r.id=p.id where p.cap is null";
            try (Fixture f = new Fixture(sql, probe, ints(2, 1, 0, 3), build, ints(2, 0, 1), null, null)) {
                f.assertResults(sql);
            }
        });
    }

    @Test
    public void testColumnTopsProbeSymbolsAndRebinding() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("alter table r add column tag symbol");
            execute("insert into r values (1, '2022-01-01', 15, 150, 'left')");
            bindVariableService.setDouble(0, 2);
            bindVariableService.setStr(1, "ES");
            String sql = "select r.tag, p.country, sum(r.energy_kwh * $1) energy" + OUTER + " where p.country = $2";
            try (Fixture f = new Fixture(sql, "select tag, energy_kwh, plant_id, reading_ts, irradiance_wm2 from r",
                    ints(4, 2, 0, 1, 3), "p", ints(0, 1, 2), null, null)) {
                f.assertResults(sql);
                execute("truncate table p");
                execute("insert into p values (1, 'IT', 17), (1, 'IT', null)");
                bindVariableService.setDouble(0, 7);
                bindVariableService.setStr(1, "IT");
                f.assertResults(sql);
                execute("truncate table p");
                f.assertResults(sql);
            }
        });
    }

    @Test
    public void testCompileCloseEarlyCloseAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (Fixture ignored = new Fixture(AGGREGATES + OUTER)) {
                // No cursor acquisition.
            }
            try (Fixture f = new Fixture(AGGREGATES + OUTER)) {
                try (RecordCursor cursor = f.getCursor()) {
                    Assert.assertEquals(-1, cursor.size());
                    Assert.assertNull(cursor.getSymbolTable(0).valueOf(SymbolTable.VALUE_IS_NULL));
                }
                f.assertResults(AGGREGATES + OUTER);
                try (RecordCursor cursor = f.getCursor()) {
                    Assert.assertTrue(cursor.hasNext());
                }
                f.assertResults(AGGREGATES + OUTER);
            }
        });
    }

    @Test
    public void testConcurrentProbeAndWorkerFailureReuse() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 10);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 10);
        assertMemoryLeak(() -> {
            createTables();
            execute("insert into r select 1, timestamp_sequence('2022-01-01', 1000000L), x::double, x::double from long_sequence(1000)");
            Hook hook = new Hook();
            String sql = AGGREGATES + OUTER + " where p.installed_kwp is null";
            try (Fixture f = new Fixture(sql, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2), null, hook);
                 Reducers reducers = new Reducers()) {
                CountDownLatch acquired = new CountDownLatch(2);
                f.factory.getAtom().getPerWorkerLocks().setTestAcquireLatch(acquired);
                hook.gate = new CountDownLatch(2);
                f.assertResults(sql);
                Assert.assertEquals(0, acquired.getCount());
                Assert.assertEquals(WORKERS + 1, hook.initCount.get());
                Assert.assertTrue("probe functions must run concurrently", hook.maxActive.get() >= 2);
                Assert.assertEquals(0, f.factory.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                hook.gate = null;
                hook.fail = true;
                acquired = new CountDownLatch(1);
                f.factory.getAtom().getPerWorkerLocks().setTestAcquireLatch(acquired);
                try (RecordCursor cursor = f.getCursor()) {
                    cursor.hasNext();
                    Assert.fail();
                } catch (CairoException expected) {
                    Assert.assertTrue(expected.getFlyweightMessage().toString().contains("injected probe failure"));
                }
                Assert.assertEquals(0, f.factory.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                Assert.assertEquals(0, acquired.getCount());
                hook.fail = false;
                f.assertResults(sql);
            }
        });
    }

    @Test
    public void testCancellationInsideDuplicateChainAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("truncate table r");
            execute("truncate table p");
            execute("insert into r values (1, '2020-01-01', 10, 100)");
            execute("insert into p select 1, 'ES', null::double from long_sequence(100000)");
            Hook hook = new Hook();
            String sql = AGGREGATES + OUTER + " where p.installed_kwp is null";
            SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
            AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine);
            ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
            try (Fixture f = new Fixture(sql, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2), null, hook)) {
                hook.cancel = breaker;
                try (RecordCursor cursor = f.getCursor()) {
                    cursor.hasNext();
                    Assert.fail();
                } catch (CairoException expected) {
                    Assert.assertTrue(expected.isInterruption());
                }
                Assert.assertEquals("cancellation must stop within one duplicate loop", 32, hook.calls.get());
                Assert.assertEquals(0, f.factory.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                hook.cancel = null;
                breaker.reset();
                f.assertResults(sql);
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
            }
        });
    }

    @Test
    public void testInitializationFailureAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            Hook hook = new Hook();
            String sql = AGGREGATES + OUTER + " where p.installed_kwp is null";
            try (Fixture f = new Fixture(sql, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2), null, hook)) {
                hook.failInit = true;
                try (RecordCursor ignored = f.getCursor()) {
                    Assert.fail();
                } catch (SqlException expected) {
                    Assert.assertEquals("injected init failure", expected.getFlyweightMessage().toString());
                }
                Assert.assertTrue(hook.closed.get() > 0);
                Assert.assertFalse(sqlExecutionContext.getCloneSymbolTables());
                hook.failInit = false;
                f.assertResults(sql);
            }
        });
    }

    @Test
    public void testEmptyInnerSkipsProbeButOuterScans() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("truncate table p");
            Hook hook = new Hook();
            hook.fail = true;
            try (Fixture inner = new Fixture(AGGREGATES + INNER, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2), "energy_kwh > 0", hook);
                 Fixture outer = new Fixture(AGGREGATES + OUTER, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2), "energy_kwh > 0", hook)) {
                inner.assertResults(AGGREGATES + INNER);
                Assert.assertEquals(0, hook.calls.get());
                try (RecordCursor cursor = outer.getCursor()) {
                    cursor.hasNext();
                    Assert.fail();
                } catch (CairoException expected) {
                    Assert.assertTrue(expected.getFlyweightMessage().toString().contains("injected probe failure"));
                }
            }
        });
    }

    @Test
    public void testMemoryLimitsDuringBuildAndReduceAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String sql = AGGREGATES + OUTER;
            try (Fixture f = new Fixture(sql); LimitedMemoryTracker tracker = new LimitedMemoryTracker(1)) {
                MemoryTracker previous = sqlExecutionContext.getMemoryTracker();
                sqlExecutionContext.setMemoryTracker(tracker);
                try {
                    try (RecordCursor ignored = f.factory.getCursor(sqlExecutionContext)) {
                        Assert.fail();
                    } catch (CairoException expected) {
                        Assert.assertTrue(expected.isOutOfMemory());
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                    tracker.setLimit(100_000_000);
                    CountDownLatch acquired = new CountDownLatch(1);
                    f.factory.getAtom().getPerWorkerLocks().setTestAcquireLatch(acquired);
                    try (RecordCursor cursor = f.factory.getCursor(sqlExecutionContext); Reducers reducers = new Reducers()) {
                        Assert.assertTrue("build must be charged before probe", tracker.getUsed() > 0);
                        tracker.setLimit(tracker.getUsed());
                        cursor.hasNext();
                        Assert.fail();
                    } catch (CairoException expected) {
                        Assert.assertTrue(expected.isOutOfMemory());
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(0, acquired.getCount());
                    Assert.assertEquals(0, f.factory.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    tracker.setLimit(100_000_000);
                    try (RecordCursor cursor = f.factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertTrue(tracker.getUsed() > 0);
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                } finally {
                    sqlExecutionContext.setMemoryTracker(previous);
                }
                f.assertResults(sql);
            }
        });
    }

    @Test
    public void testMixedParquetNativeReuseAndDecoderFailure() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String sql = AGGREGATES + OUTER;
            try (Fixture f = new Fixture(sql)) {
                f.assertResults(sql);
                execute("alter table r convert partition to parquet where reading_ts < '2020-02-01'");
                f.assertResults(sql);
                execute("alter table r convert partition to parquet where reading_ts >= '2020-02-01'");
                try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(100_000_000)) {
                    MemoryTracker previous = sqlExecutionContext.getMemoryTracker();
                    sqlExecutionContext.setMemoryTracker(tracker);
                    try {
                        CountDownLatch acquired = new CountDownLatch(1);
                        f.factory.getAtom().getPerWorkerLocks().setTestAcquireLatch(acquired);
                        try (RecordCursor cursor = f.factory.getCursor(sqlExecutionContext); Reducers reducers = new Reducers()) {
                            tracker.setLimit(tracker.getUsed());
                            cursor.hasNext();
                            Assert.fail();
                        } catch (CairoException expected) {
                            Assert.assertTrue(expected.isOutOfMemory());
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                        Assert.assertEquals(0, acquired.getCount());
                        Assert.assertEquals(0, f.factory.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    } finally {
                        sqlExecutionContext.setMemoryTracker(previous);
                    }
                }
                f.assertResults(sql);
                execute("alter table r convert partition to native where reading_ts < '2020-02-01'");
                f.assertResults(sql);
            }
        });
    }

    @Test
    public void testLogicalTypeConversion() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("alter table r convert partition to parquet where reading_ts < '2020-02-01'");
            execute("alter table r alter column energy_kwh type float");
            String sql = AGGREGATES.replace("sum(r.energy_kwh)", "sum(r.energy_kwh::double)") + OUTER;
            try (Fixture f = new Fixture(sql)) {
                f.assertResults(sql);
            }
        });
    }

    @Test
    public void testConstructionFailureClosesOwnedResources() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (Fixture ignored = new Fixture("select count(*)" + INNER)) {
                Assert.fail("unkeyed factory inputs must be rejected");
            } catch (IllegalArgumentException expected) {
                Assert.assertEquals("unsupported fused hash join execution inputs", expected.getMessage());
            }
        });
    }

    private static void appendRow(List<String> rows, Record record, RecordMetadata metadata) {
        StringBuilder row = new StringBuilder();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            if (i > 0) {
                row.append('|');
            }
            switch (ColumnType.tagOf(metadata.getColumnType(i))) {
                case ColumnType.SYMBOL -> row.append(record.getSymA(i));
                case ColumnType.BOOLEAN -> row.append(record.getBool(i));
                case ColumnType.BYTE -> row.append(record.getByte(i));
                case ColumnType.SHORT -> row.append(record.getShort(i));
                case ColumnType.CHAR -> row.append(record.getChar(i));
                case ColumnType.INT -> row.append(record.getInt(i));
                case ColumnType.DATE -> row.append(record.getDate(i));
                case ColumnType.TIMESTAMP -> row.append(record.getTimestamp(i));
                case ColumnType.FLOAT -> row.append(record.getFloat(i));
                case ColumnType.LONG -> row.append(record.getLong(i));
                case ColumnType.DOUBLE -> row.append(record.getDouble(i));
                default -> Assert.fail("unhandled result type: " + metadata.getColumnType(i));
            }
        }
        rows.add(row.toString());
    }

    private void createTables() throws Exception {
        execute("create table r (plant_id int, reading_ts timestamp, energy_kwh double, irradiance_wm2 double) timestamp(reading_ts) partition by month");
        execute("create table p (plant_id int, country symbol, installed_kwp double)");
        execute("insert into r values (1, '2020-01-01', 10, 100), (3, '2020-01-02', 30, 300), "
                + "(1, '2020-01-03', 20, 200), (2, '2020-02-01', 40, null), (null, '2021-01-01', 50, 500)");
        execute("insert into p values (1, 'ES', 5), (1, 'ES', 7), (1, 'IT', null), (2, null, null), (null, 'ES', 11)");
    }

    private static IntList ints(int... values) {
        IntList list = new IntList();
        for (int value : values) {
            list.add(value);
        }
        return list;
    }


    // Ordinary planner children share the enclosing query registration. Compile
    // the two inputs without top-level QueryProgress wrappers, then wrap the fused root.
    private RecordCursorFactory childFactory(String sql) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             SqlCodeGenerator generator = new SqlCodeGenerator(configuration,
                     new FunctionParser(configuration, engine.getFunctionFactoryCache()), new PostOrderTreeTraversalAlgo(),
                     new ObjectPool<>(QueryColumn.FACTORY, 16), new ObjectPool<>(ExpressionNode.FACTORY, 16))) {
            return generator.generate((IQueryModel) compiler.generateExecutionModel(sql, sqlExecutionContext), sqlExecutionContext);
        }
    }

    private static List<String> rows(RecordCursor cursor, RecordMetadata metadata) {
        List<String> rows = new ArrayList<>();
        while (cursor.hasNext()) {
            appendRow(rows, cursor.getRecord(), metadata);
        }
        Collections.sort(rows);
        return rows;
    }

    private static class Hook {
        final AtomicInteger active = new AtomicInteger();
        final AtomicInteger calls = new AtomicInteger();
        final AtomicInteger closed = new AtomicInteger();
        final AtomicInteger maxActive = new AtomicInteger();
        final AtomicInteger initCount = new AtomicInteger();
        volatile AtomicBooleanCircuitBreaker cancel;
        volatile boolean fail;
        volatile boolean failInit;
        volatile CountDownLatch gate;

        void run() {
            int count = calls.incrementAndGet();
            int running = active.incrementAndGet();
            maxActive.accumulateAndGet(running, Math::max);
            try {
                CountDownLatch latch = gate;
                if (latch != null && latch.getCount() > 0) {
                    latch.countDown();
                    try {
                        Assert.assertTrue("concurrent probe gate timed out", latch.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }
                if (fail) {
                    throw CairoException.nonCritical().put("injected probe failure");
                }
                if (cancel != null && count == 32) {
                    cancel.cancel();
                }
            } finally {
                active.decrementAndGet();
            }
        }
    }

    private static class HookFilter extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final Hook hook;

        HookFilter(Function arg, Hook hook) {
            this.arg = arg;
            this.hook = hook;
        }

        @Override
        public void cursorClosed() {
            hook.closed.incrementAndGet();
            arg.cursorClosed();
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record record) {
            hook.run();
            return arg.getBool(record);
        }

        @Override
        public void init(SymbolTableSource source, SqlExecutionContext context) throws SqlException {
            hook.initCount.incrementAndGet();
            if (hook.failInit) {
                throw SqlException.$(0, "injected init failure");
            }
            arg.init(source, context);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private class Fixture implements Closeable {
        private AsyncHashJoinGroupByRecordCursorFactory factory;
        private RecordCursorFactory queryFactory;

        Fixture(String sql) throws Exception {
            this(sql, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2), null, null);
        }

        Fixture(String sql, String probeSql, IntList probeColumns, String buildSql,
                IntList buildColumns, String probeFilterSql, Hook hook) throws Exception {
            RecordCursorFactory probeFactory = null;
            RecordCursorFactory buildFactory = null;
            HashJoinGroupByFunctions functions = null;
            AsyncFilterContext filterContext = null;
            try {
                probeFactory = childFactory(probeSql);
                buildFactory = childFactory(buildSql);
                FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache()) {
                    @Override
                    public Function parseFunction(ExpressionNode node, RecordMetadata metadata, SqlExecutionContext context) throws SqlException {
                        Function function = super.parseFunction(node, metadata, context);
                        return hook != null && function.getType() == ColumnType.BOOLEAN ? new HookFilter(function, hook) : function;
                    }
                };
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     SqlCodeGenerator generator = new SqlCodeGenerator(configuration, parser, new PostOrderTreeTraversalAlgo(),
                             new ObjectPool<>(QueryColumn.FACTORY, 16), new ObjectPool<>(ExpressionNode.FACTORY, 16))) {
                    IQueryModel model = (IQueryModel) compiler.generateExecutionModel(sql, sqlExecutionContext);
                    while (model.getSelectModelType() != IQueryModel.SELECT_MODEL_GROUP_BY) {
                        model = model.getNestedModel();
                    }
                    HashJoinGroupByCandidate candidate = SqlCodeGenerator.getHashJoinGroupByCandidate(model,
                            new FunctionParser(configuration, engine.getFunctionFactoryCache()), sqlExecutionContext);
                    Assert.assertNotNull(sql, candidate);
                    try (HashJoinGroupByMetadata metadata = new HashJoinGroupByMetadata(configuration, candidate,
                            probeFactory.getMetadata(), probeColumns, buildFactory.getMetadata(), buildColumns)) {
                        functions = generator.compileHashJoinGroupByFunctions(model, metadata, WORKERS, sqlExecutionContext);
                        Function probeFilter = null;
                        ObjList<Function> workerFilters = null;
                        if (probeFilterSql != null) {
                            ExpressionNode expression = compiler.testParseExpression(probeFilterSql, QueryModel.FACTORY.newInstance());
                            probeFilter = parser.parseFunction(expression, probeFactory.getMetadata(), sqlExecutionContext);
                            workerFilters = new ObjList<>();
                            for (int i = 0; i < WORKERS; i++) {
                                workerFilters.add(parser.parseFunction(expression, probeFactory.getMetadata(), sqlExecutionContext));
                            }
                        }
                        filterContext = new AsyncFilterContext(configuration, null, null, null,
                                probeFilter, null, workerFilters, WORKERS, 0, 0, 0);
                        RecordCursorFactory probeOwned = probeFactory;
                        RecordCursorFactory buildOwned = buildFactory;
                        HashJoinGroupByFunctions functionsOwned = functions;
                        AsyncFilterContext filtersOwned = filterContext;
                        probeFactory = null;
                        buildFactory = null;
                        functions = null;
                        filterContext = null;
                        factory = new AsyncHashJoinGroupByRecordCursorFactory(engine, probeOwned, buildOwned, metadata,
                                functionsOwned, filtersOwned, candidate.getPhysicalJoinType() == IQueryModel.JOIN_LEFT_OUTER, WORKERS);
                    }
                    queryFactory = new QueryProgress(engine.getQueryRegistry(), sql, factory);
                }
            } finally {
                Misc.free(probeFactory);
                Misc.free(buildFactory);
                Misc.free(functions);
                Misc.free(filterContext);
            }
        }

        @Override
        public void close() {
            queryFactory = Misc.free(queryFactory);
            factory = null;
        }

        RecordCursor getCursor() throws SqlException {
            return queryFactory.getCursor(sqlExecutionContext);
        }

        void assertResults(String sql) throws Exception {
            List<String> expected;
            try (RecordCursorFactory baseline = select(sql); RecordCursor cursor = baseline.getCursor(sqlExecutionContext)) {
                expected = rows(cursor, baseline.getMetadata());
            }
            Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_OTHER, factory.getScanDirection());
            try (RecordCursor cursor = getCursor()) {
                Assert.assertEquals(expected, rows(cursor, factory.getMetadata()));
                Assert.assertEquals(expected.size(), cursor.size());
                cursor.toTop();
                Assert.assertEquals(expected, rows(cursor, factory.getMetadata()));
                cursor.toTop();
                if (cursor.hasNext()) {
                    List<String> a = new ArrayList<>();
                    List<String> b = new ArrayList<>();
                    appendRow(a, cursor.getRecord(), factory.getMetadata());
                    cursor.recordAt(cursor.getRecordB(), cursor.getRecord().getRowId());
                    appendRow(b, cursor.getRecordB(), factory.getMetadata());
                    Assert.assertEquals(a, b);
                }
            }
        }
    }

    /** Dedicated queue consumers also exercise no-affinity work stealing into acquired slots. */
    private class Reducers implements Closeable {
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final List<Thread> threads = new ArrayList<>();

        Reducers() {
            for (int i = 0; i < WORKERS; i++) {
                Thread thread = new Thread(() -> {
                    try (UnorderedPageFrameReduceJob job = new UnorderedPageFrameReduceJob(engine, engine.getMessageBus())) {
                        while (running.get()) {
                            if (!job.run(Job.RUNNING_STATUS)) {
                                Thread.onSpinWait();
                            }
                        }
                    } catch (Throwable th) {
                        error.compareAndSet(null, th);
                    }
                });
                threads.add(thread);
                thread.start();
            }
        }

        @Override
        public void close() {
            running.set(false);
            for (Thread thread : threads) {
                try {
                    thread.join(10000);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                Assert.assertFalse("reducer did not drain", thread.isAlive());
            }
            if (error.get() != null) {
                throw new AssertionError(error.get());
            }
        }
    }
}
