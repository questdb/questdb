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
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.map.MapValue;
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
import io.questdb.griffin.PriorityMetadata;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.groupby.SumDoubleGroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByMergeShardJob;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.orderby.SortedLightRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncFilterContext;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.mp.Job;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
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
    private static final String SCALAR_AGGREGATES = "select sum(r.energy_kwh) energy, avg(r.irradiance_wm2) irradiance, "
            + "sum(p.installed_kwp) capacity, count(*) n, count(p.country) countries";
    private static final String INNER = " from r join p on r.plant_id=p.plant_id";
    private static final String OUTER = " from r left join p on r.plant_id=p.plant_id";
    private static final int WORKERS = 3;
    private int frameRows;
    private int factoryWorkerCount = WORKERS;

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
        assertCompileCloseEarlyCloseAndReuse(true);
    }

    @Test
    public void testScalarCompileCloseEarlyCloseAndReuse() throws Exception {
        assertCompileCloseEarlyCloseAndReuse(false);
    }

    @Test
    public void testConcurrentProbeAndWorkerFailureReuse() throws Exception {
        assertConcurrentProbeAndWorkerFailureReuse(true);
    }

    @Test
    public void testScalarConcurrentProbeAndWorkerFailureReuse() throws Exception {
        assertConcurrentProbeAndWorkerFailureReuse(false);
    }

    @Test
    public void testCancellationInsideDuplicateChainAndReuse() throws Exception {
        assertCancellationInsideDuplicateChainAndReuse(true);
    }

    @Test
    public void testScalarCancellationInsideDuplicateChainAndReuse() throws Exception {
        assertCancellationInsideDuplicateChainAndReuse(false);
    }

    @Test
    public void testInitializationFailureAndReuse() throws Exception {
        assertInitializationFailureAndReuse(true);
    }

    @Test
    public void testScalarInitializationFailureAndReuse() throws Exception {
        assertInitializationFailureAndReuse(false);
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
                    try (RecordCursor ignored = f.getRawCursor()) {
                        Assert.fail();
                    } catch (CairoException expected) {
                        Assert.assertTrue(expected.isOutOfMemory());
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                    tracker.setLimit(100_000_000);
                    CountDownLatch acquired = new CountDownLatch(1);
                    f.factory.getAtom().getPerWorkerLocks().setTestAcquireLatch(acquired);
                    try (RecordCursor cursor = f.getRawCursor(); Reducers reducers = new Reducers()) {
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
                    try (RecordCursor cursor = f.getRawCursor()) {
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
                        try (RecordCursor cursor = f.getRawCursor(); Reducers reducers = new Reducers()) {
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
            factoryWorkerCount = 0;
            try (Fixture ignored = new Fixture("select count(*)" + INNER)) {
                Assert.fail("zero worker slots must be rejected");
            } catch (IllegalArgumentException expected) {
                Assert.assertEquals("unsupported fused hash join execution inputs", expected.getMessage());
            }
        });
    }

    @Test
    public void testOrderedSolarQueryBothMergePaths() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("insert into r values (1, '2020-01-04', null, null), (1, '2020-01-05', 70, 800), "
                    + "(2, '2020-02-02', null, null)");
            frameRows = 2;
            for (boolean sharded : new boolean[]{false, true}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, sharded ? 1 : Integer.MAX_VALUE);
                for (String join : new String[]{INNER, OUTER, " from p right join r on r.plant_id=p.plant_id"}) {
                    String probeSql = "r where reading_ts >= '2020-01-01' and reading_ts < '2025-01-01'";
                    String predicate = " where r.reading_ts >= '2020-01-01' and r.reading_ts < '2025-01-01'";
                    String buildSql = "p";
                    if (join.equals(INNER)) {
                        predicate += " and p.country in ('ES','IT')";
                        buildSql += " where country in ('ES','IT')";
                    }
                    String sql = AGGREGATES + join + predicate;
                    try (Fixture f = new Fixture(sql, probeSql, ints(0, 1, 2, 3), buildSql, ints(0, 1, 2), null, null)) {
                        f.projectAndSort(new String[]{"country", "yr", "mo", "energy", "irradiance", "energy / nullif(capacity, 0)"},
                                new String[]{"country", "yr", "mo", "total_energy_kwh", "avg_irradiance", "specific_yield_kwh_kwp"}, 1, 2, 3);
                        String expected = "select p.country, year(r.reading_ts) yr, month(r.reading_ts) mo, "
                                + "sum(r.energy_kwh) total_energy_kwh, avg(r.irradiance_wm2) avg_irradiance, "
                                + "sum(r.energy_kwh) / nullif(sum(p.installed_kwp), 0) specific_yield_kwh_kwp"
                                + join + predicate + " order by country, yr, mo";
                        f.assertOrderedResults(expected, sharded);
                        f.assertOrderedResults(expected, sharded);
                    }
                }
            }
        });
    }

    @Test
    public void testHighCardinalityConcurrentMergeBothPaths() throws Exception {
        assertMemoryLeak(() -> {
            createMergeTables();
            for (boolean sharded : new boolean[]{false, true}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, sharded ? 1 : Integer.MAX_VALUE);
                Hook hook = new Hook();
                String sql = mergeSql();
                try (Fixture f = new Fixture(sql, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2), null, hook);
                     Reducers reducers = new Reducers()) {
                    hook.gate = new CountDownLatch(2);
                    hook.mergeGate = sharded ? new CountDownLatch(2) : null;
                    f.assertResults(sql, sharded);
                    Assert.assertTrue("must merge overlapping partial states", hook.mergeCalls.get() > 0);
                    if (sharded) {
                        Assert.assertEquals("merge workers must run concurrently", 0, hook.mergeGate.getCount());
                    }
                    hook.gate = null;
                    hook.mergeGate = null;
                    f.assertResults(sql, sharded);
                    Assert.assertEquals(0, f.factory.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                }
            }
        });
    }

    @Test
    public void testMergeFailureDrainsAndReusesBothPaths() throws Exception {
        assertMemoryLeak(() -> {
            createMergeTables();
            for (boolean sharded : new boolean[]{false, true}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, sharded ? 1 : Integer.MAX_VALUE);
                Hook hook = new Hook();
                String sql = mergeSql();
                try (Fixture f = new Fixture(sql, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2), null, hook);
                     LimitedMemoryTracker tracker = new LimitedMemoryTracker(100_000_000);
                     Reducers reducers = new Reducers()) {
                    MemoryTracker previous = sqlExecutionContext.getMemoryTracker();
                    sqlExecutionContext.setMemoryTracker(tracker);
                    hook.gate = new CountDownLatch(2);
                    hook.mergeGate = sharded ? new CountDownLatch(2) : null;
                    hook.failMerge = true;
                    try {
                        try (RecordCursor cursor = f.getRawCursor()) {
                            cursor.hasNext();
                            Assert.fail("expected a merge failure");
                        } catch (CairoException expected) {
                            Assert.assertTrue(expected.getFlyweightMessage().toString().contains("injected merge failure"));
                        }
                        Assert.assertTrue(hook.mergeCalls.get() > 0);
                        Assert.assertEquals(0, tracker.getUsed());
                        Assert.assertEquals(0, f.factory.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        hook.failMerge = false;
                        hook.gate = null;
                        hook.mergeGate = null;
                        try (RecordCursor cursor = f.getRawCursor()) {
                            Assert.assertEquals(512, rows(cursor, f.factory.getMetadata()).size());
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                    } finally {
                        sqlExecutionContext.setMemoryTracker(previous);
                    }
                    f.assertResults(sql, sharded);
                }
            }
        });
    }

    @Test
    public void testShardedMergeMemoryLimitDrainsAndReuses() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1);
        assertMemoryLeak(() -> {
            createMergeTables();
            Hook hook = new Hook();
            String sql = mergeSql();
            try (Fixture f = new Fixture(sql, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2), null, hook);
                 LimitedMemoryTracker tracker = new LimitedMemoryTracker(100_000_000);
                 Reducers reducers = new Reducers()) {
                MemoryTracker previous = sqlExecutionContext.getMemoryTracker();
                sqlExecutionContext.setMemoryTracker(tracker);
                hook.gate = new CountDownLatch(2);
                hook.mergeTracker = tracker;
                try {
                    try (RecordCursor cursor = f.getRawCursor()) {
                        cursor.hasNext();
                        Assert.fail("expected destination allocation to fail during merge");
                    } catch (CairoException expected) {
                        Assert.assertTrue(expected.isOutOfMemory());
                        Assert.assertTrue(expected.getFlyweightMessage().toString().contains("query memory limit exceeded"));
                    }
                    Assert.assertTrue("breach must happen after merge began", hook.mergeCalls.get() > 0);
                    Assert.assertTrue("sources and build must be charged at merge", hook.mergeBytes > 0);
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(0, f.factory.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    hook.gate = null;
                    hook.mergeTracker = null;
                    tracker.setLimit(100_000_000);
                    try (RecordCursor cursor = f.getRawCursor()) {
                        Assert.assertEquals(512, rows(cursor, f.factory.getMetadata()).size());
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                } finally {
                    sqlExecutionContext.setMemoryTracker(previous);
                }
                f.assertResults(sql, true);
            }
        });
    }

    @Test
    public void testMergeCancellationDrainsAndReusesBothPaths() throws Exception {
        assertMemoryLeak(() -> {
            createMergeTables();
            SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
            AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine);
            ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
            try {
                for (boolean sharded : new boolean[]{false, true}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, sharded ? 1 : Integer.MAX_VALUE);
                    Hook hook = new Hook();
                    String sql = mergeSql();
                    try (Fixture f = new Fixture(sql, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2), null, hook);
                         LimitedMemoryTracker tracker = new LimitedMemoryTracker(100_000_000);
                         Reducers reducers = new Reducers()) {
                        MemoryTracker previousTracker = sqlExecutionContext.getMemoryTracker();
                        sqlExecutionContext.setMemoryTracker(tracker);
                        hook.gate = new CountDownLatch(2);
                        hook.mergeCancel = breaker;
                        try {
                            try (RecordCursor cursor = f.getRawCursor()) {
                                cursor.hasNext();
                                Assert.fail("expected merge cancellation");
                            } catch (CairoException expected) {
                                Assert.assertTrue(expected.isInterruption());
                            }
                            Assert.assertTrue(hook.mergeCalls.get() > 0);
                            Assert.assertEquals(0, tracker.getUsed());
                            Assert.assertEquals(0, f.factory.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            hook.gate = null;
                            hook.mergeCancel = null;
                            breaker.reset();
                            try (RecordCursor cursor = f.getRawCursor()) {
                                Assert.assertEquals(512, rows(cursor, f.factory.getMetadata()).size());
                            }
                            Assert.assertEquals(0, tracker.getUsed());
                        } finally {
                            sqlExecutionContext.setMemoryTracker(previousTracker);
                        }
                        f.assertResults(sql, sharded);
                    }
                }
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
            }
        });
    }

    @Test
    public void testOwnerMergeDestinationMemoryLimitAndReuse() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, Integer.MAX_VALUE);
        assertMemoryLeak(() -> {
            createMergeTables();
            Hook hook = new Hook();
            String sql = mergeSql();
            try (Fixture f = new Fixture(sql, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2), null, hook);
                 LimitedMemoryTracker tracker = new LimitedMemoryTracker(100_000_000);
                 Reducers reducers = new Reducers()) {
                MemoryTracker previous = sqlExecutionContext.getMemoryTracker();
                sqlExecutionContext.setMemoryTracker(tracker);
                // All nine frames belong to workers, leaving the owner destination unopened.
                // The frame cursor folds the short tail into the preceding frames.
                CountDownLatch acquired = new CountDownLatch(9);
                f.factory.getAtom().getPerWorkerLocks().setTestAcquireLatch(acquired);
                hook.reduceTracker = tracker;
                hook.reduceLimitAt = 20006; // 10003 readings, two matching build payloads each
                try {
                    try (RecordCursor cursor = f.getRawCursor()) {
                        cursor.hasNext();
                        Assert.fail("expected owner destination allocation to breach the live-state limit");
                    } catch (CairoException expected) {
                        Assert.assertTrue(expected.isOutOfMemory());
                    }
                    Assert.assertEquals(0, acquired.getCount());
                    Assert.assertEquals(20006, hook.calls.get());
                    Assert.assertEquals("the destination must fail before any merge update", 0, hook.mergeCalls.get());
                    Assert.assertTrue(hook.mergeBytes > 0);
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(0, f.factory.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    hook.reduceTracker = null;
                    f.factory.getAtom().getPerWorkerLocks().setTestAcquireLatch(null);
                    tracker.setLimit(100_000_000);
                    try (RecordCursor cursor = f.getRawCursor()) {
                        Assert.assertEquals(512, rows(cursor, f.factory.getMetadata()).size());
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                } finally {
                    sqlExecutionContext.setMemoryTracker(previous);
                }
                f.assertResults(sql, false);
            }
        });
    }

    @Test
    public void testShardedOutputEmptyBuildAndDictionaryReuse() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1);
        assertMemoryLeak(() -> {
            createTables();
            for (String join : new String[]{INNER, OUTER}) {
                String sql = AGGREGATES + join;
                try (Fixture f = new Fixture(sql)) {
                    f.assertResults(sql, true);
                    try (RecordCursor cursor = f.getCursor()) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertTrue(f.factory.getAtom().isSharded());
                    }
                    execute("truncate table p");
                    f.assertResults(sql, true);
                    execute("insert into p values (1, 'FR', 17), (1, 'DE', 19), (null, 'FR', 11)");
                    f.assertResults(sql, true);
                }
            }
        });
    }

    @Test
    public void testTenThousandJoinedGroupsBothMergePaths() throws Exception {
        assertMemoryLeak(() -> {
            createMergeTables();
            String sql = "select r.reading_ts, p.country, sum(r.energy_kwh) energy, avg(r.irradiance_wm2) irradiance, "
                    + "sum(p.installed_kwp) capacity" + OUTER;
            for (boolean sharded : new boolean[]{false, true}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, sharded ? 1 : Integer.MAX_VALUE);
                try (Fixture f = new Fixture(sql); Reducers reducers = new Reducers()) {
                    f.assertResults(sql, sharded);
                    f.assertResults(sql, sharded);
                }
            }
        });
    }

    @Test
    public void testScalarAllAggregateTypesConcurrent() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 10);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 10);
        assertMemoryLeak(() -> {
            createTables();
            execute("insert into r select (x % 3)::int, timestamp_sequence('2022-01-01', 1000000L), "
                    + "(x % 8)::double, case when x % 3=0 then null else x::double end from long_sequence(1003)");
            String sql = SCALAR_AGGREGATES + ", avg(p.installed_kwp) avg_capacity, count(r.plant_id) ri, "
                    + "count(p.plant_id) pi, count(r.plant_id::long) rl, count(p.plant_id::long) pl, "
                    + "count(r.irradiance_wm2) rd, count(p.installed_kwp) pd" + OUTER + " where r.energy_kwh >= 0";
            Hook hook = new Hook();
            try (Fixture f = new Fixture(sql, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2), "energy_kwh >= 0", hook);
                 Reducers reducers = new Reducers()) {
                f.factory.getAtom().getPerWorkerLocks().setTestAcquireLatch(new CountDownLatch(2));
                hook.gate = new CountDownLatch(2);
                f.assertResults(sql, false);
                Assert.assertTrue(hook.maxActive.get() >= 2);
                Assert.assertTrue(hook.mergeCalls.get() > 0);
                Assert.assertEquals(0, f.factory.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
            }
        });
    }

    @Test
    public void testScalarStateMemoryLimitAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String sql = SCALAR_AGGREGATES + OUTER;
            try (Fixture f = new Fixture(sql); LimitedMemoryTracker tracker = new LimitedMemoryTracker(100_000_000)) {
                MemoryTracker previous = sqlExecutionContext.getMemoryTracker();
                sqlExecutionContext.setMemoryTracker(tracker);
                try {
                    long initializedBytes;
                    try (RecordCursor cursor = f.getRawCursor()) {
                        initializedBytes = tracker.getUsed();
                        Assert.assertTrue(initializedBytes > f.factory.getMetrics().getBuildBytes());
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertFalse(f.factory.getAtom().isSharded());
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                    // The build still fits. Reject the final slot's scalar allocation
                    // after earlier scalar states have been allocated under this tracker.
                    Assert.assertTrue(initializedBytes - 1 > f.factory.getMetrics().getBuildBytes());
                    tracker.setLimit(initializedBytes - 1);
                    try (RecordCursor ignored = f.getRawCursor()) {
                        Assert.fail("expected scalar state allocation to breach the query limit");
                    } catch (CairoException expected) {
                        Assert.assertTrue(expected.isOutOfMemory());
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(0, f.factory.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    tracker.setLimit(100_000_000);
                    try (RecordCursor cursor = f.getRawCursor()) {
                        Assert.assertTrue(cursor.hasNext());
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
    public void testScalarMergeFailureAndReuse() throws Exception {
        assertScalarMergeFailureAndReuse(false);
    }

    @Test
    public void testScalarMergeCancellationAndReuse() throws Exception {
        assertScalarMergeFailureAndReuse(true);
    }

    private void assertScalarMergeFailureAndReuse(boolean cancel) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 10);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 10);
        assertMemoryLeak(() -> {
            createTables();
            execute("insert into r select 1, timestamp_sequence('2022-01-01', 1000000L), x::double, x::double from long_sequence(1003)");
            Hook hook = new Hook();
            SqlExecutionCircuitBreaker previousBreaker = sqlExecutionContext.getCircuitBreaker();
            AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine);
            ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
            String sql = SCALAR_AGGREGATES + OUTER + " where p.installed_kwp is null";
            try (Fixture f = new Fixture(sql, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2), null, hook);
                 LimitedMemoryTracker tracker = new LimitedMemoryTracker(100_000_000);
                 Reducers reducers = new Reducers()) {
                MemoryTracker previous = sqlExecutionContext.getMemoryTracker();
                sqlExecutionContext.setMemoryTracker(tracker);
                try {
                    f.factory.getAtom().getPerWorkerLocks().setTestAcquireLatch(new CountDownLatch(2));
                    hook.gate = new CountDownLatch(2);
                    hook.failMerge = !cancel;
                    hook.mergeCancel = cancel ? breaker : null;
                    try (RecordCursor cursor = f.getRawCursor()) {
                        cursor.hasNext();
                        Assert.fail("expected a scalar merge failure");
                    } catch (CairoException expected) {
                        if (cancel) {
                            Assert.assertTrue(expected.isInterruption());
                        } else {
                            Assert.assertTrue(expected.getFlyweightMessage().toString().contains("injected merge failure"));
                        }
                    }
                    Assert.assertTrue(hook.mergeCalls.get() > 0);
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(0, f.factory.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    hook.gate = null;
                    hook.failMerge = false;
                    hook.mergeCancel = null;
                    breaker.reset();
                    f.assertResults(sql);
                    Assert.assertEquals(0, tracker.getUsed());
                } finally {
                    sqlExecutionContext.setMemoryTracker(previous);
                }
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(previousBreaker);
            }
        });
    }

    private void assertConcurrentProbeAndWorkerFailureReuse(boolean keyed) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 10);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 10);
        assertMemoryLeak(() -> {
            createTables();
            execute("insert into r select 1, timestamp_sequence('2022-01-01', 1000000L), x::double, x::double from long_sequence(1000)");
            Hook hook = new Hook();
            String sql = (keyed ? AGGREGATES : SCALAR_AGGREGATES) + OUTER + " where p.installed_kwp is null";
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

    private void assertCancellationInsideDuplicateChainAndReuse(boolean keyed) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("truncate table r");
            execute("truncate table p");
            execute("insert into r values (1, '2020-01-01', 10, 100)");
            execute("insert into p select 1, 'ES', null::double from long_sequence(100000)");
            Hook hook = new Hook();
            String sql = (keyed ? AGGREGATES : SCALAR_AGGREGATES) + OUTER + " where p.installed_kwp is null";
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

    private void assertInitializationFailureAndReuse(boolean keyed) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            Hook hook = new Hook();
            String sql = (keyed ? AGGREGATES : SCALAR_AGGREGATES) + OUTER + " where p.installed_kwp is null";
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

    private void assertCompileCloseEarlyCloseAndReuse(boolean keyed) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (Fixture ignored = new Fixture((keyed ? AGGREGATES : SCALAR_AGGREGATES) + OUTER)) {
                // No cursor acquisition.
            }
            try (Fixture f = new Fixture((keyed ? AGGREGATES : SCALAR_AGGREGATES) + OUTER)) {
                try (RecordCursor cursor = f.getCursor()) {
                    Assert.assertEquals(keyed ? -1 : 1, cursor.size());
                    if (keyed) {
                        Assert.assertNull(cursor.getSymbolTable(0).valueOf(SymbolTable.VALUE_IS_NULL));
                    }
                }
                f.assertResults((keyed ? AGGREGATES : SCALAR_AGGREGATES) + OUTER);
                try (RecordCursor cursor = f.getCursor()) {
                    Assert.assertTrue(cursor.hasNext());
                }
                f.assertResults((keyed ? AGGREGATES : SCALAR_AGGREGATES) + OUTER);
            }
        });
    }

    private void createMergeTables() throws Exception {
        createTables();
        execute("truncate table r");
        execute("truncate table p");
        execute("insert into r select (x % 512)::int, timestamp_sequence('2020-01-01', 1000000L), "
                + "case when x % 7 = 0 then null else x::double end, "
                + "case when x % 3 = 0 then null else (x % 8)::double end from long_sequence(10003)");
        execute("insert into p select (x % 512)::int, 'ES', 2.0 from long_sequence(1024)");
        frameRows = 1024;
    }

    private static String mergeSql() {
        return "select r.plant_id, sum(r.energy_kwh) energy, avg(r.irradiance_wm2) irradiance, "
                + "sum(p.installed_kwp) capacity, count(*) pairs" + OUTER + " where p.country is not null";
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
        final AtomicInteger mergeCalls = new AtomicInteger();
        volatile CountDownLatch mergeGate;
        volatile boolean failMerge;
        volatile LimitedMemoryTracker mergeTracker;
        volatile AtomicBooleanCircuitBreaker mergeCancel;
        volatile long mergeBytes;
        volatile LimitedMemoryTracker reduceTracker;
        volatile int reduceLimitAt;

        void merge() {
            mergeCalls.incrementAndGet();
            CountDownLatch latch = mergeGate;
            if (latch != null && latch.getCount() > 0) {
                latch.countDown();
                try {
                    Assert.assertTrue("concurrent merge gate timed out", latch.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
            if (failMerge) {
                throw CairoException.nonCritical().put("injected merge failure");
            }
            if (mergeCancel != null) {
                mergeCancel.cancel();
            }
            LimitedMemoryTracker tracker = mergeTracker;
            if (tracker != null) {
                mergeBytes = tracker.getUsed();
                tracker.setLimit(1);
            }
        }

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
                if (reduceTracker != null && count == reduceLimitAt) {
                    mergeBytes = reduceTracker.getUsed();
                    reduceTracker.setLimit(mergeBytes);
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
                        if (hook != null) {
                            // Decorate only the updater's borrowed function list, after eligibility.
                            // The function container retains ownership and normal initialization.
                            for (int slot = -1; slot < WORKERS; slot++) {
                                ObjList<GroupByFunction> decorated = new ObjList<>();
                                ObjList<GroupByFunction> originals = functions.getGroupByFunctions(slot);
                                boolean wrapped = false;
                                for (int i = 0; i < originals.size(); i++) {
                                    GroupByFunction original = originals.getQuick(i);
                                    if (!wrapped && original instanceof SumDoubleGroupByFunction sum) {
                                        SumDoubleGroupByFunction wrapper = new SumDoubleGroupByFunction(sum.getArg()) {
                                            @Override
                                            public void merge(MapValue dest, MapValue src) {
                                                hook.merge();
                                                sum.merge(dest, src);
                                            }
                                        };
                                        wrapper.initValueIndex(sum.getValueIndex());
                                        decorated.add(wrapper);
                                        wrapped = true;
                                    } else {
                                        decorated.add(original);
                                    }
                                }
                                functions.getUpdater(slot).setFunctions(decorated);
                            }
                        }
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
                                functionsOwned, filtersOwned, candidate.getPhysicalJoinType() == IQueryModel.JOIN_LEFT_OUTER, factoryWorkerCount);
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
            if (frameRows > 0) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
            }
            return queryFactory.getCursor(sqlExecutionContext);
        }

        RecordCursor getRawCursor() throws SqlException {
            if (frameRows > 0) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
            }
            return factory.getCursor(sqlExecutionContext);
        }

        void projectAndSort(String[] expressions, String[] aliases, int... sortColumns) throws Exception {
            GenericRecordMetadata metadata = new GenericRecordMetadata();
            int reserved = expressions.length + 1;
            PriorityMetadata priority = new PriorityMetadata(reserved, factory.getMetadata());
            ObjList<Function> projection = new ObjList<>();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
                for (int i = 0; i < expressions.length; i++) {
                    Function function = parser.parseFunction(compiler.testParseExpression(expressions[i], QueryModel.FACTORY.newInstance()),
                            priority, sqlExecutionContext);
                    projection.add(function);
                    TableColumnMetadata column = new TableColumnMetadata(aliases[i], function.getType(), IndexType.NONE, 0,
                            function instanceof SymbolFunction symbol && symbol.isSymbolTableStatic(), null);
                    metadata.add(column);
                    priority.add(column);
                }
            }
            // The QueryProgress already owning the fused factory remains the enclosing registration.
            RecordCursorFactory virtual = new VirtualRecordCursorFactory(metadata, priority, projection, queryFactory, reserved);
            queryFactory = virtual;
            ListColumnFilter order = new ListColumnFilter();
            for (int column : sortColumns) {
                order.add(column);
            }
            queryFactory = new SortedLightRecordCursorFactory(configuration, metadata, virtual,
                    new RecordComparatorCompiler(new BytecodeAssembler()).newInstance(metadata, order), order);
        }

        void assertOrderedResults(String sql, boolean sharded) throws Exception {
            List<String> expected = new ArrayList<>();
            try (RecordCursorFactory baseline = select(sql); RecordCursor cursor = baseline.getCursor(sqlExecutionContext)) {
                Assert.assertEquals(baseline.getMetadata().getColumnCount(), queryFactory.getMetadata().getColumnCount());
                for (int i = 0; i < baseline.getMetadata().getColumnCount(); i++) {
                    Assert.assertEquals(baseline.getMetadata().getColumnName(i), queryFactory.getMetadata().getColumnName(i));
                    Assert.assertEquals(baseline.getMetadata().getColumnType(i), queryFactory.getMetadata().getColumnType(i));
                }
                while (cursor.hasNext()) {
                    appendRow(expected, cursor.getRecord(), baseline.getMetadata());
                }
            }
            try (RecordCursor cursor = getCursor()) {
                for (int pass = 0; pass < 2; pass++) {
                    List<String> actual = new ArrayList<>();
                    while (cursor.hasNext()) {
                        appendRow(actual, cursor.getRecord(), queryFactory.getMetadata());
                    }
                    Assert.assertEquals(expected, actual);
                    Assert.assertEquals(sharded, factory.getAtom().isSharded());
                    cursor.toTop();
                }
            }
        }

        void assertResults(String sql) throws Exception {
            assertResults(sql, null);
        }

        void assertResults(String sql, Boolean sharded) throws Exception {
            List<String> expected;
            try (RecordCursorFactory baseline = select(sql); RecordCursor cursor = baseline.getCursor(sqlExecutionContext)) {
                expected = rows(cursor, baseline.getMetadata());
            }
            Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_OTHER, factory.getScanDirection());
            try (RecordCursor cursor = getCursor()) {
                Assert.assertEquals(expected, rows(cursor, factory.getMetadata()));
                Assert.assertEquals(expected.size(), cursor.size());
                if (sharded != null) {
                    Assert.assertEquals(sharded.booleanValue(), factory.getAtom().isSharded());
                }
                cursor.toTop();
                Assert.assertEquals(expected, rows(cursor, factory.getMetadata()));
                cursor.toTop();
                if (factory.recordCursorSupportsRandomAccess()) {
                    LongList rowIds = new LongList();
                    while (cursor.hasNext()) {
                        rowIds.add(cursor.getRecord().getRowId());
                    }
                    List<String> randomAccess = new ArrayList<>();
                    for (int i = rowIds.size() - 1; i >= 0; i--) {
                        cursor.recordAt(cursor.getRecordB(), rowIds.getQuick(i));
                        appendRow(randomAccess, cursor.getRecordB(), factory.getMetadata());
                    }
                    Collections.sort(randomAccess);
                    Assert.assertEquals(expected, randomAccess);
                }
                cursor.toTop();
                RecordCursor.Counter remaining = new RecordCursor.Counter();
                boolean first = cursor.hasNext();
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), remaining);
                Assert.assertEquals(expected.size() - (first ? 1 : 0), remaining.get());
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
                        GroupByMergeShardJob mergeJob = new GroupByMergeShardJob(engine.getMessageBus());
                        while (running.get()) {
                            boolean useful = job.run(Job.RUNNING_STATUS);
                            useful |= mergeJob.run(Job.RUNNING_STATUS);
                            if (!useful) {
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
