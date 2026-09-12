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
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.HashJoinGroupByCandidate;
import io.questdb.griffin.HashJoinGroupByFunctions;
import io.questdb.griffin.HashJoinGroupByMetadata;
import io.questdb.griffin.PostOrderTreeTraversalAlgo;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.groupby.SumDoubleGroupByFunction;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.HashJoinGroupByRecord;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HashJoinGroupByFunctionsTest extends AbstractCairoTest {
    private static final String AGGREGATES = "select p.country, year(r.reading_ts) yr, month(r.reading_ts) mo, "
            + "sum(r.energy_kwh) energy, avg(r.irradiance_wm2) irradiance, sum(p.installed_kwp) capacity";
    private static final String INNER = " from r join p on r.plant_id=p.plant_id";
    private static final String OUTER = " from r left join p on r.plant_id=p.plant_id";
    private static final int WORKERS = 3;

    @Test
    public void testBindRebindingAndDictionaryReplacement() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            bindVariableService.setDouble(0, 2);
            bindVariableService.setStr(1, "ES");
            String sql = "select p.country, year(r.reading_ts) yr, sum(r.energy_kwh * $1) energy" + OUTER
                    + " where p.country = $2";
            try (Fixture fixture = new Fixture(sql)) {
                fixture.assertResults(sql);
                bindVariableService.setDouble(0, 7);
                bindVariableService.setStr(1, "IT");
                execute("truncate table p");
                execute("insert into p values (1, 'IT', 13), (1, 'IT', null), (2, 'ES', 17)");
                fixture.assertResults(sql);
                execute("truncate table p");
                fixture.assertResults(sql);
            }
        });
    }

    @Test
    public void testEmptyBuildAndNullSymbols() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String sql = AGGREGATES + OUTER;
            try (Fixture fixture = new Fixture(sql)) {
                fixture.assertResults(sql);
                execute("truncate table p");
                fixture.assertResults(sql);
                execute("insert into p values (1, null, null)");
                fixture.assertResults(sql);
            }
        });
    }

    @Test
    public void testInitializationStateIsOfferedOncePerExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            TrackingParser parser = new TrackingParser();
            String sql = "select year(r.reading_ts) yr, sum(r.energy_kwh) energy" + OUTER + " where p.installed_kwp is null";
            try (Fixture fixture = new Fixture(sql, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2), parser);
                 RecordCursor cursor = fixture.probeFactory.getCursor(sqlExecutionContext)) {
                ObjList<SymbolTableSource> sources = new ObjList<>();
                for (int i = 0; i <= WORKERS; i++) {
                    sources.add(cursor);
                }
                for (int execution = 1; execution <= 2; execution++) {
                    fixture.functions.init(sources, sqlExecutionContext);
                    for (int slot = -1; slot < WORKERS; slot++) {
                        TrackingDouble key = (TrackingDouble) fixture.functions.getKeyFunctions(slot).getQuick(0);
                        TrackingDouble arg = (TrackingDouble) ((UnaryFunction) fixture.functions.getGroupByFunctions(slot).getQuick(0)).getArg();
                        TrackingBoolean filter = (TrackingBoolean) fixture.functions.getFilter(slot);
                        for (State state : new State[]{key.state, arg.state, filter.state}) {
                            Assert.assertEquals(execution, state.initCount);
                            Assert.assertEquals(execution, state.value);
                            Assert.assertEquals(slot < 0 ? execution : 0, state.ownerEvaluations);
                            Assert.assertEquals(slot < 0 ? execution * WORKERS : 0, state.offerCount);
                        }
                        // Row evaluation must not initialize or reevaluate query-constant state.
                        for (int row = 0; row < 20; row++) {
                            Assert.assertEquals(execution, arg.getDouble(null), 0);
                            Assert.assertTrue(filter.getBool(null));
                        }
                    }
                    fixture.functions.cursorClosed();
                    for (int i = 0; i < parser.states.size(); i++) {
                        Assert.assertEquals(execution, parser.states.getQuick(i).cursorClosedCount);
                    }
                }
                Assert.assertFalse(sqlExecutionContext.getCloneSymbolTables());
            }
            parser.assertClosedOnce();
        });
    }

    @Test
    public void testInitializationFailureRestoresContextAndAllowsReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            TrackingParser parser = new TrackingParser();
            String sql = "select year(r.reading_ts) yr, sum(r.energy_kwh) energy" + OUTER + " where p.installed_kwp is null";
            try (Fixture fixture = new Fixture(sql, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2), parser);
                 RecordCursor cursor = fixture.probeFactory.getCursor(sqlExecutionContext)) {
                ObjList<SymbolTableSource> sources = new ObjList<>();
                for (int i = 0; i <= WORKERS; i++) {
                    sources.add(cursor);
                }
                TrackingDouble arg = (TrackingDouble) ((UnaryFunction) fixture.functions.getGroupByFunctions(1).getQuick(0)).getArg();
                arg.state.failInit = true;
                try {
                    fixture.functions.init(sources, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException expected) {
                    Assert.assertEquals("injected init failure", expected.getFlyweightMessage().toString());
                }
                Assert.assertFalse(sqlExecutionContext.getCloneSymbolTables());
                fixture.functions.cursorClosed();
                arg.state.failInit = false;
                fixture.functions.init(sources, sqlExecutionContext);
                Assert.assertEquals(2, arg.state.value);
                Assert.assertEquals(0, arg.state.ownerEvaluations);
                fixture.functions.cursorClosed();
            }
            parser.assertClosedOnce();
        });
    }

    @Test
    public void testPartialCompilationClosesFunctionsOnce() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Fail after owner assembly and partway through either projection or filter clones.
            for (int failAt : new int[]{4, 11}) {
                TrackingParser parser = new TrackingParser();
                parser.failAt = failAt;
                String sql = "select year(r.reading_ts) yr, sum(r.energy_kwh) energy" + OUTER + " where p.installed_kwp is null";
                try (Fixture ignored = new Fixture(sql, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2), parser)) {
                    Assert.fail();
                } catch (SqlException expected) {
                    Assert.assertEquals("injected compile failure", expected.getFlyweightMessage().toString());
                }
                parser.assertClosedOnce();
            }
        });
    }

    @Test
    public void testOriginalExpressionsAndParentInitialization() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String sql = AGGREGATES + INNER
                    + " where r.reading_ts >= '2020-01-01' and r.reading_ts < '2025-01-01' and p.country in ('ES','IT')"
                    + " group by p.country, year(r.reading_ts), month(r.reading_ts) order by p.country, yr, mo";
            try (Fixture fixture = new Fixture(sql,
                    "r where reading_ts >= '2020-01-01' and reading_ts < '2025-01-01'", ints(0, 1, 2, 3),
                    "p where country in ('ES','IT')", ints(0, 1, 2))) {
                fixture.assertResults(sql);
            }
        });
    }

    @Test
    public void testOwnerWithoutWorkers() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String sql = AGGREGATES + OUTER;
            try (Fixture fixture = new Fixture(sql, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2),
                    new FunctionParser(configuration, engine.getFunctionFactoryCache()), 0)) {
                fixture.assertResults(sql);
            }
        });
    }

    @Test
    public void testOuterFiltersAndCounts() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String predicate : new String[]{"p.installed_kwp is null", "p.installed_kwp = 42", "p.country in ('ES','IT')", "p.country is null"}) {
                String sql = "select year(r.reading_ts) yr, count(*) pairs, count(p.plant_id) ids, "
                        + "count(p.country) countries, sum(coalesce(p.installed_kwp, 0.0)) capacity, sum(r.energy_kwh) energy"
                        + OUTER + " where " + predicate;
                try (Fixture fixture = new Fixture(sql)) {
                    fixture.assertResults(sql);
                }
            }
        });
    }

    @Test
    public void testProjectionAboveJoinAndFilterOnlyPayload() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String sql = "select yr, sum(e) energy from (select year(r.reading_ts) yr, r.energy_kwh e, p.installed_kwp capacity"
                    + OUTER + ") where capacity is null";
            try (Fixture fixture = new Fixture(sql)) {
                Assert.assertEquals(1, fixture.metadata.getPayloadMetadata().getColumnCount());
                Assert.assertEquals("installed_kwp", fixture.metadata.getPayloadMetadata().getColumnName(0));
                fixture.assertResults(sql);
            }
        });
    }

    @Test
    public void testPrunedReorderedNormalizedRight() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String probe = "select energy_kwh e, reading_ts ts, plant_id id, irradiance_wm2 irr from r";
            String build = "select installed_kwp cap, plant_id id, country c from p";
            String sql = "select p.c country, year(r.ts) yr, month(r.ts) mo, sum(r.e) energy, avg(r.irr) irradiance, sum(p.cap) capacity"
                    + " from (" + build + ") p right join (" + probe + ") r on r.id=p.id where p.cap is null";
            try (Fixture fixture = new Fixture(sql, probe, ints(2, 1, 0, 3), build, ints(2, 0, 1))) {
                Assert.assertTrue(fixture.outer);
                Assert.assertEquals(2, fixture.metadata.getProbeKeyColumn());
                Assert.assertEquals(1, fixture.metadata.getBuildKeyColumn());
                Assert.assertEquals("[2,0]", fixture.metadata.getBuildColumns().toString());
                Assert.assertEquals("r.e", fixture.metadata.getJoinedMetadata().getColumnName(0));
                Assert.assertEquals("p.c", fixture.metadata.getJoinedMetadata().getColumnName(4));
                fixture.assertResults(sql);
            }
        });
    }

    @Test
    public void testRejectsMissingAndMistypedProjectionMappings() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (IntList mapping : new IntList[]{ints(1, 0, 2, 3), ints(0, 1, 2, 9), ints(0, 1, 2)}) {
                try (Fixture ignored = new Fixture(AGGREGATES + INNER, "r", mapping, "p", ints(0, 1, 2))) {
                    Assert.fail("invalid mapping accepted: " + mapping);
                } catch (SqlException expected) {
                    Assert.assertTrue(expected.getFlyweightMessage().toString(),
                            expected.getFlyweightMessage().toString().contains("hash join input"));
                }
            }
        });
    }

    @Test
    public void testProbeSymbolsWithReorderedProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("alter table r add column tag symbol");
            execute("insert into r values (1, '2022-01-01', 15, 150, 'left')");
            String sql = "select r.tag, p.country, sum(r.energy_kwh) energy" + OUTER;
            try (Fixture fixture = new Fixture(sql,
                    "select tag, energy_kwh, plant_id, reading_ts, irradiance_wm2 from r", ints(4, 2, 0, 1, 3), "p", ints(0, 1, 2))) {
                fixture.assertResults(sql);
            }
        });
    }

    @Test
    public void testSlotRecordsKeepIndependentSymbolsAndMissState() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (Fixture fixture = new Fixture(AGGREGATES + OUTER);
                 IntHashJoinBuild build = new IntHashJoinBuild(fixture.metadata.getPayloadMetadata(), fixture.metadata.getBuildColumns(), 4, 2)) {
                build.open(sqlExecutionContext.getMemoryTracker(), sqlExecutionContext.getCircuitBreaker());
                FrozenHashJoinBuild frozen;
                try (RecordCursor cursor = fixture.buildFactory.getCursor(sqlExecutionContext)) {
                    frozen = build.build(cursor, fixture.metadata.getBuildKeyColumn());
                }
                try (RecordCursor cursor = fixture.probeFactory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    FrozenHashJoinBuild.Probe a = frozen.newProbe(sqlExecutionContext.getCircuitBreaker());
                    FrozenHashJoinBuild.Probe b = frozen.newProbe(sqlExecutionContext.getCircuitBreaker());
                    HashJoinGroupByRecord left = fixture.metadata.newRecord();
                    HashJoinGroupByRecord right = fixture.metadata.newRecord();
                    left.of(cursor.getRecord(), cursor, a);
                    right.of(cursor.getRecord(), cursor, b);
                    a.find(1);
                    a.next();
                    left.setHasMatch(true);
                    CharSequence savedA = left.getSymA(4);
                    CharSequence savedB = left.getSymB(4);
                    Assert.assertEquals("IT", savedA.toString());
                    Assert.assertEquals(10, left.getDouble(2), 0);
                    b.find(Numbers.INT_NULL);
                    b.next();
                    right.setHasMatch(true);
                    Assert.assertEquals("ES", right.getSymA(4).toString());
                    Assert.assertEquals("IT", savedA.toString());
                    Assert.assertEquals("IT", savedB.toString());
                    Assert.assertNotSame(left.getSymbolTable(4), right.getSymbolTable(4));
                    Assert.assertNotSame(left.getSymbolTable(4), left.newSymbolTable(4));
                    left.setHasMatch(false);
                    Assert.assertNull(left.getSymA(4));
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, left.getInt(4));
                    Assert.assertTrue(Double.isNaN(left.getDouble(5)));
                    Assert.assertEquals("ES", right.getSymB(4).toString());
                    left.setHasMatch(true);
                    Assert.assertEquals("IT", left.getSymA(4).toString());
                    left.clear();
                    left.of(cursor.getRecord(), cursor, a);
                    Assert.assertNull(left.getSymA(4));
                    left.setHasMatch(true);
                    Assert.assertEquals("IT", left.getSymA(4).toString());
                    left.clear();
                    right.clear();
                }
            }
        });
    }

    @Test
    public void testTypedJoinedGettersAndOuterNulls() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("create table typed (id int, b boolean, y byte, s short, c char, i int, l long, d date, t timestamp, n timestamp_ns, f float, v double, sym symbol)");
            execute("insert into typed values (1, true, 2, 3, 'Q', 5, 6, 7, 8, 9, 10.5, 11.5, 'typed'), "
                    + "(2, null, null, null, null, null, null, null, null, null, null, null, null)");
            String sql = "select p.b, p.y, p.s, p.c, p.i, p.l, p.d, p.t, p.n, p.f, p.v, p.sym, count(*) pairs "
                    + "from r left join typed p on r.plant_id=p.id";
            try (Fixture fixture = new Fixture(sql, "r", ints(0, 1, 2, 3), "typed", ints(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))) {
                fixture.assertResults(sql);
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

    private static class State {
        private int closeCount;
        private int cursorClosedCount;
        private int donated;
        private boolean failInit;
        private int initCount;
        private int offerCount;
        private int ownerEvaluations;
        private int value;

        private void init() throws SqlException {
            initCount++;
            if (failInit) {
                throw SqlException.$(0, "injected init failure");
            }
            value = donated > 0 ? donated : ++ownerEvaluations;
            donated = 0;
        }

        private void offer(State target) {
            offerCount++;
            target.donated = value;
        }
    }

    private static class TrackingBoolean extends BooleanFunction {
        private final State state;

        private TrackingBoolean(State state) {
            this.state = state;
        }

        @Override
        public void close() {
            state.closeCount++;
        }

        @Override
        public void cursorClosed() {
            state.cursorClosedCount++;
        }

        @Override
        public boolean getBool(Record record) {
            return state.value > 0;
        }

        @Override
        public void init(SymbolTableSource source, SqlExecutionContext context) throws SqlException {
            state.init();
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void offerStateTo(Function target) {
            state.offer(((TrackingBoolean) target).state);
        }
    }

    private static class TrackingDouble extends DoubleFunction {
        private final State state;

        private TrackingDouble(State state) {
            this.state = state;
        }

        @Override
        public void close() {
            state.closeCount++;
        }

        @Override
        public void cursorClosed() {
            state.cursorClosedCount++;
        }

        @Override
        public double getDouble(Record record) {
            return state.value;
        }

        @Override
        public void init(SymbolTableSource source, SqlExecutionContext context) throws SqlException {
            state.init();
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void offerStateTo(Function target) {
            state.offer(((TrackingDouble) target).state);
        }
    }

    private static class TrackingParser extends FunctionParser {
        private final ObjList<State> states = new ObjList<>();
        private int failAt = -1;
        private int parseCount;

        private TrackingParser() {
            super(configuration, engine.getFunctionFactoryCache());
        }

        @Override
        public Function parseFunction(ExpressionNode node, RecordMetadata metadata, SqlExecutionContext context) throws SqlException {
            if (++parseCount == failAt) {
                throw SqlException.$(0, "injected compile failure");
            }
            State state = new State();
            states.add(state);
            if ("sum".contentEquals(node.token)) {
                return new SumDoubleGroupByFunction(new TrackingDouble(state));
            }
            if ("year".contentEquals(node.token)) {
                return new TrackingDouble(state);
            }
            return new TrackingBoolean(state);
        }

        private void assertClosedOnce() {
            for (int i = 0; i < states.size(); i++) {
                Assert.assertEquals("function " + i, 1, states.getQuick(i).closeCount);
            }
        }
    }

    private class Fixture implements Closeable {
        private RecordCursorFactory buildFactory;
        private HashJoinGroupByFunctions functions;
        private HashJoinGroupByMetadata metadata;
        private boolean outer;
        private final int workers;
        private RecordCursorFactory probeFactory;

        private Fixture(String sql) throws Exception {
            this(sql, "r", ints(0, 1, 2, 3), "p", ints(0, 1, 2));
        }

        private Fixture(String sql, String probeSql, IntList probeColumns, String buildSql, IntList buildColumns) throws Exception {
            this(sql, probeSql, probeColumns, buildSql, buildColumns, new FunctionParser(configuration, engine.getFunctionFactoryCache()));
        }

        private Fixture(String sql, String probeSql, IntList probeColumns, String buildSql, IntList buildColumns, FunctionParser parser) throws Exception {
            this(sql, probeSql, probeColumns, buildSql, buildColumns, parser, WORKERS);
        }

        private Fixture(String sql, String probeSql, IntList probeColumns, String buildSql, IntList buildColumns, FunctionParser parser, int workers) throws Exception {
            this.workers = workers;
            try {
                probeFactory = select(probeSql);
                buildFactory = select(buildSql);
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     SqlCodeGenerator generator = new SqlCodeGenerator(configuration, parser, new PostOrderTreeTraversalAlgo(),
                             new ObjectPool<>(QueryColumn.FACTORY, 16), new ObjectPool<>(ExpressionNode.FACTORY, 16))) {
                    IQueryModel model = (IQueryModel) compiler.generateExecutionModel(sql, sqlExecutionContext);
                    while (model.getSelectModelType() != IQueryModel.SELECT_MODEL_GROUP_BY) {
                        model = model.getNestedModel();
                    }
                    HashJoinGroupByCandidate candidate = SqlCodeGenerator.getHashJoinGroupByCandidate(model, new FunctionParser(configuration, engine.getFunctionFactoryCache()), sqlExecutionContext);
                    Assert.assertNotNull(sql, candidate);
                    outer = candidate.getPhysicalJoinType() == IQueryModel.JOIN_LEFT_OUTER;
                    metadata = new HashJoinGroupByMetadata(configuration, candidate, probeFactory.getMetadata(), probeColumns,
                            buildFactory.getMetadata(), buildColumns);
                    functions = generator.compileHashJoinGroupByFunctions(model, metadata, workers, sqlExecutionContext);
                }
            } catch (Throwable th) {
                Misc.free(this, th);
                throw th;
            }
        }

        @Override
        public void close() {
            functions = Misc.free(functions);
            metadata = Misc.free(metadata);
            probeFactory = Misc.free(probeFactory);
            buildFactory = Misc.free(buildFactory);
        }

        private void assertResults(String sql) throws Exception {
            List<String> expected = new ArrayList<>();
            try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                    appendRow(expected, cursor.getRecord(), factory.getMetadata());
                }
            }
            Collections.sort(expected);
            // Each acquired slot, including the owner, evaluates the same pairs using
            // its own functions and build view. Scheduling and merging are later tasks.
            try (IntHashJoinBuild build = new IntHashJoinBuild(metadata.getPayloadMetadata(), metadata.getBuildColumns(), 4, 2)) {
                build.open(sqlExecutionContext.getMemoryTracker(), sqlExecutionContext.getCircuitBreaker());
                FrozenHashJoinBuild frozen;
                try (RecordCursor cursor = buildFactory.getCursor(sqlExecutionContext)) {
                    frozen = build.build(cursor, metadata.getBuildKeyColumn());
                }
                try (RecordCursor cursor = probeFactory.getCursor(sqlExecutionContext)) {
                    ObjList<HashJoinGroupByRecord> records = new ObjList<>();
                    ObjList<FrozenHashJoinBuild.Probe> probes = new ObjList<>();
                    for (int i = 0; i <= workers; i++) {
                        FrozenHashJoinBuild.Probe probe = frozen.newProbe(sqlExecutionContext.getCircuitBreaker());
                        HashJoinGroupByRecord record = metadata.newRecord();
                        record.of(cursor.getRecord(), cursor, probe);
                        probes.add(probe);
                        records.add(record);
                    }
                    functions.init(records, sqlExecutionContext);
                    // A parent initializes before any joined pair or aggregate output exists.
                    Function parentSymbol = null;
                    Function parentRatio = null;
                    int energyIndex = functions.getOutputMetadata().getColumnIndexQuiet("energy");
                    int capacityIndex = functions.getOutputMetadata().getColumnIndexQuiet("capacity");
                    if (energyIndex >= 0 && capacityIndex >= 0) {
                        try (SqlCompiler compiler = engine.getSqlCompiler()) {
                            parentRatio = new FunctionParser(configuration, engine.getFunctionFactoryCache()).parseFunction(
                                    compiler.testParseExpression("energy / nullif(capacity, 0)", QueryModel.FACTORY.newInstance()),
                                    functions.getOutputMetadata(), sqlExecutionContext);
                        }
                        parentRatio.init(functions, sqlExecutionContext);
                    }
                    if (functions.getOutputMetadata().getColumnType(0) == ColumnType.SYMBOL) {
                        try (SqlCompiler compiler = engine.getSqlCompiler()) {
                            parentSymbol = new FunctionParser(configuration, engine.getFunctionFactoryCache()).parseFunction(
                                    compiler.testParseExpression(functions.getOutputMetadata().getColumnName(0), QueryModel.FACTORY.newInstance()),
                                    functions.getOutputMetadata(), sqlExecutionContext);
                        }
                        parentSymbol.init(functions, sqlExecutionContext);
                        Assert.assertNull(functions.getSymbolTable(0).valueOf(SymbolTable.VALUE_IS_NULL));
                    }
                    try {
                        for (int slot = -1; slot < workers; slot++) {
                            cursor.toTop();
                            HashJoinGroupByRecord record = records.getQuick(slot + 1);
                            FrozenHashJoinBuild.Probe probe = probes.getQuick(slot + 1);
                            List<String> actual = new ArrayList<>();
                            try (Map map = MapFactory.createUnorderedMap(configuration, functions.getKeyTypes(), functions.getValueTypes(), false)) {
                                while (cursor.hasNext()) {
                                    probe.find(cursor.getRecord().getInt(metadata.getProbeKeyColumn()));
                                    if (probe.hasNext()) {
                                        do {
                                            probe.next();
                                            record.setHasMatch(true);
                                            update(map, record, slot);
                                        } while (probe.hasNext());
                                    } else if (outer) {
                                        record.setHasMatch(false);
                                        update(map, record, slot);
                                    }
                                }
                                VirtualRecord output = new VirtualRecord(functions.getOutputFunctions());
                                try (MapRecordCursor result = map.getCursor()) {
                                    output.of(result.getRecord());
                                    while (result.hasNext()) {
                                        appendRow(actual, output, functions.getOutputMetadata());
                                        if (parentRatio != null) {
                                            double capacity = output.getDouble(capacityIndex);
                                            double expectedRatio = capacity == 0 ? Double.NaN : output.getDouble(energyIndex) / capacity;
                                            Assert.assertEquals(expectedRatio, parentRatio.getDouble(output), 0);
                                        }
                                        if (parentSymbol != null) {
                                            Assert.assertEquals(String.valueOf(output.getSymA(0)), String.valueOf(parentSymbol.getSymbol(output)));
                                        }
                                    }
                                }
                            }
                            Collections.sort(actual);
                            Assert.assertEquals("slot " + slot + ": " + sql, expected, actual);
                        }
                    } finally {
                        Misc.free(parentSymbol);
                        Misc.free(parentRatio);
                        functions.cursorClosed();
                        for (int i = 0; i < records.size(); i++) {
                            records.getQuick(i).clear();
                        }
                    }
                }
            }
        }

        private void update(Map map, HashJoinGroupByRecord record, int slot) {
            Function filter = functions.getFilter(slot);
            if (filter != null && !filter.getBool(record)) {
                return;
            }
            MapKey key = map.withKey();
            functions.getMapSink(slot).copy(record, key);
            MapValue value = key.createValue();
            if (value.isNew()) {
                functions.getUpdater(slot).updateNew(value, record, 0);
            } else {
                functions.getUpdater(slot).updateExisting(value, record, 0);
            }
        }
    }
}
