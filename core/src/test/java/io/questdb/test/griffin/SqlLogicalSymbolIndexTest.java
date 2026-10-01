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

import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalSymbolIndexTest extends AbstractCairoTest {
    @Test
    public void testEqualityValuesAndOperandOrder() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final ObjList<String> predicates = new ObjList<>("s='A'", "'A'=s", "s=null", "s=''", "s=''''", "s=null::CHAR", "s='A'::CHAR");
            final ObjList<String> predicatesRows = new ObjList<>("id\n1\n4\n5\n", "id\n1\n4\n5\n", "id\n3\n", "id\n7\n", "id\n6\n", "id\n3\n", "id\n1\n4\n5\n");
            for (int i = 0; i < predicates.size(); i++) {
                assertIndex(
                        "SELECT id FROM lp_index WHERE " + predicates.getQuick(i),
                        "Index forward scan",
                        "SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Frame forward scan on: lp_index",
                        predicatesRows.getQuick(i)
                );
            }
            assertRows("SELECT id FROM lp_index WHERE s='A'", "id\n1\n4\n5\n");
            assertRows("SELECT id FROM lp_index WHERE s=null::CHAR", "id\n3\n");
            assertRows("SELECT id FROM lp_index WHERE s=''''", "id\n6\n");
        });
    }

    @Test
    public void testResidualsKeepNativeIndexFiltering() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final ObjList<String> residuals = new ObjList<>("id>1", "length(v)>0", "upper(v)='FOUR'", "true");
            final ObjList<String> residualsRows = new ObjList<>("id\n4\n5\n", "id\n1\n4\n5\n", "id\n4\n", "id\n1\n4\n5\n");
            final String[] shapes = {"SelectedRecord > PageFrame > Index forward scan on: s > Frame forward scan on: lp_index", "SelectedRecord > PageFrame > Index forward scan on: s > Frame forward scan on: lp_index", "SelectedRecord > PageFrame > Index forward scan on: s > Frame forward scan on: lp_index", "SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Frame forward scan on: lp_index"};
            for (int i = 0; i < residuals.size(); i++) {
                assertIndex(
                        "SELECT id FROM lp_index WHERE s='A' AND " + residuals.getQuick(i),
                        "Index forward scan",
                        shapes[i],
                        residualsRows.getQuick(i)
                );
            }
            assertIndexExactPlan("SELECT id FROM lp_index WHERE s='A' AND false", "Empty table", "Empty table\n", "id\n");
            bindVariableService.setBoolean(0, true);
            final String sql = "SELECT id FROM lp_index WHERE s='A' AND $1";
            assertIndex(
                    sql,
                    "Index forward scan",
                    "SelectedRecord > PageFrame > Index forward scan on: s > Frame forward scan on: lp_index",
                    """
                    id
                    1
                    4
                    5
                    """
            );
            try (RecordCursorFactory factory = compile(sql)) {
                assertRows(factory, "id\n1\n4\n5\n");
                bindVariableService.setBoolean(0, false);
                assertRows(factory, "id\n");
                bindVariableService.setBoolean(0, true);
                assertRows(factory, "id\n1\n4\n5\n");
            }
        });
    }

    @Test
    public void testNoTimestampAndDeletedColumnLayouts() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            execute("CREATE TABLE lp_unordered(unused INT,id INT,s SYMBOL INDEX,v VARCHAR)");
            execute("INSERT INTO lp_unordered SELECT unused,id,s,v FROM lp_index");
            execute("ALTER TABLE lp_unordered DROP COLUMN unused");
            assertIndex(
                    "SELECT id FROM lp_unordered WHERE s='A'",
                    "Index forward scan",
                    "SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Frame forward scan on: lp_unordered",
                    """
                    id
                    1
                    4
                    5
                    """
            );
            assertIndexExactPlan(
                    "SELECT s AS key,id FROM lp_unordered WHERE s='A' AND length(v)>0 ORDER BY id",
                    "Index forward scan",
                    """
                    SelectedRecord
                        Encode sort light
                          keys: [id]
                            SelectedRecord
                                PageFrame
                                    Index forward scan on: s
                                      filter: s=1 and 0<length(v)
                                    Frame forward scan on: lp_unordered
                    """,
                    """
                    key	id
                    A	1
                    A	4
                    A	5
                    """
            );
            assertRows("SELECT id FROM lp_unordered WHERE s='A' AND id>1", "id\n4\n5\n");
        });
    }

    @Test
    public void testIntervalsAndOrderAdvice() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final ObjList<String> orders = new ObjList<>("ts", "ts DESC", "s", "s DESC", "s,ts", "s DESC,ts DESC");
            final ObjList<String> ordersRows = new ObjList<>("id\n1\n4\n5\n", "id\n5\n4\n1\n", "id\n1\n4\n5\n", "id\n1\n4\n5\n", "id\n1\n4\n5\n", "id\n5\n4\n1\n");
            final ObjList<String> ordersRows2 = new ObjList<>("id\n4\n5\n", "id\n5\n4\n", "id\n4\n5\n", "id\n4\n5\n", "id\n4\n5\n", "id\n5\n4\n");
            final String[] shapes3 = {"SelectedRecord > SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Interval forward scan on: lp_index", "SelectedRecord > SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index backward scan on: s > Interval backward scan on: lp_index", "SelectedRecord > SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Interval forward scan on: lp_index", "SelectedRecord > SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Interval forward scan on: lp_index", "SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Interval forward scan on: lp_index", "SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index backward scan on: s > Interval forward scan on: lp_index"};
            final String[] shapes2 = {"SelectedRecord > SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Frame forward scan on: lp_index", "SelectedRecord > SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index backward scan on: s > Frame backward scan on: lp_index", "SelectedRecord > Encode sort light > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Frame forward scan on: lp_index", "SelectedRecord > Encode sort light > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Frame forward scan on: lp_index", "SelectedRecord > Encode sort light > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Frame forward scan on: lp_index", "SelectedRecord > Encode sort light > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Frame forward scan on: lp_index"};
            for (int i = 0; i < orders.size(); i++) {
                assertIndex(
                        "SELECT id FROM lp_index WHERE s='A' ORDER BY " + orders.getQuick(i),
                        "Index ",
                        shapes2[i],
                        ordersRows.getQuick(i)
                );
                assertIndex(
                        "SELECT id FROM lp_index WHERE s='A' AND ts>='2020-01-02T00:00:00.000000Z' AND ts<'2020-01-03T00:00:00.000000Z' ORDER BY " + orders.getQuick(i),
                        "Index ",
                        shapes3[i],
                        ordersRows2.getQuick(i)
                );
            }
            bindVariableService.setTimestamp(0, 1_577_923_200_000_000L);
            assertIndex(
                    "SELECT id FROM lp_index WHERE s='A' AND ts>=$1 ORDER BY ts DESC",
                    "Index backward scan",
                    "SelectedRecord > SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index backward scan on: s > Interval backward scan on: lp_index",
                    """
                    id
                    5
                    4
                    """
            );
            assertIndex(
                    "SELECT id FROM lp_index WHERE s='A' AND ts>'2020-01-03' AND ts<'2020-01-01'",
                    "Index ",
                    "SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Interval forward scan on: lp_index",
                    "id\n"
            );
        });
    }

    @Test
    public void testCandidateSelectionKeepsCountCapacityAndTraversalOrder() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            assertIndex(
                    "SELECT id FROM lp_index WHERE s='A' AND t='Y'",
                    "on: s",
                    "SelectedRecord > PageFrame > Index forward scan on: s > Frame forward scan on: lp_index",
                    """
                    id
                    4
                    """
            );
            assertIndex(
                    "SELECT id FROM lp_index WHERE t='Y' AND s='A'",
                    "on: s",
                    "SelectedRecord > PageFrame > Index forward scan on: s > Frame forward scan on: lp_index",
                    """
                    id
                    4
                    """
            );
            execute("CREATE TABLE lp_capacity(id INT,s SYMBOL CAPACITY 16 INDEX,t SYMBOL CAPACITY 128 INDEX)");
            execute("INSERT INTO lp_capacity VALUES(1,'A','X'),(2,'B','Y'),(3,'A','Y')");
            assertIndex(
                    "SELECT id FROM lp_capacity WHERE s='A' AND t='X'",
                    "on: t",
                    "SelectedRecord > PageFrame > Index forward scan on: t > Frame forward scan on: lp_capacity",
                    """
                    id
                    1
                    """
            );
            assertIndex(
                    "SELECT id FROM lp_capacity WHERE t='X' AND s='A'",
                    "on: t",
                    "SelectedRecord > PageFrame > Index forward scan on: t > Frame forward scan on: lp_capacity",
                    """
                    id
                    1
                    """
            );
            execute("CREATE TABLE lp_tie(id INT,s SYMBOL CAPACITY 128 INDEX,t SYMBOL CAPACITY 128 INDEX)");
            execute("INSERT INTO lp_tie VALUES(1,'A','X'),(2,'B','Y'),(3,'A','Y')");
            assertIndex(
                    "SELECT id FROM lp_tie WHERE s='A' AND t='X'",
                    "on: t",
                    "SelectedRecord > PageFrame > Index forward scan on: t > Frame forward scan on: lp_tie",
                    """
                    id
                    1
                    """
            );
            assertIndex(
                    "SELECT id FROM lp_tie WHERE t='X' AND s='A'",
                    "on: s",
                    "SelectedRecord > PageFrame > Index forward scan on: s > Frame forward scan on: lp_tie",
                    """
                    id
                    1
                    """
            );
        });
    }

    @Test
    public void testCoveringResidualLimitAndOrder() throws Exception {
        assertMemoryLeak(() -> {
            createRows(true);
            assertIndex(
                    "SELECT id FROM lp_index WHERE s='A'",
                    "CoveringIndex",
                    "SelectedRecord > CoveringIndex on: s with: id",
                    """
                    id
                    1
                    4
                    5
                    """
            );
            final boolean wasParallel = sqlExecutionContext.isParallelFilterEnabled();
            final int previousMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setParallelFilterEnabled(true);
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            try {
                final String sql = "SELECT id FROM lp_index WHERE s='A' AND length(v)>0";
                assertIndex(
                        sql,
                        "Async Filter",
                        "SelectedRecord > Async Filter workers: 1 > CoveringIndex on: s with: id, v",
                        """
                        id
                        1
                        4
                        5
                        """
                );
                assertIndex(
                        sql + " LIMIT 2",
                        "Async Filter",
                        "SelectedRecord > Async Filter workers: 1 > CoveringIndex on: s with: id, v",
                        """
                        id
                        1
                        4
                        """
                );
                assertIndex(
                        sql + " LIMIT -2",
                        "Async Filter",
                        "SelectedRecord > Async Filter workers: 1 > CoveringIndex on: s with: id, v",
                        """
                        id
                        4
                        5
                        """
                );
                assertIndexExactPlan(
                        sql + " ORDER BY ts LIMIT 2",
                        "Async Filter",
                        """
                        SelectedRecord
                            SelectedRecord
                                Async Filter workers: 1
                                  limit: 2
                                  filter: 0<length(v)
                                    CoveringIndex on: s with: id, v, ts
                                      filter: s='A'
                        """,
                        """
                        id
                        1
                        4
                        """
                );
                assertIndexExactPlan(
                        sql + " ORDER BY ts DESC LIMIT 2",
                        "Async Top K",
                        """
                        SelectedRecord
                            SelectedRecord
                                Async Top K lo: 2 workers: 1
                                  filter: 0<length(v)
                                  keys: [ts desc]
                                    CoveringIndex on: s with: id, v, ts
                                      filter: s='A'
                        """,
                        """
                        id
                        5
                        4
                        """
                );
                assertIndex(
                        sql + " ORDER BY id DESC LIMIT 2",
                        "CoveringIndex",
                        "SelectedRecord > Async Top K lo: 2 workers: 1 > CoveringIndex on: s with: id, v",
                        """
                        id
                        5
                        4
                        """
                );
                bindVariableService.setLong(0, 2);
                final String limited = sql + " LIMIT $1";
                assertIndex(
                        limited,
                        "Async Filter",
                        "SelectedRecord > Async Filter workers: 1 > CoveringIndex on: s with: id, v",
                        """
                        id
                        1
                        4
                        """
                );
                try (RecordCursorFactory factory = compile(limited)) {
                    assertRows(factory, "id\n1\n4\n");
                    bindVariableService.setLong(0, -2);
                    assertRows(factory, "id\n4\n5\n");
                }
                sqlExecutionContext.setParallelFilterEnabled(false);
                assertIndex(
                        sql,
                        "Filter",
                        "SelectedRecord > Filter filter: 0<length(v) > CoveringIndex on: s with: id, v",
                        """
                        id
                        1
                        4
                        5
                        """
                );
            } finally {
                sqlExecutionContext.setParallelFilterEnabled(wasParallel);
                sqlExecutionContext.setJitMode(previousMode);
            }
        });
    }

    @Test
    public void testCoveringNullColumnTopAndDeferredKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_top(id INT,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO lp_top VALUES(1,'2020-01-01T00:00:01'),(2,'2020-01-01T00:00:02')");
            execute("ALTER TABLE lp_top ADD COLUMN s SYMBOL");
            execute("INSERT INTO lp_top VALUES(3,'2020-01-01T00:00:03','A'),(4,'2020-01-02T00:00:01','A')");
            execute("ALTER TABLE lp_top ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE(id,ts)");
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertCoveringBackup(
                    "SELECT id FROM lp_top WHERE s=null AND id>1",
                    "SelectedRecord > Filter filter: 1<id > CoveringIndex backup: true on: s with: id",
                    """
                    id
                    2
                    """
            );
            bindVariableService.setStr(0, "A");
            final String sql = "SELECT id FROM lp_top WHERE s=$1 AND id>1 LIMIT 2";
            assertCoveringBackup(
                    sql,
                    "Limit value: 2 > SelectedRecord > Filter filter: 1<id > CoveringIndex backup: true on: s with: id",
                    """
                    id
                    3
                    4
                    """
            );
            try (RecordCursorFactory factory = compile(sql)) {
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                        .skipRandomAccessProbe().sizeMayVary().returns("id\n3\n4\n");
                bindVariableService.setStr(0, null);
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                        .skipRandomAccessProbe().sizeMayVary().returns("id\n2\n");
                bindVariableService.setStr(0, "missing");
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                        .skipRandomAccessProbe().sizeMayVary().returns("id\n");
            }
        });
    }

    @Test
    public void testRetainedFactoriesSeeRebindingAndNewSymbols() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            bindVariableService.setStr(0, "A");
            final RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                retained = compiler.compile("SELECT id FROM lp_index WHERE s=$1 AND id>1", sqlExecutionContext).getRecordCursorFactory();
                try {
                    try (RecordCursorFactory ignored = compiler.compile("SELECT id FROM lp_index WHERE s='B'", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                    compiler.clear();
                } catch (Throwable th) {
                    retained.close();
                    throw th;
                }
            }
            try (RecordCursorFactory factory = retained) {
                assertRows(factory, "id\n4\n5\n");
                bindVariableService.setStr(0, null);
                assertRows(factory, "id\n3\n");
                bindVariableService.setStr(0, "new");
                assertRows(factory, "id\n");
                execute("INSERT INTO lp_index VALUES(98,8,'new','X','eight','2020-01-03')");
                assertRows(factory, "id\n8\n");
            }
            final String missing = "SELECT id FROM lp_index WHERE s='later'";
            assertIndex(
                    missing,
                    "deferred: true",
                    "SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s deferred: true > Frame forward scan on: lp_index",
                    "id\n"
            );
            try (RecordCursorFactory factory = compile(missing)) {
                assertRows(factory, "id\n");
                execute("INSERT INTO lp_index VALUES(99,9,'later','Y','nine','2020-01-03T00:00:01')");
                assertRows(factory, "id\n9\n");
            }
        });
    }

    @Test
    public void testFilterAndLimitBoundariesRemainObservable() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final String sql = "SELECT id FROM (SELECT id,s FROM lp_index LIMIT 3) WHERE s='A'";
            assertIndex(
                    sql,
                    "Limit",
                    "SelectedRecord > Filter filter: s='A' > Limit value: 3 > PageFrame > Row forward scan > Frame forward scan on: lp_index",
                    """
                    id
                    1
                    """
            );
            assertIndex(
                    "SELECT id FROM (SELECT id,s FROM lp_index WHERE s='A' LIMIT 2) WHERE id>1",
                    "Limit",
                    "Filter filter: 1<id > Limit value: 2 > SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Frame forward scan on: lp_index",
                    """
                    id
                    4
                    """
            );
        });
    }

    private void assertIndex(String sql, String specialization, String shape, String expected) throws Exception {
        try (RecordCursorFactory factory = compile(sql)) {
            final TextPlanSink sink = new TextPlanSink();
            sink.of(factory, sqlExecutionContext);
            TestUtils.assertContains(sink.getSink(), specialization);
            TestUtils.assertEquals(shape, PlanShape.of(factory, sqlExecutionContext));
            assertRows(factory, expected);
        }
    }

    private void assertIndexExactPlan(String sql, String specialization, String plan, String expected) throws Exception {
        try (RecordCursorFactory factory = compile(sql)) {
            final TextPlanSink sink = new TextPlanSink();
            sink.of(factory, sqlExecutionContext);
            TestUtils.assertContains(sink.getSink(), specialization);
            final StringSink text = new StringSink();
            for (int i = 1, n = sink.getLineCount(); i <= n; i++) {
                text.put(sink.getLine(i)).put('\n');
            }
            TestUtils.assertEquals(plan, text);
            assertRows(factory, expected);
        }
    }

    private void assertRows(String sql, String expected) throws Exception {
        try (RecordCursorFactory factory = compile(sql)) {
            assertRows(factory, expected);
        }
    }

    private void assertCoveringBackup(String sql, String shape, String expected) throws Exception {
        try (RecordCursorFactory factory = compile(sql)) {
            final TextPlanSink sink = new TextPlanSink();
            sink.of(factory, sqlExecutionContext);
            TestUtils.assertContains(sink.getSink(), "backup: true");
            TestUtils.assertEquals(shape, PlanShape.of(factory, sqlExecutionContext));
            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().noRandomAccess()
                    .skipRandomAccessProbe().sizeMayVary().returns(expected);
        }
    }

    private void assertRows(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                .sizeMayVary().returns(expected);
    }

    private RecordCursorFactory compile(String sql) throws SqlException {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            return compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
        }
    }

    private void createRows(boolean isCovering) throws SqlException {
        execute("CREATE TABLE lp_index(unused INT,id INT,s SYMBOL" + (isCovering ? "" : " INDEX")
                + ",t SYMBOL INDEX,v VARCHAR,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("""
                INSERT INTO lp_index VALUES
                (91,1,'A','X','one','2020-01-01T23:59:58'),
                (92,2,'B','Y','two','2020-01-01T23:59:59'),
                (93,3,null,'X','three','2020-01-02T00:00:00'),
                (94,4,'A','Y','four','2020-01-02T00:00:01'),
                (95,5,'A','Z','five','2020-01-02T00:00:02'),
                (96,6,'''','Z','six','2020-01-02T00:00:03'),
                (97,7,'','X','seven','2020-01-02T00:00:04')
                """);
        if (isCovering) {
            execute("ALTER TABLE lp_index ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE(id,v,ts)");
            engine.releaseAllWriters();
            engine.releaseAllReaders();
        }
    }
}
