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

import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LatestByPlanningTest extends AbstractCairoTest {
    @Test
    public void testDerivedSourcesKeepNullKeysAndInputLimits() throws Exception {
        for (boolean nanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(nanos);
                for (String key : new String[]{"s", "u", "k", "s,u"}) {
                    final String sql = "SELECT id FROM (SELECT id,s,u,k,ts FROM lp_latest LIMIT 4) LATEST ON ts PARTITION BY " + key + " ORDER BY id";
                    assertLatest(
                            sql,
                            "LatestBy light",
                            "Encode sort light > SelectedRecord > LatestBy light order_by_timestamp: true > Limit value: 4 > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                            """
                                    id
                                    2
                                    3
                                    4
                                    """
                    );
                }
                assertLatest(
                        "SELECT id FROM (SELECT id,s,ts FROM lp_latest LIMIT 4) WHERE id<4 LATEST ON ts PARTITION BY s",
                        "LatestBy light",
                        "SelectedRecord > LatestBy light order_by_timestamp: true > Filter filter: id<4 > Limit value: 4 > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                        """
                                id
                                1
                                2
                                3
                                """
                );
                assertRows("SELECT id FROM (SELECT id,s,ts FROM lp_latest LIMIT 4) WHERE id<4 LATEST ON ts PARTITION BY s ORDER BY id", "id\n1\n2\n3\n");
                assertLatest(
                        "SELECT id FROM (SELECT id,s,ts FROM lp_latest LIMIT 0) LATEST ON ts PARTITION BY s",
                        "LatestBy light",
                        "SelectedRecord > LatestBy light order_by_timestamp: true > Limit value: 0 > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                        "id\n"
                );
                execute("DROP TABLE lp_latest");
            });
        }
    }

    @Test
    public void testDerivedTimestampOrderingAndAliases() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final String[] orders = {"ts", "ts DESC", "id DESC"};
            final String[] orderShapes = {
                    "Encode sort light > SelectedRecord > LatestBy light order_by_timestamp: true > Limit value: 6 > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                    "Encode sort light > SelectedRecord > LatestBy light order_by_timestamp: false > Limit value: 6 > PageFrame > Row backward scan > Frame backward scan on: lp_latest",
                    "Encode sort light > SelectedRecord > LatestBy light order_by_timestamp: false > Async Top K lo: 6 workers: 1 > PageFrame > Row forward scan > Frame forward scan on: lp_latest"
            };
            for (int i = 0; i < orders.length; i++) {
                final String sql = "SELECT id FROM (SELECT id,s,ts FROM lp_latest ORDER BY " + orders[i] + " LIMIT 6) LATEST ON ts PARTITION BY s ORDER BY id";
                assertLatest(sql, "LatestBy light", orderShapes[i], i == 0 ? "id\n3\n5\n6\n" : "id\n3\n4\n6\n");
            }
            assertLatest(
                    "SELECT value FROM (SELECT id value,s key,ts stamp FROM lp_latest) LATEST ON stamp PARTITION BY key ORDER BY value",
                    "LatestBy light",
                    "Encode sort light > SelectedRecord > LatestBy light order_by_timestamp: true > SelectedRecord > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                    """
                            value
                            3
                            5
                            6
                            """
            );
            assertLatest(
                    "SELECT * FROM (SELECT ts,id i1,id i2 FROM lp_latest) WHERE i1>0 AND i2<5 LATEST ON ts PARTITION BY i1",
                    "LatestBy light",
                    "LatestBy light order_by_timestamp: true > SelectedRecord > Async JIT Filter workers: 1 > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                    """
                            ts	i1	i2
                            2020-01-01T23:59:58.000000Z	1	1
                            2020-01-01T23:59:59.000000Z	2	2
                            2020-01-02T00:00:00.000000Z	3	3
                            2020-01-02T00:00:00.000001Z	4	4
                            """
            );
            assertLatest(
                    "SELECT ts FROM (SELECT id,s,ts FROM lp_latest) LATEST ON ts PARTITION BY s ORDER BY ts",
                    "Encode sort",
                    "Encode sort light > SelectedRecord > LatestBy light order_by_timestamp: true > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                    """
                            ts
                            2020-01-02T00:00:00.000000Z
                            2020-01-02T00:00:00.000001Z
                            2020-01-02T00:00:01.000000Z
                            """
            );
        });
    }

    @Test
    public void testNonRandomAccessDerivedLatestUsesExistingReplayFactory() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final String source = "SELECT ts,s,max(id) id FROM lp_latest SAMPLE BY 1s ALIGN TO FIRST OBSERVATION";
            final String latest = "SELECT * FROM (" + source + ") LATEST ON ts PARTITION BY s";
            assertLatest(
                    latest,
                    "LatestBy",
                    "LatestBy > Sample By > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                    """
                            ts	s	id
                            2020-01-02T00:00:00.000000Z		3
                            2020-01-02T00:00:00.000000Z	A	5
                            2020-01-02T00:00:01.000000Z	B	6
                            """
            );
            assertLatest(
                    "SELECT id,ts FROM (" + latest + ")",
                    "LatestBy",
                    "SelectedRecord > LatestBy > Sample By > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                    """
                            id	ts
                            3	2020-01-02T00:00:00.000000Z
                            5	2020-01-02T00:00:00.000000Z
                            6	2020-01-02T00:00:01.000000Z
                            """
            );
            assertLatest(
                    "WITH q AS (" + source + ") SELECT id FROM q LATEST ON ts PARTITION BY s ORDER BY id",
                    "LatestBy",
                    "Encode sort > SelectedRecord > LatestBy > Sample By > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                    """
                            id
                            3
                            5
                            6
                            """
            );
            assertRows("SELECT id FROM (" + latest + ") ORDER BY id", "id\n3\n5\n6\n");
            assertLatest(
                    "SELECT a.id,b.id FROM (" + latest + ") a ASOF JOIN lp_latest b",
                    "AsOf",
                    "SelectedRecord > AsOf Join Fast > SelectedRecord > LatestBy > Sample By > PageFrame > Row forward scan > Frame forward scan on: lp_latest > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                    """
                            id	id1
                            3	3
                            5	3
                            6	6
                            """
            );
            assertLatest(
                    "SELECT ts,max(id) id FROM (" + latest + ") SAMPLE BY 1s ALIGN TO FIRST OBSERVATION",
                    "LatestBy",
                    "Sample By > SelectedRecord > LatestBy > Sample By > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                    """
                            ts	id
                            2020-01-02T00:00:00.000000Z	5
                            2020-01-02T00:00:01.000000Z	6
                            """
            );
        });
    }

    @Test
    public void testDerivedLatestTimestampDesignationIsObservedByParents() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final String latest = "SELECT * FROM (SELECT id,s,ts FROM lp_latest) LATEST ON ts PARTITION BY s";
            assertLatest(
                    latest,
                    "LatestBy light",
                    "LatestBy light order_by_timestamp: true > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                    """
                            id	s	ts
                            5	A	2020-01-02T00:00:00.000001Z
                            6	B	2020-01-02T00:00:01.000000Z
                            3		2020-01-02T00:00:00.000000Z
                            """
            );
            assertLatest(
                    "SELECT id,ts FROM (" + latest + ")",
                    "LatestBy light",
                    "SelectedRecord > LatestBy light order_by_timestamp: true > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                    """
                            id	ts
                            5	2020-01-02T00:00:00.000001Z
                            6	2020-01-02T00:00:01.000000Z
                            3	2020-01-02T00:00:00.000000Z
                            """
            );
            try (RecordCursorFactory factory = select("SELECT * FROM (" + latest + ") TIMESTAMP(ts)")) {
                Assert.assertEquals(2, factory.getMetadata().getTimestampIndex());
                TestUtils.assertContains(planText(factory), "LatestBy light");
                final StringSink sink = new StringSink();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
                }
                TestUtils.assertEquals("""
                        id	s	ts
                        5	A	2020-01-02T00:00:00.000001Z
                        6	B	2020-01-02T00:00:01.000000Z
                        3		2020-01-02T00:00:00.000000Z
                        """, sink);
            }
            assertLatest(
                    "SELECT id FROM (" + latest + ") ORDER BY ts",
                    "Encode sort",
                    "SelectedRecord > Encode sort light > SelectedRecord > LatestBy light order_by_timestamp: true > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                    """
                            id
                            3
                            5
                            6
                            """
            );
            assertQuery("SELECT a.id,b.id FROM (" + latest + ") a ASOF JOIN lp_latest b").noLeakCheck().fails(23, "TIMESTAMP column is required but not provided");
            assertQuery("SELECT ts,max(id) FROM (" + latest + ") SAMPLE BY 1s ALIGN TO FIRST OBSERVATION").noLeakCheck().fails(24, "TIMESTAMP column is required but not provided");
        });
    }

    @Test
    public void testDerivedLatestRetainsFactoriesAndRuntimeLimits() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            bindVariableService.setLong(0, 4);
            final String sql = "SELECT id FROM (SELECT id,s,ts FROM lp_latest LIMIT $1) LATEST ON ts PARTITION BY s ORDER BY id";
            assertLatest(
                    sql,
                    "LatestBy light",
                    "Encode sort light > SelectedRecord > LatestBy light order_by_timestamp: true > Limit value: $0::long > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                    """
                            id
                            2
                            3
                            4
                            """
            );
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    compiler.clear();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT id FROM lp_latest LATEST ON ts PARTITION BY s", sqlExecutionContext).getRecordCursorFactory()) {
                        compiler.clear();
                    }
                }
                assertFactory(retained).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns("id\n2\n3\n4\n");
                bindVariableService.setLong(0, 6);
                assertFactory(retained).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns("id\n3\n5\n6\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testDerivedLatestDiagnosticsAndRecovery() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            assertQuery("SELECT * FROM (SELECT id,s FROM lp_latest) LATEST BY s").noLeakCheck().fails(50, "'on' expected");
            assertQuery("SELECT * FROM (SELECT id,s FROM lp_latest) LATEST ON ts PARTITION BY s").noLeakCheck().fails(53, "Invalid column: ts");
            assertQuery("SELECT * FROM (SELECT id,s,ts FROM lp_latest) LATEST ON id PARTITION BY s").noLeakCheck().fails(56, "not a TIMESTAMP");
            assertQuery("SELECT * FROM (SELECT id,s,ts FROM lp_latest) LATEST ON ts PARTITION BY missing").noLeakCheck().fails(72, "Invalid column: missing");
            assertLatest(
                    "SELECT id FROM (SELECT id,s,ts FROM lp_latest LIMIT 4) LATEST ON ts PARTITION BY s",
                    "LatestBy light",
                    "SelectedRecord > LatestBy light order_by_timestamp: true > Limit value: 4 > PageFrame > Row forward scan > Frame forward scan on: lp_latest",
                    """
                            id
                            4
                            2
                            3
                            """
            );
        });
    }

    @Test
    public void testCaseResidualReopensAndReleasesNativeArguments() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_latest_case(id INT,a SYMBOL,b SYMBOL,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lp_latest_case VALUES"
                    + "(1,'A','X',1),(2,'B','Y',2),(3,null,null,3),"
                    + "(4,'A','X',4),(5,'A','X',5),(6,'B','Y',6)");
            final ObjList<String> predicates = new ObjList<>(2);
            predicates.add("CASE WHEN id<5 THEN id>1 ELSE false END");
            predicates.add("CASE WHEN id<5 THEN id IN (2,3,4) ELSE false END");
            for (int i = 0; i < predicates.size(); i++) {
                final String sql = "SELECT id FROM lp_latest_case WHERE " + predicates.getQuick(i)
                        + " LATEST ON ts PARTITION BY a,b";
                assertLatest(
                        sql,
                        "LatestByAllSymbolsFiltered",
                        "SelectedRecord > LatestByAllSymbolsFiltered > Row backward scan > Frame backward scan on: lp_latest_case",
                        """
                                id
                                2
                                3
                                4
                                """
                );
                try (RecordCursorFactory unopened = select(sql)) {
                    Assert.assertNotNull(unopened);
                }
            }
        });
    }

    @Test
    public void testDeprecatedLatestByKeepsPhysicalOrderWithoutDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_latest(id INT,k INT,ts TIMESTAMP)");
            execute("INSERT INTO lp_latest VALUES(1,1,1),(2,2,2),(3,1,3)");
            assertLatest(
                    "SELECT id FROM lp_latest LATEST BY k",
                    "LatestByAllFiltered",
                    "SelectedRecord > LatestByAllFiltered > Row backward scan > Frame backward scan on: lp_latest",
                    """
                            id
                            2
                            3
                            """
            );
            assertLatest(
                    "SELECT id FROM lp_latest LATEST BY k WHERE id<3",
                    "LatestByAllFiltered",
                    "SelectedRecord > LatestByAllFiltered > Row backward scan > Frame backward scan on: lp_latest",
                    """
                            id
                            1
                            2
                            """
            );
            assertQuery("SELECT * FROM lp_latest LATEST ON ts PARTITION BY k").noLeakCheck().fails(34, "latest by over a table requires designated TIMESTAMP");
        });
    }

    @Test
    public void testAllFourNativeSpecializationsKeepLatestRows() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            assertLatest(
                    "SELECT id,s,ts FROM lp_latest LATEST ON ts PARTITION BY s",
                    "LatestByAllIndexed",
                    "LatestByAllIndexed > Async index backward scan on: s workers: 1 > Frame backward scan on: lp_latest",
                    """
                            id	s	ts
                            3		2020-01-02T00:00:00.000000Z
                            5	A	2020-01-02T00:00:00.000001Z
                            6	B	2020-01-02T00:00:01.000000Z
                            """
            );
            assertLatest(
                    "SELECT id,u,ts FROM lp_latest LATEST ON ts PARTITION BY u",
                    "LatestByDeferredListValuesFiltered",
                    "LatestByDeferredListValuesFiltered > Frame backward scan on: lp_latest",
                    """
                            id	u	ts
                            3		2020-01-02T00:00:00.000000Z
                            5	X	2020-01-02T00:00:00.000001Z
                            6	Y	2020-01-02T00:00:01.000000Z
                            """
            );
            assertLatest(
                    "SELECT id,s,u,ts FROM lp_latest LATEST ON ts PARTITION BY s,u",
                    "LatestByAllSymbolsFiltered",
                    "LatestByAllSymbolsFiltered > Row backward scan > Frame backward scan on: lp_latest",
                    """
                            id	s	u	ts
                            3			2020-01-02T00:00:00.000000Z
                            5	A	X	2020-01-02T00:00:00.000001Z
                            6	B	Y	2020-01-02T00:00:01.000000Z
                            """
            );
            assertLatest(
                    "SELECT id,k,ts FROM lp_latest LATEST ON ts PARTITION BY k",
                    "LatestByAllFiltered",
                    "LatestByAllFiltered > Row backward scan > Frame backward scan on: lp_latest",
                    """
                            id	k	ts
                            3	null	2020-01-02T00:00:00.000000Z
                            5	1	2020-01-02T00:00:00.000001Z
                            6	2	2020-01-02T00:00:01.000000Z
                            """
            );
            assertLatest(
                    "SELECT id,k,v,ts FROM lp_latest LATEST ON ts PARTITION BY k,v",
                    "LatestByAllFiltered",
                    "LatestByAllFiltered > Row backward scan > Frame backward scan on: lp_latest",
                    """
                            id	k	v	ts
                            3	null		2020-01-02T00:00:00.000000Z
                            5	1	alpha	2020-01-02T00:00:00.000001Z
                            6	2	beta	2020-01-02T00:00:01.000000Z
                            """
            );
            assertLatest(
                    "SELECT id FROM lp_latest LATEST ON ts PARTITION BY s",
                    "LatestByAllIndexed",
                    "SelectedRecord > LatestByAllIndexed > Async index backward scan on: s workers: 1 > Frame backward scan on: lp_latest",
                    """
                            id
                            3
                            5
                            6
                            """
            );
        });
    }

    @Test
    public void testIntervalsKeepNativeSpecializationsAndTimestampPrecision() throws Exception {
        for (boolean isNanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(isNanos);
                final String bound = isNanos ? "'2020-01-02T00:00:00.000000001Z'" : "'2020-01-02T00:00:00.000001Z'";
                final ObjList<String> keys = new ObjList<>(4);
                keys.add("s");
                keys.add("u");
                keys.add("s,u");
                keys.add("k");
                final String[] shapes = {"SelectedRecord > LatestByDeferredListValuesFiltered > Interval backward scan on: lp_latest", "SelectedRecord > LatestByDeferredListValuesFiltered > Interval backward scan on: lp_latest", "SelectedRecord > LatestByAllSymbolsFiltered > Row backward scan > Interval backward scan on: lp_latest", "SelectedRecord > LatestByAllFiltered > Row backward scan > Interval backward scan on: lp_latest"};
                for (int i = 0, n = keys.size(); i < n; i++) {
                    assertLatest(
                            "SELECT id,ts FROM lp_latest WHERE ts<" + bound + " LATEST ON ts PARTITION BY " + keys.getQuick(i),
                            "Interval backward scan",
                            shapes[i],
                            isNanos ? """
                                    id	ts
                                    1	2020-01-01T23:59:58.000000000Z
                                    2	2020-01-01T23:59:59.000000000Z
                                    3	2020-01-02T00:00:00.000000000Z
                                    """ : """
                                    id	ts
                                    1	2020-01-01T23:59:58.000000Z
                                    2	2020-01-01T23:59:59.000000Z
                                    3	2020-01-02T00:00:00.000000Z
                                    """
                    );
                }
                final String first = isNanos ? "'2020-01-01T23:59:59.000000000Z'" : "'2020-01-01T23:59:59.000000Z'";
                assertLatest(
                        "SELECT id FROM lp_latest WHERE ts=" + bound
                                + " OR ts=" + first + " LATEST ON ts PARTITION BY s",
                        "Interval backward scan",
                        "SelectedRecord > LatestByDeferredListValuesFiltered > Interval backward scan on: lp_latest",
                        """
                                id
                                2
                                5
                                """
                );
                assertLatest(
                        "SELECT id FROM lp_latest WHERE ts>" + bound
                                + " AND ts<" + first + " LATEST ON ts PARTITION BY s",
                        "Interval backward scan",
                        "SelectedRecord > LatestByDeferredListValuesFiltered > Interval backward scan on: lp_latest",
                        "id\n"
                );
                execute("DROP TABLE lp_latest");
            });
        }
    }

    @Test
    public void testOuterFilterOrderLimitAndAggregationObserveLatestRows() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            // Filtering the historical input would incorrectly revive id=1.
            assertRows("SELECT id FROM (SELECT id FROM lp_latest LATEST ON ts PARTITION BY s) WHERE id=1", "id\n");
            assertLatest(
                    "SELECT id FROM lp_latest LATEST ON ts PARTITION BY s ORDER BY ts DESC LIMIT 2",
                    "LatestByAllIndexed",
                    "SelectedRecord > Encode sort light lo: 2 > SelectedRecord > LatestByAllIndexed > Async index backward scan on: s workers: 1 > Frame backward scan on: lp_latest",
                    """
                            id
                            6
                            5
                            """
            );
            assertLatest(
                    "SELECT id FROM lp_latest LATEST ON ts PARTITION BY k ORDER BY id DESC LIMIT 2",
                    "LatestByAllFiltered",
                    "Encode sort light lo: 2 > SelectedRecord > LatestByAllFiltered > Row backward scan > Frame backward scan on: lp_latest",
                    """
                            id
                            6
                            5
                            """
            );
            assertLatest(
                    "SELECT id,ts FROM lp_latest LATEST ON ts PARTITION BY s ORDER BY ts DESC",
                    "LatestByAllIndexed",
                    "Encode sort light > SelectedRecord > LatestByAllIndexed > Async index backward scan on: s workers: 1 > Frame backward scan on: lp_latest",
                    """
                            id	ts
                            6	2020-01-02T00:00:01.000000Z
                            5	2020-01-02T00:00:00.000001Z
                            3	2020-01-02T00:00:00.000000Z
                            """
            );
            assertLatest(
                    "SELECT count() FROM lp_latest LATEST ON ts PARTITION BY s",
                    "LatestByAllIndexed",
                    "Count > LatestByAllIndexed > Async index backward scan on: s workers: 1 > Frame backward scan on: lp_latest",
                    """
                            count
                            3
                            """
            );
            assertLatest(
                    "SELECT sum(id) FROM lp_latest LATEST ON ts PARTITION BY k",
                    "LatestByAllFiltered",
                    "GroupBy vectorized: false > LatestByAllFiltered > Row backward scan > Frame backward scan on: lp_latest",
                    """
                            sum
                            14
                            """
            );
        });
    }

    @Test
    public void testRetainedFactorySurvivesCompilerReuseAndNewSymbolValues() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final String sql = "SELECT id,s FROM lp_latest LATEST ON ts PARTITION BY s";
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    try (RecordCursorFactory ignored = compiler.compile(
                            "SELECT missing FROM lp_latest", sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        Assert.fail("invalid column must fail after retaining the factory");
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column: missing");
                    }
                    try (RecordCursorFactory ignored = compiler.compile(
                            "SELECT id FROM lp_latest LATEST ON ts PARTITION BY k", sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        compiler.clear();
                    }
                }
                execute("INSERT INTO lp_latest VALUES(7,'new','new',4,'new','2020-01-03T00:00:00.000000Z')");
                assertFactory(retained).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                        .sizeMayVary().returns("id\ts\n3\t\n5\tA\n6\tB\n7\tnew\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testTimestampAndKeyValidation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_latest(id INT,k INT,ts TIMESTAMP,other TIMESTAMP,arr DOUBLE[]) TIMESTAMP(ts)");
            assertQuery("SELECT * FROM lp_latest LATEST ON other PARTITION BY missing").noLeakCheck().fails(34, "latest by over a table requires designated TIMESTAMP");
            assertQuery("SELECT * FROM lp_latest LATEST ON id PARTITION BY missing").noLeakCheck().fails(34, "not a TIMESTAMP");
            assertQuery("SELECT * FROM lp_latest LATEST ON missing PARTITION BY k").noLeakCheck().fails(34, "Invalid column: missing");
            assertQuery("SELECT * FROM lp_latest LATEST ON ts PARTITION BY missing").noLeakCheck().fails(50, "Invalid column: missing");
            assertQuery("SELECT * FROM lp_latest LATEST ON ts PARTITION BY arr").noLeakCheck().fails(50, "arr (DOUBLE[]): invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, VARCHAR, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON");
            assertLatest(
                    "SELECT id FROM lp_latest LATEST ON ts PARTITION BY k",
                    "LatestByAllFiltered",
                    "SelectedRecord > LatestByAllFiltered > Row backward scan > Frame backward scan on: lp_latest",
                    "id\n"
            );
        });
    }

    @Test
    public void testSourceResidualFiltersChooseExistingFactoriesBeforeLatest() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final ObjList<String> keys = new ObjList<>(4);
            keys.add("s");
            keys.add("u");
            keys.add("s,u");
            keys.add("k");
            final String[] shapes3 = {"SelectedRecord > LatestByAllIndexed > Async index backward scan on: s workers: 1 > Frame backward scan on: lp_latest", "SelectedRecord > LatestByDeferredListValuesFiltered > Frame backward scan on: lp_latest", "SelectedRecord > LatestByAllSymbolsFiltered > Row backward scan > Frame backward scan on: lp_latest", "SelectedRecord > LatestByAllFiltered > Row backward scan > Frame backward scan on: lp_latest"};
            final String[] shapes2 = {"SelectedRecord > LatestByDeferredListValuesFiltered > Frame backward scan on: lp_latest", "SelectedRecord > LatestByDeferredListValuesFiltered > Frame backward scan on: lp_latest", "SelectedRecord > LatestByAllSymbolsFiltered > Row backward scan > Frame backward scan on: lp_latest", "SelectedRecord > LatestByAllFiltered > Row backward scan > Frame backward scan on: lp_latest"};
            for (int i = 0, n = keys.size(); i < n; i++) {
                final String key = keys.getQuick(i);
                final String factory = i < 2 ? "LatestByDeferredListValuesFiltered"
                        : i == 2 ? "LatestByAllSymbolsFiltered" : "LatestByAllFiltered";
                final String sql = "SELECT id FROM lp_latest WHERE id<5 LATEST ON ts PARTITION BY " + key;
                assertLatest(
                        sql,
                        factory,
                        shapes2[i],
                        """
                                id
                                2
                                3
                                4
                                """
                );
                assertLatest(
                        "SELECT id FROM lp_latest WHERE true LATEST ON ts PARTITION BY " + key,
                        i == 0 ? "LatestByAllIndexed" : factory,
                        shapes3[i],
                        """
                                id
                                3
                                5
                                6
                                """
                );
                assertLatest(
                        "SELECT id FROM lp_latest WHERE false LATEST ON ts PARTITION BY " + key,
                        "Empty table",
                        "SelectedRecord > Empty table",
                        "id\n"
                );
            }
            assertLatest(
                    "SELECT id FROM lp_latest WHERE ts>='2020-01-02T00:00:00.000000Z'"
                            + " AND id<5 LATEST ON ts PARTITION BY s,u",
                    "LatestByAllSymbolsFiltered",
                    "SelectedRecord > LatestByAllSymbolsFiltered > Row backward scan > Interval backward scan on: lp_latest",
                    """
                            id
                            3
                            4
                            """
            );
            assertRows("SELECT id FROM (SELECT id FROM lp_latest WHERE id<5 LATEST ON ts PARTITION BY s,u)"
                    + " WHERE id<4", "id\n2\n3\n");
            assertLatest(
                    "SELECT id FROM lp_latest WHERE length(v)>0 AND id<5 LATEST ON ts PARTITION BY s,u",
                    "LatestByAllSymbolsFiltered",
                    "SelectedRecord > LatestByAllSymbolsFiltered > Row backward scan > Frame backward scan on: lp_latest",
                    """
                            id
                            2
                            4
                            """
            );
        });
    }

    @Test
    public void testResidualFactoryReopensAfterCompilerCloseAndBindChanges() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            bindVariableService.setInt(0, 4);
            final String sql = "SELECT id FROM lp_latest WHERE id<$1 LATEST ON ts PARTITION BY s,u";
            try (RecordCursorFactory factory = select(sql)) {
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                        .sizeMayVary().returns("id\n1\n2\n3\n");
                bindVariableService.setInt(0, 5);
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                        .sizeMayVary().returns("id\n2\n3\n4\n");
            }
            bindVariableService.clear();
            bindVariableService.setBoolean(0, true);
            final String runtime = "SELECT id FROM lp_latest WHERE $1 LATEST ON ts PARTITION BY s,u";
            assertLatest(
                    runtime,
                    "LatestByAllSymbolsFiltered",
                    "SelectedRecord > LatestByAllSymbolsFiltered > Row backward scan > Frame backward scan on: lp_latest",
                    """
                            id
                            3
                            5
                            6
                            """
            );
            try (RecordCursorFactory factory = select(runtime)) {
                bindVariableService.setBoolean(0, false);
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                        .sizeMayVary().returns("id\n");
                bindVariableService.setBoolean(0, true);
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                        .sizeMayVary().returns("id\n3\n5\n6\n");
            }
        });
    }

    @Test
    public void testSelfComparisonIntrinsics() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            assertLatest(
                    "SELECT id FROM lp_latest WHERE id<id LATEST ON ts PARTITION BY s",
                    "Empty table",
                    "SelectedRecord > Empty table",
                    "id\n"
            );
            assertLatest(
                    "SELECT id FROM lp_latest WHERE id=id LATEST ON ts PARTITION BY s",
                    "LatestBy",
                    "SelectedRecord > LatestByDeferredListValuesFiltered > Frame backward scan on: lp_latest",
                    """
                            id
                            3
                            5
                            6
                            """
            );
            assertLatest(
                    "SELECT id FROM lp_latest WHERE k<>k LATEST ON ts PARTITION BY s",
                    "Empty table",
                    "SelectedRecord > Empty table",
                    "id\n"
            );
            assertLatest(
                    "SELECT id FROM lp_latest WHERE id<=id OR k=1 LATEST ON ts PARTITION BY s",
                    "LatestBy",
                    "SelectedRecord > LatestByDeferredListValuesFiltered > Frame backward scan on: lp_latest",
                    """
                            id
                            3
                            5
                            6
                            """
            );
        });
    }

    @Test
    public void testSymbolKeyListsAndExclusions() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final ObjList<String> predicates = new ObjList<>("s IN ('A','B')", "s IN ('A','C')", "s IN ('A',null) AND k>0",
                    "s NOT IN ('A')", "s!='A'", "s IN ('A','B') AND s!='B'", "s IN ('A','B') AND s='C'",
                    "s IN ($1,'B')", "s!=$1", "s IN ('A','B') AND u='Y'");
            final ObjList<String> symbolRows = new ObjList<>("id\n5\n6\n", "id\n5\n", "id\n5\n",
                    "id\n3\n6\n", "id\n3\n6\n", "id\n5\n", "id\n",
                    "id\n5\n6\n", "id\n3\n6\n", "id\n6\n");
            final ObjList<String> plainRows = new ObjList<>("id\n5\n6\n", "id\n5\n", "id\n5\n",
                    "id\n3\n6\n", "id\n3\n6\n", "id\n5\n", "id\n",
                    "id\n6\n", "id\n3\n5\n6\n", "id\n6\n");
            bindVariableService.setStr(0, "A");
            final String[] shapes5 = {"SelectedRecord > LatestByDeferredListValuesFiltered > Frame backward scan on: lp_latest", "SelectedRecord > LatestByDeferredListValuesFiltered > Frame backward scan on: lp_latest", "SelectedRecord > LatestByDeferredListValuesFiltered > Frame backward scan on: lp_latest", "SelectedRecord > LatestByDeferredListValuesFiltered > Frame backward scan on: lp_latest", "SelectedRecord > LatestByDeferredListValuesFiltered > Frame backward scan on: lp_latest", "SelectedRecord > LatestByValueFiltered > Row backward scan > Frame backward scan on: lp_latest", "SelectedRecord > Empty table", "SelectedRecord > LatestByDeferredListValuesFiltered > Frame backward scan on: lp_latest", "SelectedRecord > LatestByDeferredListValuesFiltered > Frame backward scan on: lp_latest", "SelectedRecord > LatestByValueFiltered > Row backward scan > Frame backward scan on: lp_latest"};
            final String[] shapes4 = {"SelectedRecord > Index backward scan on: s > Frame backward scan on: lp_latest", "SelectedRecord > Index backward scan on: s > Frame backward scan on: lp_latest", "SelectedRecord > Index backward scan on: s > Frame backward scan on: lp_latest", "SelectedRecord > LatestByDeferredListValuesFiltered > Frame backward scan on: lp_latest", "SelectedRecord > LatestByDeferredListValuesFiltered > Frame backward scan on: lp_latest", "SelectedRecord > PageFrame > Index backward scan on: s > Frame backward scan on: lp_latest", "SelectedRecord > Empty table", "SelectedRecord > Index backward scan on: s > Frame backward scan on: lp_latest", "SelectedRecord > LatestByDeferredListValuesFiltered > Frame backward scan on: lp_latest", "SelectedRecord > Index backward scan on: s > Frame backward scan on: lp_latest"};
            for (int i = 0, n = predicates.size(); i < n; i++) {
                assertLatest(
                        "SELECT id FROM lp_latest WHERE " + predicates.getQuick(i) + " LATEST ON ts PARTITION BY s",
                        "",
                        shapes4[i],
                        symbolRows.getQuick(i)
                );
                assertLatest(
                        "SELECT id FROM lp_latest WHERE " + predicates.getQuick(i).replace("s", "u")
                                .replace("'A'", "'X'").replace("'B'", "'Y'") + " LATEST ON ts PARTITION BY u",
                        "",
                        shapes5[i],
                        plainRows.getQuick(i)
                );
            }
        });
    }

    @Test
    public void testSymbolEqualitiesKeepResolvedDeferredAndNullSelectors() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final ObjList<String> keys = new ObjList<>(2);
            keys.add("s");
            keys.add("u");
            final String[] shapes10 = {"SelectedRecord > PageFrame > Index backward scan on: s deferred: true > Frame backward scan on: lp_latest", "SelectedRecord > LatestByValueDeferredFiltered > Frame backward scan on: lp_latest"};
            final String[] shapes9 = {"SelectedRecord > PageFrame > Index backward scan on: s > Frame backward scan on: lp_latest", "SelectedRecord > LatestByValueFiltered > Row backward scan > Frame backward scan on: lp_latest"};
            final String[] shapes8 = {"SelectedRecord > PageFrame > Index backward scan on: s > Frame backward scan on: lp_latest", "SelectedRecord > LatestByValueFiltered > Row backward scan > Frame backward scan on: lp_latest"};
            final String[] shapes7 = {"SelectedRecord > PageFrame > Index backward scan on: s > Frame backward scan on: lp_latest", "SelectedRecord > LatestByValueFiltered > Row backward scan > Frame backward scan on: lp_latest"};
            final String[] shapes6 = {"SelectedRecord > PageFrame > Index backward scan on: s > Frame backward scan on: lp_latest", "SelectedRecord > LatestByValueFiltered > Row backward scan > Frame backward scan on: lp_latest"};
            for (int i = 0; i < keys.size(); i++) {
                final String key = keys.getQuick(i);
                final String value = i == 0 ? "'A'" : "'X'";
                final String specialization = i == 0 ? "Index backward scan" : "LatestByValueFiltered";
                final String suffix = " LATEST ON ts PARTITION BY " + key;
                assertLatest(
                        "SELECT id FROM lp_latest WHERE " + key + "=" + value + suffix,
                        specialization,
                        shapes6[i],
                        """
                                id
                                5
                                """
                );
                assertLatest(
                        "SELECT id FROM lp_latest WHERE " + value + "=" + key + suffix,
                        specialization,
                        shapes7[i],
                        """
                                id
                                5
                                """
                );
                assertRows("SELECT id FROM lp_latest WHERE " + key + "=" + value + suffix, "id\n5\n");
                assertLatest(
                        "SELECT id FROM lp_latest WHERE " + key + " IS NULL" + suffix,
                        specialization,
                        shapes8[i],
                        """
                                id
                                3
                                """
                );
                assertLatest(
                        "SELECT id FROM lp_latest WHERE " + key + "=null::CHAR" + suffix,
                        specialization,
                        shapes9[i],
                        """
                                id
                                3
                                """
                );
                assertLatest(
                        "SELECT id FROM lp_latest WHERE " + key + "='missing'" + suffix,
                        i == 0 ? "deferred: true" : "LatestByValueDeferredFiltered",
                        shapes10[i],
                        "id\n"
                );
            }
            execute("INSERT INTO lp_latest VALUES(7,'''','''',7,'quote','2020-01-03')");
            assertLatest(
                    "SELECT id FROM lp_latest WHERE s='''' LATEST ON ts PARTITION BY s",
                    "Index backward scan",
                    "SelectedRecord > PageFrame > Index backward scan on: s > Frame backward scan on: lp_latest",
                    """
                            id
                            7
                            """
            );
        });
    }

    @Test
    public void testSymbolEqualityKeepsSourceFilterAndIntervalBeforeLatest() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final ObjList<String> predicates = new ObjList<>(3);
            predicates.add("s='A' AND id<5");
            predicates.add("id<5 AND s='A'");
            predicates.add("ts>='2020-01-02T00:00:00.000000Z' AND s='A' AND id<5");
            final String[] shapes11 = {"SelectedRecord > Index backward scan on: s > Frame backward scan on: lp_latest", "SelectedRecord > Index backward scan on: s > Frame backward scan on: lp_latest", "SelectedRecord > Index backward scan on: s > Interval backward scan on: lp_latest"};
            for (int i = 0; i < predicates.size(); i++) {
                final String sql = "SELECT id FROM lp_latest WHERE " + predicates.getQuick(i) + " LATEST ON ts PARTITION BY s";
                assertLatest(
                        sql,
                        "Index backward scan",
                        shapes11[i],
                        """
                                id
                                4
                                """
                );
            }
            assertLatest(
                    "SELECT id FROM lp_latest WHERE u='X' AND id<5 LATEST ON ts PARTITION BY u",
                    "LatestByValueFiltered",
                    "SelectedRecord > LatestByValueFiltered > Row backward scan > Frame backward scan on: lp_latest",
                    """
                            id
                            4
                            """
            );
            final String ordered = "SELECT id FROM lp_latest WHERE ts>='2020-01-02T00:00:00.000000Z'"
                    + " AND s='A' AND id<5 LATEST ON ts PARTITION BY s ORDER BY ts DESC";
            assertLatest(
                    ordered,
                    "Interval backward scan",
                    "SelectedRecord > Encode sort light > SelectedRecord > Index backward scan on: s > Interval backward scan on: lp_latest",
                    """
                            id
                            4
                            """
            );
            assertRows("SELECT id FROM (SELECT id FROM lp_latest WHERE s='A' LATEST ON ts PARTITION BY s) WHERE id<5", "id\n");
            assertLatest(
                    "SELECT count() FROM lp_latest WHERE s='A' LATEST ON ts PARTITION BY s",
                    "Index backward scan",
                    "Count > PageFrame > Index backward scan on: s > Frame backward scan on: lp_latest",
                    """
                            count
                            1
                            """
            );
        });
    }

    @Test
    public void testSymbolSelectorRetainedFactoryRebindsAndFindsNewSymbols() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            bindVariableService.setStr(0, "A");
            bindVariableService.setStr(1, "X");
            bindVariableService.setInt(2, 5);
            final String indexedSql = "SELECT id FROM lp_latest WHERE s=$1 AND id<$3 LATEST ON ts PARTITION BY s";
            final String plainSql = "SELECT id FROM lp_latest WHERE $2=u AND id<$3 LATEST ON ts PARTITION BY u";
            assertLatest(
                    indexedSql,
                    "Index backward scan",
                    "SelectedRecord > Index backward scan on: s > Frame backward scan on: lp_latest",
                    """
                            id
                            4
                            """
            );
            assertLatest(
                    plainSql,
                    "LatestByValueDeferredFiltered",
                    "SelectedRecord > LatestByValueDeferredFiltered > Frame backward scan on: lp_latest",
                    """
                            id
                            4
                            """
            );
            try (RecordCursorFactory indexed = select(indexedSql); RecordCursorFactory plain = select(plainSql)) {
                assertFactory(indexed).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns("id\n4\n");
                assertFactory(plain).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns("id\n4\n");
                bindVariableService.setInt(2, 10);
                bindVariableService.setStr(0, null);
                bindVariableService.setStr(1, null);
                assertFactory(indexed).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns("id\n3\n");
                assertFactory(plain).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns("id\n3\n");
                bindVariableService.setStr(0, "new");
                bindVariableService.setStr(1, "new");
                assertFactory(indexed).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns("id\n");
                assertFactory(plain).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns("id\n");
                execute("INSERT INTO lp_latest VALUES(7,'new','new',7,'new','2020-01-03')");
                assertFactory(indexed).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns("id\n7\n");
                assertFactory(plain).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns("id\n7\n");
            }
        });
    }

    @Test
    public void testCoveringSymbolSelectorKeepsNullColumnTopBackup() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_latest(id INT,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO lp_latest VALUES(1,'2020-01-01T00:00:01'),(2,'2020-01-01T00:00:02')");
            execute("ALTER TABLE lp_latest ADD COLUMN s SYMBOL");
            execute("INSERT INTO lp_latest VALUES(3,'2020-01-01T00:00:03','A'),(4,'2020-01-02T00:00:01','A')");
            execute("ALTER TABLE lp_latest ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE(id,ts)");
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertLatest(
                    "SELECT id FROM lp_latest WHERE s='A' LATEST ON ts PARTITION BY s",
                    "CoveringIndex",
                    "SelectedRecord > CoveringIndex op: latest on: s with: id",
                    """
                            id
                            4
                            """
            );
            final String nullSql = "SELECT id FROM lp_latest WHERE s=null AND id<2 LATEST ON ts PARTITION BY s";
            assertCoveringBackup(
                    nullSql,
                    "SelectedRecord > CoveringIndex backup: true op: latest on: s with: id",
                    """
                            id
                            1
                            """
            );
            try (RecordCursorFactory factory = select(nullSql)) {
                // The covering factory declares no random access, while its backup cursor provides it.
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().noRandomAccess()
                        .skipRandomAccessProbe().sizeMayVary().returns("id\n1\n");
            }
            bindVariableService.setStr(0, "A");
            final String bindSql = "SELECT id FROM lp_latest WHERE s=$1 AND id<4 LATEST ON ts PARTITION BY s";
            assertCoveringBackup(
                    bindSql,
                    "SelectedRecord > CoveringIndex backup: true op: latest on: s with: id",
                    """
                            id
                            3
                            """
            );
            try (RecordCursorFactory factory = select(bindSql)) {
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().noRandomAccess()
                        .skipRandomAccessProbe().sizeMayVary().returns("id\n3\n");
                bindVariableService.setStr(0, null);
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().noRandomAccess()
                        .skipRandomAccessProbe().sizeMayVary().returns("id\n2\n");
            }
        });
    }

    @Test
    public void testParquetResidualPrunesWithEitherPruningMode() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            execute("ALTER TABLE lp_latest CONVERT PARTITION TO PARQUET WHERE ts<'2020-01-02'");
            final boolean wasPruningEnabled = sqlExecutionContext.isParquetRowGroupPruningEnabled();
            final String sql = "SELECT id FROM lp_latest WHERE id<3 LATEST ON ts PARTITION BY k";
            try {
                sqlExecutionContext.setParquetRowGroupPruningEnabled(true);
                assertLatest(
                        sql,
                        "LatestByAllFiltered",
                        "SelectedRecord > LatestByAllFiltered > Row backward scan > Frame backward scan on: lp_latest",
                        """
                                id
                                1
                                2
                                """
                );
                assertLatest(
                        "SELECT id FROM lp_latest WHERE false LATEST ON ts PARTITION BY k",
                        "Empty table",
                        "SelectedRecord > Empty table",
                        "id\n"
                );
                sqlExecutionContext.setParquetRowGroupPruningEnabled(false);
                assertLatest(
                        sql,
                        "LatestByAllFiltered",
                        "SelectedRecord > LatestByAllFiltered > Row backward scan > Frame backward scan on: lp_latest",
                        """
                                id
                                1
                                2
                                """
                );
            } finally {
                sqlExecutionContext.setParquetRowGroupPruningEnabled(wasPruningEnabled);
            }
        });
    }

    private void assertCoveringBackup(String sql, String shape, String expected) throws Exception {
        assertPlanShape(sql, shape);
        assertQuery(sql)
                .noLeakCheck()
                .withPlanContaining("backup: true")
                .inferTimestamp()
                .noRandomAccess()
                .skipRandomAccessProbe()
                .sizeMayVary()
                .returns(expected);
    }

    private void assertLatest(String sql, String specialization, String shape, String expected) throws Exception {
        assertPlanShape(sql, shape);
        assertQuery(sql)
                .noLeakCheck()
                .withPlanContaining(specialization)
                .inferTimestamp()
                .inferRandomAccess()
                .sizeMayVary()
                .returns(expected);
    }

    private void assertPlanShape(String sql, String shape) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            TestUtils.assertEquals(shape, PlanShape.of(factory, sqlExecutionContext));
        }
    }

    private void assertRows(String sql, String expected) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            assertRowsOnly(factory, expected);
        }
    }

    private void createRows(boolean isNanos) throws SqlException {
        execute("CREATE TABLE lp_latest(id INT,s SYMBOL INDEX,u SYMBOL,k INT,v VARCHAR,ts "
                + (isNanos ? "TIMESTAMP_NS" : "TIMESTAMP") + ") TIMESTAMP(ts) PARTITION BY DAY");
        final String tick = isNanos ? "2020-01-02T00:00:00.000000001Z" : "2020-01-02T00:00:00.000001Z";
        execute("INSERT INTO lp_latest VALUES"
                + "(1,'A','X',1,'alpha','2020-01-01T23:59:58.000000Z'),"
                + "(2,'B','Y',2,'beta','2020-01-01T23:59:59.000000Z'),"
                + "(3,null,null,null,null,'2020-01-02T00:00:00.000000Z'),"
                + "(4,'A','X',1,'alpha','" + tick + "'),"
                + "(5,'A','X',1,'alpha','" + tick + "'),"
                + "(6,'B','Y',2,'beta','2020-01-02T00:00:01.000000Z')");
    }
}
