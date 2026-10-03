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

public class SymbolInIndexTest extends AbstractCairoTest {
    @Test
    public void testLiteralValuesDeduplicateBeforeFactorySelection() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            assertIndex(
                    "SELECT id FROM lp_in WHERE s IN ('A')",
                    "DeferredSingleSymbolFilterPageFrame",
                    "SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Frame forward scan on: lp_in",
                    """
                            id
                            1
                            4
                            5
                            """
            );
            assertIndex(
                    "SELECT id FROM lp_in WHERE s IN ('A','A')",
                    "DeferredSingleSymbolFilterPageFrame",
                    "SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Frame forward scan on: lp_in",
                    """
                            id
                            1
                            4
                            5
                            """
            );
            assertIndex(
                    "SELECT id FROM lp_in WHERE s IN (NULL,NULL)",
                    "DeferredSingleSymbolFilterPageFrame",
                    "SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Frame forward scan on: lp_in",
                    """
                            id
                            3
                            """
            );
            final ObjList<String> values = new ObjList<>("'A','B'", "'B','A','B'", "NULL,'A',NULL", "'','''','中'", "'missing','A'", "'A'::CHAR,NULL::CHAR");
            final ObjList<String> valuesRows = new ObjList<>("id\n1\n2\n4\n5\n8\n", "id\n1\n2\n4\n5\n8\n", "id\n1\n3\n4\n5\n", "id\n6\n7\n9\n", "id\n1\n4\n5\n", "id\n1\n3\n4\n5\n");
            final String[] shapes = {"SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in", "SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in", "SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in", "SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in", "SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s deferred: true > Index forward scan on: s > Frame forward scan on: lp_in", "SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in"};
            for (int i = 0; i < values.size(); i++) {
                assertIndex(
                        "SELECT id FROM lp_in WHERE s IN (" + values.getQuick(i) + ')',
                        "FilterOnValues",
                        shapes[i],
                        valuesRows.getQuick(i)
                );
            }
            assertRows("SELECT id FROM lp_in WHERE s IN ('B','A','B') ORDER BY ts", "id\n1\n2\n4\n5\n8\n");
            assertRows("SELECT id FROM lp_in WHERE s IN (NULL,'A',NULL) ORDER BY ts", "id\n1\n3\n4\n5\n");
            assertRows("SELECT id FROM lp_in WHERE s IN ('','''','中') ORDER BY ts", "id\n6\n7\n9\n");
        });
    }

    @Test
    public void testDeferredKeysDeduplicateAndRebindAfterCompilerClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            bindVariableService.setStr(0, "A");
            bindVariableService.setStr(1, "B");
            final String sql = "SELECT id FROM lp_in WHERE s IN ($1,$2,$1) ORDER BY ts";
            assertIndex(
                    sql,
                    "FilterOnValues",
                    "SelectedRecord > SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s deferred: true > Index forward scan on: s deferred: true > Frame forward scan on: lp_in",
                    """
                            id
                            1
                            2
                            4
                            5
                            8
                            """
            );
            try (RecordCursorFactory factory = compileRetained(sql)) {
                assertRows(factory, "id\n1\n2\n4\n5\n8\n");
                bindVariableService.setStr(1, "A");
                assertRows(factory, "id\n1\n4\n5\n");
                bindVariableService.setStr(0, null);
                bindVariableService.setStr(1, null);
                assertRows(factory, "id\n3\n");
                bindVariableService.setStr(0, "later");
                bindVariableService.setStr(1, "missing");
                assertRows(factory, "id\n");
                execute("INSERT INTO lp_in VALUES(101,11,'later','X','eleven','2020-01-03')");
                assertRows(factory, "id\n11\n");
                bindVariableService.setStr(1, "A");
                assertRows(factory, "id\n1\n4\n5\n11\n");
            }
        });
    }

    @Test
    public void testLiteralParameterSpellingKeepsDistinctIdentity() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            bindVariableService.setStr(0, "A");
            try (RecordCursorFactory factory = compileRetained("SELECT id FROM lp_in WHERE s IN ('$1',$1) ORDER BY ts")) {
                assertRows(factory, "id\n1\n4\n5\n10\n");
                bindVariableService.setStr(0, "B");
                assertRows(factory, "id\n2\n8\n10\n");
                bindVariableService.setStr(0, "$1");
                assertRows(factory, "id\n10\n");
            }
        });
    }

    @Test
    public void testOrderAdviceAcrossPartitionsAndIntervals() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final ObjList<String> orders = new ObjList<>("ts", "ts DESC", "s", "s DESC", "s,ts", "s DESC,ts DESC", "s,ts DESC", "id DESC");
            final ObjList<String> ordersRows = new ObjList<>("id\n1\n2\n4\n5\n8\n", "id\n8\n5\n4\n2\n1\n", "id\n1\n4\n5\n2\n8\n", "id\n2\n8\n1\n4\n5\n", "id\n1\n4\n5\n2\n8\n", "id\n8\n2\n5\n4\n1\n", "id\n5\n4\n1\n8\n2\n", "id\n8\n5\n4\n2\n1\n");
            final ObjList<String> ordersRows2 = new ObjList<>("id\n4\n5\n8\n", "id\n8\n5\n4\n", "id\n4\n5\n8\n", "id\n8\n4\n5\n", "id\n4\n5\n8\n", "id\n8\n5\n4\n", "id\n5\n4\n8\n", "id\n8\n5\n4\n");
            final String[] shapes3 = {"SelectedRecord > SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s > Index forward scan on: s > Interval forward scan on: lp_in", "SelectedRecord > Encode sort light > SelectedRecord > FilterOnValues symbolOrder: desc > Cursor-order scan > Index forward scan on: s > Index forward scan on: s > Interval backward scan on: lp_in", "SelectedRecord > SelectedRecord > FilterOnValues symbolOrder: asc > Cursor-order scan > Index forward scan on: s > Index forward scan on: s > Interval forward scan on: lp_in", "SelectedRecord > SelectedRecord > FilterOnValues symbolOrder: desc > Cursor-order scan > Index forward scan on: s > Index forward scan on: s > Interval forward scan on: lp_in", "SelectedRecord > FilterOnValues symbolOrder: asc > Cursor-order scan > Index forward scan on: s > Index forward scan on: s > Interval forward scan on: lp_in", "SelectedRecord > FilterOnValues symbolOrder: desc > Cursor-order scan > Index backward scan on: s > Index backward scan on: s > Interval forward scan on: lp_in", "SelectedRecord > FilterOnValues symbolOrder: asc > Cursor-order scan > Index backward scan on: s > Index backward scan on: s > Interval forward scan on: lp_in", "Encode sort light > SelectedRecord > FilterOnValues symbolOrder: desc > Cursor-order scan > Index forward scan on: s > Index forward scan on: s > Interval forward scan on: lp_in"};
            final String[] shapes2 = {"SelectedRecord > SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in", "SelectedRecord > Encode sort light > SelectedRecord > FilterOnValues symbolOrder: desc > Cursor-order scan > Index forward scan on: s > Index forward scan on: s > Frame backward scan on: lp_in", "SelectedRecord > Encode sort light > FilterOnValues symbolOrder: desc > Cursor-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in", "SelectedRecord > Encode sort light > FilterOnValues symbolOrder: desc > Cursor-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in", "SelectedRecord > Encode sort light > FilterOnValues symbolOrder: desc > Cursor-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in", "SelectedRecord > Encode sort light > FilterOnValues symbolOrder: desc > Cursor-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in", "SelectedRecord > Encode sort light > FilterOnValues symbolOrder: desc > Cursor-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in", "Encode sort light > SelectedRecord > FilterOnValues symbolOrder: desc > Cursor-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in"};
            for (int i = 0; i < orders.size(); i++) {
                assertIndex(
                        "SELECT id FROM lp_in WHERE s IN ('A','B') ORDER BY " + orders.getQuick(i),
                        "FilterOnValues",
                        shapes2[i],
                        ordersRows.getQuick(i)
                );
                assertIndex(
                        "SELECT id FROM lp_in WHERE s IN ('A','B') AND ts>='2020-01-02T00:00:00.000000Z'"
                                + " AND ts<'2020-01-03T00:00:00.000000Z' ORDER BY " + orders.getQuick(i),
                        "FilterOnValues",
                        shapes3[i],
                        ordersRows2.getQuick(i)
                );
            }
            assertRows("SELECT id FROM lp_in WHERE s IN ('A','B') ORDER BY ts DESC LIMIT 3", "id\n8\n5\n4\n");
            assertRows("SELECT id FROM lp_in WHERE s IN ('A','B') ORDER BY s DESC,ts DESC LIMIT 3", "id\n8\n2\n5\n");
            bindVariableService.setTimestamp(0, 1_577_923_200_000_000L);
            assertIndex(
                    "SELECT id FROM lp_in WHERE s IN ('A','B') AND ts>=$1 ORDER BY ts DESC",
                    "FilterOnValues",
                    "SelectedRecord > Encode sort light > SelectedRecord > FilterOnValues symbolOrder: desc > Cursor-order scan > Index forward scan on: s > Index forward scan on: s > Interval backward scan on: lp_in",
                    """
                            id
                            8
                            5
                            4
                            """
            );
        });
    }

    @Test
    public void testResidualsAndLimitBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final ObjList<String> tails = new ObjList<>("", " LIMIT 3", " LIMIT -3", " LIMIT 1,4", " ORDER BY ts DESC LIMIT 2", " ORDER BY id DESC LIMIT -2");
            final ObjList<String> tailsRows = new ObjList<>("id\n1\n2\n4\n5\n8\n", "id\n1\n2\n4\n", "id\n4\n5\n8\n", "id\n2\n4\n5\n", "id\n8\n5\n", "id\n2\n1\n");
            final String[] shapes4 = {"SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in", "Limit value: 3 > SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in", "Limit value: -3 > SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in", "Limit left: 1 right: 4 > SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in", "SelectedRecord > Encode sort light lo: 2 > SelectedRecord > FilterOnValues symbolOrder: desc > Cursor-order scan > Index forward scan on: s > Index forward scan on: s > Frame backward scan on: lp_in", "Encode sort light lo: -2 > SelectedRecord > FilterOnValues symbolOrder: desc > Cursor-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in"};
            for (int i = 0; i < tails.size(); i++) {
                assertIndex(
                        "SELECT id FROM lp_in WHERE s IN ('A','B') AND length(v)>0" + tails.getQuick(i),
                        "FilterOnValues",
                        shapes4[i],
                        tailsRows.getQuick(i)
                );
            }
            bindVariableService.setBoolean(0, true);
            try (RecordCursorFactory factory = compileRetained("SELECT id FROM lp_in WHERE s IN ('A','B') AND $1 ORDER BY ts")) {
                assertRows(factory, "id\n1\n2\n4\n5\n8\n");
                bindVariableService.setBoolean(0, false);
                assertRows(factory, "id\n");
            }
            assertIndex(
                    "SELECT id FROM (SELECT id,s FROM lp_in LIMIT 3) WHERE s IN ('A','B')",
                    "Limit",
                    "SelectedRecord > Filter filter: s in [A,B] > Limit value: 3 > PageFrame > Row forward scan > Frame forward scan on: lp_in",
                    """
                            id
                            1
                            2
                            """
            );
            assertRows("SELECT id FROM (SELECT id,s FROM lp_in WHERE s IN ('A','B') LIMIT 3) WHERE id>2", "id\n4\n");
        });
    }

    @Test
    public void testNoTimestampAndDeletedColumnLayouts() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            execute("CREATE TABLE lp_in_unordered(unused INT,id INT,s SYMBOL INDEX,v VARCHAR)");
            execute("INSERT INTO lp_in_unordered SELECT unused,id,s,v FROM lp_in");
            execute("ALTER TABLE lp_in_unordered DROP COLUMN unused");
            assertIndex(
                    "SELECT id FROM lp_in_unordered WHERE s IN ('A','B')",
                    "FilterOnValues",
                    "SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in_unordered",
                    """
                            id
                            1
                            2
                            4
                            5
                            8
                            """
            );
            assertIndexExactPlan(
                    "SELECT s AS key,id FROM lp_in_unordered WHERE s IN ('A','B') AND length(v)>0 ORDER BY id",
                    "FilterOnValues",
                    """
                            SelectedRecord
                                Encode sort light
                                  keys: [id]
                                    SelectedRecord
                                        FilterOnValues symbolOrder: desc
                                            Cursor-order scan
                                                Index forward scan on: s
                                                  filter: s=1 and 0<length(v)
                                                Index forward scan on: s
                                                  filter: s=2 and 0<length(v)
                                            Frame forward scan on: lp_in_unordered
                            """,
                    """
                            key	id
                            A	1
                            B	2
                            A	4
                            A	5
                            B	8
                            """
            );
            assertRows("SELECT id FROM lp_in_unordered WHERE s IN ('A','B') AND id>2 ORDER BY id", "id\n4\n5\n8\n");
        });
    }

    @Test
    public void testCoveringResidualLimitsKeepMultiKeyBackwardScanSerial() throws Exception {
        assertMemoryLeak(() -> {
            createRows(true);
            final boolean wasParallel = sqlExecutionContext.isParallelFilterEnabled();
            final int previousMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setParallelFilterEnabled(true);
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            try {
                final String sql = "SELECT id FROM lp_in WHERE s IN ('A','B') AND length(v)>0";
                assertIndex(
                        sql,
                        "Async Filter",
                        "SelectedRecord > Async Filter workers: 1 > CoveringIndex on: s with: id, v",
                        """
                                id
                                1
                                2
                                4
                                5
                                8
                                """
                );
                assertIndex(
                        sql + " LIMIT 3",
                        "Async Filter",
                        "SelectedRecord > Async Filter workers: 1 > CoveringIndex on: s with: id, v",
                        """
                                id
                                1
                                2
                                4
                                """
                );
                assertIndex(
                        sql + " LIMIT -3",
                        "Filter filter:",
                        "Limit value: -3 > SelectedRecord > Filter filter: 0<length(v) > CoveringIndex on: s with: id, v",
                        """
                                id
                                4
                                5
                                8
                                """
                );
                assertIndexExactPlan(
                        sql + " ORDER BY ts LIMIT 3",
                        "Async Filter",
                        """
                                SelectedRecord
                                    SelectedRecord
                                        Async Filter workers: 1
                                          limit: 3
                                          filter: 0<length(v)
                                            CoveringIndex on: s with: id, v, ts
                                              filter: s IN ['A','B']
                                """,
                        """
                                id
                                1
                                2
                                4
                                """
                );
                assertIndexExactPlan(
                        sql + " ORDER BY ts DESC LIMIT 3",
                        "Async Top K",
                        """
                                SelectedRecord
                                    SelectedRecord
                                        Async Top K lo: 3 workers: 1
                                          filter: 0<length(v)
                                          keys: [ts desc]
                                            CoveringIndex on: s with: id, v, ts
                                              filter: s IN ['A','B']
                                """,
                        """
                                id
                                8
                                5
                                4
                                """
                );
                assertIndex(
                        sql + " ORDER BY id DESC LIMIT 3",
                        "CoveringIndex",
                        "SelectedRecord > Async Top K lo: 3 workers: 1 > CoveringIndex on: s with: id, v",
                        """
                                id
                                8
                                5
                                4
                                """
                );
                assertRows(sql + " LIMIT 3", "id\n1\n2\n4\n");
                assertRows(sql + " LIMIT -3", "id\n4\n5\n8\n");
                bindVariableService.setLong(0, 3);
                assertIndex(
                        sql + " LIMIT $1",
                        "Filter filter:",
                        "Limit value: $0::long > SelectedRecord > Filter filter: 0<length(v) > CoveringIndex on: s with: id, v",
                        """
                                id
                                1
                                2
                                4
                                """
                );
                try (RecordCursorFactory factory = compileRetained(sql + " LIMIT $1")) {
                    assertRows(factory, "id\n1\n2\n4\n");
                    bindVariableService.setLong(0, -3);
                    assertRows(factory, "id\n4\n5\n8\n");
                }
                sqlExecutionContext.setParallelFilterEnabled(false);
                assertIndex(
                        sql,
                        "Filter filter:",
                        "SelectedRecord > Filter filter: 0<length(v) > CoveringIndex on: s with: id, v",
                        """
                                id
                                1
                                2
                                4
                                5
                                8
                                """
                );
            } finally {
                sqlExecutionContext.setParallelFilterEnabled(wasParallel);
                sqlExecutionContext.setJitMode(previousMode);
            }
        });
    }

    @Test
    public void testCoveringKeyOrderAndNullColumnTopBackup() throws Exception {
        assertMemoryLeak(() -> {
            createRows(true);
            assertIndex(
                    "SELECT id FROM lp_in WHERE s IN ('A','B')",
                    "CoveringIndex",
                    "SelectedRecord > CoveringIndex on: s with: id",
                    """
                            id
                            1
                            2
                            4
                            5
                            8
                            """
            );
            assertIndex(
                    "SELECT id FROM lp_in WHERE s IN ('A','B') ORDER BY s,ts DESC",
                    "CoveringIndex",
                    "SelectedRecord > Encode sort > CoveringIndex on: s with: id, ts",
                    """
                            id
                            5
                            4
                            1
                            8
                            2
                            """
            );
            assertIndex(
                    "SELECT id FROM lp_in WHERE s IN ('A','B') AND ts>='2020-01-02T00:00:00.000000Z'"
                            + " AND ts<'2020-01-03T00:00:00.000000Z' ORDER BY s,ts DESC",
                    "FilterOnValues",
                    "SelectedRecord > FilterOnValues symbolOrder: asc > Cursor-order scan > Index backward scan on: s > Index backward scan on: s > Interval forward scan on: lp_in",
                    """
                            id
                            5
                            4
                            8
                            """
            );
            execute("CREATE TABLE lp_in_top(id INT,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO lp_in_top VALUES(1,'2020-01-01T00:00:01'),(2,'2020-01-01T00:00:02')");
            execute("ALTER TABLE lp_in_top ADD COLUMN s SYMBOL");
            execute("INSERT INTO lp_in_top VALUES(3,'2020-01-01T00:00:03','A'),(4,'2020-01-02T00:00:01','B')");
            execute("ALTER TABLE lp_in_top ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE(id,ts)");
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertCoveringBackup(
                    "SELECT id FROM lp_in_top WHERE s IN (NULL,'A') AND id>1",
                    "SelectedRecord > Filter filter: 1<id > CoveringIndex backup: true on: s with: id",
                    """
                            id
                            2
                            3
                            """
            );
            bindVariableService.setStr(0, "A");
            final String sql = "SELECT id FROM lp_in_top WHERE s IN ($1,'B') AND id>1 LIMIT 3";
            assertCoveringBackup(
                    sql,
                    "Limit value: 3 > SelectedRecord > Filter filter: 1<id > CoveringIndex backup: true on: s with: id",
                    """
                            id
                            3
                            4
                            """
            );
            try (RecordCursorFactory factory = compileRetained(sql)) {
                assertRows(factory, "id\n3\n4\n");
                bindVariableService.setStr(0, null);
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                        .skipRandomAccessProbe().sizeMayVary().returns("id\n2\n4\n");
                bindVariableService.setStr(0, "B");
                assertRows(factory, "id\n4\n");
                bindVariableService.setStr(0, "missing");
                assertRows(factory, "id\n4\n");
            }
        });
    }

    @Test
    public void testSelectedColumnKeepsIndexHeuristics() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            assertIndex(
                    "SELECT id FROM lp_in WHERE s IN ('A','B') AND t='Y'",
                    "on: s",
                    "SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in",
                    """
                            id
                            2
                            4
                            8
                            """
            );
            assertIndex(
                    "SELECT id FROM lp_in WHERE t='Y' AND s IN ('A','B')",
                    "on: s",
                    "SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in",
                    """
                            id
                            2
                            4
                            8
                            """
            );
            assertIndex(
                    "SELECT id FROM lp_in WHERE t IN ('X','Y') AND s IN ('A','B')",
                    "on: s",
                    "SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in",
                    """
                            id
                            1
                            2
                            4
                            8
                            """
            );
            execute("CREATE TABLE lp_in_capacity(id INT,s SYMBOL CAPACITY 16 INDEX,t SYMBOL CAPACITY 128 INDEX)");
            execute("INSERT INTO lp_in_capacity VALUES(1,'A','X'),(2,'B','Y'),(3,'A','Y')");
            assertIndex(
                    "SELECT id FROM lp_in_capacity WHERE s IN ('A','B') AND t IN ('X','Y')",
                    "on: t",
                    "SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: t > Index forward scan on: t > Frame forward scan on: lp_in_capacity",
                    """
                            id
                            1
                            2
                            3
                            """
            );
            execute("CREATE TABLE lp_in_tie(id INT,s SYMBOL CAPACITY 128 INDEX,t SYMBOL CAPACITY 128 INDEX)");
            execute("INSERT INTO lp_in_tie VALUES(1,'A','X'),(2,'B','Y'),(3,'A','Y')");
            assertIndex(
                    "SELECT id FROM lp_in_tie WHERE s IN ('A','B') AND t IN ('X','Y')",
                    "on: t",
                    "SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: t > Index forward scan on: t > Frame forward scan on: lp_in_tie",
                    """
                            id
                            1
                            2
                            3
                            """
            );
            assertIndex(
                    "SELECT id FROM lp_in_tie WHERE t IN ('X','Y') AND s IN ('A','B')",
                    "on: s",
                    "SelectedRecord > FilterOnValues > Table-order scan > Index forward scan on: s > Index forward scan on: s > Frame forward scan on: lp_in_tie",
                    """
                            id
                            1
                            2
                            3
                            """
            );
        });
    }

    @Test
    public void testIntersectionsAndExclusions() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final ObjList<String> predicates = new ObjList<>("s IN ('A','B') AND s='A'", "s='A' AND s IN ('A','B')",
                    "s IN ('A','B') AND s IN ('B','中')", "s IN ('A','B') AND s IN ('中')", "s NOT IN ('A','B')",
                    "s!='A'", "s!='A' AND s NOT IN ('B')", "s IN ('A','B') AND s!='A'", "s IN ('A','B') AND s NOT IN ('A','B')",
                    "s::SYMBOL IN ('A','B')");
            final ObjList<String> predicatesRows = new ObjList<>("id\n1\n4\n5\n", "id\n1\n4\n5\n", "id\n2\n8\n", "id\n", "id\n3\n6\n7\n9\n10\n", "id\n2\n3\n6\n7\n8\n9\n10\n", "id\n3\n6\n7\n9\n10\n", "id\n2\n8\n", "id\n", "id\n1\n2\n4\n5\n8\n");
            final String[] shapes5 = {"SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Frame forward scan on: lp_in", "SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Frame forward scan on: lp_in", "SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Frame forward scan on: lp_in", "SelectedRecord > Empty table", "SelectedRecord > FilterOnExcludedValues > Table-order scan > Frame forward scan on: lp_in", "SelectedRecord > FilterOnExcludedValues > Table-order scan > Frame forward scan on: lp_in", "SelectedRecord > FilterOnExcludedValues > Table-order scan > Frame forward scan on: lp_in", "SelectedRecord > DeferredSingleSymbolFilterPageFrame > Index forward scan on: s > Frame forward scan on: lp_in", "SelectedRecord > Empty table", "SelectedRecord > Async Filter workers: 1 > PageFrame > Row forward scan > Frame forward scan on: lp_in"};
            for (int i = 0; i < predicates.size(); i++) {
                assertIndex("SELECT id FROM lp_in WHERE " + predicates.getQuick(i), "", shapes5[i], predicatesRows.getQuick(i));
            }
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
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
    }

    private RecordCursorFactory compile(String sql) throws SqlException {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            return compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
        }
    }

    private RecordCursorFactory compileRetained(String sql) throws SqlException {
        final RecordCursorFactory retained;
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
            try {
                try (RecordCursorFactory ignored = compiler.compile("SELECT 1", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertNotNull(ignored);
                }
                compiler.clear();
            } catch (Throwable th) {
                retained.close();
                throw th;
            }
        }
        return retained;
    }

    private void createRows(boolean isCovering) throws SqlException {
        execute("CREATE TABLE lp_in(unused INT,id INT,s SYMBOL" + (isCovering ? "" : " INDEX")
                + ",t SYMBOL INDEX,v VARCHAR,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("""
                INSERT INTO lp_in VALUES
                (91,1,'A','X','one','2020-01-01T23:59:58'),
                (92,2,'B','Y','two','2020-01-01T23:59:59'),
                (93,3,NULL,'X','three','2020-01-02T00:00:00'),
                (94,4,'A','Y','four','2020-01-02T00:00:01'),
                (95,5,'A','Z','five','2020-01-02T00:00:02'),
                (96,6,'''','Z','six','2020-01-02T00:00:03'),
                (97,7,'','X','seven','2020-01-02T00:00:04'),
                (98,8,'B','Y','eight','2020-01-02T00:00:05'),
                (99,9,'中','X','nine','2020-01-02T00:00:06'),
                (100,10,'$1','Z','ten','2020-01-02T00:00:07')
                """);
        if (isCovering) {
            execute("ALTER TABLE lp_in ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE(id,v,ts)");
            engine.releaseAllWriters();
            engine.releaseAllReaders();
        }
    }
}
