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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.jit.JitUtil;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class SqlLogicalMultiJoinTest extends AbstractCairoTest {
    @Test
    public void testDerivedPredicatesGroupingOrderingAndLimits() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRows("SELECT a.id aid,b.id bid,c.id cid FROM (SELECT id,k,v+1 value FROM lp_multi_a) a "
                    + "JOIN lp_multi_b b ON a.k=b.k JOIN lp_multi_c c ON b.k=c.k "
                    + "WHERE a.value<c.v ORDER BY cid DESC,aid,bid LIMIT 2", false,
                    """
                    aid	bid	cid
                    2	12	22
                    1	11	21
                    """,
                    """
                    Limit value: 2
                        Encode sort
                          keys: [cid desc, aid, bid]
                            SelectedRecord
                                Filter filter: a.value<c.v
                                    Hash Join Light
                                      condition: c.k=b.k
                                        Hash Join Light
                                          condition: b.k=a.k
                                            VirtualRecord
                                              functions: [id,k,v+1]
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_multi_a
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_multi_b
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_c
                    """);
            assertRows("SELECT b.k,count() n,sum(a.v) total FROM lp_multi_a a "
                    + "JOIN lp_multi_b b ON a.k=b.k JOIN lp_multi_c c ON b.k=c.k GROUP BY b.k ORDER BY b.k", false,
                    """
                    k	n	total
                    null	1	null
                    1	1	10
                    2	1	20
                    """,
                    """
                    Encode sort light
                      keys: [k]
                        GroupBy vectorized: false
                          keys: [k]
                          values: [count(*),sum(a.v)]
                            Hash Join Light
                              condition: c.k=b.k
                                Hash Join Light
                                  condition: b.k=a.k
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_multi_a
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_b
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_multi_c
                    """);
            assertRows("SELECT cid FROM (SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a "
                    + "JOIN lp_multi_b b ON a.k=b.k JOIN lp_multi_c c ON b.k=c.k) q WHERE aid>1 AND bid>0 ORDER BY cid", false,
                    """
                    cid
                    22
                    23
                    """,
                    """
                    Encode sort
                      keys: [cid]
                        SelectedRecord
                            Hash Join Light
                              condition: c.k=b.k
                                Hash Join Light
                                  condition: b.k=a.k
                                    Async JIT Filter workers: 1
                                      filter: 1<id
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                    Hash
                                        Async JIT Filter workers: 1
                                          filter: 0<id
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_b
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_multi_c
                    """);
            assertRows("SELECT a.id aid,b.id bid,c.id cid FROM (SELECT id,k FROM lp_multi_a ORDER BY id DESC LIMIT 2) a "
                    + "JOIN lp_multi_b b ON a.k=b.k JOIN lp_multi_c c ON b.k=c.k ORDER BY aid,bid,cid", false,
                    """
                    aid	bid	cid
                    2	12	22
                    3	13	23
                    """,
                    """
                    Encode sort
                      keys: [aid, bid, cid]
                        SelectedRecord
                            Hash Join Light
                              condition: c.k=b.k
                                Hash Join Light
                                  condition: b.k=a.k
                                    Async Top K lo: 2 workers: 1
                                      filter: null
                                      keys: [id desc]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_b
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_multi_c
                    """);
        });
    }

    @Test
    public void testEmittedEqualityCreatesMissingDependency() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_emit_a(x INT,y INT)");
            execute("CREATE TABLE lp_emit_b(x INT,y INT)");
            execute("CREATE TABLE lp_emit_c(x INT,y INT)");
            execute("INSERT INTO lp_emit_a VALUES(1,10)");
            execute("INSERT INTO lp_emit_b VALUES(1,20),(2,21)");
            execute("INSERT INTO lp_emit_c VALUES(1,30)");
            // The emitted a.x=b.x context has no linked dependency. The planner
            // must execute both original equalities.
            assertRows("SELECT a.y av,b.y bv,c.y cv FROM lp_emit_a a CROSS JOIN lp_emit_b b "
                    + "JOIN lp_emit_c c ON a.x=c.x AND b.x=c.x ORDER BY av,bv,cv", false, "av\tbv\tcv\n10\t20\t30\n");
        });
    }

    @Test
    public void testFactoriesAndPlanSurviveCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a JOIN lp_multi_b b ON a.k=b.k "
                    + "JOIN lp_multi_c c ON b.k=c.k WHERE b.v IN (11,21) AND a.id>0 ORDER BY aid,bid,cid";
            RecordCursorFactory retained = null;
            final String expected = """
                    aid	bid	cid
                    1	11	21
                    2	12	22
                    """;
            final String expectedPlan;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
                }
                try {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    expectedPlan = plan(retained);
                    try (RecordCursorFactory other = compiler.compile("SELECT count() FROM lp_multi_a", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(other);
                    }
                    compiler.clear();
                } catch (Throwable th) {
                    if (retained != null) {
                        retained.close();
                    }
                    throw th;
                }
            }
            try (RecordCursorFactory factory = retained) {
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
                TestUtils.assertEquals(expectedPlan, plan(factory));
            }
        });
    }

    @Test
    public void testGeneratedEqualityGraphsMatchTupleOracle() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0; i < 5; i++) {
                execute("CREATE TABLE lp_graph_" + i + "(id INT,x INT,y INT)");
                execute("INSERT INTO lp_graph_" + i + " VALUES(1,0,0),(2,0,1),(3,1,0)");
            }
            final Random random = new Random(81347);
            final IntList leftColumns = new IntList();
            final IntList rightColumns = new IntList();
            final int[] values = {0, 0, 0, 1, 1, 0};
            final int[] rowIndexes = new int[5];
            for (int graph = 0; graph < 16; graph++) {
                final int sourceCount = 3 + graph % 3;
                final int equalityCount = 2 + random.nextInt(7);
                leftColumns.clear();
                rightColumns.clear();
                final StringSink sql = new StringSink();
                final StringSink expected = new StringSink();
                sql.put("SELECT ");
                for (int i = 0; i < sourceCount; i++) {
                    if (i > 0) {
                        sql.put(',');
                        expected.put('\t');
                    }
                    sql.put('t').put(i).put(".id i").put(i);
                    expected.put('i').put(i);
                }
                expected.put('\n');
                sql.put(" FROM ");
                int tupleCount = 1;
                for (int i = 0; i < sourceCount; i++) {
                    if (i > 0) {
                        sql.put(" CROSS JOIN ");
                    }
                    sql.put("lp_graph_").put(i).put(" t").put(i);
                    tupleCount *= 3;
                }
                sql.put(" WHERE ");
                for (int i = 0; i < equalityCount; i++) {
                    final int left = random.nextInt(2 * sourceCount);
                    final int right = random.nextInt(2 * sourceCount);
                    leftColumns.add(left);
                    rightColumns.add(right);
                    if (i > 0) {
                        sql.put(" AND ");
                    }
                    sql.put('t').put(left / 2).put(left % 2 == 0 ? ".x=" : ".y=")
                            .put('t').put(right / 2).put(right % 2 == 0 ? ".x" : ".y");
                }
                sql.put(" ORDER BY ");
                for (int i = 0; i < sourceCount; i++) {
                    if (i > 0) {
                        sql.put(',');
                    }
                    sql.put('i').put(i);
                }
                // Enumerate original SQL tuples, not optimizer contexts or emitted
                // keys. Every original equality must hold independently.
                for (int tuple = 0; tuple < tupleCount; tuple++) {
                    int remaining = tuple;
                    for (int i = sourceCount - 1; i >= 0; i--) {
                        rowIndexes[i] = remaining % 3;
                        remaining /= 3;
                    }
                    boolean isMatch = true;
                    for (int i = 0; i < equalityCount; i++) {
                        final int left = leftColumns.getQuick(i);
                        final int right = rightColumns.getQuick(i);
                        if (values[rowIndexes[left / 2] * 2 + left % 2] != values[rowIndexes[right / 2] * 2 + right % 2]) {
                            isMatch = false;
                            break;
                        }
                    }
                    if (isMatch) {
                        for (int i = 0; i < sourceCount; i++) {
                            if (i > 0) {
                                expected.put('\t');
                            }
                            expected.put(rowIndexes[i] + 1);
                        }
                        expected.put('\n');
                    }
                }
                assertRows(sql.toString(), false, expected.toString());
            }
        });
    }

    @Test
    public void testLexicalWildcardOrderSurvivesPhysicalSourceReordering() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                // The disconnected a source executes after the b/c hash join,
                // while SELECT * keeps a,b,c column order and names.
                assertRows("SELECT * FROM lp_multi_a a CROSS JOIN lp_multi_b b JOIN lp_multi_c c ON b.k=c.k",
                        false,
                        """
                        id	k	v	s	ts	id1	k1	v1	s1	ts1	id2	k2	v2	s2	ts2
                        1	1	10	one	2020-01-01T00:00:00.000000Z	11	1	11	one	2020-01-01T00:00:00.000000Z	21	1	12	one	2020-01-01T00:00:00.000000Z
                        2	2	20	two	2020-01-02T00:00:00.000000Z	11	1	11	one	2020-01-01T00:00:00.000000Z	21	1	12	one	2020-01-01T00:00:00.000000Z
                        3	null	null		2020-01-03T00:00:00.000000Z	11	1	11	one	2020-01-01T00:00:00.000000Z	21	1	12	one	2020-01-01T00:00:00.000000Z
                        1	1	10	one	2020-01-01T00:00:00.000000Z	12	2	21	two	2020-01-02T00:00:00.000000Z	22	2	22	two	2020-01-02T00:00:00.000000Z
                        2	2	20	two	2020-01-02T00:00:00.000000Z	12	2	21	two	2020-01-02T00:00:00.000000Z	22	2	22	two	2020-01-02T00:00:00.000000Z
                        3	null	null		2020-01-03T00:00:00.000000Z	12	2	21	two	2020-01-02T00:00:00.000000Z	22	2	22	two	2020-01-02T00:00:00.000000Z
                        1	1	10	one	2020-01-01T00:00:00.000000Z	13	null	null		2020-01-03T00:00:00.000000Z	23	null	null		2020-01-03T00:00:00.000000Z
                        2	2	20	two	2020-01-02T00:00:00.000000Z	13	null	null		2020-01-03T00:00:00.000000Z	23	null	null		2020-01-03T00:00:00.000000Z
                        3	null	null		2020-01-03T00:00:00.000000Z	13	null	null		2020-01-03T00:00:00.000000Z	23	null	null		2020-01-03T00:00:00.000000Z
                        """,
                        """
                        SelectedRecord
                            Cross Join
                                Hash Join Light
                                  condition: c.k=b.k
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_multi_b
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_c
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: lp_multi_a
                        """);
                assertRows("SELECT * FROM lp_multi_a a CROSS JOIN lp_multi_b b JOIN lp_multi_c c ON b.k=c.k "
                        + "ORDER BY 1,6,11", false,
                        """
                        id	k	v	s	ts	id1	k1	v1	s1	ts1	id2	k2	v2	s2	ts2
                        1	1	10	one	2020-01-01T00:00:00.000000Z	11	1	11	one	2020-01-01T00:00:00.000000Z	21	1	12	one	2020-01-01T00:00:00.000000Z
                        1	1	10	one	2020-01-01T00:00:00.000000Z	12	2	21	two	2020-01-02T00:00:00.000000Z	22	2	22	two	2020-01-02T00:00:00.000000Z
                        1	1	10	one	2020-01-01T00:00:00.000000Z	13	null	null		2020-01-03T00:00:00.000000Z	23	null	null		2020-01-03T00:00:00.000000Z
                        2	2	20	two	2020-01-02T00:00:00.000000Z	11	1	11	one	2020-01-01T00:00:00.000000Z	21	1	12	one	2020-01-01T00:00:00.000000Z
                        2	2	20	two	2020-01-02T00:00:00.000000Z	12	2	21	two	2020-01-02T00:00:00.000000Z	22	2	22	two	2020-01-02T00:00:00.000000Z
                        2	2	20	two	2020-01-02T00:00:00.000000Z	13	null	null		2020-01-03T00:00:00.000000Z	23	null	null		2020-01-03T00:00:00.000000Z
                        3	null	null		2020-01-03T00:00:00.000000Z	11	1	11	one	2020-01-01T00:00:00.000000Z	21	1	12	one	2020-01-01T00:00:00.000000Z
                        3	null	null		2020-01-03T00:00:00.000000Z	12	2	21	two	2020-01-02T00:00:00.000000Z	22	2	22	two	2020-01-02T00:00:00.000000Z
                        3	null	null		2020-01-03T00:00:00.000000Z	13	null	null		2020-01-03T00:00:00.000000Z	23	null	null		2020-01-03T00:00:00.000000Z
                        """,
                        """
                        Encode sort
                          keys: [id, id1, id2]
                            SelectedRecord
                                Cross Join
                                    Hash Join Light
                                      condition: c.k=b.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_b
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_c
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_multi_a
                        """);
                assertRows("SELECT c.*,a.id aid,b.id bid FROM lp_multi_a a CROSS JOIN lp_multi_b b "
                        + "JOIN lp_multi_c c ON b.k=c.k ORDER BY aid,bid,id", false,
                        """
                        id	k	v	s	ts	aid	bid
                        21	1	12	one	2020-01-01T00:00:00.000000Z	1	11
                        22	2	22	two	2020-01-02T00:00:00.000000Z	1	12
                        23	null	null		2020-01-03T00:00:00.000000Z	1	13
                        21	1	12	one	2020-01-01T00:00:00.000000Z	2	11
                        22	2	22	two	2020-01-02T00:00:00.000000Z	2	12
                        23	null	null		2020-01-03T00:00:00.000000Z	2	13
                        21	1	12	one	2020-01-01T00:00:00.000000Z	3	11
                        22	2	22	two	2020-01-02T00:00:00.000000Z	3	12
                        23	null	null		2020-01-03T00:00:00.000000Z	3	13
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, id]
                            SelectedRecord
                                Cross Join
                                    Hash Join Light
                                      condition: c.k=b.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_b
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_c
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_multi_a
                        """);
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a CROSS JOIN lp_multi_b b "
                        + "CROSS JOIN lp_multi_c c ORDER BY aid,bid,cid", false,
                        """
                        aid	bid	cid
                        1	11	21
                        1	11	22
                        1	11	23
                        1	12	21
                        1	12	22
                        1	12	23
                        1	13	21
                        1	13	22
                        1	13	23
                        2	11	21
                        2	11	22
                        2	11	23
                        2	12	21
                        2	12	22
                        2	12	23
                        2	13	21
                        2	13	22
                        2	13	23
                        3	11	21
                        3	11	22
                        3	11	23
                        3	12	21
                        3	12	22
                        3	12	23
                        3	13	21
                        3	13	22
                        3	13	23
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid]
                            SelectedRecord
                                Cross Join
                                    Cross Join
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_b
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_multi_c
                        """);
            }
            {
                // The disconnected a source executes after the b/c hash join,
                // while SELECT * keeps a,b,c column order and names.
                assertRows("SELECT * FROM lp_multi_a a CROSS JOIN lp_multi_b b JOIN lp_multi_c c ON b.k=c.k",
                        true,
                        """
                        id	k	v	s	ts	id1	k1	v1	s1	ts1	id2	k2	v2	s2	ts2
                        1	1	10	one	2020-01-01T00:00:00.000000Z	11	1	11	one	2020-01-01T00:00:00.000000Z	21	1	12	one	2020-01-01T00:00:00.000000Z
                        2	2	20	two	2020-01-02T00:00:00.000000Z	11	1	11	one	2020-01-01T00:00:00.000000Z	21	1	12	one	2020-01-01T00:00:00.000000Z
                        3	null	null		2020-01-03T00:00:00.000000Z	11	1	11	one	2020-01-01T00:00:00.000000Z	21	1	12	one	2020-01-01T00:00:00.000000Z
                        1	1	10	one	2020-01-01T00:00:00.000000Z	12	2	21	two	2020-01-02T00:00:00.000000Z	22	2	22	two	2020-01-02T00:00:00.000000Z
                        2	2	20	two	2020-01-02T00:00:00.000000Z	12	2	21	two	2020-01-02T00:00:00.000000Z	22	2	22	two	2020-01-02T00:00:00.000000Z
                        3	null	null		2020-01-03T00:00:00.000000Z	12	2	21	two	2020-01-02T00:00:00.000000Z	22	2	22	two	2020-01-02T00:00:00.000000Z
                        1	1	10	one	2020-01-01T00:00:00.000000Z	13	null	null		2020-01-03T00:00:00.000000Z	23	null	null		2020-01-03T00:00:00.000000Z
                        2	2	20	two	2020-01-02T00:00:00.000000Z	13	null	null		2020-01-03T00:00:00.000000Z	23	null	null		2020-01-03T00:00:00.000000Z
                        3	null	null		2020-01-03T00:00:00.000000Z	13	null	null		2020-01-03T00:00:00.000000Z	23	null	null		2020-01-03T00:00:00.000000Z
                        """,
                        """
                        SelectedRecord
                            Cross Join
                                Hash Join
                                  condition: c.k=b.k
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_multi_b
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_c
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: lp_multi_a
                        """);
                assertRows("SELECT * FROM lp_multi_a a CROSS JOIN lp_multi_b b JOIN lp_multi_c c ON b.k=c.k "
                        + "ORDER BY 1,6,11", true,
                        """
                        id	k	v	s	ts	id1	k1	v1	s1	ts1	id2	k2	v2	s2	ts2
                        1	1	10	one	2020-01-01T00:00:00.000000Z	11	1	11	one	2020-01-01T00:00:00.000000Z	21	1	12	one	2020-01-01T00:00:00.000000Z
                        1	1	10	one	2020-01-01T00:00:00.000000Z	12	2	21	two	2020-01-02T00:00:00.000000Z	22	2	22	two	2020-01-02T00:00:00.000000Z
                        1	1	10	one	2020-01-01T00:00:00.000000Z	13	null	null		2020-01-03T00:00:00.000000Z	23	null	null		2020-01-03T00:00:00.000000Z
                        2	2	20	two	2020-01-02T00:00:00.000000Z	11	1	11	one	2020-01-01T00:00:00.000000Z	21	1	12	one	2020-01-01T00:00:00.000000Z
                        2	2	20	two	2020-01-02T00:00:00.000000Z	12	2	21	two	2020-01-02T00:00:00.000000Z	22	2	22	two	2020-01-02T00:00:00.000000Z
                        2	2	20	two	2020-01-02T00:00:00.000000Z	13	null	null		2020-01-03T00:00:00.000000Z	23	null	null		2020-01-03T00:00:00.000000Z
                        3	null	null		2020-01-03T00:00:00.000000Z	11	1	11	one	2020-01-01T00:00:00.000000Z	21	1	12	one	2020-01-01T00:00:00.000000Z
                        3	null	null		2020-01-03T00:00:00.000000Z	12	2	21	two	2020-01-02T00:00:00.000000Z	22	2	22	two	2020-01-02T00:00:00.000000Z
                        3	null	null		2020-01-03T00:00:00.000000Z	13	null	null		2020-01-03T00:00:00.000000Z	23	null	null		2020-01-03T00:00:00.000000Z
                        """,
                        """
                        Encode sort
                          keys: [id, id1, id2]
                            SelectedRecord
                                Cross Join
                                    Hash Join
                                      condition: c.k=b.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_b
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_c
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_multi_a
                        """);
                assertRows("SELECT c.*,a.id aid,b.id bid FROM lp_multi_a a CROSS JOIN lp_multi_b b "
                        + "JOIN lp_multi_c c ON b.k=c.k ORDER BY aid,bid,id", true,
                        """
                        id	k	v	s	ts	aid	bid
                        21	1	12	one	2020-01-01T00:00:00.000000Z	1	11
                        22	2	22	two	2020-01-02T00:00:00.000000Z	1	12
                        23	null	null		2020-01-03T00:00:00.000000Z	1	13
                        21	1	12	one	2020-01-01T00:00:00.000000Z	2	11
                        22	2	22	two	2020-01-02T00:00:00.000000Z	2	12
                        23	null	null		2020-01-03T00:00:00.000000Z	2	13
                        21	1	12	one	2020-01-01T00:00:00.000000Z	3	11
                        22	2	22	two	2020-01-02T00:00:00.000000Z	3	12
                        23	null	null		2020-01-03T00:00:00.000000Z	3	13
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, id]
                            SelectedRecord
                                Cross Join
                                    Hash Join
                                      condition: c.k=b.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_b
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_c
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_multi_a
                        """);
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a CROSS JOIN lp_multi_b b "
                        + "CROSS JOIN lp_multi_c c ORDER BY aid,bid,cid", true,
                        """
                        aid	bid	cid
                        1	11	21
                        1	11	22
                        1	11	23
                        1	12	21
                        1	12	22
                        1	12	23
                        1	13	21
                        1	13	22
                        1	13	23
                        2	11	21
                        2	11	22
                        2	11	23
                        2	12	21
                        2	12	22
                        2	12	23
                        2	13	21
                        2	13	22
                        2	13	23
                        3	11	21
                        3	11	22
                        3	11	23
                        3	12	21
                        3	12	22
                        3	12	23
                        3	13	21
                        3	13	22
                        3	13	23
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid]
                            SelectedRecord
                                Cross Join
                                    Cross Join
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_b
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_multi_c
                        """);
            }
        });
    }

    @Test
    public void testMixedEqualityTransitivityDoesNotChangeOuterMatching() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a LEFT JOIN lp_multi_b b "
                    + "ON a.id=b.id JOIN lp_multi_c c ON a.k=c.k AND b.k=c.k ORDER BY aid,cid";
            for (boolean isFullFat : new boolean[]{false, true}) {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    compiler.setFullFatJoins(isFullFat);
                    try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary()
                                .returns("aid\tbid\tcid\n3\tnull\t23\n");
                    }
                }
            }
        });
    }

    @Test
    public void testMixedForwardOnReference() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a LEFT JOIN lp_multi_b b "
                        + "ON a.k=c.k JOIN lp_multi_c c ON a.k=c.k ORDER BY aid,bid,cid", false,
                        """
                        aid	bid	cid
                        1	11	21
                        1	12	21
                        1	13	21
                        2	11	22
                        2	12	22
                        2	13	22
                        3	11	23
                        3	12	23
                        3	13	23
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid]
                            SelectedRecord
                                Nested Loop Left Join
                                  filter: a.k=c.k
                                    Hash Join Light
                                      condition: c.k=a.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_c
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_multi_b
                        """);
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a LEFT JOIN lp_multi_b b "
                        + "ON b.k=c.k AND b.v>11 JOIN lp_multi_c c ON a.k=c.k ORDER BY aid,bid,cid", false,
                        """
                        aid	bid	cid
                        1	null	21
                        2	12	22
                        3	null	23
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid]
                            SelectedRecord
                                Nested Loop Left Join
                                  filter: (b.k=c.k and 11<b.v)
                                    Hash Join Light
                                      condition: c.k=a.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_c
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_multi_b
                        """);
            }
            {
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a LEFT JOIN lp_multi_b b "
                        + "ON a.k=c.k JOIN lp_multi_c c ON a.k=c.k ORDER BY aid,bid,cid", true,
                        """
                        aid	bid	cid
                        1	11	21
                        1	12	21
                        1	13	21
                        2	11	22
                        2	12	22
                        2	13	22
                        3	11	23
                        3	12	23
                        3	13	23
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid]
                            SelectedRecord
                                Nested Loop Left Join
                                  filter: a.k=c.k
                                    Hash Join
                                      condition: c.k=a.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_c
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_multi_b
                        """);
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a LEFT JOIN lp_multi_b b "
                        + "ON b.k=c.k AND b.v>11 JOIN lp_multi_c c ON a.k=c.k ORDER BY aid,bid,cid", true,
                        """
                        aid	bid	cid
                        1	null	21
                        2	12	22
                        3	null	23
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid]
                            SelectedRecord
                                Nested Loop Left Join
                                  filter: (b.k=c.k and 11<b.v)
                                    Hash Join
                                      condition: c.k=a.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_c
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_multi_b
                        """);
            }
        });
    }

    @Test
    public void testMixedOuterRegionUsesOrderedJoinInputs() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a LEFT JOIN lp_multi_b b "
                        + "ON a.k=b.k JOIN lp_multi_c c ON a.k=c.k ORDER BY aid,bid,cid", false,
                        """
                        aid	bid	cid
                        1	11	21
                        2	12	22
                        3	13	23
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid]
                            SelectedRecord
                                Hash Join Light
                                  condition: c.k=a.k
                                    Hash Left Outer Join Light
                                      condition: b.k=a.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_b
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_c
                        """);
            }
            {
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a LEFT JOIN lp_multi_b b "
                        + "ON a.k=b.k JOIN lp_multi_c c ON a.k=c.k ORDER BY aid,bid,cid", true,
                        """
                        aid	bid	cid
                        1	11	21
                        2	12	22
                        3	13	23
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid]
                            SelectedRecord
                                Hash Join
                                  condition: c.k=a.k
                                    Hash Left Outer Join
                                      condition: b.k=a.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_b
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_c
                        """);
            }
        });
    }

    @Test
    public void testMultipleDonorsKeepEarlierStolenEquality() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0; i < 5; i++) {
                execute("CREATE TABLE lp_donor_" + i + "(x INT,y INT)");
            }
            execute("INSERT INTO lp_donor_0 VALUES(0,0)");
            execute("INSERT INTO lp_donor_1 VALUES(2,3)");
            execute("INSERT INTO lp_donor_2 VALUES(2,5)");
            execute("INSERT INTO lp_donor_3 VALUES(5,7)");
            execute("INSERT INTO lp_donor_4 VALUES(0,3)");
            // Reusing a captured null target context overwrote the first donor's
            // equality. The one candidate tuple violates t1.y=t3.y and must not
            // be returned.
            assertRows("SELECT t0.x FROM lp_donor_0 t0 CROSS JOIN lp_donor_1 t1 CROSS JOIN lp_donor_2 t2 "
                    + "CROSS JOIN lp_donor_3 t3 CROSS JOIN lp_donor_4 t4 WHERE t3.x=t2.y AND t1.x=t2.x "
                    + "AND t1.y=t3.y AND t4.x=t0.x AND t1.y=t4.y", false, "x\n");
        });
    }

    @Test
    public void testNativeTimestampConjunctWithNanosecondLiteral() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a JOIN lp_multi_b b ON a.k=b.k "
                    + "JOIN lp_multi_c c ON b.k=c.k WHERE a.ts<'2020-01-01T00:00:00.000000001Z' "
                    + "AND c.v>0 ORDER BY aid,bid,cid", false,
                    """
                    aid	bid	cid
                    1	11	21
                    """,
                    """
                    Encode sort
                      keys: [aid, bid, cid]
                        SelectedRecord
                            Hash Join Light
                              condition: c.k=b.k
                                Hash Join Light
                                  condition: b.k=a.k
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: lp_multi_a
                                          intervals: [("MIN","2020-01-01T00:00:00.000000Z")]
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_b
                                Hash
                                    Async JIT Filter workers: 1
                                      filter: 0<v
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_c
                    """);
            assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a JOIN lp_multi_b b ON a.k=b.k "
                    + "JOIN lp_multi_c c ON b.k=c.k WHERE a.ts>='2020-01-01T00:00:00.000000001Z' "
                    + "AND b.v IN (11,21) AND c.v>0 ORDER BY aid,bid,cid", false,
                    """
                    aid	bid	cid
                    2	12	22
                    """,
                    """
                    Encode sort
                      keys: [aid, bid, cid]
                        SelectedRecord
                            Hash Join Light
                              condition: c.k=b.k
                                Hash Join Light
                                  condition: b.k=a.k
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: lp_multi_a
                                          intervals: [("2020-01-01T00:00:00.000001Z","MAX")]
                                    Hash
                                        Async JIT Filter workers: 1
                                          filter: v in [11,21]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_b
                                Hash
                                    Async JIT Filter workers: 1
                                      filter: 0<v
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_c
                    """);
        });
    }

    @Test
    public void testNestedEmissionsRetainAllOriginalEqualities() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0; i < 4; i++) {
                execute("CREATE TABLE lp_emit_chain_" + i + "(x INT,y INT)");
            }
            execute("INSERT INTO lp_emit_chain_0 VALUES(0,2)");
            execute("INSERT INTO lp_emit_chain_1 VALUES(2,3)");
            execute("INSERT INTO lp_emit_chain_2 VALUES(0,0)");
            execute("INSERT INTO lp_emit_chain_3 VALUES(3,0)");
            // A one-level emitted queue drops the equality connecting t1.y to
            // t2.x, which would return an invalid tuple.
            assertRows("SELECT t0.x FROM lp_emit_chain_0 t0 CROSS JOIN lp_emit_chain_1 t1 CROSS JOIN lp_emit_chain_2 t2 "
                    + "CROSS JOIN lp_emit_chain_3 t3 WHERE t1.x=t0.y AND t3.x=t1.y AND t1.y=t2.x "
                    + "AND t2.x=t3.y AND t3.y=t0.x AND t3.y=t2.y", false, "x\n");
        });
    }

    @Test
    public void testThreeAndFourSourceKeysForwardReferencesAndShorthand() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a JOIN lp_multi_b b ON a.k=b.k "
                        + "JOIN lp_multi_c c ON b.k=c.k ORDER BY aid,bid,cid", false,
                        """
                        aid	bid	cid
                        1	11	21
                        2	12	22
                        3	13	23
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid]
                            SelectedRecord
                                Hash Join Light
                                  condition: c.k=b.k
                                    Hash Join Light
                                      condition: b.k=a.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_b
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_c
                        """);
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a JOIN lp_multi_b b ON a.k=c.k "
                        + "JOIN lp_multi_c c ON a.k=b.k ORDER BY aid,bid,cid", false,
                        """
                        aid	bid	cid
                        1	11	21
                        2	12	22
                        3	13	23
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid]
                            SelectedRecord
                                Hash Join Light
                                  condition: c.k=a.k
                                    Hash Join Light
                                      condition: b.k=a.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_b
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_c
                        """);
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a JOIN lp_multi_b b ON(k) "
                        + "JOIN lp_multi_c c ON(k) ORDER BY aid,bid,cid", false,
                        """
                        aid	bid	cid
                        1	11	21
                        2	12	22
                        3	13	23
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid]
                            SelectedRecord
                                Hash Join Light
                                  condition: c.k=a.k
                                    Hash Join Light
                                      condition: b.k=a.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_b
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_c
                        """);
                assertRows("SELECT a.id aid,b.id bid,c.id cid,d.id did FROM lp_multi_a a JOIN lp_multi_b b ON a.k=b.k "
                        + "JOIN lp_multi_c c ON b.k=c.k JOIN lp_multi_d d ON c.k=d.k ORDER BY aid,bid,cid,did", false,
                        """
                        aid	bid	cid	did
                        1	11	21	31
                        2	12	22	32
                        3	13	23	33
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid, did]
                            SelectedRecord
                                Hash Join Light
                                  condition: d.k=c.k
                                    Hash Join Light
                                      condition: c.k=b.k
                                        Hash Join Light
                                          condition: b.k=a.k
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_a
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_multi_b
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_c
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_d
                        """);
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a JOIN lp_multi_b b ON a.s=b.s "
                        + "JOIN lp_multi_c c ON b.s=c.s ORDER BY aid,bid,cid", false,
                        """
                        aid	bid	cid
                        1	11	21
                        2	12	22
                        3	13	23
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid]
                            SelectedRecord
                                Hash Join Light
                                  condition: c.s=b.s
                                  symbolKeyJoin: true
                                    Hash Join Light
                                      condition: b.s=a.s
                                      symbolKeyJoin: true
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_b
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_c
                        """);
            }
            {
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a JOIN lp_multi_b b ON a.k=b.k "
                        + "JOIN lp_multi_c c ON b.k=c.k ORDER BY aid,bid,cid", true,
                        """
                        aid	bid	cid
                        1	11	21
                        2	12	22
                        3	13	23
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid]
                            SelectedRecord
                                Hash Join
                                  condition: c.k=b.k
                                    Hash Join
                                      condition: b.k=a.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_b
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_c
                        """);
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a JOIN lp_multi_b b ON a.k=c.k "
                        + "JOIN lp_multi_c c ON a.k=b.k ORDER BY aid,bid,cid", true,
                        """
                        aid	bid	cid
                        1	11	21
                        2	12	22
                        3	13	23
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid]
                            SelectedRecord
                                Hash Join
                                  condition: c.k=a.k
                                    Hash Join
                                      condition: b.k=a.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_b
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_c
                        """);
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a JOIN lp_multi_b b ON(k) "
                        + "JOIN lp_multi_c c ON(k) ORDER BY aid,bid,cid", true,
                        """
                        aid	bid	cid
                        1	11	21
                        2	12	22
                        3	13	23
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid]
                            SelectedRecord
                                Hash Join
                                  condition: c.k=a.k
                                    Hash Join
                                      condition: b.k=a.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_b
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_c
                        """);
                assertRows("SELECT a.id aid,b.id bid,c.id cid,d.id did FROM lp_multi_a a JOIN lp_multi_b b ON a.k=b.k "
                        + "JOIN lp_multi_c c ON b.k=c.k JOIN lp_multi_d d ON c.k=d.k ORDER BY aid,bid,cid,did", true,
                        """
                        aid	bid	cid	did
                        1	11	21	31
                        2	12	22	32
                        3	13	23	33
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid, did]
                            SelectedRecord
                                Hash Join
                                  condition: d.k=c.k
                                    Hash Join
                                      condition: c.k=b.k
                                        Hash Join
                                          condition: b.k=a.k
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_a
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_multi_b
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_c
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_d
                        """);
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_multi_a a JOIN lp_multi_b b ON a.s=b.s "
                        + "JOIN lp_multi_c c ON b.s=c.s ORDER BY aid,bid,cid", true,
                        """
                        aid	bid	cid
                        1	11	21
                        2	12	22
                        3	13	23
                        """,
                        """
                        Encode sort
                          keys: [aid, bid, cid]
                            SelectedRecord
                                Hash Join
                                  condition: c.s=b.s
                                  symbolKeyJoin: true
                                    Hash Join
                                      condition: b.s=a.s
                                      symbolKeyJoin: true
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_a
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_multi_b
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_multi_c
                        """);
            }
        });
    }

    private void assertRows(String sql, boolean isFullFat, String expected) throws Exception {
        assertRows(sql, isFullFat, expected, null);
    }

    private void assertRows(String sql, boolean isFullFat, String expected, String expectedPlan) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            compiler.setFullFatJoins(isFullFat);
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
                if (expectedPlan != null) {
                    TestUtils.assertEquals(JitUtil.isJitSupported() ? expectedPlan : expectedPlan.replace("Async JIT", "Async"), plan(factory));
                }
            }
        }
    }

    private void createRows() throws Exception {
        final ObjList<String> names = new ObjList<>();
        names.add("a");
        names.add("b");
        names.add("c");
        names.add("d");
        for (int i = 0; i < names.size(); i++) {
            final String name = "lp_multi_" + names.getQuick(i);
            final int base = 10 * i;
            execute("CREATE TABLE " + name + "(id INT,k INT,v INT,s SYMBOL,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO " + name + " VALUES(" + (base + 1) + ",1," + (10 + i) + ",'one','2020-01-01'),("
                    + (base + 2) + ",2," + (20 + i) + ",'two','2020-01-02'),(" + (base + 3) + ",null,null,null,'2020-01-03')");
        }
    }

    private String plan(RecordCursorFactory factory) {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, sqlExecutionContext);
        final StringSink text = new StringSink();
        for (int i = 1, n = sink.getLineCount(); i <= n; i++) {
            text.put(sink.getLine(i)).put('\n');
        }
        return text.toString();
    }
}
