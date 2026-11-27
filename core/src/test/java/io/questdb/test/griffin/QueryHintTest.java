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
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class QueryHintTest extends AbstractCairoTest {
    private static final String ASOF_ROWS = "lid\trid\n1\t11\n2\t11\n3\t12\n4\tnull\n";

    @Test
    public void testAlgorithmsAndHintPrecedence() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertHint("asof_dense(l r)", "ASOF", "ON(sym)", "AsOf Join Dense Single Symbol", ASOF_ROWS);
            assertHint("asof_dense(l r)", "ASOF", "ON(k)", "AsOf Join Dense", ASOF_ROWS);
            assertHint("asof_index(l r)", "ASOF", "ON(sym)", "AsOf Join Indexed Scan", ASOF_ROWS);
            assertHint("asof_memoized(l r)", "ASOF", "ON(sym)", "AsOf Join Memoized Scan", "driveByCache: false", ASOF_ROWS);
            assertHint("asof_memoized_driveby(l r)", "ASOF", "ON(sym)", "AsOf Join Memoized Scan", "driveByCache: true", ASOF_ROWS);
            assertHint("asof_linear(l r)", "ASOF", "ON(sym)", "AsOf Join Light", ASOF_ROWS);
            assertHint("asof_linear(l r)", "LT", "", "Lt Join", """
                    lid	rid
                    1	10
                    2	11
                    3	13
                    4	13
                    """);
            assertHint("asof_linear(l r) asof_dense(l r) asof_index(l r)", "ASOF", "ON(sym)", "AsOf Join Light", ASOF_ROWS);
            assertHint("asof_dense(l r) asof_index(l r) asof_memoized(l r)", "ASOF", "ON(sym)", "AsOf Join Dense Single Symbol", ASOF_ROWS);
            assertHint("asof_index(l r) asof_memoized(l r)", "ASOF", "ON(sym)", "AsOf Join Indexed Scan", ASOF_ROWS);
            assertHint("asof_dense(l r)", "LT", "", "Lt Join Fast", """
                    lid	rid
                    1	10
                    2	11
                    3	13
                    4	13
                    """);
            assertHint("asof_index(l r)", "ASOF", "ON(k)", "AsOf Join Fast", ASOF_ROWS);
            assertHint("asof_memoized(l r)", "ASOF", "ON(k)", "AsOf Join Fast", ASOF_ROWS);
            assertHint("asof_dense(l r)", "ASOF", "", "AsOf Join Fast", """
                    lid	rid
                    1	11
                    2	12
                    3	13
                    4	13
                    """);
        });
    }

    @Test
    public void testCompilerGeneratedAliases() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertHintPlan("SELECT /*+ asof_dense(_xQdbA1 r) */ * FROM (SELECT * FROM lp_hint_m) ASOF JOIN lp_hint_s r ON(sym)", "AsOf Join Dense Single Symbol", """
                    id	k	sym	ts	id1	k1	sym1	ts1
                    1	1	A	2020-01-01T00:00:01.000000Z	11	1	A	2020-01-01T00:00:01.000000Z
                    2	1	A	2020-01-01T00:00:02.000000Z	11	1	A	2020-01-01T00:00:01.000000Z
                    3	2	B	2020-01-01T00:00:03.000000Z	12	2	B	2020-01-01T00:00:02.000000Z
                    4	3	C	2020-01-01T00:00:04.000000Z	null	null	\t
                    """);
            assertHintPlan("SELECT /*+ asof_dense(lp_hint_m _xQdbA1) */ * FROM lp_hint_m ASOF JOIN (SELECT * FROM lp_hint_s) ON(sym)", "AsOf Join Dense Single Symbol", """
                    id	k	sym	ts	id1	k1	sym1	ts1
                    1	1	A	2020-01-01T00:00:01.000000Z	11	1	A	2020-01-01T00:00:01.000000Z
                    2	1	A	2020-01-01T00:00:02.000000Z	11	1	A	2020-01-01T00:00:01.000000Z
                    3	2	B	2020-01-01T00:00:03.000000Z	12	2	B	2020-01-01T00:00:02.000000Z
                    4	3	C	2020-01-01T00:00:04.000000Z	null	null	\t
                    """);
            assertHintPlan("SELECT /*+ asof_dense(_xQdbA1 r) */ _xQdbA1.id lid,r.id rid FROM (SELECT id,sym FROM lp_hint_m) ASOF JOIN lp_hint_s r ON(sym)", "AsOf Join Dense Single Symbol", ASOF_ROWS);
            assertQuery("SELECT _xQdbA1.ts FROM (SELECT id,sym FROM lp_hint_m) ASOF JOIN lp_hint_s r ON(sym)").noLeakCheck().fails(7, "Invalid column: _xQdbA1.ts");
            assertHintPlan("SELECT * FROM lp_hint_m _xQdbA1 ASOF JOIN (SELECT * FROM lp_hint_s) ON(sym)", "AsOf Join Fast", """
                    id	k	sym	ts	id1	k1	sym1	ts1
                    1	1	A	2020-01-01T00:00:01.000000Z	11	1	A	2020-01-01T00:00:01.000000Z
                    2	1	A	2020-01-01T00:00:02.000000Z	11	1	A	2020-01-01T00:00:01.000000Z
                    3	2	B	2020-01-01T00:00:03.000000Z	12	2	B	2020-01-01T00:00:02.000000Z
                    4	3	C	2020-01-01T00:00:04.000000Z	null	null	\t
                    """);
            assertQuery("SELECT * FROM lp_hint_m _xQdbA0 ASOF JOIN (SELECT * FROM lp_hint_s) ON(sym)").noLeakCheck().fails(0, "Duplicate table or alias: _xQdbA0");
        });
    }

    @Test
    public void testCteHintsRemainInsideTheirLexicalScope() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertCteHints("", "asof_dense(l r)", "AsOf Join Fast", ASOF_ROWS);
            assertCteHints("asof_dense(l r)", "", "AsOf Join Dense Single Symbol", ASOF_ROWS);
            assertCteHints("asof_dense(l r)", "asof_dense(a b)", "AsOf Join Dense Single Symbol", ASOF_ROWS);
            assertCteHints("asof_dense(a b)", "asof_dense(l r)", "AsOf Join Fast", ASOF_ROWS);
            assertCteHints("asof_dense(l r)", "asof_linear(l r)", "AsOf Join Dense Single Symbol", ASOF_ROWS);
            assertCteHints("asof_linear(l r)", "asof_dense(l r)", "AsOf Join Light", ASOF_ROWS);
        });
    }

    @Test
    public void testFactoriesRetainHintsAcrossCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT /*+ asof_memoized_driveby(l r) */ l.id lid,r.id rid FROM lp_hint_m l ASOF JOIN lp_hint_s r ON(sym)";
            RecordCursorFactory retained = null;
            RecordCursorFactory explain = null;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    explain = compiler.compile("EXPLAIN " + sql, sqlExecutionContext).getRecordCursorFactory();
                    TestUtils.assertContains(printFactory(explain), "driveByCache: true");
                    try (RecordCursorFactory factory = compiler.compile("SELECT l.id lid,r.id rid FROM lp_hint_m l ASOF JOIN lp_hint_s r ON(sym)", sqlExecutionContext).getRecordCursorFactory()) {
                        TestUtils.assertContains(planText(factory), "AsOf Join Fast");
                        assertRowsOnly(factory, ASOF_ROWS);
                    }
                    compiler.clear();
                } catch (Throwable th) {
                    Misc.free(retained, th);
                    Misc.free(explain, th);
                    throw th;
                }
            }
            try (RecordCursorFactory factory = retained; RecordCursorFactory explanation = explain) {
                TestUtils.assertContains(planText(factory), "driveByCache: true");
                assertRowsOnly(factory, ASOF_ROWS);
                TestUtils.assertEquals("""
                        QUERY PLAN
                        SelectedRecord
                            AsOf Join Memoized Scan
                              condition: r.sym=l.sym
                              driveByCache: true
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: lp_hint_m
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: lp_hint_s
                        """, printFactory(explanation));
            }
        });
    }

    @Test
    public void testMatchingAndUnknownHints() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertHint("ASOF_DENSE(R L)", "ASOF", "ON(sym)", "AsOf Join Dense Single Symbol", ASOF_ROWS);
            assertHint("asof_dense(lp_hint_m lp_hint_s)", "ASOF", "ON(sym)", "AsOf Join Fast", ASOF_ROWS);
            assertHint("asof_dense(ll rr)", "ASOF", "ON(sym)", "AsOf Join Fast", ASOF_ROWS);
            assertHint("asof_dense(l)", "ASOF", "ON(sym)", "AsOf Join Fast", ASOF_ROWS);
            assertHint("asof_dense()", "ASOF", "ON(sym)", "AsOf Join Fast", ASOF_ROWS);
            assertHint("asof_dense", "ASOF", "ON(sym)", "AsOf Join Fast", ASOF_ROWS);
            assertHint("unrecognized_hint(l r)", "ASOF", "ON(sym)", "AsOf Join Fast", ASOF_ROWS);
            assertHint("unrecognized_hint(l r) asof_dense(l r)", "ASOF", "ON(sym)", "AsOf Join Dense Single Symbol", ASOF_ROWS);
            assertHintPlan("SELECT /*+ asof_dense(lp_hint_m lp_hint_s) */ lp_hint_m.id lid,lp_hint_s.id rid FROM lp_hint_m ASOF JOIN lp_hint_s ON(sym)", "AsOf Join Dense Single Symbol", ASOF_ROWS);
            assertHintPlan("SELECT /*+ asof_dense(\"L\" \"R\") */ l.id lid,r.id rid FROM lp_hint_m \"L\" ASOF JOIN lp_hint_s \"R\" ON(sym)", "AsOf Join Dense Single Symbol", ASOF_ROWS);
        });
    }

    @Test
    public void testNestedParentHintsOverrideMatchingChildNames() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertNestedHints("asof_dense(a b)", "asof_dense(l r)", "AsOf Join Dense Single Symbol", ASOF_ROWS);
            assertNestedHints("asof_dense(l r)", "asof_dense(a b)", "AsOf Join Fast", ASOF_ROWS);
            assertNestedHints("asof_dense(l r)", "asof_linear(l r)", "AsOf Join Light", ASOF_ROWS);
            assertNestedHints("asof_linear(l r)", "asof_dense(l r)", "AsOf Join Light", ASOF_ROWS);
            assertHintPlan("SELECT /*+ asof_dense(l r) */ l.id lid,r.id rid FROM lp_hint_m l ASOF JOIN lp_hint_s r ON(sym) "
                    + "UNION ALL SELECT /*+ asof_dense(a b) */ l.id lid,r.id rid FROM lp_hint_m l ASOF JOIN lp_hint_s r ON(sym)", "AsOf Join Dense Single Symbol", """
                    lid	rid
                    1	11
                    2	11
                    3	12
                    4	null
                    1	11
                    2	11
                    3	12
                    4	null
                    """);
        });
    }

    @Test
    public void testScanHints() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertHintPlan("SELECT /*+ no_index */ id FROM lp_hint_s WHERE k=1", "Async JIT Filter", """
                    id
                    10
                    11
                    13
                    """);
            assertHintPlan("SELECT /*+ no_index */ id FROM lp_hint_s WHERE sym='A' LATEST ON ts PARTITION BY sym", "LatestBy", """
                    id
                    13
                    """);
            assertHintPlan("SELECT id FROM lp_hint_s WHERE sym='A'", "Index forward scan", """
                    id
                    10
                    11
                    13
                    """);
            assertHintPlan("SELECT /*+ no_covering */ id FROM lp_hint_s WHERE sym IN ('A','B')", "on: sym", """
                    id
                    10
                    11
                    12
                    13
                    """);
            assertHintPlan("SELECT /*+ force_use_covering */ id FROM lp_hint_s WHERE sym=$1", "on: sym", "id\n");
            assertHintPlan("SELECT /*+ no_symbol_pattern_index */ id FROM lp_hint_s WHERE sym LIKE 'A%'", "Async", """
                    id
                    10
                    11
                    13
                    """);
            assertHintPlan("SELECT /*+ enable_pre_touch(lp_hint_m) */ id FROM lp_hint_m WHERE k>1", "Async", """
                    id
                    3
                    4
                    """);
            assertHintPlan("SELECT /*+ markout_horizon(l r) */ l.id, l.ts + r.x AS h FROM lp_hint_m l CROSS JOIN long_sequence(2) r ORDER BY l.ts + r.x",
                    "Markout Horizon Join", """
                            id	h
                            1	2020-01-01T00:00:01.000001Z
                            1	2020-01-01T00:00:01.000002Z
                            2	2020-01-01T00:00:02.000001Z
                            2	2020-01-01T00:00:02.000002Z
                            3	2020-01-01T00:00:03.000001Z
                            3	2020-01-01T00:00:03.000002Z
                            4	2020-01-01T00:00:04.000001Z
                            4	2020-01-01T00:00:04.000002Z
                            """);
        });
    }

    private void assertCteHints(String inner, String outer, String algorithm, String expected) throws Exception {
        assertHintPlan("WITH q AS (SELECT /*+ " + inner + " */ l.id lid,r.id rid FROM lp_hint_m l ASOF JOIN lp_hint_s r ON(sym)) "
                + "SELECT /*+ " + outer + " */ * FROM q", algorithm, expected);
    }

    private void assertHint(String hint, String join, String on, String algorithm, String expected) throws Exception {
        assertHint(hint, join, on, algorithm, null, expected);
    }

    private void assertHint(String hint, String join, String on, String algorithm, String property, String expected) throws Exception {
        assertHintPlan("SELECT /*+ " + hint + " */ l.id lid,r.id rid FROM lp_hint_m l " + join + " JOIN lp_hint_s r " + on, algorithm, property, expected);
    }

    private void assertHintPlan(String sql, String algorithm, String expected) throws Exception {
        assertHintPlan(sql, algorithm, null, expected);
    }

    private void assertHintPlan(String sql, String algorithm, String property, String expected) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            final String plan = planText(factory);
            TestUtils.assertContains(plan, algorithm);
            if (property != null) {
                TestUtils.assertContains(plan, property);
            }
            assertRowsOnly(factory, expected);
        }
    }

    private void assertNestedHints(String inner, String outer, String algorithm, String expected) throws Exception {
        assertHintPlan("SELECT /*+ " + outer + " */ * FROM (SELECT /*+ " + inner + " */ l.id lid,r.id rid FROM lp_hint_m l ASOF JOIN lp_hint_s r ON(sym))", algorithm, expected);
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE lp_hint_m(id INT,k INT,sym SYMBOL,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE lp_hint_s(id INT,k INT,sym SYMBOL INDEX,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO lp_hint_m VALUES(1,1,'A','2020-01-01T00:00:01Z'),(2,1,'A','2020-01-01T00:00:02Z'),"
                + "(3,2,'B','2020-01-01T00:00:03Z'),(4,3,'C','2020-01-01T00:00:04Z')");
        execute("INSERT INTO lp_hint_s VALUES(10,1,'A','2020-01-01T00:00:00.500000Z'),(11,1,'A','2020-01-01T00:00:01Z'),"
                + "(12,2,'B','2020-01-01T00:00:02Z'),(13,1,'A','2020-01-01T00:00:02.500000Z')");
    }
}
