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
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MixedJoinTest extends AbstractCairoTest {
    @Test
    public void testEarlyWhereRemainsAboveLaterNullExtension() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (boolean isFullFat : new boolean[]{false, true}) {
                for (int kind = 0; kind < 2; kind++) {
                    final String operation = kind == 0 ? "RIGHT" : "FULL";
                    final String source = " FROM lp_mix_a a LEFT JOIN lp_mix_b b ON a.k=b.k " + operation
                            + " JOIN lp_mix_c c ON b.k=c.k";
                    assertRows("SELECT a.id aid,b.id bid,c.id cid" + source + " WHERE a.id=null ORDER BY cid",
                            "aid\tbid\tcid\nnull\tnull\t400\n", isFullFat,
                            kind == 0 ? "Hash Right Outer Join" : "Hash Full Outer Join");
                    assertRows("SELECT aid,bid,cid FROM (SELECT a.id aid,b.id bid,c.id cid" + source
                                    + ") WHERE aid=null ORDER BY cid",
                            "aid\tbid\tcid\nnull\tnull\t400\n", isFullFat, null);
                }
            }
        });
    }

    @Test
    public void testInnerOnRemainsAtItsMatchingBoundary() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (boolean isFullFat : new boolean[]{false, true}) {
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_mix_a a JOIN lp_mix_b b ON a.k=b.k AND a.id>1"
                                + " FULL JOIN lp_mix_c c ON b.k=c.k ORDER BY aid,cid",
                        "aid\tbid\tcid\nnull\tnull\t100\nnull\tnull\t400\n2\t20\t200\n", isFullFat, "Hash Full Outer Join");
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_mix_a a LEFT JOIN lp_mix_b b ON a.k=b.k"
                                + " JOIN lp_mix_c c ON b.id=null AND c.k=4 ORDER BY aid",
                        "aid\tbid\tcid\n3\tnull\t400\n", isFullFat, null);
            }
        });
    }

    @Test
    public void testMixedFactoriesRetainPrefixBindingsAfterCompilerReuse() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT a.id aid,b.s bs,upper(b.s) upper_bs,c.s cs FROM lp_mix_a a"
                    + " ASOF JOIN lp_mix_b b ON a.s=b.s LEFT JOIN lp_mix_c c ON b.s=c.s AND b.s IN('A','B') ORDER BY aid";
            final String expected = "aid\tbs\tupper_bs\tcs\n1\tA\tA\tA\n2\tB\tB\tB\n3\t\t\t\n";
            for (boolean isFullFat : new boolean[]{false, true}) {
                assertRows(sql, expected, isFullFat, "symbolKeyJoin: true");
                RecordCursorFactory retained = null;
                try {
                    final String expectedPlan;
                    try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                        compiler.setFullFatJoins(isFullFat);
                        retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                        expectedPlan = planText(retained);
                        assertRowsOnly(retained, expected);
                        try (RecordCursorFactory ignored = compiler.compile("SELECT count() FROM lp_mix_b", sqlExecutionContext).getRecordCursorFactory()) {
                            Assert.assertNotNull(ignored);
                        }
                        compiler.clear();
                    }
                    assertRowsOnly(retained, expected);
                    Assert.assertEquals(expectedPlan, planText(retained));
                } finally {
                    Misc.free(retained);
                }
            }
        });
    }

    @Test
    public void testOrdinaryAndOuterKeysResolveAgainstTheWholePrefix() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (boolean isFullFat : new boolean[]{false, true}) {
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_mix_a a JOIN lp_mix_b b ON a.k=b.k"
                                + " LEFT JOIN lp_mix_c c ON b.k=c.k AND c.id>100 ORDER BY aid,cid",
                        "aid\tbid\tcid\n1\t10\tnull\n2\t20\t200\n", isFullFat, "Hash Left Outer Join");
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_mix_a a LEFT JOIN lp_mix_b b ON a.k=b.k"
                                + " JOIN lp_mix_c c ON b.k=c.k ORDER BY aid,cid",
                        "aid\tbid\tcid\n1\t10\t100\n2\t20\t200\n", isFullFat, "Hash Join");
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_mix_a a LEFT JOIN lp_mix_b b ON a.k=b.k"
                                + " LEFT JOIN lp_mix_c c ON b.k=c.k WHERE c.id=null ORDER BY aid",
                        "aid\tbid\tcid\n3\tnull\tnull\n", isFullFat, "Hash Left Outer Join");
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_mix_a a LEFT JOIN lp_mix_b b ON a.k=b.k"
                                + " RIGHT JOIN lp_mix_c c ON b.k=c.k ORDER BY aid,cid",
                        "aid\tbid\tcid\nnull\tnull\t400\n1\t10\t100\n2\t20\t200\n", isFullFat, "Hash Right Outer Join");
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_mix_a a LEFT JOIN lp_mix_b b ON a.k=b.k"
                                + " FULL JOIN lp_mix_c c ON b.k=c.k ORDER BY aid,cid",
                        "aid\tbid\tcid\nnull\tnull\t400\n1\t10\t100\n2\t20\t200\n3\tnull\tnull\n", isFullFat, "Hash Full Outer Join");
            }
        });
    }

    @Test
    public void testSpliceEmissionsSurviveLaterJoinAndWhere() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            execute("CREATE TABLE lp_mix_events(id INT,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lp_mix_events VALUES(1,'2024-01-01T00:00:01'),(3,'2024-01-01T00:00:03')");
            execute("CREATE TABLE lp_mix_updates(id INT,k INT,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lp_mix_updates VALUES(20,2,'2024-01-01T00:00:00')");
            final String source = " FROM lp_mix_events a SPLICE JOIN lp_mix_updates b LEFT JOIN lp_mix_c c ON b.k=c.k";
            assertRows("SELECT a.id aid,b.id bid,c.id cid" + source + " ORDER BY aid",
                    "aid\tbid\tcid\nnull\t20\t200\n1\t20\t200\n3\t20\t200\n", false, "Splice Join");
            assertRows("SELECT a.id aid,b.id bid,c.id cid" + source + " WHERE a.id=null ORDER BY aid",
                    "aid\tbid\tcid\nnull\t20\t200\n", false, "Splice Join");
        });
    }

    @Test
    public void testTemporalJoinsConsumeIntermediateTimestampAndHiddenColumns() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final ObjList<String> temporal = new ObjList<>("ASOF", "LT");
            for (boolean isFullFat : new boolean[]{false, true}) {
                for (int i = 0; i < temporal.size(); i++) {
                    assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_mix_a a LEFT JOIN lp_mix_b b ON a.k=b.k "
                                    + temporal.getQuick(i) + " JOIN lp_mix_c c ON b.k=c.k",
                            "aid\tbid\tcid\n1\t10\t100\n2\t20\t200\n3\tnull\tnull\n", isFullFat,
                            i == 0 ? "AsOf Join" : "Lt Join");
                }
                assertRows("SELECT a.id aid,b.id bid,c.id cid FROM (SELECT id,k FROM lp_mix_a) a"
                                + " ASOF JOIN (SELECT id,k FROM lp_mix_b) b ON a.k=b.k LT JOIN (SELECT id,k FROM lp_mix_c) c ON b.k=c.k",
                        "aid\tbid\tcid\n1\t10\t100\n2\t20\t200\n3\tnull\tnull\n", isFullFat, "Lt Join");
            }
        });
    }

    @Test
    public void testTimestampLossAndInvalidLaterJoinReleaseEarlierFactories() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final ObjList<String> operations = new ObjList<>("RIGHT", "FULL", "SPLICE");
            {
                final int i = 0;
                assertQuery("SELECT a.id FROM lp_mix_a a " + operations.getQuick(i)
                        + " JOIN lp_mix_b b ON a.k=b.k ASOF JOIN lp_mix_c c ON a.k=c.k").noLeakCheck().fails(61, "left side of time series join has no timestamp");
            }
            {
                final int i = 1;
                assertQuery("SELECT a.id FROM lp_mix_a a " + operations.getQuick(i)
                        + " JOIN lp_mix_b b ON a.k=b.k ASOF JOIN lp_mix_c c ON a.k=c.k").noLeakCheck().fails(60, "left side of time series join has no timestamp");
            }
            {
                final int i = 2;
                assertQuery("SELECT a.id FROM lp_mix_a a " + operations.getQuick(i)
                        + " JOIN lp_mix_b b ON a.k=b.k ASOF JOIN lp_mix_c c ON a.k=c.k").noLeakCheck().fails(62, "left side of time series join has no timestamp");
            }
            assertQuery("SELECT a.id FROM lp_mix_a a LEFT JOIN lp_mix_b b ON a.k=b.k"
                    + " ASOF JOIN (SELECT * FROM lp_mix_c ORDER BY ts DESC) c ON a.k=c.k").noLeakCheck().fails(60, "right side of time series join doesn't have ASC timestamp order");
            assertQuery("SELECT a.id FROM lp_mix_a a LEFT JOIN lp_mix_b b ON a.k=b.k"
                    + " ASOF JOIN lp_mix_c c ON a.k=c.k TOLERANCE 0s").noLeakCheck().fails(102, "zero is not a valid tolerance value");
            assertRows("SELECT a.id aid,b.id bid,c.id cid FROM lp_mix_a a LEFT JOIN lp_mix_b b ON a.k=b.k"
                            + " ASOF JOIN lp_mix_c c ON a.k=c.k",
                    "aid\tbid\tcid\n1\t10\t100\n2\t20\t200\n3\tnull\tnull\n", false, "AsOf Join");
        });
    }

    private void assertRows(String sql, String expected, boolean isFullFat, String algorithm) throws Exception {
        final int jitMode = sqlExecutionContext.getJitMode();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            compiler.setFullFatJoins(isFullFat);
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                assertRowsOnly(factory, expected);
                if (algorithm != null) {
                    TestUtils.assertContains(planText(factory), algorithm);
                }
            }
        } finally {
            sqlExecutionContext.setJitMode(jitMode);
        }
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_mix_a(id INT,k INT,s SYMBOL,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("CREATE TABLE lp_mix_b(id INT,k INT,s SYMBOL,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("CREATE TABLE lp_mix_c(id INT,k INT,s SYMBOL,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO lp_mix_a VALUES(1,1,'A','2024-01-01T00:00:01'),(2,2,'B','2024-01-01T00:00:03'),(3,3,'C','2024-01-01T00:00:05')");
        execute("INSERT INTO lp_mix_b VALUES(20,2,'B','2024-01-01T00:00:00'),(10,1,'A','2024-01-01T00:00:00'),(40,4,'D','2024-01-01T00:00:04')");
        execute("INSERT INTO lp_mix_c VALUES(100,1,'A','2024-01-01T00:00:00'),(200,2,'B','2024-01-01T00:00:02'),(400,4,'D','2024-01-01T00:00:04')");
    }
}
