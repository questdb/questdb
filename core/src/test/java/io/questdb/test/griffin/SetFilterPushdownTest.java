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
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SetFilterPushdownTest extends AbstractCairoTest {
    @Test
    public void testAllSetOperationsKeepTupleAndDuplicateSemantics() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            // Balance the overlapping tuple; unmatched duplicates must survive EXCEPT ALL.
            execute("INSERT INTO lp_sp_b VALUES ('2024-01-02',2)");
            final ObjList<String> operations = new ObjList<>("UNION ALL", "UNION", "EXCEPT", "EXCEPT ALL", "INTERSECT", "INTERSECT ALL");
            final ObjList<String> expected = new ObjList<>(
                    "id\n2\n2\n2\n2\n3\n3\n4\n4\n5\n", "id\n2\n3\n4\n5\n",
                    "id\n3\n", "id\n3\n3\n", "id\n2\n", "id\n2\n2\n"
            );
            for (int i = 0, n = operations.size(); i < n; i++) {
                final String sql = "SELECT id FROM (SELECT ta time,id FROM lp_sp_a " + operations.getQuick(i)
                        + " SELECT tb other,id FROM lp_sp_b) WHERE time>='2024-01-02T00:00:00.000000Z' ORDER BY id";
                assertRowsAndIntervalScans(sql, expected.getQuick(i), 2);
            }
        });
    }

    @Test
    public void testAliasesThreeBranchesAndConjunctResidualKeepTheirScopes() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String source = "(SELECT id,ta time FROM lp_sp_a UNION ALL SELECT id,tb other FROM lp_sp_b"
                    + " UNION ALL SELECT id,ta changed FROM lp_sp_a)";
            assertRowsAndIntervalScans("SELECT id FROM " + source + " WHERE time>='2024-01-03T00:00:00.000000Z' AND id<5 ORDER BY id",
                    "id\n3\n3\n3\n3\n4\n4\n", 3);
            assertRowsAndIntervalScans("SELECT id FROM " + source + " WHERE time='2024-01-02T00:00:00.000000Z'"
                            + " OR time='2024-01-04T00:00:00.000000Z' ORDER BY id",
                    "id\n2\n2\n2\n2\n2\n5\n", 3);
            assertLogicalResidual("SELECT id FROM " + source + " WHERE id>3 ORDER BY id",
                    "id\n4\n4\n5\n", 1);
        });
    }

    @Test
    public void testLimitAndLatestBranchesFilterAboveTheirBarriers() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sp_events (ts TIMESTAMP,id INT,k SYMBOL) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sp_events VALUES ('2024-01-01T00:00:00',1,'A'),('2024-01-01T01:00:00',2,'A'),('2024-01-01T02:00:00',3,'A')");
            execute("CREATE TABLE lp_sp_plain (ts TIMESTAMP,id INT,k SYMBOL) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sp_plain VALUES ('2024-01-01T01:00:00',30,'B')");
            final String limited = "SELECT id FROM (SELECT * FROM (SELECT ts,id FROM lp_sp_events LIMIT 1)"
                    + " UNION ALL SELECT ts,id FROM lp_sp_plain) WHERE hour(ts)>0 ORDER BY id";
            assertRowsAndIntervalScans(limited, "id\n30\n", 0);
            assertLogicalResidual(limited, "id\n30\n", 0);
            final String latest = " FROM (SELECT ts,id FROM lp_sp_events LATEST ON ts PARTITION BY k"
                    + " UNION ALL SELECT ts,id FROM lp_sp_plain) WHERE hour(ts)<2 ORDER BY id";
            assertRowsAndIntervalScans("SELECT ts,id" + latest, "ts\tid\n2024-01-01T01:00:00.000000Z\t30\n", 0);
            assertLogicalResidual("SELECT id" + latest, "id\n30\n", 0);
            final String setLimit = "SELECT id FROM (SELECT ts,id FROM lp_sp_events UNION ALL SELECT ts,id FROM lp_sp_plain LIMIT 1)"
                    + " WHERE hour(ts)>0 ORDER BY id";
            assertRowsAndIntervalScans(setLimit, "id\n", 0);
        });
    }

    @Test
    public void testPrecisionAndNullCastsDistributeIntoBranches() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sp_micro (ts TIMESTAMP,id INT) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_sp_nano (ts TIMESTAMP_NS,id INT) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sp_micro VALUES ('2024-01-01T00:00:00',1),('2024-01-01T01:00:00',2)");
            execute("INSERT INTO lp_sp_nano VALUES ('2024-01-01T00:00:00',3),('2024-01-01T02:00:00',4)");
            final String mixed = "SELECT id FROM (SELECT ts,id FROM lp_sp_micro UNION ALL SELECT ts,id FROM lp_sp_nano)"
                    + " WHERE hour(ts)>0 ORDER BY id";
            assertRowsAndIntervalScans(mixed, "id\n2\n4\n", 0);
            assertLogicalResidual(mixed, "id\n2\n4\n", 0);
            final String nulls = " FROM (SELECT ts,id FROM lp_sp_micro UNION ALL SELECT null ts,id FROM lp_sp_nano)"
                    + " WHERE hour(ts)>0 ORDER BY id";
            assertRowsAndIntervalScans("SELECT ts,id" + nulls, "ts\tid\n2024-01-01T01:00:00.000000Z\t2\n", 0);
            assertLogicalResidual("SELECT id" + nulls, "id\n2\n", 0);
            assertRowsAndIntervalScans("SELECT id FROM (SELECT ts,id FROM lp_sp_micro UNION ALL SELECT ts,id FROM lp_sp_nano)"
                    + " WHERE ts>'2024-01-01T00:00:00.000000000Z' ORDER BY id", "id\n2\n4\n", 2);
        });
    }

    @Test
    public void testMixedPrecisionTimestampBoundsBindPerBranch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sp_micro (ts TIMESTAMP,id INT) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_sp_nano (ts TIMESTAMP_NS,id INT) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sp_micro VALUES (1,5),('2024-01-01T00:00:00.000000Z',1),('2024-01-01T00:00:00.000001Z',2)");
            execute("INSERT INTO lp_sp_nano VALUES (1,6),('2024-01-01T00:00:00.000000001Z',3),('2024-01-01T00:00:00.000001000Z',4)");
            final String set = " FROM (SELECT id,ts FROM lp_sp_micro UNION ALL SELECT id,ts FROM lp_sp_nano)";
            final String nano = "'2024-01-01T00:00:00.000000001Z'";
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts=" + nano + " ORDER BY id", "id\n3\n", 1);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts>=" + nano + " ORDER BY id", "id\n2\n3\n4\n", 2);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts!=" + nano + " ORDER BY id", "id\n1\n2\n4\n5\n6\n", 1);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts IN (" + nano + ") ORDER BY id", "id\n3\n", 1);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts BETWEEN " + nano + " AND '2025' ORDER BY id", "id\n2\n3\n4\n", 2);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts=1 ORDER BY id", "id\n5\n6\n", 2);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts>=1 AND ts<" + nano + " AND id>0 ORDER BY id", "id\n1\n5\n6\n", 2);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts=" + nano + " OR id=5 ORDER BY id", "id\n3\n5\n", 0);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts+0L!=" + nano + " ORDER BY id", "id\n1\n2\n4\n5\n6\n", 0);
            assertRowsAndIntervalScans("SELECT id FROM (SELECT id,ts AS t FROM lp_sp_micro UNION ALL SELECT id,ts FROM lp_sp_nano) x"
                    + " WHERE x.t=" + nano + " ORDER BY id", "id\n3\n", 1);
            assertRowsAndIntervalScans("SELECT id FROM (SELECT id,ts FROM (SELECT id,ts FROM lp_sp_micro UNION ALL SELECT id,ts FROM lp_sp_nano))"
                    + " WHERE ts=" + nano + " ORDER BY id", "id\n3\n", 1);
            assertRowsAndIntervalScans("SELECT id FROM (SELECT id,ts FROM lp_sp_micro UNION ALL (SELECT id,ts FROM lp_sp_nano LIMIT 2))"
                    + " WHERE ts=" + nano + " ORDER BY id", "id\n3\n", 0);
            assertRowsAndIntervalScans("SELECT id FROM (SELECT id,ts FROM lp_sp_micro EXCEPT SELECT id,ts FROM lp_sp_nano)"
                    + " WHERE ts<" + nano + " ORDER BY id", "id\n1\n5\n", 2);
            assertRowsAndIntervalScans("SELECT a.id FROM lp_sp_micro a JOIN (SELECT id,ts FROM lp_sp_micro UNION ALL SELECT id,ts FROM lp_sp_nano) b"
                    + " ON a.id=b.id WHERE b.ts=" + nano, "id\n", 1);
            assertRowsAndIntervalScans("SELECT a.id,b.id FROM lp_sp_micro a LEFT JOIN (SELECT id,ts FROM lp_sp_micro UNION ALL SELECT id,ts FROM lp_sp_nano) b"
                    + " ON a.id=b.id AND b.ts=" + nano + " ORDER BY a.id", "id\tid1\n1\tnull\n2\tnull\n5\tnull\n", 0);
            bindVariableService.clear();
            bindVariableService.setLong(0, 1);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts=$1 ORDER BY id", "id\n5\n6\n", 2);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts>$1 ORDER BY id", "id\n1\n2\n3\n4\n", 2);
            assertRowsAndIntervalScans("SELECT id FROM lp_sp_micro WHERE ts=$1", "id\n5\n", 1);
            assertRowsAndIntervalScans("SELECT id FROM lp_sp_nano WHERE ts!=$1 ORDER BY id", "id\n3\n4\n", 1);
            assertRowsAndIntervalScans("SELECT id FROM (SELECT id,ts FROM lp_sp_micro UNION ALL SELECT id,ts FROM lp_sp_micro) WHERE ts=$1",
                    "id\n5\n5\n", 2);
        });
    }

    @Test
    public void testRetainedBranchPredicatesSurviveCompilerReuseAndParameterRebind() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setTimestamp(0, 1_704_153_600_000_000L);
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT id FROM (SELECT ta ts,id FROM lp_sp_a UNION ALL SELECT tb ts,id FROM lp_sp_b)"
                            + " WHERE ts>=$1 AND hour(ts)=0 ORDER BY id", sqlExecutionContext).getRecordCursorFactory();
                    assertRows(retained, "id\n2\n2\n2\n3\n3\n4\n4\n5\n");
                    try (RecordCursorFactory ignored = compiler.compile("SELECT id FROM lp_sp_a WHERE id>1", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                    compiler.clear();
                }
                bindVariableService.setTimestamp(0, 1_704_240_000_000_000L);
                assertRows(retained, "id\n3\n3\n4\n4\n5\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testSharedDefinitionsKeepConsumerPredicatesIndependent() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsAndIntervalScans("WITH q AS (SELECT ta ts,id FROM lp_sp_a UNION ALL SELECT tb ts,id FROM lp_sp_b)"
                            + " SELECT * FROM (SELECT id FROM q WHERE ts='2024-01-02T00:00:00.000000Z'"
                            + " UNION ALL SELECT id FROM q WHERE ts='2024-01-04T00:00:00.000000Z') ORDER BY id",
                    "id\n2\n2\n2\n5\n", 4);
        });
    }

    private static int countSetResiduals(LogicalPlan plan) {
        int count = plan instanceof FilterPlan filter && filter.getInput() instanceof SetOperationPlan ? 1 : 0;
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            count += countSetResiduals(plan.inputAt(i));
        }
        return count;
    }

    private void assertRowsAndIntervalScans(String sql, String expected, int intervalScans) throws Exception {
        final int jitMode = sqlExecutionContext.getJitMode();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (RecordCursorFactory factory = select(sql)) {
            assertRows(factory, expected);
            final TextPlanSink sink = new TextPlanSink();
            sink.of(factory, sqlExecutionContext);
            int count = 0;
            for (int line = 1; line <= sink.getLineCount(); line++) {
                if (sink.getLine(line).toString().contains("Interval forward scan")) {
                    count++;
                }
            }
            Assert.assertEquals(sql + "\n" + sink.getSink(), intervalScans, count);
        } finally {
            sqlExecutionContext.setJitMode(jitMode);
        }
    }

    private void assertLogicalResidual(String sql, String expected, int expectedResiduals) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertEquals(expectedResiduals, countSetResiduals(compiler.getPlanForTesting()));
                assertRows(factory, expected);
            }
        }
    }

    private void assertRows(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_sp_a (ta TIMESTAMP,id INT) TIMESTAMP(ta) PARTITION BY DAY");
        execute("CREATE TABLE lp_sp_b (tb TIMESTAMP,id INT) TIMESTAMP(tb) PARTITION BY DAY");
        execute("INSERT INTO lp_sp_a VALUES ('2024-01-01',1),('2024-01-02',2),('2024-01-02',2),('2024-01-03',3),('2024-01-03',3)");
        execute("INSERT INTO lp_sp_b VALUES ('2024-01-02',2),('2024-01-03',4),('2024-01-03',4),('2024-01-04',5)");
    }
}
