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
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts=1 ORDER BY id", "id\n6\n", 1);
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
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts=$1 ORDER BY id", "id\n6\n", 1);
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts>$1 ORDER BY id", "id\n1\n2\n3\n4\n5\n", 1);
            assertRowsAndIntervalScans("SELECT id FROM lp_sp_micro WHERE ts=$1", "id\n5\n", 1);
            assertRowsAndIntervalScans("SELECT id FROM lp_sp_nano WHERE ts!=$1 ORDER BY id", "id\n3\n4\n", 1);
            assertRowsAndIntervalScans("SELECT id FROM (SELECT id,ts FROM lp_sp_micro UNION ALL SELECT id,ts FROM lp_sp_micro) WHERE ts=$1",
                    "id\n5\n5\n", 2);
        });
    }

    @Test
    public void testNanosecondFloorStridesBindPerBranchOnlyWhenExact() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sp_values (t TIMESTAMP)");
            execute("INSERT INTO lp_sp_values VALUES (-1001),(-1000),(-999),(-1),(0),(1),(999),(1000),(1001),(NULL)");
            assertFloorPrecisionMismatches("n", "");
            assertFloorPrecisionMismatches("1000n", "");
            assertFloorPrecisionMismatches("2000n", "");
            assertFloorPrecisionMismatches("500n", "");
            assertFloorPrecisionMismatches("3n", "-1001\n-1000\n-1\n1\n1000\n1001\n");
            assertFloorPrecisionMismatches("1500n", "-1000\n-1\n1001\n");

            execute("CREATE TABLE lp_sp_micro (ts TIMESTAMP,id INT) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_sp_nano (ts TIMESTAMP_NS,id INT) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sp_micro VALUES (0,1),(1,2),(2,3),(999,4),(1000,5),(1001,6)");
            execute("INSERT INTO lp_sp_nano VALUES (0,11),(1,12),(999,13),(1000,14),(1001,15),(1500,16)");
            execute("CREATE TABLE lp_sp_one (x INT)");
            execute("INSERT INTO lp_sp_one VALUES (1)");
            final String set = "(SELECT ts c,id FROM lp_sp_micro UNION ALL SELECT ts,id FROM lp_sp_nano)";
            assertNanosecondFloorBinding(set, "n", "1970-01-01T00:00:00.000001Z", "2\n14\n", 2, 0);
            assertNanosecondFloorBinding(set, "1000n", "1970-01-01T00:00:00.000001Z", "2\n14\n15\n16\n", 2, 0);
            assertNanosecondFloorBinding(set, "2000n", "1970-01-01T00:00:00.000000Z", "1\n2\n11\n12\n13\n14\n15\n16\n", 2, 0);
            assertNanosecondFloorBinding(set, "500n", "1970-01-01T00:00:00.000001Z", "2\n14\n15\n", 1, 0);
            assertNanosecondFloorBinding(set, "3n", "1970-01-01T00:00:00.000000999Z", "2\n13\n14\n15\n", 1, 1);
            assertNanosecondFloorBinding(set, "1500n", "1970-01-01T00:00:00.000001500Z", "3\n16\n", 1, 1);
        });
    }

    @Test
    public void testNullBranchFiltersOverUnifiedType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sp_micro (ts TIMESTAMP,id INT) TIMESTAMP(ts)");
            execute("INSERT INTO lp_sp_micro VALUES ('2024-01-01T00:00:00',1),('2024-01-01T01:00:00',2)");
            final String set = " FROM (SELECT ts,id FROM lp_sp_micro UNION ALL SELECT null,id+2 FROM lp_sp_micro)";
            assertQuery("SELECT id" + set + " WHERE typeOf(ts)='TIMESTAMP' ORDER BY id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\n1\n2\n3\n4\n");
            assertQuery("SELECT id" + set + " WHERE ts IS NULL ORDER BY id")
                    .noLeakCheck()
                    .returns("id\n3\n4\n");
            assertRowsAndIntervalScans("SELECT id" + set + " WHERE ts>'2024-01-01T00:30' ORDER BY id", "id\n2\n", 1);
            assertLogicalResidual("SELECT id" + set + " WHERE ts>'2024-01-01T00:30' ORDER BY id", "id\n2\n", 0);
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
    public void testPrecisionDependentFiltersBindOverUnifiedType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sp_micro (ts TIMESTAMP,id INT,d DATE) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_sp_nano (ts TIMESTAMP_NS,id INT) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO lp_sp_micro VALUES
                        (1,1,'1970-01-01T00:00:00.001Z'),
                        ('2024-01-01T00:00:00.000000Z',2,'2024-01-01T00:00:00.000Z'),
                        ('2024-01-01T00:00:00.000001Z',3,NULL)
                    """);
            execute("INSERT INTO lp_sp_nano VALUES (1,11),('2024-01-01T00:00:00.000000001Z',12),('2024-01-01T00:00:00.000001000Z',13)");
            execute("CREATE TABLE lp_sp_one (x INT)");
            execute("INSERT INTO lp_sp_one VALUES (1)");
            final String precisions = "(SELECT ts c,id FROM lp_sp_micro UNION ALL SELECT ts,id FROM lp_sp_nano)";
            assertPrecisionFilter(precisions, "typeOf({c})='TIMESTAMP'", "");
            assertPrecisionFilter(precisions, "{c}::STRING LIKE '%000Z'", "1\n2\n3\n13\n");
            assertPrecisionFilter(precisions, "{c}=1", "11\n");
            assertPrecisionFilter(precisions, "{c}::LONG=1000", "1\n");
            final String dates = "(SELECT d c,id FROM lp_sp_micro UNION ALL SELECT ts,id+10 FROM lp_sp_micro)";
            assertPrecisionFilter(dates, "typeOf({c})='TIMESTAMP'", "1\n2\n3\n11\n12\n13\n");
            assertPrecisionFilter(dates, "{c}::STRING LIKE '%000Z'", "1\n2\n12\n");
            assertPrecisionFilter(dates, "{c}=1", "11\n");
            assertPrecisionFilter(dates, "{c}::LONG=1000", "1\n");
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE c::LONG=1000 ORDER BY id", "id\n1\n", 1);
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE year(c)=2024 AND hour(c)=0 ORDER BY id", "id\n2\n3\n12\n13\n", 0);
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE c>1+1 ORDER BY id", "id\n1\n2\n3\n12\n13\n", 1);
            assertRowsAndIntervalScans("SELECT id FROM " + precisions + " WHERE c>dateadd('y',-100,now()) ORDER BY id",
                    "id\n1\n2\n3\n11\n12\n13\n", 2);
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE c<'2024-01-01'::TIMESTAMP_NS+1 ORDER BY id", "id\n1\n2\n11\n", 0);
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE timestamp_floor('h',c)='2024-01-01' ORDER BY id", "id\n2\n3\n12\n13\n", 0);
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE dateadd('h',1,c)>'2024-01-01T00:30' ORDER BY id", "id\n2\n3\n12\n13\n", 0);
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE extract(hour FROM c)=0 ORDER BY id", "id\n1\n2\n3\n11\n12\n13\n", 0);
            assertPrecisionFilter(precisions, "dateadd('n',1,{c})='2024-01-01T00:00:00.000000001Z'", "2\n");
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE dateadd('n',1,c)='2024-01-01T00:00:00.000000001Z' ORDER BY id", "id\n2\n", 1);
            assertPrecisionFilter(precisions, "timestamp_floor('3n',{c})='1970-01-01T00:00:00.000000999Z'::TIMESTAMP_NS", "1\n");
            assertLogicalResidual("SELECT id FROM " + precisions + " WHERE timestamp_floor('3n',c)='1970-01-01T00:00:00.000000999Z'::TIMESTAMP_NS ORDER BY id",
                    "id\n1\n", 1);
            assertPrecisionFilter(precisions, "timestamp_floor('3U',{c})='1970-01-01T00:00:00.000000Z'", "1\n11\n");
            assertPrecisionFilter(precisions, "timestamp_ceil('n',{c})='1970-01-01T00:00:00.000001Z'", "1\n");
            assertPrecisionFilter(precisions, "date_trunc('nanosecond',{c})='1970-01-01T00:00:00.000001Z'", "1\n");
            assertPrecisionFilter(precisions, "to_utc({c},'Europe/Berlin')='1969-12-31T23:00:00.000001Z'", "1\n");
            assertPrecisionFilter(precisions, "to_timezone({c},'Europe/Berlin')='1970-01-01T01:00:00.000001Z'", "1\n");
            bindVariableService.clear();
            bindVariableService.setStr(0, "2024-01-01T00:00:00.000000001Z");
            assertRowsAndIntervalScans("SELECT id FROM " + precisions + " WHERE c>=$1 ORDER BY id", "id\n3\n12\n13\n", 2);
            assertLogicalResidual("SELECT id FROM " + dates + " WHERE c>=$1 ORDER BY id", "id\n13\n", 1);
            assertPrecisionFilter(dates, "{c}>=$1", "13\n");
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
                    assertRowsOnly(retained, "id\n2\n2\n2\n3\n3\n4\n4\n5\n");
                    try (RecordCursorFactory ignored = compiler.compile("SELECT id FROM lp_sp_a WHERE id>1", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                    compiler.clear();
                }
                bindVariableService.setTimestamp(0, 1_704_240_000_000_000L);
                assertRowsOnly(retained, "id\n3\n3\n4\n4\n5\n");
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

    @Test
    public void testTextAndTimestampFiltersBindOverUnifiedType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_sp_ts (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO lp_sp_ts VALUES ('2024-01-01T03:00:00'),('2024-01-02T02:00:00')");
            execute("CREATE TABLE lp_sp_text (sym SYMBOL,s STRING,v VARCHAR,u UUID,ip IPv4)");
            execute("""
                    INSERT INTO lp_sp_text VALUES
                        ('1q','1q','1q','10000000-0000-0000-0000-000000000000','1.1.1.1'),
                        ('q','q','q','f0000000-0000-0000-0000-000000000000','9.9.9.9')
                    """);
            execute("CREATE TABLE lp_sp_one (x INT)");
            execute("INSERT INTO lp_sp_one VALUES (1)");
            final ObjList<String> columns = new ObjList<>("sym", "s", "v", "u", "ip");
            final ObjList<String> lows = new ObjList<>("1q", "1q", "1q", "10000000-0000-0000-0000-000000000000", "1.1.1.1");
            final ObjList<String> highs = new ObjList<>("q", "q", "q", "f0000000-0000-0000-0000-000000000000", "9.9.9.9");
            for (int i = 0, n = columns.size(); i < n; i++) {
                final String column = columns.getQuick(i);
                final String high = highs.getQuick(i);
                final String in = " IN ('2024-01-02T02','" + high + "')";
                for (int order = 0; order < 2; order++) {
                    final String set = order == 0
                            ? "(SELECT " + column + " c FROM lp_sp_text UNION ALL SELECT ts FROM lp_sp_ts)"
                            : "(SELECT ts c FROM lp_sp_ts UNION ALL SELECT " + column + " FROM lp_sp_text)";
                    assertQuery("SELECT c, c>'2024-01-02T02' gt, c='2024-01-02T02' eq, c" + in + " inl, c LIKE '2024-01-02%' lk FROM "
                            + set + " ORDER BY c")
                            .noLeakCheck()
                            .expectSize()
                            .returns("c\tgt\teq\tinl\tlk\n"
                                    + lows.getQuick(i) + "\tfalse\tfalse\tfalse\tfalse\n"
                                    + "2024-01-01T03:00:00.000000Z\tfalse\tfalse\tfalse\tfalse\n"
                                    + "2024-01-02T02:00:00.000000Z\ttrue\tfalse\tfalse\ttrue\n"
                                    + high + "\ttrue\tfalse\ttrue\tfalse\n");
                    assertUnifiedFilter(set, ">'2024-01-02T02'", "2024-01-02T02:00:00.000000Z\n" + high + "\n");
                    assertUnifiedFilter(set, "='2024-01-02T02'", "");
                    assertUnifiedFilter(set, in, high + "\n");
                    assertUnifiedFilter(set, " LIKE '2024-01-02%'", "2024-01-02T02:00:00.000000Z\n");
                }
            }
        });
    }

    private static int countSetResiduals(LogicalPlan plan) {
        int count = plan instanceof FilterPlan filter && filter.getChild() instanceof SetOperationPlan ? 1 : 0;
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            count += countSetResiduals(plan.inputAt(i));
        }
        return count;
    }

    private void assertFloorPrecisionMismatches(String stride, String expected) throws Exception {
        assertQuery("SELECT t::LONG v FROM lp_sp_values WHERE timestamp_floor('" + stride + "',t)!=timestamp_floor('" + stride + "',t::TIMESTAMP_NS) ORDER BY v")
                .noLeakCheck()
                .returns("v\n" + expected);
    }

    private void assertLogicalResidual(String sql, String expected, int expectedResiduals) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertEquals(expectedResiduals, countSetResiduals(compiler.getPlanForTesting()));
                assertRowsOnly(factory, expected);
            }
        }
    }

    private void assertNanosecondFloorBinding(String set, String stride, String floor, String expected, int intervalScans, int residuals) throws Exception {
        final String predicate = "timestamp_floor('" + stride + "',{c})='" + floor + "'";
        assertPrecisionFilter(set, predicate, expected);
        final String sql = "SELECT id FROM " + set + " WHERE " + predicate.replace("{c}", "c") + " ORDER BY id";
        assertRowsAndIntervalScans(sql, "id\n" + expected, intervalScans);
        assertLogicalResidual(sql, "id\n" + expected, residuals);
    }

    private void assertPrecisionFilter(String set, String predicate, String expected) throws Exception {
        final String rows = "id\n" + expected;
        assertQuery("SELECT id FROM " + set + " WHERE " + predicate.replace("{c}", "c") + " ORDER BY id")
                .noLeakCheck()
                .sizeMayVary()
                .returns(rows);
        assertQuery("SELECT u.id FROM " + set + " u JOIN lp_sp_one k ON " + predicate.replace("{c}", "u.c") + " ORDER BY u.id")
                .noLeakCheck()
                .sizeMayVary()
                .returns(rows);
        assertQuery("SELECT id FROM (SELECT id," + predicate.replace("{c}", "c") + " k FROM " + set + ") WHERE k ORDER BY id")
                .noLeakCheck()
                .sizeMayVary()
                .returns(rows);
    }

    private void assertRowsAndIntervalScans(String sql, String expected, int intervalScans) throws Exception {
        final int jitMode = sqlExecutionContext.getJitMode();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (RecordCursorFactory factory = select(sql)) {
            assertRowsOnly(factory, expected);
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

    private void assertUnifiedFilter(String set, String predicate, String expected) throws Exception {
        final String rows = "c\n" + expected;
        assertQuery("SELECT c FROM " + set + " WHERE c" + predicate + " ORDER BY c")
                .noLeakCheck()
                .returns(rows);
        assertQuery("SELECT c FROM (SELECT c::STRING c FROM " + set + ") WHERE c" + predicate + " ORDER BY c")
                .noLeakCheck()
                .returns(rows);
        assertQuery("SELECT u.c FROM " + set + " u JOIN lp_sp_one k ON u.c" + predicate + " ORDER BY u.c")
                .noLeakCheck()
                .returns(rows);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_sp_a (ta TIMESTAMP,id INT) TIMESTAMP(ta) PARTITION BY DAY");
        execute("CREATE TABLE lp_sp_b (tb TIMESTAMP,id INT) TIMESTAMP(tb) PARTITION BY DAY");
        execute("INSERT INTO lp_sp_a VALUES ('2024-01-01',1),('2024-01-02',2),('2024-01-02',2),('2024-01-03',3),('2024-01-03',3)");
        execute("INSERT INTO lp_sp_b VALUES ('2024-01-02',2),('2024-01-03',4),('2024-01-03',4),('2024-01-04',5)");
    }
}
