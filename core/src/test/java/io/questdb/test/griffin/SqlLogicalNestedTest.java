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
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalNestedTest extends AbstractCairoTest {
    @Test
    public void testAliasedAndUnnamedDerivedSources() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertNested("SELECT q.id FROM (SELECT id,unused FROM lp_nested) q ORDER BY q.id", "id\n1\n2\n3\n4\n", 1);
            assertNested("SELECT id FROM (SELECT id,unused FROM lp_nested) ORDER BY id", "id\n1\n2\n3\n4\n", 1);
            assertNested("SELECT q.* FROM (SELECT id,label FROM lp_nested) q ORDER BY q.id", "id\tlabel\n1\ta\n2\tb\n3\tc\n4\td\n", 2);
        });
    }

    @Test
    public void testDuplicateTimestampProjectionMetadata() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertNested("SELECT b FROM (SELECT ts AS a,ts AS b FROM lp_nested)", """
                    b
                    2020-01-01T00:00:00.000000Z
                    2020-01-01T00:00:01.000000Z
                    2020-01-01T00:00:02.000000Z
                    2020-01-01T00:00:03.000000Z
                    """, 1);
        });
    }

    @Test
    public void testFactorySurvivesCompilerResetAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("""
                            SELECT q.adjusted,q.label
                            FROM (SELECT id+1 AS adjusted,label,ts FROM lp_nested ORDER BY ts DESC LIMIT 3) q
                            WHERE q.adjusted>2 ORDER BY q.adjusted
                            """, sqlExecutionContext).getRecordCursorFactory();
                    assertResult(retained, "adjusted\tlabel\n3\tb\n5\td\n");
                    try (RecordCursorFactory other = compiler.compile(
                            "SELECT id+2 AS later FROM (SELECT id FROM lp_nested WHERE active) ORDER BY later", sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        assertResult(other, "later\n4\n5\n6\n");
                    }
                    compiler.clear();
                    assertResult(retained, "adjusted\tlabel\n3\tb\n5\td\n");
                }
                assertResult(retained, "adjusted\tlabel\n3\tb\n5\td\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testInvalidQualifiedWildcardFails() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT wrong.* FROM (SELECT id FROM lp_nested) q").noLeakCheck().fails(7, "invalid table alias");
        });
    }

    @Test
    public void testInvalidQualifiedWildcardOverUnaliasedJoinSourcesFails() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT z.*, count() FROM (SELECT 1 a) CROSS JOIN (SELECT 2 b)").noLeakCheck().fails(7, "invalid table alias");
            assertQuery("SELECT z.*, count() FROM (SELECT 1 a) JOIN (SELECT 2 b) ON a = b").noLeakCheck().fails(7, "invalid table alias");
        });
    }

    @Test
    public void testNestedOrderAndLimitPreserveBarriers() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertNested("""
                    SELECT id FROM (SELECT id,ts FROM lp_nested ORDER BY ts DESC LIMIT 3)
                    ORDER BY id LIMIT 2
                    """, "id\n1\n2\n", 2);
            assertNested("""
                    SELECT id FROM (SELECT id FROM lp_nested ORDER BY id DESC LIMIT 3)
                    WHERE id<4 ORDER BY id
                    """, "id\n2\n3\n", 1);
        });
    }

    @Test
    public void testPruningRetainsHiddenOrderDependencies() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertNested("""
                    SELECT q.id FROM
                    (SELECT unused,id,label FROM lp_nested ORDER BY ts DESC LIMIT 3) q
                    ORDER BY q.id
                    """, "id\n1\n2\n4\n", 2);
            assertNested("""
                    SELECT q.id FROM
                    (SELECT unused,id,label FROM lp_nested ORDER BY id+1 DESC LIMIT 2) q
                    ORDER BY q.id
                    """, "id\n3\n4\n", 1);
        });
    }

    @Test
    public void testRepeatedAliasesRemainLocalToTheirScope() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertNested("""
                    SELECT q.value FROM
                    (SELECT q.id AS value FROM (SELECT id FROM lp_nested) q) q
                    ORDER BY q.value
                    """, "value\n1\n2\n3\n4\n", 1);
        });
    }

    @Test
    public void testWhereOverComputedChildRetainsPredicateInputs() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertNested("""
                    SELECT q.next FROM
                    (SELECT id+1 AS next,id+2 AS tested,unused FROM lp_nested) q
                    WHERE q.tested>4 ORDER BY q.next
                    """, "next\n4\n5\n", 1);
        });
    }

    private void assertNested(String sql, String expected, int expectedSourceColumnCount) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            LogicalPlan source = compiler.getLogicalPlanForTesting();
            while (source.inputCount() > 0) {
                source = source.inputAt(0);
            }
            Assert.assertEquals(expectedSourceColumnCount, source.getOutput().getColumnCount());
            Assert.assertEquals(-1, source.getOutput().getColumnIndexQuiet("unused"));
            assertResult(factory, expected);
        }
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("""
                CREATE TABLE lp_nested (unused INT,id INT,active BOOLEAN,label STRING,ts TIMESTAMP)
                TIMESTAMP(ts)
                """);
        execute("""
                INSERT INTO lp_nested VALUES
                    (10,3,TRUE,'c','2020-01-01T00:00:00.000000Z'),
                    (20,1,FALSE,'a','2020-01-01T00:00:01.000000Z'),
                    (30,2,TRUE,'b','2020-01-01T00:00:02.000000Z'),
                    (40,4,TRUE,'d','2020-01-01T00:00:03.000000Z')
                """);
    }
}
