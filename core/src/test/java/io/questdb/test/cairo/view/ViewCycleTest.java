/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.view;

import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.fail;

public class ViewCycleTest extends AbstractViewTest {

    @Test
    public void testAlterViewCreatesCycleViaJoin() throws Exception {
        // v1 -> table1
        // v2 -> v1
        // ALTER v1 -> table1 JOIN v2 (creates cycle via join)
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            createView(VIEW1, "select * from " + TABLE1);
            createView(VIEW2, "select * from " + VIEW1);

            assertView1AlterFailure("select * from " + TABLE2 + " t2 JOIN " + VIEW2 + " v2 ON t2.k = v2.k");
        });
    }

    @Test
    public void testAlterViewCreatesCycleViaLeftJoin() throws Exception {
        // v1 -> table1
        // v2 -> v1
        // ALTER v1 -> table1 LEFT JOIN v2 (creates cycle via left join)
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            createView(VIEW1, "select * from " + TABLE1);
            createView(VIEW2, "select * from " + VIEW1);

            assertView1AlterFailure("select * from " + TABLE2 + " t2 LEFT JOIN " + VIEW2 + " v2 ON t2.k = v2.k");
        });
    }

    @Test
    public void testAlterViewCreatesCycleViaSubquery() throws Exception {
        // v1 -> table1
        // v2 -> v1
        // ALTER v1 -> (select * from v2) subquery (creates cycle)
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            createView(VIEW1, "select * from " + TABLE1);
            createView(VIEW2, "select * from " + VIEW1);

            assertView1AlterFailure("select * from (select * from " + VIEW2 + ")");
        });
    }

    @Test
    public void testAlterViewCreatesCycleViaUnion() throws Exception {
        // v1 -> table1
        // v2 -> v1
        // ALTER v1 -> table1 UNION v2 (creates cycle via union)
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            createView(VIEW1, "select * from " + TABLE1);
            createView(VIEW2, "select * from " + VIEW1);

            assertView1AlterFailure("select * from " + TABLE1 + " UNION select * from " + VIEW2);
        });
    }

    @Test
    public void testAlterViewCreatesSelfReference() throws Exception {
        // Direct self-reference: v1 -> v1
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            String originalSql = "select * from " + TABLE1;
            createView(VIEW1, originalSql);

            String sqlBefore = getView1DefinitionSql();

            try {
                execute("ALTER VIEW " + VIEW1 + " AS (select * from " + VIEW1 + ")");
                Assert.fail("Expected cycle detection to prevent this");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "circular dependency detected: view 'view1' cannot reference itself");
            }

            assertViewDefinition(VIEW1, sqlBefore, TABLE1);

            // the view still work with the original definition
            assertQueryNoLeakCheck(
                    """
                            ts	k	k2	v
                            1970-01-01T00:00:00.000000Z	k0	k2_0	0
                            1970-01-01T00:00:10.000000Z	k1	k2_1	1
                            1970-01-01T00:00:20.000000Z	k2	k2_2	2
                            1970-01-01T00:00:30.000000Z	k3	k2_3	3
                            1970-01-01T00:00:40.000000Z	k4	k2_4	4
                            1970-01-01T00:00:50.000000Z	k5	k2_5	5
                            1970-01-01T00:01:00.000000Z	k6	k2_6	6
                            1970-01-01T00:01:10.000000Z	k7	k2_7	7
                            1970-01-01T00:01:20.000000Z	k8	k2_8	8
                            """,
                    VIEW1,
                    null,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testAlterViewCreatesSelfReferenceViaJoin() throws Exception {
        // Direct self-reference via join: v1 JOIN v1
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createView(VIEW1, "select * from " + TABLE1);

            assertView1AlterFailure("select * from " + TABLE1 + " t1 JOIN " + VIEW1 + " v1 ON t1.k = v1.k");
        });
    }

    @Test
    public void testAlterViewCreatesSelfReferenceViaUnion() throws Exception {
        // Direct self-reference via union: table1 UNION v1
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createView(VIEW1, "select * from " + TABLE1);

            assertView1AlterFailure("select * from " + TABLE1 + " UNION select * from " + VIEW1);
        });
    }

    @Test
    public void testAlterViewCreatesThreeLevelCycle() throws Exception {
        // v1 -> table1
        // v2 -> v1
        // v3 -> v2
        // ALTER v1 -> v3  (creates cycle: v1 -> v3 -> v2 -> v1)
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            createView(VIEW1, "select * from " + TABLE1);
            createView(VIEW2, "select * from " + VIEW1);
            createView(VIEW3, "select * from " + VIEW2);
            drainWalAndViewQueues();

            String sqlBefore = getView1DefinitionSql();

            try {
                execute("ALTER VIEW " + VIEW1 + " AS (select * from " + VIEW3 + ")");
                fail("Expected cycle detection to prevent this");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "circular dependency detected: 'view1' cannot depend on 'view3' because 'view3' already depends on 'view1'");
            }

            assertViewDefinition(VIEW1, sqlBefore, TABLE1);

            String expected = """
                    ts	k	k2	v
                    1970-01-01T00:00:00.000000Z	k0	k2_0	0
                    1970-01-01T00:00:10.000000Z	k1	k2_1	1
                    1970-01-01T00:00:20.000000Z	k2	k2_2	2
                    1970-01-01T00:00:30.000000Z	k3	k2_3	3
                    1970-01-01T00:00:40.000000Z	k4	k2_4	4
                    1970-01-01T00:00:50.000000Z	k5	k2_5	5
                    1970-01-01T00:01:00.000000Z	k6	k2_6	6
                    1970-01-01T00:01:10.000000Z	k7	k2_7	7
                    1970-01-01T00:01:20.000000Z	k8	k2_8	8
                    """;

            // all views still work with original definitions
            assertQueryNoLeakCheck(
                    expected,
                    VIEW1,
                    null,
                    "ts",
                    true,
                    true
            );
            assertQueryNoLeakCheck(
                    expected,
                    VIEW2,
                    null,
                    "ts",
                    true,
                    true
            );
            assertQueryNoLeakCheck(
                    expected,
                    VIEW3,
                    null,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testAlterViewCreatesTwoLevelCycle() throws Exception {
        // v1 -> table1 (initially)
        // v2 -> v1
        // ALTER v1 -> v2  (creates cycle: v1 -> v2 -> v1)
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            createView(VIEW1, "select * from " + TABLE1);
            createView(VIEW2, "select * from " + VIEW1);

            String sqlBefore = getView1DefinitionSql();

            assertExceptionNoLeakCheck("ALTER VIEW " + VIEW1 + " AS (select * from " + VIEW2 + ")", 20, "circular dependency detected: 'view1' cannot depend on 'view2' because 'view2' already depends on 'view1'", sqlExecutionContext);
            assertViewDefinition(VIEW1, sqlBefore, TABLE1);

            String expected = """
                    ts	k	k2	v
                    1970-01-01T00:00:00.000000Z	k0	k2_0	0
                    1970-01-01T00:00:10.000000Z	k1	k2_1	1
                    1970-01-01T00:00:20.000000Z	k2	k2_2	2
                    1970-01-01T00:00:30.000000Z	k3	k2_3	3
                    1970-01-01T00:00:40.000000Z	k4	k2_4	4
                    1970-01-01T00:00:50.000000Z	k5	k2_5	5
                    1970-01-01T00:01:00.000000Z	k6	k2_6	6
                    1970-01-01T00:01:10.000000Z	k7	k2_7	7
                    1970-01-01T00:01:20.000000Z	k8	k2_8	8
                    """;

            assertQueryNoLeakCheck(
                    expected,
                    VIEW1,
                    null,
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    expected,
                    VIEW2,
                    null,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testAlterViewCycleWithMultipleViewsInQuery() throws Exception {
        // v1 -> table1
        // v2 -> v1
        // v3 -> table1
        // ALTER v1 -> v2 JOIN v3 (cycle through v2)
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            createView(VIEW1, "select * from " + TABLE1);
            createView(VIEW2, "select * from " + VIEW1);
            createView(VIEW3, "select * from " + TABLE1);
            drainWalAndViewQueues();

            assertView1AlterFailure("select v2.ts, v2.k, v2.v from " + VIEW2 + " v2 JOIN " + VIEW3 + " v3 ON v2.k = v3.k");
        });
    }

    @Test
    public void testAlterViewNoCycleWithJoin() throws Exception {
        // Valid case: v1 joins two independent tables/views - no cycle
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            createView(VIEW1, "select * from " + TABLE1);
            createView(VIEW2, "select * from " + TABLE2);

            // ALTER v1 to join table1 and view2 - this is valid (no cycle)
            execute("ALTER VIEW " + VIEW1 + " AS (select t1.ts, t1.k, t1.v from " + TABLE1 + " t1 JOIN " + VIEW2 + " v2 ON t1.k = v2.k)");

            // Verify the view works
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tv
                            1970-01-01T00:00:00.000000Z\tk0\t0
                            1970-01-01T00:00:10.000000Z\tk1\t1
                            1970-01-01T00:00:20.000000Z\tk2\t2
                            1970-01-01T00:00:30.000000Z\tk3\t3
                            1970-01-01T00:00:40.000000Z\tk4\t4
                            1970-01-01T00:00:50.000000Z\tk5\t5
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    VIEW1,
                    null,
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testAlterViewNoCycleWithNestedViews() throws Exception {
        // Valid case: deep nesting without cycles
        // v1 -> table1, v2 -> v1, v3 -> v2
        // ALTER v1 -> table1 (just change query, still no cycle)
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            createView(VIEW1, "select * from " + TABLE1);
            createView(VIEW2, "select * from " + VIEW1);
            createView(VIEW3, "select * from " + VIEW2);
            drainWalAndViewQueues();

            // ALTER v1 to a different query on table1 - still valid
            execute("ALTER VIEW " + VIEW1 + " AS (select ts, k, v from " + TABLE1 + " where v > 3)");

            // All views should still work
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tv
                            1970-01-01T00:00:40.000000Z\tk4\t4
                            1970-01-01T00:00:50.000000Z\tk5\t5
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    VIEW1,
                    null,
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testAlterViewNoCycleWithUnion() throws Exception {
        // Valid case: v1 unions two independent sources - no cycle
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            createView(VIEW1, "select * from " + TABLE1);
            createView(VIEW2, "select * from " + TABLE2);

            // ALTER v1 to union table1 and view2 - this is valid (no cycle)
            execute("ALTER VIEW " + VIEW1 + " AS (select * from " + TABLE1 + " UNION ALL select * from " + VIEW2 + ")");

            // Verify the view works (should have rows from both tables)
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tk2\tv
                            1970-01-01T00:00:00.000000Z\tk0\tk2_0\t0
                            1970-01-01T00:00:10.000000Z\tk1\tk2_1\t1
                            1970-01-01T00:00:20.000000Z\tk2\tk2_2\t2
                            1970-01-01T00:00:30.000000Z\tk3\tk2_3\t3
                            1970-01-01T00:00:40.000000Z\tk4\tk2_4\t4
                            1970-01-01T00:00:50.000000Z\tk5\tk2_5\t5
                            1970-01-01T00:01:00.000000Z\tk6\tk2_6\t6
                            1970-01-01T00:01:10.000000Z\tk7\tk2_7\t7
                            1970-01-01T00:01:20.000000Z\tk8\tk2_8\t8
                            1970-01-01T00:00:00.000000Z\tk0\tk2_0\t0
                            1970-01-01T00:00:10.000000Z\tk1\tk2_1\t1
                            1970-01-01T00:00:20.000000Z\tk2\tk2_2\t2
                            1970-01-01T00:00:30.000000Z\tk3\tk2_3\t3
                            1970-01-01T00:00:40.000000Z\tk4\tk2_4\t4
                            1970-01-01T00:00:50.000000Z\tk5\tk2_5\t5
                            1970-01-01T00:01:00.000000Z\tk6\tk2_6\t6
                            1970-01-01T00:01:10.000000Z\tk7\tk2_7\t7
                            1970-01-01T00:01:20.000000Z\tk8\tk2_8\t8
                            """,
                    VIEW1,
                    null,
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testCreateViewCycleAtCreationTime() throws Exception {
        // v1 -> table1
        // Create v2 -> v1
        // Create v3 -> v2
        // Try to alter v1 -> v3 (would create cycle)
        // Then create v4 that references v3 - should work
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            createView(VIEW1, "select * from " + TABLE1);
            createView(VIEW2, "select * from " + VIEW1);
            createView(VIEW3, "select * from " + VIEW2);
            drainWalAndViewQueues();

            // Creating v4 -> v3 should work (no cycle)
            execute("CREATE VIEW view4 AS (select * from " + VIEW3 + ")");
            drainWalAndViewQueues();

            // Verify view4 works
            assertQueryNoLeakCheck(
                    """
                            ts	k	k2	v
                            1970-01-01T00:00:00.000000Z	k0	k2_0	0
                            1970-01-01T00:00:10.000000Z	k1	k2_1	1
                            1970-01-01T00:00:20.000000Z	k2	k2_2	2
                            1970-01-01T00:00:30.000000Z	k3	k2_3	3
                            1970-01-01T00:00:40.000000Z	k4	k2_4	4
                            1970-01-01T00:00:50.000000Z	k5	k2_5	5
                            1970-01-01T00:01:00.000000Z	k6	k2_6	6
                            1970-01-01T00:01:10.000000Z	k7	k2_7	7
                            1970-01-01T00:01:20.000000Z	k8	k2_8	8
                            """,
                    "view4",
                    null,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testCreateViewWithSelfReference() throws Exception {
        // Attempting to create a view that references itself should fail
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            try {
                execute("CREATE VIEW " + VIEW1 + " AS (select * from " + VIEW1 + ")");
                fail("Expected creation to fail due to self-reference");
            } catch (SqlException e) {
                // View doesn't exist yet, so this should fail with "view does not exist"
                TestUtils.assertContains(e.getFlyweightMessage(), "does not exist");
            }
        });
    }

    @Test
    public void testCycleDetectionNoWalApply() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createView(VIEW1, "select * from " + TABLE1, TABLE1);
            createView(VIEW2, "select * from " + TABLE1, TABLE1);

            try {
                execute("CREATE OR REPLACE VIEW " + VIEW1 + " AS (" + VIEW2 + ")");
                execute("CREATE OR REPLACE VIEW " + VIEW2 + " AS (" + VIEW1 + ")");
                Assert.fail("expected SqlException due to cycle in view definitions");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "circular dependency detected: 'view2' cannot depend on 'view1' because 'view1' already depends on 'view2'");
            }
        });
    }
}