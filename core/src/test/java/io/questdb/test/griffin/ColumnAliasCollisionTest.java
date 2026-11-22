/*******************************************************************************
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

import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

public class ColumnAliasCollisionTest extends AbstractCairoTest {

    @Before
    public void setUpTrades() throws Exception {
        execute("CREATE TABLE trades (id INT, qty INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO trades VALUES (1, 10, '2024-01-01T00:00:00.000000Z')");
        execute("INSERT INTO trades VALUES (2, 20, '2024-01-01T00:01:00.000000Z')");
    }

    @Test
    public void testCaseA_ArithmeticOperationFollowedBySourceColumnAlias() throws Exception {
        assertQuery("SELECT qty * 10 AS f, id AS qty FROM trades")
                .expectSize()
                .returns(
                        "f\tqty\n" +
                        "100\t1\n" +
                        "200\t2\n"
                );
    }

    @Test
    public void testCaseB_ArithmeticOperationFollowedByTimestampAsSourceColumnAlias() throws Exception {
        assertQuery("SELECT qty * 10 AS f, ts AS qty FROM trades")
                .timestamp("qty")
                .expectSize()
                .returns(
                        "f\tqty\n" +
                        "100\t2024-01-01T00:00:00.000000Z\n" +
                        "200\t2024-01-01T00:01:00.000000Z\n"
                );
    }

    @Test
    public void testCaseC_ExpressionWithImplicitGroupByAndCount() throws Exception {
        assertQuery("SELECT qty + 1 AS f, id AS qty, count() FROM trades")
                .expectSize()
                .returns(
                        "f\tqty\tcount\n" +
                        "11\t1\t1\n" +
                        "21\t2\t1\n"
                );
    }

    @Test
    public void testCaseD_WindowFunctionFollowedBySourceColumnAlias() throws Exception {
        assertQuery("SELECT lead(id) OVER () AS n, qty AS id FROM trades")
                .expectSize()
                .returns(
                        "n\tid\n" +
                        "2\t10\n" +
                        "null\t20\n"
                );
    }

    @Test
    public void testCaseE_WorkingCase_SourceColumnAliasBeforeExpression() throws Exception {
        assertQuery("SELECT ts AS qty, qty * 10 AS f FROM trades")
                .timestamp("qty")
                .expectSize()
                .returns(
                        "qty\tf\n" +
                        "2024-01-01T00:00:00.000000Z\t100\n" +
                        "2024-01-01T00:01:00.000000Z\t200\n"
                );
    }

    @Test
    public void testCaseF_WorkingCase_SingleAlias() throws Exception {
        assertQuery("SELECT id AS qty FROM trades")
                .expectSize()
                .returns(
                        "qty\n" +
                        "1\n" +
                        "2\n"
                );
    }

    @Test
    public void testGenuineDuplicateExplicitAliases() throws Exception {
        assertQuery("SELECT id AS qty, ts AS qty FROM trades")
                .fails(21, "Duplicate column [name=qty]");
    }

    @Test
    public void testGenuineDuplicateExplicitAliasesWithEarlierExpression() throws Exception {
        assertQuery("SELECT qty * 10 AS f, id AS qty, ts AS qty FROM trades")
                .fails(36, "Duplicate column [name=qty]");
    }

    @Test
    public void testExplicitAliasCollidingWithSubsequentSourceColumn() throws Exception {
        assertQuery("SELECT qty * 10 AS f, id AS qty, qty FROM trades")
                .expectSize()
                .returns(
                        "f\tqty\tqty1\n" +
                        "100\t1\t10\n" +
                        "200\t2\t20\n"
                );
    }

    @Test
    public void testMultipleExplicitAliasesCollidingWithSourceColumns() throws Exception {
        assertQuery("SELECT qty * 10 AS f, id * 2 AS g, id AS qty, ts AS id FROM trades")
                .timestamp("id")
                .expectSize()
                .returns(
                        "f\tg\tqty\tid\n" +
                        "100\t2\t1\t2024-01-01T00:00:00.000000Z\n" +
                        "200\t4\t2\t2024-01-01T00:01:00.000000Z\n"
                );
    }

    @Test
    public void testWildcardWithCollidingExplicitAliasAfter() throws Exception {
        assertQuery("SELECT *, id AS qty FROM trades")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "id\tqty\tts\tqty1\n" +
                        "1\t10\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "2\t20\t2024-01-01T00:01:00.000000Z\t2\n"
                );
    }

    @Test
    public void testWildcardWithCollidingExplicitAliasBefore() throws Exception {
        assertQuery("SELECT qty * 10 AS f, id AS qty, * FROM trades")
                .timestamp("ts")
                .expectSize()
                .returns(
                        "f\tqty\tid\tqty1\tts\n" +
                        "100\t1\t1\t10\t2024-01-01T00:00:00.000000Z\n" +
                        "200\t2\t2\t20\t2024-01-01T00:01:00.000000Z\n"
                );
    }

    @Test
    public void testDistinctWithSourceColumnAliasCollision() throws Exception {
        assertQuery("SELECT DISTINCT qty * 10 AS f, id AS qty FROM trades")
                .expectSize()
                .returns(
                        "f\tqty\n" +
                        "100\t1\n" +
                        "200\t2\n"
                );
    }

    @Test
    public void testExplicitGroupByWithSourceColumnAliasCollision() throws Exception {
        assertQuery("SELECT qty * 10 AS f, id AS qty, count() FROM trades GROUP BY qty * 10, id")
                .expectSize()
                .returns(
                        "f\tqty\tcount\n" +
                        "100\t1\t1\n" +
                        "200\t2\t1\n"
                );
    }

    @Test
    public void testWindowFunctionWithExpressionAndAliasCollision() throws Exception {
        assertQuery("SELECT row_number() OVER () AS rn, qty * 10 AS f, id AS qty FROM trades")
                .noRandomAccess()
                .expectSize()
                .returns(
                        "rn\tf\tqty\n" +
                        "1\t100\t1\n" +
                        "2\t200\t2\n"
                );
    }

    @Test
    public void testWindowFunctionPartitionAndOrderByWithAliasCollision() throws Exception {
        assertQuery("SELECT row_number() OVER (PARTITION BY id ORDER BY ts) AS rn, qty * 10 AS f, id AS qty FROM trades")
                .noRandomAccess()
                .expectSize()
                .returns(
                        "rn\tf\tqty\n" +
                        "1\t100\t1\n" +
                        "1\t200\t2\n"
                );
    }

    @Test
    public void testSubsequentReferenceToSourceColumnAfterExplicitAlias() throws Exception {
        assertQuery("SELECT qty * 10 AS f, id AS qty, qty + 5 AS g FROM trades")
                .expectSize()
                .returns(
                        "f\tqty\tg\n" +
                        "100\t1\t15\n" +
                        "200\t2\t25\n"
                );
    }

    @Test
    public void testPriorReferenceToDifferentSourceColumnBeforeExplicitAlias() throws Exception {
        assertQuery("SELECT id + 1 AS f, qty AS id FROM trades")
                .expectSize()
                .returns(
                        "f\tid\n" +
                        "2\t10\n" +
                        "3\t20\n"
                );
    }

    @Test
    public void testDesignatedTimestampAliasedAndReplaced() throws Exception {
        assertQuery("SELECT ts AS qty, qty * 10 AS ts, id FROM trades")
                .timestamp("qty")
                .expectSize()
                .returns(
                        "qty\tts\tid\n" +
                        "2024-01-01T00:00:00.000000Z\t100\t1\n" +
                        "2024-01-01T00:01:00.000000Z\t200\t2\n"
                );
    }
}
