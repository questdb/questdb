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

package io.questdb.test.griffin.engine.join;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A LATERAL body whose WHERE clause holds the correlation key b.k = o.k and a condition that
 * compares an outer column with a sub-query returns the rows of the body run once per outer row.
 * <p>
 * The sub-query {@code (SELECT count() FROM c)} returns 2. Outer row 1 (k = 1, v = 5) selects b row
 * (1, 10), and outer row 2 (k = 2, v = 50) selects b row (2, 20). {@code b.v - o.v} is 5 for outer row 1
 * and -30 for outer row 2, so {@code b.v - o.v > 2} keeps the row of outer row 1 only;
 * {@code o.v > 2} holds for both outer rows.
 */
public class LateralJoinSubQueryPredicateTest extends AbstractCairoTest {

    @Test
    public void testCrossJoinConditionReadsInnerAndOuterColumns() throws Exception {
        assertRows(
                "SELECT o.id, t.v FROM o CROSS JOIN LATERAL (SELECT b.v FROM b WHERE b.k = o.k AND b.v - o.v > (SELECT count() FROM c)) t ORDER BY 1",
                """
                        id\tv
                        1\t10
                        """
        );
    }

    @Test
    public void testCrossJoinConditionReadsOuterColumnOnly() throws Exception {
        assertRows(
                "SELECT o.id, t.v FROM o CROSS JOIN LATERAL (SELECT b.v FROM b WHERE b.k = o.k AND o.v > (SELECT count() FROM c)) t ORDER BY 1",
                """
                        id\tv
                        1\t10
                        2\t20
                        """
        );
    }

    @Test
    public void testLeftJoinCaseConditionReadsInnerAndOuterColumns() throws Exception {
        // testLeftJoinConditionReadsInnerAndOuterColumns() with the condition in a CASE expression
        assertRows(
                "SELECT o.id, t.v FROM o LEFT JOIN LATERAL (SELECT b.v FROM b WHERE b.k = o.k AND CASE WHEN b.v - o.v > (SELECT count() FROM c) THEN true ELSE false END) t ON true ORDER BY 1",
                """
                        id\tv
                        1\t10
                        2\tnull
                        """
        );
    }

    @Test
    public void testLeftJoinConditionReadsInnerAndOuterColumns() throws Exception {
        // the body has no row for outer row 2, so the LEFT JOIN returns it with NULL
        assertRows(
                "SELECT o.id, t.v FROM o LEFT JOIN LATERAL (SELECT b.v FROM b WHERE b.k = o.k AND b.v - o.v > (SELECT count() FROM c)) t ON true ORDER BY 1",
                """
                        id\tv
                        1\t10
                        2\tnull
                        """
        );
    }

    @Test
    public void testLeftJoinNestedCaseConditionReadsInnerAndOuterColumns() throws Exception {
        // testLeftJoinNestedConditionReadsInnerAndOuterColumns() with the condition in a CASE expression
        assertRows(
                "SELECT o.id, t.v FROM o LEFT JOIN LATERAL (SELECT * FROM (SELECT * FROM (SELECT * FROM b WHERE b.k = o.k)) s WHERE s.k = o.k AND CASE WHEN s.v - o.v > (SELECT count() FROM c) THEN true ELSE false END) t ON true ORDER BY 1",
                """
                        id\tv
                        1\t10
                        2\tnull
                        """
        );
    }

    @Test
    public void testLeftJoinNestedConditionReadsInnerAndOuterColumns() throws Exception {
        // testLeftJoinConditionReadsInnerAndOuterColumns() with the body's rows in two nested sub-queries,
        // so the condition with the sub-query reads the outer row in a layer above the one that joins it
        assertRows(
                "SELECT o.id, t.v FROM o LEFT JOIN LATERAL (SELECT * FROM (SELECT * FROM (SELECT * FROM b WHERE b.k = o.k)) s WHERE s.k = o.k AND s.v - o.v > (SELECT count() FROM c)) t ON true ORDER BY 1",
                """
                        id\tv
                        1\t10
                        2\tnull
                        """
        );
    }

    @Test
    public void testLeftJoinNestedConditionReadsInnerAndOuterColumnsFirst() throws Exception {
        // testLeftJoinNestedConditionReadsInnerAndOuterColumns() with the condition with the sub-query first
        assertRows(
                "SELECT o.id, t.v FROM o LEFT JOIN LATERAL (SELECT * FROM (SELECT * FROM (SELECT * FROM b WHERE b.k = o.k)) s WHERE s.v - o.v > (SELECT count() FROM c) AND s.k = o.k) t ON true ORDER BY 1",
                """
                        id\tv
                        1\t10
                        2\tnull
                        """
        );
    }

    @Test
    public void testLeftJoinNestedConditionsWithSubQueryReadOuterColumnOrNot() throws Exception {
        // testLeftJoinNestedConditionReadsInnerAndOuterColumns() with a condition with a sub-query that reads
        // no outer column ahead of the others: b.v > 2 * 8 drops the b row of outer row 1 (v = 10)
        assertRows(
                "SELECT o.id, t.v FROM o LEFT JOIN LATERAL (SELECT * FROM (SELECT * FROM (SELECT * FROM b WHERE b.k = o.k)) s WHERE s.v > (SELECT max(x) * 8 FROM c) AND s.k = o.k AND s.v - o.v > (SELECT count() FROM c)) t ON true ORDER BY 1",
                """
                        id\tv
                        1\tnull
                        2\tnull
                        """
        );
    }

    private void assertRows(String sql, String expected) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE o (id INT, k INT, v INT)");
            execute("CREATE TABLE b (k INT, v INT)");
            execute("CREATE TABLE c (x INT)");
            execute("INSERT INTO o VALUES (1, 1, 5), (2, 2, 50)");
            execute("INSERT INTO b VALUES (1, 10), (2, 20)");
            execute("INSERT INTO c VALUES (1), (2)");
            assertQuery(sql)
                    .noLeakCheck()
                    .returns(expected);
        });
    }
}
