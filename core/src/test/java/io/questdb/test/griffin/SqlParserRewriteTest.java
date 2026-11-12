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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SqlParserRewriteTest extends AbstractCairoTest {
    @Test
    public void testCaseChangesArgumentLayout() throws Exception {
        assertQuery("""
                SELECT CASE WHEN flag THEN x END searched,
                       CASE x WHEN 1 THEN hit ELSE miss END simple,
                       CASE WHEN x = 1 THEN hit END synthesized
                FROM (SELECT x, x % 2 = 1 flag, 10 hit, 20 miss FROM long_sequence(3))
                """)
                .expectSize()
                .returns("""
                        searched	simple	synthesized
                        1	10	10
                        null	20	null
                        3	20	null
                        """);
    }

    @Test
    public void testDeclaredArgumentExpandsAfterRewrites() throws Exception {
        assertQuery("DECLARE @value := x + 1 SELECT sum(@value) OVER () s FROM long_sequence(3)")
                .expectSize()
                .returns("""
                        s
                        9.0
                        9.0
                        9.0
                        """);
    }

    @Test
    public void testFlatCallsOfDifferentArities() throws Exception {
        assertQuery("""
                SELECT abs(n) positive, concat(a, b) joined, substring(s, lo, len) piece
                FROM (SELECT -x n, 'a' a, 'b' b, 'abcd' s, 2 lo, 2 len FROM long_sequence(2))
                """)
                .expectSize()
                .returns("""
                        positive	joined	piece
                        1	ab	bc
                        2	ab	bc
                        """);
    }

    @Test
    public void testFlatWindowCallRewritesComplexSpecification() throws Exception {
        assertQuery("""
                SELECT id, sum(v) OVER (
                    PARTITION BY CASE WHEN json_extract(j, '$.v')::DOUBLE > 2 THEN a ELSE b END || suffix
                    ORDER BY id
                    ROWS BETWEEN ('1'::NUMERIC::DECIMAL(2, 0))::INT PRECEDING AND CURRENT ROW
                ) s
                FROM (
                    SELECT x id, x::DOUBLE v, '{"v":' || x || '}' j,
                           'a' a, 'b' b, '!' suffix
                    FROM long_sequence(4)
                )
                ORDER BY id
                """)
                .expectSize()
                .returns("""
                        id	s
                        1	1.0
                        2	3.0
                        3	3.0
                        4	7.0
                        """);
    }

    @Test
    public void testFlatWindowCallsAndCountStar() throws Exception {
        assertQuery("SELECT row_number() OVER () rn, sum(x) OVER () s, count(*) OVER () c FROM long_sequence(3)")
                .expectSize()
                .returns("""
                        rn	s	c
                        1	6.0	3
                        2	6.0	3
                        3	6.0	3
                        """);
    }

    @Test
    public void testSimpleCaseColumnKeyPreservesError() throws Exception {
        assertQuery("SELECT CASE x WHEN target THEN hit ELSE miss END FROM "
                + "(SELECT x, 1 target, 10 hit, 20 miss FROM long_sequence(3))")
                .fails(19, "constant expected");
    }

    @Test
    public void testWindowRewriteErrorsPrecedeOuterCastErrors() throws Exception {
        assertQuery("SELECT sum(x) OVER (ORDER BY x::bad_window) + x::bad_outer FROM long_sequence(1)")
                .fails(32, "unsupported cast");
    }
}
