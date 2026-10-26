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
 * A LATERAL body that joins a sub-query keyed on an outer column, such as
 * {@code (SELECT * FROM c WHERE x = o.k)}, returns the rows of the body run once per outer row.
 * LateralJoinRewriter joins a copy of the outer rows into such a sub-query and removes the copy where
 * the body allows. The tests check that the join to the sub-query keeps every condition of its ON
 * clause when the body reads another outer column, o.k2, that the sub-query does not compare for
 * equality, and so does the LATERAL join itself; that the layers of a nested sub-query still read the
 * columns of the copy; and that a name of a column of the sub-query that another relation has as well
 * stays ambiguous. The comments derive the expected rows per outer row from the data below; DuckDB
 * returns the same rows when it runs each body once per outer row, with = as IS NOT DISTINCT FROM and
 * NULL sorting first.
 * <p>
 * Per outer row, the sub-query holds c rows 30, 31 and 32 for outer rows 1 and 2 (k = 1), row 33 for
 * outer row 3 (k = 2) and row 34 for outer row 4, as = matches its NULL k to the NULL x of row 34; no
 * outer row selects row 35. {@code c.y < o.k2} keeps rows 30 and 31 for outer row 1 (k2 = 5), row 30 for
 * outer row 2 (k2 = 2), no row for outer row 3, whose k2 is NULL, and row 34 for outer row 4 (k2 = 3).
 * {@code a RIGHT JOIN d ON d.x = a.x} returns d row 40 with a row 10, and d row 41 with NULL.
 */
public class LateralJoinSubQueryOnClauseTest extends AbstractCairoTest {
    // the rows of c.y = o.k2: c row 32 (y = 5) for outer row 1, and c row 33 for outer row 3, as = matches
    // its NULL y to the NULL k2, each with both rows of a RIGHT JOIN d
    private static final String EQUALITY_ROWS = """
            id\taid\tdid\tcid
            1\tnull\t41\t32
            1\t10\t40\t32
            3\tnull\t41\t33
            3\t10\t40\t33
            """;
    // the rows of d JOIN c ON c.y < d.y: d row 40 (y = 3) with c rows 30 and 34, and d row 41 (y = 5) with
    // c rows 30, 31 and 34; the NULL y of c row 33 matches nothing
    private static final String INNER_JOIN_ON_D_Y_ROWS = """
            id\tdid\tcid
            1\t40\t30
            1\t41\t30
            1\t41\t31
            2\t40\t30
            2\t41\t30
            2\t41\t31
            4\t40\t34
            4\t41\t34
            """;
    // the rows that c.y < o.k2 keeps, each with both rows of a RIGHT JOIN d
    private static final String RIGHT_JOIN_ROWS = """
            id\taid\tdid\tcid
            1\tnull\t41\t30
            1\tnull\t41\t31
            1\t10\t40\t30
            1\t10\t40\t31
            2\tnull\t41\t30
            2\t10\t40\t30
            4\tnull\t41\t34
            4\t10\t40\t34
            """;

    // a FULL JOIN d returns a row 10 with d row 40, a row 11 with NULL and d row 41 with NULL.
    @Test
    public void testFullJoinThenSubQueryOnOtherOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a FULL JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < o.k2) l ORDER BY 1, 2, 3, 4",
                """
                        id\taid\tdid\tcid
                        1\tnull\t41\t30
                        1\tnull\t41\t31
                        1\t10\t40\t30
                        1\t10\t40\t31
                        1\t11\tnull\t30
                        1\t11\tnull\t31
                        2\tnull\t41\t30
                        2\t10\t40\t30
                        2\t11\tnull\t30
                        4\tnull\t41\t34
                        4\t10\t40\t34
                        4\t11\tnull\t34
                        """
        );
    }

    // The sub-query keyed on o.k sits in another sub-query, which passes its rows on through a
    // wildcard. c.y < d.y pairs d row 40 (y = 3) with c rows 30 and 34, and d row 41 (y = 5) with c
    // rows 30, 31 and 34.
    @Test
    public void testInnerJoinThenNestedSubQuery() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.did, l.cid FROM o JOIN LATERAL (SELECT d.id did, c.id cid FROM d JOIN (SELECT * FROM (SELECT * FROM c WHERE x = o.k)) c ON c.y < d.y) l ORDER BY 1, 2, 3",
                INNER_JOIN_ON_D_Y_ROWS
        );
    }

    // testInnerJoinThenNestedSubQuery() with a projection of o.k as kk between the wildcard and the
    // sub-query keyed on o.k, which equates them; outer row 4 has a NULL o.k.
    @Test
    public void testInnerJoinThenNestedSubQueryProjectingOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.did, l.cid, l.kk FROM o JOIN LATERAL (SELECT d.id did, c.id cid, c.kk FROM d JOIN (SELECT * FROM (SELECT id, x, y, o.k kk FROM c WHERE x = o.k)) c ON c.y < d.y) l ORDER BY 1, 2, 3, 4",
                """
                        id\tdid\tcid\tkk
                        1\t40\t30\t1
                        1\t41\t30\t1
                        1\t41\t31\t1
                        2\t40\t30\t1
                        2\t41\t30\t1
                        2\t41\t31\t1
                        4\t40\t34\tnull
                        4\t41\t34\tnull
                        """
        );
    }

    // testInnerJoinThenNestedSubQuery() with a projection between the wildcard and the sub-query keyed
    // on o.k.
    @Test
    public void testInnerJoinThenNestedSubQueryWithProjection() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.did, l.cid FROM o JOIN LATERAL (SELECT d.id did, c.id cid FROM d JOIN (SELECT * FROM (SELECT id, x, y FROM c WHERE x = o.k)) c ON c.y < d.y) l ORDER BY 1, 2, 3",
                INNER_JOIN_ON_D_Y_ROWS
        );
    }

    // testInnerJoinThenNestedSubQueryWithProjection() with a wildcard between the projection and the
    // sub-query keyed on o.k.
    @Test
    public void testInnerJoinThenNestedSubQueryWithProjectionOverWildcard() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.did, l.cid FROM o JOIN LATERAL (SELECT d.id did, c.id cid FROM d JOIN (SELECT * FROM (SELECT id, x, y FROM (SELECT * FROM c WHERE x = o.k))) c ON c.y < d.y) l ORDER BY 1, 2, 3",
                INNER_JOIN_ON_D_Y_ROWS
        );
    }

    // testInnerJoinThenNestedSubQueryWithProjectionOverWildcard() with two projections over the
    // wildcard.
    @Test
    public void testInnerJoinThenNestedSubQueryWithTwoProjectionsOverWildcard() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.did, l.cid FROM o JOIN LATERAL (SELECT d.id did, c.id cid FROM d JOIN (SELECT * FROM (SELECT id, x, y FROM (SELECT id, x, y FROM (SELECT * FROM c WHERE x = o.k)))) c ON c.y < d.y) l ORDER BY 1, 2, 3",
                INNER_JOIN_ON_D_Y_ROWS
        );
    }

    // a JOIN d returns a row 10 with d row 40.
    @Test
    public void testInnerJoinThenSubQueryOnOtherOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < o.k2) l ORDER BY 1, 2, 3, 4",
                """
                        id\taid\tdid\tcid
                        1\t10\t40\t30
                        1\t10\t40\t31
                        2\t10\t40\t30
                        4\t10\t40\t34
                        """
        );
    }

    // The ON clause reads no outer column, the select list reads o.k2. c.y < d.y pairs d row 40 (y = 3)
    // with c rows 30 and 34; the NULL y of c row 33 matches nothing.
    @Test
    public void testInnerJoinThenSubQueryWhileSelectReadsOtherOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid, l.kk FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid, o.k2 kk FROM a JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < d.y) l ORDER BY 1, 2, 3, 4, 5",
                """
                        id\taid\tdid\tcid\tkk
                        1\t10\t40\t30\t5
                        2\t10\t40\t30\t2
                        4\t10\t40\t34\t3
                        """
        );
    }

    // Both sub-queries are keyed on o.k, and the ON clauses equate their x with d.x. c.y < d.y keeps c
    // row 30 for d row 40 (x = 1, y = 3), and e.y = c.y + 3 pairs it with c row 31 (y = 4). d row 41
    // (x = 3), the c row of outer row 3 (x = 2) and the NULL x of c row 34 match nothing.
    @Test
    public void testInnerJoinThenTwoSubQueriesOnOuterKey() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.cid, l.eid FROM o JOIN LATERAL (SELECT c.id cid, e.id eid FROM d JOIN (SELECT * FROM c WHERE x = o.k) c ON c.x = d.x AND c.y < d.y JOIN (SELECT * FROM c WHERE x = o.k) e ON e.x = c.x AND e.y = c.y + 3) l ORDER BY 1, 2, 3",
                """
                        id\tcid\teid
                        1\t30\t31
                        2\t30\t31
                        """
        );
    }

    // The ON clause of the LATERAL join reads k, a column of the body that passes o.k2 on, under a name
    // that o has as well, so the name is ambiguous, as it is without LATERAL.
    @Test
    public void testLateralOnAmbiguousProjectedOuterColumn() throws Exception {
        final String sql = "SELECT o.id, cid FROM o JOIN LATERAL (SELECT c.id cid, c.y cy, o.k2 k FROM c WHERE c.x = o.k) ON cy < k ORDER BY 1, 2";
        assertMemoryLeak(() -> {
            createTables();
            assertQuery(sql)
                    .noLeakCheck()
                    .fails(sql.indexOf("cy < k") + 5, "Ambiguous column [name=k]");
        });
    }

    // ON true keeps every row of the body, and outer row 3, for which the body returns none, keeps a
    // row of NULLs.
    @Test
    public void testLateralOnTrueWithBodyReadingTwoOuterColumns() throws Exception {
        assertRows(
                false,
                "SELECT o.id, l.cid FROM o LEFT JOIN LATERAL (SELECT c.id cid FROM c WHERE c.x = o.k AND c.y < o.k2) l ON true ORDER BY 1, 2",
                """
                        id\tcid
                        1\t30
                        1\t31
                        2\t30
                        3\tnull
                        4\t34
                        """
        );
    }

    // The ON clause of the LATERAL join reads the columns of the body by their bare names; kk passes
    // o.k2 on.
    @Test
    public void testLateralOnUnqualifiedProjectedOuterColumn() throws Exception {
        assertRows(
                false,
                "SELECT o.id, cid FROM o JOIN LATERAL (SELECT c.id cid, c.y cy, o.k2 kk FROM c WHERE c.x = o.k) ON cy < kk ORDER BY 1, 2",
                """
                        id\tcid
                        1\t30
                        1\t31
                        2\t30
                        4\t34
                        """
        );
    }

    // The body keeps the c rows that c.y < o.k2 keeps, and ON l.cid > 30 drops c row 30. The join reads
    // c without the outer rows: it keys on c.x = o.k, compares c.y with o.k2 after the join, and the ON
    // clause filters c before it.
    @Test
    public void testLateralOnWithBodyReadingTwoOuterColumns() throws Exception {
        assertRowsAndPlan(
                false,
                "SELECT o.id, l.cid, l.cy FROM o JOIN LATERAL (SELECT c.id cid, c.y cy FROM c WHERE c.x = o.k AND c.y < o.k2) l ON l.cid > 30 ORDER BY 1, 2, 3",
                """
                        id\tcid\tcy
                        1\t31\t4
                        4\t34\t2
                        """,
                "Filter filter: l.cy<o.k2",
                "Hash Join Light",
                "condition: l.x=o.k",
                "filter: 30<id"
        );
    }

    // Outer row 2, whose only c row ON l.cid > 30 drops, and outer row 3, for which the body returns no
    // row, keep a row of NULLs.
    @Test
    public void testLateralOnWithBodyReadingTwoOuterColumnsLeftLateral() throws Exception {
        assertRows(
                false,
                "SELECT o.id, l.cid, l.cy FROM o LEFT JOIN LATERAL (SELECT c.id cid, c.y cy FROM c WHERE c.x = o.k AND c.y < o.k2) l ON l.cid > 30 ORDER BY 1, 2, 3",
                """
                        id\tcid\tcy
                        1\t31\t4
                        2\tnull\tnull
                        3\tnull\tnull
                        4\t34\t2
                        """
        );
    }

    // The body keeps the c rows that c.y < o.k2 keeps.
    @Test
    public void testLateralWithBodyReadingTwoOuterColumns() throws Exception {
        assertRows(
                false,
                "SELECT o.id, l.cid FROM o JOIN LATERAL (SELECT c.id cid FROM c WHERE c.x = o.k AND c.y < o.k2) l ORDER BY 1, 2",
                """
                        id\tcid
                        1\t30
                        1\t31
                        2\t30
                        4\t34
                        """
        );
    }

    // a LEFT JOIN d returns a row 10 with d row 40, and a row 11 with NULL.
    @Test
    public void testLeftJoinThenSubQueryOnOtherOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a LEFT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < o.k2) l ORDER BY 1, 2, 3, 4",
                """
                        id\taid\tdid\tcid
                        1\t10\t40\t30
                        1\t10\t40\t31
                        1\t11\tnull\t30
                        1\t11\tnull\t31
                        2\t10\t40\t30
                        2\t11\tnull\t30
                        4\t10\t40\t34
                        4\t11\tnull\t34
                        """
        );
    }

    // Outer row 3, for which c.y < o.k2 keeps no c row, keeps a row of NULLs.
    @Test
    public void testLeftJoinThenSubQueryOnOtherOuterColumnLeftLateral() throws Exception {
        assertRows(
                false,
                "SELECT o.id, l.aid, l.did, l.cid FROM o LEFT JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a LEFT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < o.k2) l ORDER BY 1, 2, 3, 4",
                """
                        id\taid\tdid\tcid
                        1\t10\t40\t30
                        1\t10\t40\t31
                        1\t11\tnull\t30
                        1\t11\tnull\t31
                        2\t10\t40\t30
                        2\t11\tnull\t30
                        3\tnull\tnull\tnull
                        4\t10\t40\t34
                        4\t11\tnull\t34
                        """
        );
    }

    // y > 1 drops c row 30, and c row 33, whose y is NULL. c.y < o.k2 then keeps c row 31 for outer row 1
    // and c row 34 for outer row 4.
    @Test
    public void testRightJoinThenFilteredSubQueryOnOtherOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k AND y > 1) c ON c.y < o.k2) l ORDER BY 1, 2, 3, 4",
                """
                        id\taid\tdid\tcid
                        1\tnull\t41\t31
                        1\t10\t40\t31
                        4\tnull\t41\t34
                        4\t10\t40\t34
                        """
        );
    }

    // The LEFT join keeps both rows of the RIGHT join for outer row 3 with a NULL cid, as c.y < o.k2 keeps
    // no c row there.
    @Test
    public void testRightJoinThenLeftJoinSubQueryOnOtherOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x LEFT JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < o.k2) l ORDER BY 1, 2, 3, 4",
                """
                        id\taid\tdid\tcid
                        1\tnull\t41\t30
                        1\tnull\t41\t31
                        1\t10\t40\t30
                        1\t10\t40\t31
                        2\tnull\t41\t30
                        2\t10\t40\t30
                        3\tnull\t41\tnull
                        3\t10\t40\tnull
                        4\tnull\t41\t34
                        4\t10\t40\t34
                        """
        );
    }

    // The sub-query keyed on o.k sits in another sub-query, which holds the same rows.
    @Test
    public void testRightJoinThenNestedSubQueryOnOtherOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM (SELECT * FROM c WHERE x = o.k)) c ON c.y < o.k2) l ORDER BY 1, 2, 3, 4",
                RIGHT_JOIN_ROWS
        );
    }

    // testRightJoinThenNestedSubQueryOnOtherOuterColumn() with a projection over a wildcard between the
    // two sub-queries.
    @Test
    public void testRightJoinThenNestedSubQueryWithProjectionOnOtherOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM (SELECT id, x, y FROM (SELECT * FROM c WHERE x = o.k))) c ON c.y < o.k2) l ORDER BY 1, 2, 3, 4",
                RIGHT_JOIN_ROWS
        );
    }

    // The ON clause reads y, a column of the sub-query that passes o.k2 on, under a name that d has
    // as well, so the name is ambiguous, as it is without LATERAL.
    @Test
    public void testRightJoinThenSubQueryOnAmbiguousProjectedOuterColumn() throws Exception {
        final String sql = "SELECT o.id, l.did, l.cid FROM o JOIN LATERAL (SELECT d.id did, cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT id cid, x cx, o.k2 y FROM c WHERE x = o.k) ON y BETWEEN 0 AND 4) l ORDER BY 1, 2, 3";
        assertMemoryLeak(() -> {
            createTables();
            assertQuery(sql)
                    .noLeakCheck()
                    .fails(sql.indexOf("y BETWEEN"), "Ambiguous column [name=y]");
        });
    }

    // The join reads c without a copy of the outer rows of its own: it keys on c.x and compares c.y with
    // the o.k2 of the outer rows after the join.
    @Test
    public void testRightJoinThenSubQueryOnOtherOuterColumn() throws Exception {
        assertRowsAndPlan(
                true,
                "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < o.k2) l ORDER BY 1, 2, 3, 4",
                RIGHT_JOIN_ROWS,
                "Filter filter: c.y<__qdb_outer_ref__0.__qdb_outer_ref__0_k2",
                "Hash Join Light",
                "condition: c.x=__qdb_outer_ref__0_k"
        );
    }

    // c.x = d.x pairs the c rows that c.y < o.k2 keeps with d row 40 only, whose x is 1: d row 41 has
    // x = 3, and the NULL x of c row 34 matches neither.
    @Test
    public void testRightJoinThenSubQueryOnOtherOuterColumnAndJoinKey() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < o.k2 AND c.x = d.x) l ORDER BY 1, 2, 3, 4",
                """
                        id\taid\tdid\tcid
                        1\t10\t40\t30
                        1\t10\t40\t31
                        2\t10\t40\t30
                        """
        );
    }

    // The count of the rows of the body, per outer row, and 0 for outer row 3, for which c.y < o.k2 keeps
    // no c row.
    @Test
    public void testRightJoinThenSubQueryOnOtherOuterColumnCount() throws Exception {
        assertRows(
                false,
                "SELECT o.id, l.n FROM o JOIN LATERAL (SELECT count(*) n FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < o.k2) l ORDER BY 1",
                """
                        id\tn
                        1\t4
                        2\t2
                        3\t0
                        4\t2
                        """
        );
    }

    @Test
    public void testRightJoinThenSubQueryOnOtherOuterColumnCrossLateral() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid FROM o CROSS JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < o.k2) l ORDER BY 1, 2, 3, 4",
                RIGHT_JOIN_ROWS
        );
    }

    @Test
    public void testRightJoinThenSubQueryOnOtherOuterColumnEquality() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y = o.k2) l ORDER BY 1, 2, 3, 4",
                EQUALITY_ROWS
        );
    }

    // testRightJoinThenSubQueryOnOtherOuterColumnEquality() with the equality written over
    // expressions, which are NULL where c.y and o.k2 are.
    @Test
    public void testRightJoinThenSubQueryOnOtherOuterColumnExpressionEquality() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y + 0 = o.k2 AND c.y = o.k2 + 0) l ORDER BY 1, 2, 3, 4",
                EQUALITY_ROWS
        );
    }

    // Outer row 3, for which c.y < o.k2 keeps no c row, keeps a row of NULLs.
    @Test
    public void testRightJoinThenSubQueryOnOtherOuterColumnLeftLateral() throws Exception {
        assertRows(
                false,
                "SELECT o.id, l.aid, l.did, l.cid FROM o LEFT JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < o.k2) l ORDER BY 1, 2, 3, 4",
                """
                        id\taid\tdid\tcid
                        1\tnull\t41\t30
                        1\tnull\t41\t31
                        1\t10\t40\t30
                        1\t10\t40\t31
                        2\tnull\t41\t30
                        2\t10\t40\t30
                        3\tnull\tnull\tnull
                        4\tnull\t41\t34
                        4\t10\t40\t34
                        """
        );
    }

    // The reference runs the body once per outer row, with its o.k and o.k2 as constants.
    @Test
    public void testRightJoinThenSubQueryOnOtherOuterColumnMatchesPerOuterRow() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    """
                            SELECT * FROM (
                                SELECT 1 id, a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = 1) c ON c.y < 5
                                UNION ALL
                                SELECT 2 id, a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = 1) c ON c.y < 2
                                UNION ALL
                                SELECT 3 id, a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = 2) c ON c.y < NULL
                                UNION ALL
                                SELECT 4 id, a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = NULL) c ON c.y < 3
                            ) ORDER BY 1, 2, 3, 4""",
                    "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < o.k2) l ORDER BY 1, 2, 3, 4"
            );
        });
    }

    // Per outer row, ORDER BY c.id DESC, a.id LIMIT 1 keeps the highest c row that c.y < o.k2 keeps, with
    // d row 41, whose NULL a.id sorts first.
    @Test
    public void testRightJoinThenSubQueryOnOtherOuterColumnOrderByLimit() throws Exception {
        assertRows(
                false,
                "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < o.k2 ORDER BY c.id DESC, a.id LIMIT 1) l ORDER BY 1, 2, 3, 4",
                """
                        id\taid\tdid\tcid
                        1\tnull\t41\t31
                        2\tnull\t41\t30
                        4\tnull\t41\t34
                        """
        );
    }

    // c.kk = o.k2 holds on every row, as = matches the NULL kk of outer row 3 to its NULL k2.
    @Test
    public void testRightJoinThenSubQueryOnProjectedAndOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT id, x, y, o.k2 kk FROM c WHERE x = o.k) c ON c.y < o.k2 AND c.kk = o.k2) l ORDER BY 1, 2, 3, 4",
                RIGHT_JOIN_ROWS
        );
    }

    // The sub-query passes o.k2 through as kk, so c.y < c.kk is c.y < o.k2.
    @Test
    public void testRightJoinThenSubQueryOnProjectedOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT id, x, y, o.k2 kk FROM c WHERE x = o.k) c ON c.y < c.kk) l ORDER BY 1, 2, 3, 4",
                RIGHT_JOIN_ROWS
        );
    }

    // cc.y BETWEEN 1 AND cc.kk, where kk passes o.k2 on, keeps c rows 30, 31 and 32 for outer row 1,
    // c row 30 for outer row 2 and c row 34 for outer row 4, and cc.x = d.x pairs them with d row 40
    // only; the NULL x of c row 34 matches no d row.
    @Test
    public void testRightJoinThenSubQueryOnProjectedOuterColumnBetween() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, cc.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT id, x, y, o.k2 kk FROM c WHERE x = o.k) cc ON cc.y BETWEEN 1 AND cc.kk AND cc.x = d.x) l ORDER BY 1, 2, 3, 4",
                """
                        id\taid\tdid\tcid
                        1\t10\t40\t30
                        1\t10\t40\t31
                        1\t10\t40\t32
                        2\t10\t40\t30
                        """
        );
    }

    // The ON clause reads the columns of a sub-query without an alias by their bare names; kk passes
    // o.k2 on.
    @Test
    public void testRightJoinThenSubQueryOnUnqualifiedProjectedOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.did, l.cid FROM o JOIN LATERAL (SELECT d.id did, cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT id cid, x cx, y cy, o.k2 kk FROM c WHERE x = o.k) ON cy < kk) l ORDER BY 1, 2, 3",
                """
                        id\tdid\tcid
                        1\t40\t30
                        1\t40\t31
                        1\t41\t30
                        1\t41\t31
                        2\t40\t30
                        2\t41\t30
                        4\t40\t34
                        4\t41\t34
                        """
        );
    }

    // The ON clause reads no outer column, the select list reads o.k2. c.y < d.y pairs d row 40 (y = 3)
    // with c rows 30 and 34, and d row 41 (y = 5) with c rows 30, 31 and 34.
    @Test
    public void testRightJoinThenSubQueryWhileSelectReadsOtherOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid, l.kk FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid, o.k2 kk FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < d.y) l ORDER BY 1, 2, 3, 4, 5",
                """
                        id\taid\tdid\tcid\tkk
                        1\tnull\t41\t30\t5
                        1\tnull\t41\t31\t5
                        1\t10\t40\t30\t5
                        2\tnull\t41\t30\t2
                        2\tnull\t41\t31\t2
                        2\t10\t40\t30\t2
                        4\tnull\t41\t34\t3
                        4\t10\t40\t34\t3
                        """
        );
    }

    // The ON clause reads no outer column, WHERE reads o.k2. c.y < d.y pairs d row 40 (y = 3) with c rows
    // 30 and 34, and d row 41 (y = 5) with c rows 30, 31 and 34; d.y <= o.k2 keeps d row 40 for outer rows
    // 1 and 4, and d row 41 for outer row 1.
    @Test
    public void testRightJoinThenSubQueryWhileWhereReadsOtherOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < d.y WHERE d.y <= o.k2) l ORDER BY 1, 2, 3, 4",
                """
                        id\taid\tdid\tcid
                        1\tnull\t41\t30
                        1\tnull\t41\t31
                        1\t10\t40\t30
                        4\t10\t40\t34
                        """
        );
    }

    // The second sub-query, keyed on o.k2, holds c row 32 (y = 5) for outer row 1, c row 34 (y = 2) for
    // outer row 2, c row 33 for the NULL k2 of outer row 3, and no row for outer row 4. e.x = d.x pairs c
    // row 32 with d row 40, and the NULL x of c row 34 matches no d row.
    @Test
    public void testRightJoinThenTwoSubQueriesOnOtherOuterColumns() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.did, l.cid, l.eid FROM o JOIN LATERAL (SELECT a.id aid, d.id did, c.id cid, e.id eid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < o.k2 JOIN (SELECT * FROM c WHERE y = o.k2) e ON e.x = d.x) l ORDER BY 1, 2, 3, 4, 5",
                """
                        id\taid\tdid\tcid\teid
                        1\t10\t40\t30\t32
                        1\t10\t40\t31\t32
                        """
        );
    }

    // The RIGHT join keeps every c row of the sub-query. a row 10 (x = 1) matches the c rows that
    // c.y < o.k2 keeps for outer rows 1 and 2, and the join NULL-extends the others; no a row has the NULL
    // x of c row 34.
    @Test
    public void testRightJoinToSubQueryOnOtherOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.aid, l.cid FROM o JOIN LATERAL (SELECT a.id aid, c.id cid FROM a RIGHT JOIN (SELECT * FROM c WHERE x = o.k) c ON a.x = c.x AND c.y < o.k2) l ORDER BY 1, 2, 3",
                """
                        id\taid\tcid
                        1\tnull\t32
                        1\t10\t30
                        1\t10\t31
                        2\tnull\t31
                        2\tnull\t32
                        2\t10\t30
                        3\tnull\t33
                        4\tnull\t34
                        """
        );
    }

    // The sub-query comes ahead of a RIGHT join, which keeps every d row per outer row. a JOIN c pairs
    // both a rows with the c rows that c.y < o.k2 keeps, and d row 40 (x = 1) matches the pairs with
    // c rows 30 and 31; the RIGHT join NULL-extends d row 41, and both d rows for outer rows 3 and 4,
    // whose c rows match no d row.
    @Test
    public void testSubQueryThenRightJoinOnOtherOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l.cid, l.did FROM o JOIN LATERAL (SELECT c.id cid, d.id did FROM a JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < o.k2 RIGHT JOIN d ON d.x = c.x) l ORDER BY 1, 2, 3",
                """
                        id\tcid\tdid
                        1\tnull\t41
                        1\t30\t40
                        1\t30\t40
                        1\t31\t40
                        1\t31\t40
                        2\tnull\t41
                        2\t30\t40
                        2\t30\t40
                        3\tnull\t40
                        3\tnull\t41
                        4\tnull\t40
                        4\tnull\t41
                        """
        );
    }

    // Each LATERAL join reads both outer columns. The first keeps the c rows that c.y < o.k2 keeps; the
    // second keeps c row 32 (y = 5) for outer row 1 and c row 34 (y = 2) for outer row 2, which
    // e.id > o.k keeps, and no row for the NULL o.k of outer row 4.
    @Test
    public void testTwoLateralJoinsWithInnerJoinThenSubQueryOnOtherOuterColumn() throws Exception {
        assertRows(
                true,
                "SELECT o.id, l1.cid, l2.eid FROM o JOIN LATERAL (SELECT c.id cid FROM d JOIN (SELECT * FROM c WHERE x = o.k) c ON c.y < o.k2 AND d.id = 40) l1 JOIN LATERAL (SELECT e.id eid FROM d JOIN (SELECT * FROM c WHERE y = o.k2) e ON e.id > o.k AND d.id = 40) l2 ORDER BY 1, 2, 3",
                """
                        id\tcid\teid
                        1\t30\t32
                        1\t31\t32
                        2\t30\t34
                        """
        );
    }

    private void assertRows(boolean expectSize, String sql, String expected) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize(expectSize)
                    .returns(expected);
        });
    }

    private void assertRowsAndPlan(boolean expectSize, String sql, String expected, CharSequence... planFragments) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize(expectSize)
                    .withPlanContaining(planFragments)
                    .returns(expected);
        });
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE o (id INT, k INT, k2 INT)");
        execute("CREATE TABLE a (id INT, x INT)");
        execute("CREATE TABLE d (id INT, x INT, y INT)");
        execute("CREATE TABLE c (id INT, x INT, y INT)");
        execute("INSERT INTO o VALUES (1, 1, 5), (2, 1, 2), (3, 2, NULL), (4, NULL, 3)");
        execute("INSERT INTO a VALUES (10, 1), (11, 2)");
        execute("INSERT INTO d VALUES (40, 1, 3), (41, 3, 5)");
        execute("INSERT INTO c VALUES (30, 1, 1), (31, 1, 4), (32, 1, 5), (33, 2, NULL), (34, NULL, 2), (35, 3, 0)");
    }
}
