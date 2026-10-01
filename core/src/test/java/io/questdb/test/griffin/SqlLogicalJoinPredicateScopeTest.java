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
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalJoinPredicateScopeTest extends AbstractCairoTest {
    private static final String DERIVED_TWO_SOURCES = "SELECT a.id id,a.active active,b.ts ts,b.id bid "
            + "FROM lp_scope_a a JOIN lp_scope_b b ON a.id=b.id";
    private static final String MIXED_PREDICATE = "(b.ts<'2020-01-01T00:00:00.000000001Z' OR a.active)";
    private static final String NATIVE_BOUND = "b.ts<'2020-01-01T00:00:00.000001001Z'";
    private static final String THREE_SOURCES = "SELECT a.id FROM lp_scope_a a JOIN lp_scope_c c ON a.id=c.id "
            + "JOIN lp_scope_b b ON c.id=b.id WHERE ";
    private static final String TWO_SOURCES = "SELECT a.id FROM lp_scope_a a JOIN lp_scope_b b ON a.id=b.id WHERE ";

    @Test
    public void testDerivedSourcePreservesItsOwnTimestampBoundary() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String transparent = "SELECT a.id FROM lp_scope_a a "
                    + "JOIN (SELECT id,ts FROM lp_scope_b) b ON a.id=b.id WHERE ";
            assertRows(transparent + MIXED_PREDICATE + " AND b.id>0 ORDER BY a.id", "id\n1\n2\n");
            assertRows(transparent + MIXED_PREDICATE + " AND " + NATIVE_BOUND + " ORDER BY a.id", "id\n1\n");

            // LIMIT stops source pushdown. The separate bound must retain
            // nanosecond row-comparison precision as well as the mixed OR.
            final String limited = "SELECT a.id FROM lp_scope_a a "
                    + "JOIN (SELECT id,ts FROM lp_scope_b LIMIT 3) b ON a.id=b.id WHERE ";
            assertRows(limited + MIXED_PREDICATE + " AND b.id>0 AND " + NATIVE_BOUND + " ORDER BY a.id", "id\n1\n2\n");
            assertRows(limited + MIXED_PREDICATE
                    + " AND b.ts>='2020-01-01T00:00:00.000000001Z' ORDER BY a.id", "id\n2\n");
        });
    }

    @Test
    public void testDuplicateTimestampAliasesKeepUnderlyingJoinScopes() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String query = "SELECT q.id FROM (SELECT a.id id,a.active active,b.ts early,b.ts late "
                    + "FROM lp_scope_a a JOIN lp_scope_b b ON a.id=b.id) q WHERE ";
            final String mixed = "(q.late<'2020-01-01T00:00:00.000000001Z' OR q.active)";
            assertRows(query + mixed + " ORDER BY q.id", "id\n1\n2\n");
            assertRows(query + mixed + " AND q.id>0 ORDER BY q.id", "id\n1\n2\n");
            assertRows(query + mixed + " AND q.early<'2020-01-01T00:00:00.000001001Z' ORDER BY q.id", "id\n1\n");
        });
    }

    @Test
    public void testNestedConjunctionKeepsRepeatedTimestampUsesIndependent() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String predicate = "(b.id>0 AND (" + MIXED_PREDICATE + " AND ("
                    + NATIVE_BOUND + " AND a.id>0))) ORDER BY a.id";
            assertRows(TWO_SOURCES + predicate, "id\n1\n");
            assertRows(THREE_SOURCES + predicate, "id\n1\n");
            // >= rounds a native source bound down to its MICRO precision. If
            // all uses of b.ts share the OR's scope, row 1 would be lost here.
            assertRows(THREE_SOURCES + MIXED_PREDICATE
                    + " AND (b.id>0 AND b.ts>='2020-01-01T00:00:00.000000001Z') ORDER BY a.id", "id\n1\n2\n");
        });
    }

    @Test
    public void testOuterDerivedJoinScopeSurvivesFailureAndCompilerReuse() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String from = " FROM (" + DERIVED_TWO_SOURCES + ") q WHERE ";
            final String predicate = "(q.ts<'2020-01-01T00:00:00.000000001Z' OR q.active) AND q.id IN (1,2,3)";
            final RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory ignored = compiler.compile("SELECT q.id" + from + predicate
                        + " AND q.missing>0", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.fail("expected invalid column");
                } catch (SqlException e) {
                    TestUtils.assertEquals("Invalid column: q.missing", e.getFlyweightMessage());
                    Assert.assertEquals(200, e.getPosition());
                }
                retained = compiler.compile("SELECT q.id" + from + predicate + " ORDER BY q.id", sqlExecutionContext)
                        .getRecordCursorFactory();
                try {
                    try (RecordCursorFactory other = compiler.compile("SELECT count() FROM lp_scope_a", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(other);
                    }
                    compiler.clear();
                } catch (Throwable th) {
                    retained.close();
                    throw th;
                }
            }
            try (RecordCursorFactory factory = retained) {
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns("id\n1\n2\n");
            }
        });
    }

    @Test
    public void testOuterDerivedJoinScopesKeepMixedOrAndNativeBoundsIndependent() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final ObjList<String> sources = new ObjList<>();
            sources.add(DERIVED_TWO_SOURCES);
            sources.add("SELECT a.id id,a.active active,b.ts ts,b.id bid FROM lp_scope_a a "
                    + "JOIN lp_scope_c c ON a.id=c.id JOIN lp_scope_b b ON c.id=b.id");
            sources.add("SELECT p.id,p.active,b.ts ts,b.id bid FROM (SELECT a.id id,a.active active,c.id cid "
                    + "FROM lp_scope_a a JOIN lp_scope_c c ON a.id=c.id) p JOIN lp_scope_b b ON p.cid=b.id");
            sources.add("SELECT p.id,p.active,p.ts,p.bid FROM (" + DERIVED_TWO_SOURCES + ") p");
            for (int i = 0, n = sources.size(); i < n; i++) {
                final String query = "SELECT q.id FROM (" + sources.getQuick(i) + ") q WHERE ";
                final String mixed = "(q.ts<'2020-01-01T00:00:00.000000001Z' OR q.active)";
                assertRows(query + mixed + " ORDER BY q.id", "id\n1\n2\n");
                assertRows(query + mixed + " AND q.id>0 ORDER BY q.id", "id\n1\n2\n");
                assertRows(query + "q.bid>0 AND " + mixed + " ORDER BY q.id", "id\n1\n2\n");
                assertRows(query + mixed + " AND q.ts<'2020-01-01T00:00:00.000001001Z' ORDER BY q.id", "id\n1\n");
                // A source-local conjunct still uses native MICRO bounds after
                // transparent projections; the mixed-source OR above does not.
                assertRows(query + "q.ts<'2020-01-01T00:00:00.000000001Z' AND q.id>0 ORDER BY q.id", "id\n");
            }
        });
    }

    @Test
    public void testOuterLimitedJoinRetainsRowComparisonPrecision() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String query = "SELECT q.id FROM (" + DERIVED_TWO_SOURCES + " LIMIT 2) q WHERE ";
            final String mixed = "(q.ts<'2020-01-01T00:00:00.000000001Z' OR q.active)";
            assertRows(query + mixed + " AND q.id>0 ORDER BY q.id", "id\n1\n2\n");
            assertRows(query + mixed + " AND q.ts<'2020-01-01T00:00:00.000001001Z' ORDER BY q.id", "id\n1\n2\n");
            assertRows(query + "q.ts<'2020-01-01T00:00:00.000000001Z' AND q.id>0 ORDER BY q.id", "id\n1\n");
        });
    }

    @Test
    public void testThreeSourcesDoNotTaintMixedPredicateWithSourceLocalConjunct() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            // b is the last source, so both the mixed OR and the b-only term
            // initially share a post-join placement group.
            assertRows(THREE_SOURCES + MIXED_PREDICATE + " ORDER BY a.id", "id\n1\n2\n");
            assertRows(THREE_SOURCES + MIXED_PREDICATE + " AND b.id>0 ORDER BY a.id", "id\n1\n2\n");
            assertRows(THREE_SOURCES + "b.id>0 AND " + MIXED_PREDICATE
                    + " AND c.id>0 ORDER BY a.id", "id\n1\n2\n");
            assertRows(THREE_SOURCES + MIXED_PREDICATE + " AND " + NATIVE_BOUND + " ORDER BY a.id", "id\n1\n");
        });
    }

    @Test
    public void testTwoSourcesDoNotTaintMixedPredicateWithSourceLocalConjunct() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRows(TWO_SOURCES + MIXED_PREDICATE + " ORDER BY a.id", "id\n1\n2\n");
            assertRows(TWO_SOURCES + MIXED_PREDICATE + " AND b.id>0 ORDER BY a.id", "id\n1\n2\n");
            assertRows(TWO_SOURCES + "b.id>0 AND " + MIXED_PREDICATE + " ORDER BY a.id", "id\n1\n2\n");
            assertRows(TWO_SOURCES + MIXED_PREDICATE + " AND " + NATIVE_BOUND + " ORDER BY a.id", "id\n1\n");
        });
    }

    private void assertRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_scope_a(id INT,active BOOLEAN)");
        execute("CREATE TABLE lp_scope_b(id INT,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("CREATE TABLE lp_scope_c(id INT)");
        execute("INSERT INTO lp_scope_a VALUES(1,false),(2,true),(3,false)");
        execute("""
                INSERT INTO lp_scope_b VALUES
                    (1,'2020-01-01T00:00:00.000000Z'),
                    (2,'2020-01-01T00:00:00.000001Z'),
                    (3,'2020-01-01T00:00:00.000002Z')
                """);
        execute("INSERT INTO lp_scope_c VALUES(1),(2),(3)");
    }
}
