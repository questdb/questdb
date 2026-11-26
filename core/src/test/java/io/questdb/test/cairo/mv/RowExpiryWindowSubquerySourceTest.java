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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.RowExpiryUtil;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A sub-query of a window WHEN policy may read only the view the policy is set on. Every read of the
 * view compiles the stored predicate again and nothing records what else the sub-query reads, so a
 * DROP, RENAME or column change on another table, view or table function's source would leave every
 * read of the view failing. SET EXPIRE rejects such a sub-query instead, and CREATE, where the view
 * does not exist yet, rejects every sub-query read. See "A window policy's subquery reads only its
 * view" in {@code docs/row-expiry-limitations.md}.
 * <p>
 * Shared data: base A: 1.0@d1, 3.0@d2; B: 5.0@d1, 4.0@d2. {@link #SELF_ROWS} is what the view shows
 * under a policy that expires a row below its key's maximum when the key has a raw row below 2, which
 * only A has.
 */
public class RowExpiryWindowSubquerySourceTest extends AbstractCairoTest {
    private static final String ALL_ROWS = """
            k\tv\tts
            A\t1.0\t2024-01-01T00:00:00.000000Z
            A\t3.0\t2024-01-02T00:00:00.000000Z
            B\t5.0\t2024-01-01T00:00:00.000000Z
            B\t4.0\t2024-01-02T00:00:00.000000Z
            """;
    private static final String MESSAGE = "EXPIRE ROWS predicate can read only the view the policy is set on";
    private static final String READ = "SELECT * FROM mv ORDER BY k, ts";
    private static final String SELF_ROWS = """
            k\tv\tts
            A\t3.0\t2024-01-02T00:00:00.000000Z
            B\t5.0\t2024-01-01T00:00:00.000000Z
            B\t4.0\t2024-01-02T00:00:00.000000Z
            """;
    private static final String WINDOW = "v < max(v) OVER (PARTITION BY k) AND ";

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testAlterAcceptsSubqueryReadingTheView() throws Exception {
        final String[] subqueries = {
                "k IN (SELECT k FROM mv WHERE v < 2)",
                "k IN (SELECT k FROM MV WHERE v < 2)",
                "k IN (SELECT k FROM \"mv\" WHERE v < 2)",
                "k IN (SELECT k FROM '*!*mv' WHERE v < 2)",
                "k IN (SELECT k FROM public.mv WHERE v < 2)",
                "k IN (SELECT k FROM (SELECT k, v FROM mv) WHERE v < 2)",
                "k IN (SELECT k FROM (WITH c AS (SELECT k, v FROM mv) SELECT k, v FROM c) WHERE v < 2)",
                "k IN (SELECT k FROM mv WHERE v < (SELECT min(v) + 1 FROM mv))",
        };
        assertMemoryLeak(() -> {
            createView();
            for (String subquery : subqueries) {
                final String predicate = WINDOW + subquery;
                execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN " + predicate);
                drainWalAndMatViewQueues();
                assertStoredPredicate(RowExpiryUtil.encodeWindow(predicate));
                assertQuery(READ).noLeakCheck().returns(SELF_ROWS);
            }
        });
    }

    @Test
    public void testAlterRejectsSubqueryReadingAnotherSource() throws Exception {
        // Each entry: the sub-query, and the reference the rejection points at.
        final String[][] subqueries = {
                {"k IN (SELECT k FROM lookup)", "lookup"},
                {"k IN (SELECT k FROM \"lookup\")", "\"lookup\""},
                {"k IN (SELECT k FROM '*!*lookup')", "'*!*lookup'"},
                {"k IN (SELECT k FROM public.lookup)", "lookup"},
                {"k IN (SELECT k FROM nope)", "nope"},
                {"k IN (SELECT k FROM base)", "base"},
                {"k IN (SELECT k FROM mv2)", "mv2"},
                {"k IN (SELECT k FROM vw_lookup)", "vw_lookup"},
                {"k IN (SELECT k FROM vw_mv)", "vw_mv"},
                {"k IN (SELECT 'A' FROM long_sequence(1))", "long_sequence"},
                {"k IN (SELECT m.k FROM mv m JOIN lookup l ON m.k = l.k)", "lookup"},
                {"k IN (SELECT k FROM mv UNION SELECT k FROM lookup)", "lookup"},
                {"k IN (SELECT k FROM (SELECT k FROM lookup))", "lookup"},
                {"k IN (SELECT k FROM (WITH c AS (SELECT k FROM lookup) SELECT k FROM c))", "lookup"},
                {"k IN (SELECT k FROM mv WHERE v > (SELECT min(x) FROM lookup))", "lookup"},
                {"k IN (SELECT k FROM lookup WHERE x < max(x) OVER ())", "lookup"},
                {"v > (SELECT min(x) FROM lookup)", "lookup"},
        };
        assertMemoryLeak(() -> {
            createView();
            execute("CREATE MATERIALIZED VIEW mv2 AS (SELECT * FROM base)");
            execute("CREATE VIEW vw_lookup AS (SELECT * FROM lookup)");
            execute("CREATE VIEW vw_mv AS (SELECT * FROM mv)");
            drainWalAndMatViewQueues();
            // The policy in force stays, whatever the rejected statement reads.
            final String installed = WINDOW + "k IN (SELECT k FROM mv WHERE v < 2)";
            execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN " + installed);
            drainWalAndMatViewQueues();
            for (String[] subquery : subqueries) {
                final String sql = "ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN " + WINDOW + subquery[0];
                assertExceptionNoLeakCheck(sql, sql.indexOf(subquery[1], sql.indexOf("WHEN")), MESSAGE);
            }
            drainWalAndMatViewQueues();
            assertStoredPredicate(RowExpiryUtil.encodeWindow(installed));
            assertQuery(READ).noLeakCheck().returns(SELF_ROWS);
            assertQuery("SELECT * FROM vw_mv ORDER BY k, ts").noLeakCheck().returns(SELF_ROWS);
        });
    }

    @Test
    public void testCreateRejectsSubqueryRead() throws Exception {
        // The view does not exist before CREATE, so no sub-query can read it: a read of the view's own name,
        // of its base table, or of any other object is rejected.
        assertMemoryLeak(() -> {
            createView();
            for (String source : new String[]{"mv2", "base", "lookup", "mv"}) {
                final String sql = "CREATE MATERIALIZED VIEW mv2 AS (SELECT * FROM base) EXPIRE ROWS WHEN "
                        + WINDOW + "k IN (SELECT k FROM " + source + " WHERE v < 2)";
                assertExceptionNoLeakCheck(sql, sql.indexOf("FROM " + source + " WHERE") + "FROM ".length(), MESSAGE);
                Assert.assertNull(engine.getTableTokenIfExists("mv2"));
            }
            assertQuery(READ).noLeakCheck().expectSize().returns(ALL_ROWS);
        });
    }

    private void assertStoredPredicate(String expected) {
        try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
            Assert.assertEquals(expected, metadata.getExpiryPredicate());
        }
    }

    private void createView() throws Exception {
        execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('A', 1.0, '2024-01-01T00:00:00Z'),
                ('A', 3.0, '2024-01-02T00:00:00Z'),
                ('B', 5.0, '2024-01-01T00:00:00Z'),
                ('B', 4.0, '2024-01-02T00:00:00Z')
                """);
        execute("CREATE TABLE lookup (k SYMBOL, x DOUBLE)");
        execute("INSERT INTO lookup VALUES ('A', 2.0)");
        execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base)");
        drainWalAndMatViewQueues();
        assertQuery(READ).noLeakCheck().expectSize().returns(ALL_ROWS);
    }
}
