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

package io.questdb.test.cairo.lv;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Phase 1 lifecycle and catalogue tests for live views (RFC 123). Complements
 * {@link LiveViewSmokeTest} (CREATE / DROP / refresh / restart / anchor reset)
 * with surface coverage:
 * <ul>
 *     <li>CREATE IF NOT EXISTS / DROP IF EXISTS idempotency.</li>
 *     <li>DROP of a non-existent live view raises an asserted-wording error.</li>
 *     <li>{@code tables()} reports the LV with table_type='L'.</li>
 *     <li>{@code information_schema.tables()} reports it as LIVE VIEW and not insertable.</li>
 *     <li>{@code pg_class()} reports relkind='v'.</li>
 *     <li>{@code SHOW COLUMNS} reflects the LV's projected schema, including the
 *     timestamp designation.</li>
 * </ul>
 */
public class LiveViewTest extends AbstractCairoTest {

    private void assertMutationRejected(String sql, String expectedMessageFragment) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            try {
                execute(sql);
                Assert.fail("expected SqlException for " + sql);
            } catch (SqlException e) {
                Assert.assertTrue(
                        "expected message containing '" + expectedMessageFragment + "', got: " + e.getMessage(),
                        e.getMessage().contains(expectedMessageFragment)
                );
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateLiveViewIfNotExists() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            // IF NOT EXISTS should succeed when the view already exists.
            execute("CREATE LIVE VIEW IF NOT EXISTS lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDropLiveViewIfExists() throws Exception {
        assertMemoryLeak(() -> {
            // No view exists yet — IF EXISTS must swallow the error.
            execute("DROP LIVE VIEW IF EXISTS nonexistent");
        });
    }

    @Test
    public void testDropNonExistentLiveViewFails() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("DROP LIVE VIEW nonexistent");
                Assert.fail("expected SqlException for missing live view");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("live view does not exist")
                );
            }
        });
    }

    @Test
    public void testTablesShowsLiveViewWithTypeL() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            assertSql(
                    "table_type\nL\n",
                    "SELECT table_type FROM tables() WHERE table_name = 'lv'"
            );
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInformationSchemaTablesShowsLiveView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            assertSql(
                    "table_type\tis_insertable_into\n" +
                            "LIVE VIEW\tfalse\n",
                    "SELECT table_type, is_insertable_into FROM information_schema.tables() " +
                            "WHERE table_name = 'lv'"
            );
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testPgClassReportsLiveViewAsRelkindV() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            assertSql(
                    "relkind\nv\n",
                    "SELECT relkind FROM pg_class() WHERE relname = 'lv'"
            );
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRejectAlterTable() throws Exception {
        assertMutationRejected(
                "ALTER TABLE lv ADD COLUMN x INT",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectInsertInto() throws Exception {
        assertMutationRejected(
                "INSERT INTO lv VALUES (1, '2026-01-01T00:00:00.000000Z', 1)",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectUpdate() throws Exception {
        assertMutationRejected(
                "UPDATE lv SET val = 0",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectTruncate() throws Exception {
        assertMutationRejected(
                "TRUNCATE TABLE lv",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectReindex() throws Exception {
        assertMutationRejected(
                "REINDEX TABLE lv COLUMN val LOCK EXCLUSIVE",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectVacuum() throws Exception {
        assertMutationRejected(
                "VACUUM TABLE lv",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectRename() throws Exception {
        assertMutationRejected(
                "RENAME TABLE lv TO lv2",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectDropTableOnLiveView() throws Exception {
        assertMutationRejected(
                "DROP TABLE lv",
                "table name expected, got live view name: lv"
        );
    }

    @Test
    public void testRejectDropViewOnLiveView() throws Exception {
        assertMutationRejected(
                "DROP VIEW lv",
                "view name expected, got table or materialized view name"
        );
    }

    @Test
    public void testRejectDropMaterializedViewOnLiveView() throws Exception {
        assertMutationRejected(
                "DROP MATERIALIZED VIEW lv",
                "materialized view name expected, got table or view name"
        );
    }

    @Test
    public void testShowColumnsReflectsLiveViewSchema() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT sym, price, ts, row_number() OVER (PARTITION BY sym ORDER BY ts) AS rn FROM base");
            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude\n" +
                            "sym\tSYMBOL\tfalse\t0\ttrue\t128\t0\tfalse\tfalse\t\t\n" +
                            "price\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t\n" +
                            "rn\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t\n",
                    "SHOW COLUMNS FROM lv"
            );
            execute("DROP LIVE VIEW lv");
        });
    }
}
