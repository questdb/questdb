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

/**
 * SAMPLE BY and FILL errors the statement decides are raised while binding: an outer query that prunes the
 * sub-query's columns, a predicate the optimiser folds, CREATE VIEW and a later generation error do not hide them.
 */
public class SampleByBindValidationTest extends AbstractCairoTest {
    private static final String GENERATION_ERROR_SUBQUERY = "(SELECT a.x FROM (SELECT * FROM t ORDER BY ts DESC) a ASOF JOIN t p)";
    private static final String[][] CASES = {
            {"SELECT ts, sum(x) s FROM t SAMPLE BY 1h FILL('abc')", "'abc'", "invalid fill value: 'abc'"},
            {"SELECT ts, first(ts) f FROM t SAMPLE BY 1h FILL(42)", "42",
                    "Invalid fill value: '42'. Timestamp fill value must be in quotes. Example: '2019-01-01T00:00:00.000Z'"},
            {"SELECT ts, sum(x) s FROM t SAMPLE BY 1h FILL(true)", "true", "fill value of type BOOLEAN cannot fill column of type DOUBLE"},
            {"SELECT ts, sum(x) s FROM t SAMPLE BY 1h FILL(rnd_double())", "rnd_double", "fill value must be a constant expression"},
            {"SELECT ts, first(b) f FROM t SAMPLE BY 1h FILL(NULL)", "NULL", "fill value of type NULL cannot fill column of type BOOLEAN"},
            {"SELECT ts, first(u) f FROM t SAMPLE BY 1h FILL('nope')", "'nope'", "invalid fill value: 'nope'"},
            {"SELECT ts, sum(x) a, max(x) b FROM t SAMPLE BY 1h FILL(PREV(b), PREV(a))", "PREV(b)",
                    "FILL(PREV) chains are not supported: source column is itself a cross-column PREV"},
            {"SELECT ts, sum(x) a, max(x) b FROM t SAMPLE BY 1h FILL(PREV(b), 0)", "PREV(b)",
                    "FILL(PREV) cannot reference a column that is itself filled with a constant"},
            {"SELECT ts, k, last(k) a FROM t SAMPLE BY 1h FILL(PREV(k))", "k))",
                    "FILL(PREV(k)) is not supported on SYMBOL columns; use bare FILL(PREV) instead"},
            {"SELECT ts, sum(x) a, count() b FROM t SAMPLE BY 1h FILL(PREV(b), PREV)", "b),",
                    "FILL(PREV(b)): source type LONG cannot fill target column of type DOUBLE"},
            {"SELECT ts, sum(x) a, max(x) b FROM t SAMPLE BY 1h FILL(LINEAR, 'abc')", "'abc'", "invalid fill value: 'abc'"},
            {"SELECT ts, sum(x) a, first(b) f FROM t SAMPLE BY 1h FILL(LINEAR, NULL)", "NULL",
                    "fill value of type NULL cannot fill column of type BOOLEAN"},
            {"SELECT ts, sum(x) a, max(x) b, min(x) c FROM t SAMPLE BY 1h FILL(LINEAR, NONE)", "NONE",
                    "insufficient fill values for SAMPLE BY FILL: expected 3 values but only 2 provided"},
            {"SELECT ts, k, sum(x) s FROM t SAMPLE BY 1h FILL(LINEAR, 1)", null,
                    "linear interpolation is not supported when using fill values for keyed sample by expression"},
            {"SELECT ts, sum(x) s FROM t SAMPLE BY 1h FROM rnd_timestamp(1,2,0) FILL(LINEAR)", "rnd_timestamp",
                    "from lower bound must be a constant expression convertible to a TIMESTAMP"},
            {"SELECT ts, sum(x) s FROM t SAMPLE BY 1h FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE rnd_str('a')", "rnd_str",
                    "timezone must be a constant expression of STRING or CHAR type"},
    };

    @Test
    public void testErrorPrecedesGenerationError() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String generationError = "SELECT ts FROM t WHERE x = " + GENERATION_ERROR_SUBQUERY;
            assertQuery(generationError).noLeakCheck()
                    .fails(generationError.indexOf("ASOF"), "left side of time series join doesn't have ASC timestamp order");
            for (String[] c : CASES) {
                final String sql = "SELECT ts FROM t WHERE x = " + GENERATION_ERROR_SUBQUERY + " UNION ALL SELECT ts FROM (" + c[0] + ")";
                assertQuery(sql).noLeakCheck().fails(position(sql, c), c[2]);
            }
        });
    }

    @Test
    public void testErrorInCreateView() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (String[] c : CASES) {
                final String sql = "CREATE VIEW v AS (SELECT ts FROM (" + c[0] + "))";
                assertException(sql, c[1] == null ? sql.indexOf("SELECT") : position(sql, c), c[2]);
            }
        });
    }

    @Test
    public void testErrorInFoldedPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (String[] c : CASES) {
                final String sql = "SELECT x FROM t WHERE true OR ts = (SELECT ts FROM (" + c[0] + ") LIMIT 1)";
                assertQuery(sql).noLeakCheck().fails(position(sql, c), c[2]);
            }
        });
    }

    @Test
    public void testErrorInPrunedSubquery() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (String[] c : CASES) {
                final String sql = "SELECT ts FROM (" + c[0] + ")";
                assertQuery(sql).noLeakCheck().fails(position(sql, c), c[2]);
            }
        });
    }

    @Test
    public void testInvalidTimezoneInPrunedSubquery() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT ts FROM (SELECT ts, sum(x) s FROM t SAMPLE BY 1h FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'Foo/Bar')")
                    .noLeakCheck()
                    .fails(0, "invalid timezone: Foo/Bar");
        });
    }

    @Test
    public void testPrunedFillValuesStillFill() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT ts, s FROM (SELECT ts, sum(x) s, first(u) f, max(i::SHORT) m FROM t SAMPLE BY 1h FILL(5, '22222222-2222-2222-2222-222222222222', 100_000))")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts	s
                            2024-01-01T00:00:00.000000Z	1.0
                            2024-01-01T01:00:00.000000Z	5.0
                            2024-01-01T02:00:00.000000Z	5.0
                            2024-01-01T03:00:00.000000Z	2.0
                            """);
            assertQuery("SELECT ts, array_elem_sum(arr) a FROM t SAMPLE BY 1h FILL(ARRAY[9.0])")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts	a
                            2024-01-01T00:00:00.000000Z	[1.0]
                            2024-01-01T01:00:00.000000Z	[9.0]
                            2024-01-01T02:00:00.000000Z	[9.0]
                            2024-01-01T03:00:00.000000Z	[2.0]
                            """);
            assertQuery("SELECT ts, array_elem_sum(arr) a FROM t SAMPLE BY 1h FILL('{9.0}')")
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts	a
                            2024-01-01T00:00:00.000000Z	[1.0]
                            2024-01-01T01:00:00.000000Z	[9.0]
                            2024-01-01T02:00:00.000000Z	[9.0]
                            2024-01-01T03:00:00.000000Z	[2.0]
                            """);
        });
    }

    private static void createTable() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, x DOUBLE, k SYMBOL, b BOOLEAN, u UUID, arr DOUBLE[], i INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO t VALUES
                ('2024-01-01T00:00:00', 1.0, 'a', true, '11111111-1111-1111-1111-111111111111', ARRAY[1.0], 1),
                ('2024-01-01T03:00:00', 2.0, 'b', false, null, ARRAY[2.0], 2)
                """);
    }

    private static int position(String sql, String[] c) {
        return c[1] == null ? 0 : sql.indexOf(c[1], sql.indexOf(c[0]));
    }
}
