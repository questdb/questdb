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

import io.questdb.PropertyKey;
import io.questdb.cairo.SqlJitMode;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.jit.JitUtil;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Runs sub-queries of many plan shapes (scans, index lookups, filters, sorts, Top-K, limits, keyed
 * and keyless aggregates, DISTINCT, set operations, bind variables, nested sub-queries) as IN and
 * scalar consumers, over a plain, a bitmap-indexed, a posting-covering-indexed and a
 * posting-indexed table with Parquet partitions, under every combination of the settings that
 * change the generator's physical choices (parallel GROUP BY, which also gates vectorised GROUP BY,
 * parallel filter and Parquet read, JIT, parallel top K and encoded sort). A sub-query is evaluated
 * once per execution whatever factory the generator builds for it, so the rows of every run must
 * equal those of the plain table under serial execution.
 */
public class SubqueryStabilityContractTest extends AbstractCairoTest {
    private static final String[] IN_SYM = {
            "SELECT sym FROM #T",
            "SELECT sym FROM #T WHERE ts >= '2024-01-03'",
            "SELECT sym FROM #T WHERE ts IN '2024-01-02' AND k > 2",
            "SELECT sym FROM #T WHERE sym = 'A'",
            "SELECT sym FROM #T WHERE sym = 'A' AND k > 3",
            "SELECT sym FROM #T WHERE sym IN ('A', 'C')",
            "SELECT sym FROM #T WHERE sym IN ('A', 'C') AND k < 5",
            "SELECT sym FROM #T WHERE sym != 'A'",
            "SELECT sym FROM #T WHERE sym NOT IN ('A', 'B') AND k > 1",
            "SELECT sym FROM #T WHERE sym IN ('A', 'C') ORDER BY sym",
            "SELECT sym FROM #T WHERE sym IN ('D', 'B') ORDER BY sym DESC",
            "SELECT sym FROM #T WHERE sym LIKE 'C%'",
            "SELECT sym FROM #T WHERE sym ILIKE 'b%'",
            "SELECT sym FROM #T WHERE sym = 'D' AND ts >= '2024-01-02'",
            "SELECT sym FROM (SELECT sym, v FROM #T WHERE sym = 'B') WHERE v > 2",
            "SELECT sym FROM #T WHERE k > 4",
            "SELECT sym FROM #T WHERE k * 2 > l",
            "SELECT sym FROM #T WHERE length(sym) = 1 AND k > 2",
            "SELECT sym FROM #T WHERE ts < now()",
            "SELECT sym FROM #T WHERE k = (SELECT max(k) FROM #T)",
            "SELECT sym FROM #T WHERE sym IN (SELECT sym FROM #T WHERE k = 3)",
            "SELECT sym::STRING FROM #T WHERE k > 6",
            "SELECT concat(sym, '') FROM #T WHERE k < 2",
            "SELECT sym FROM #T ORDER BY k",
            "SELECT sym FROM #T ORDER BY v DESC",
            "SELECT sym FROM #T ORDER BY sym",
            "SELECT sym FROM #T ORDER BY ts DESC",
            "SELECT sym FROM #T ORDER BY concat(sym, 'x'), ts",
            "SELECT sym FROM #T WHERE k > 1 ORDER BY concat(sym, 'x'), ts LIMIT 5",
            "SELECT sym FROM #T ORDER BY (k + 1) * (l + 2), ts",
            "SELECT s FROM (SELECT sym s, ts FROM #T WHERE k = 1 UNION ALL SELECT sym, ts FROM #T WHERE k = 2) ORDER BY concat(s, 'x'), ts",
            "SELECT s FROM (SELECT sym s, sum(l) t FROM #T GROUP BY sym ORDER BY t DESC LIMIT 2)",
            "SELECT sym FROM #T WHERE $2 > 1",
            "SELECT sym FROM #T WHERE now() > '2020-01-01'::TIMESTAMP AND k > 6",
            "SELECT sym FROM #T WHERE 1 = 2",
            "SELECT sym FROM #T ORDER BY l, ts LIMIT 5",
            "SELECT sym FROM #T WHERE k > 2 ORDER BY v, ts LIMIT 3",
            "SELECT sym FROM #T ORDER BY l LIMIT 3",
            "SELECT sym FROM #T WHERE k > 1 ORDER BY l DESC LIMIT 2",
            "SELECT sym FROM #T ORDER BY ts DESC LIMIT 3",
            "SELECT sym FROM #T LIMIT 5",
            "SELECT sym FROM #T LIMIT -3",
            "SELECT sym FROM #T LIMIT 2, 6",
            "SELECT sym FROM #T WHERE k > 2 LIMIT 3",
            "SELECT sym FROM #T WHERE sym = 'C' LIMIT 2",
            "SELECT sym FROM #T GROUP BY sym",
            "SELECT sym FROM #T WHERE ts >= '2024-01-02' GROUP BY sym",
            "SELECT sym FROM #T WHERE k > 3 GROUP BY sym",
            "SELECT sym FROM (SELECT sym, count() c FROM #T GROUP BY sym) WHERE c > 7",
            "SELECT sym FROM (SELECT sym, max(k) m FROM #T GROUP BY sym) WHERE m > 8",
            "SELECT sym FROM (SELECT sym, sum(l) s FROM #T WHERE k > 1 GROUP BY sym) WHERE s > 120",
            "SELECT sym FROM (SELECT sym, k, count() FROM #T GROUP BY sym, k) WHERE k = 4",
            "SELECT DISTINCT sym FROM #T",
            "SELECT DISTINCT sym FROM #T WHERE k > 6",
            "SELECT sym FROM #T WHERE k = 1 UNION SELECT sym FROM #T WHERE k = 2",
            "SELECT sym FROM #T WHERE k = 1 UNION ALL SELECT sym FROM #T WHERE k = 8",
            "SELECT sym FROM #T WHERE k > 3 INTERSECT SELECT sym FROM #T WHERE k < 3",
            "SELECT sym FROM #T EXCEPT SELECT sym FROM #T WHERE k = 0",
            "SELECT sym FROM #T WHERE k > 3 INTERSECT ALL SELECT sym FROM #T WHERE k < 3",
            "SELECT sym FROM #T EXCEPT ALL SELECT sym FROM #T WHERE k > 0",
            "SELECT sym FROM #T WHERE sym = 'B' ORDER BY v, ts LIMIT 2",
            "SELECT sym FROM #T WHERE sym = $1",
            "SELECT sym FROM #T WHERE sym = $1 AND k < $2",
            "SELECT sym FROM #T WHERE k > $2 LIMIT $2",
            "SELECT sym FROM #T WHERE sym IN ($1, 'D') ORDER BY sym",
            "SELECT sym FROM #T ORDER BY l DESC, ts LIMIT 4",
            "SELECT s FROM (SELECT sym s, max(k) m FROM #T GROUP BY sym) WHERE m > 8",
            "SELECT sym FROM #T WHERE sym IN ('A', 'C') ORDER BY sym, ts LIMIT 3",
            "SELECT k::SYMBOL FROM #T WHERE k < 3",
            "SELECT x::STRING FROM long_sequence(4)",
            "SELECT sym::VARCHAR FROM #T WHERE ts >= '2024-01-03' AND k > 4",
            "SELECT sym FROM #T WHERE k = 1 UNION ALL SELECT 'A'",
            "SELECT s FROM (SELECT sym s, ts FROM #T WHERE k = 1 UNION ALL SELECT sym, ts FROM #T WHERE k = 6 ORDER BY ts)",
            "SELECT s FROM (SELECT sym s, ts FROM #T WHERE k = 1 UNION ALL SELECT sym, ts FROM #T WHERE k = 6 ORDER BY ts DESC LIMIT 3)",
            "SELECT sym FROM (SELECT sym, ts FROM #T WHERE k > 1 ORDER BY ts DESC LIMIT 6) WHERE ts > '2024-01-03'",
    };
    private static final String[] SCALAR_K = {
            "SELECT max(k) - 3 FROM #T",
            "SELECT min(k) + 4 FROM #T WHERE sym = 'B'",
            "SELECT count() / 5 FROM #T",
            "SELECT count() FROM #T WHERE sym = 'Z'",
            "SELECT sum(k) % 7 FROM #T WHERE k > 2",
            "SELECT first(k) FROM #T",
            "SELECT last(k) FROM #T WHERE sym IN ('A', 'D')",
            "SELECT count_distinct(sym)::INT FROM #T",
            "SELECT max(l)::INT - 10 FROM #T WHERE ts >= '2024-01-02'",
            "SELECT k FROM #T WHERE sym = 'C' ORDER BY l LIMIT 1",
            "SELECT k FROM #T ORDER BY v DESC, ts LIMIT 1",
            "SELECT k FROM #T LIMIT 1",
            "SELECT k FROM #T LIMIT -1",
            "SELECT m FROM (SELECT sym, max(k) m FROM #T GROUP BY sym) WHERE sym = 'A'",
            "SELECT k FROM #T WHERE k = 7 UNION SELECT 7",
            "SELECT x::INT FROM long_sequence(3) WHERE x = 3",
    };
    private static final String[] SCALAR_TS = {
            "SELECT min(ts) FROM #T WHERE k > 6",
            "SELECT max(ts) - 86_400_000_000 FROM #T WHERE sym = 'B'",
            "SELECT ts FROM #T WHERE k = 3 LIMIT 1",
            "SELECT ts FROM #T WHERE sym = 'C' LIMIT -1",
            "SELECT ts FROM #T ORDER BY v DESC, ts LIMIT 1",
            "SELECT first(ts) FROM #T WHERE ts >= '2024-01-02'",
            "SELECT last(ts) FROM #T WHERE sym = 'D' AND ts < '2024-01-03'",
    };
    private static final String[] TABLES = {"t_plain", "t_bitmap", "t_posting", "t_parquet"};

    @Test
    public void testSubqueriesUnderEveryGeneratorConfiguration() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MATERIALIZATION_THRESHOLD, 1);
        assertMemoryLeak(() -> {
            createTable("t_plain", "sym SYMBOL");
            createTable("t_bitmap", "sym SYMBOL INDEX");
            createTable("t_posting", "sym SYMBOL");
            execute("ALTER TABLE t_posting ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (k, v)");
            createTable("t_parquet", "sym SYMBOL INDEX TYPE POSTING");
            execute("ALTER TABLE t_parquet CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
            engine.releaseAllWriters();

            final ObjList<String> queries = new ObjList<>();
            addQueries(queries, IN_SYM, "SELECT ts, sym, k FROM #T WHERE sym IN (", ") ORDER BY ts, k");
            addQueries(queries, SCALAR_K, "SELECT ts, sym, k FROM #T WHERE k > (", ") ORDER BY ts, k");
            addQueries(queries, SCALAR_TS, "SELECT ts, sym, k FROM #T WHERE ts >= (", ") ORDER BY ts, k");
            addQueries(queries, SCALAR_TS, "SELECT ts, sym, k FROM #T WHERE ts < (", ") AND sym = 'A' ORDER BY ts, k");

            bindVariableService.setStr(0, "B");
            bindVariableService.setInt(1, 3);
            final ObjList<String> expected = new ObjList<>();
            final StringSink failures = new StringSink();
            final StringSink actual = new StringSink();
            int runs = 0;
            final SqlExecutionContext context = sqlExecutionContext;
            final boolean isJitAvailable = JitUtil.isJitSupported();
            try {
                for (int config = 0; config < 32; config++) {
                    final boolean isParallelGroupBy = (config & 1) != 0;
                    final boolean isParallelFilter = (config & 2) != 0;
                    final boolean isJit = (config & 4) != 0;
                    final boolean isParallelTopK = (config & 8) != 0;
                    final boolean isEncodedSort = (config & 16) != 0;
                    if (isJit && !isJitAvailable) {
                        continue;
                    }
                    context.setParallelGroupByEnabled(isParallelGroupBy);
                    context.setParallelFilterEnabled(isParallelFilter);
                    context.setParallelReadParquetEnabled(isParallelFilter);
                    context.setParallelTopKEnabled(isParallelTopK);
                    context.setJitMode(isJit ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                    node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, isEncodedSort);
                    for (int t = 0; t < TABLES.length; t++) {
                        for (int q = 0, n = queries.size(); q < n; q++) {
                            final String sql = queries.getQuick(q).replace("#T", TABLES[t]);
                            String result;
                            try {
                                TestUtils.printSql(engine, context, sql, actual);
                                result = actual.toString();
                            } catch (Throwable th) {
                                result = "error: " + th;
                            }
                            runs++;
                            if (expected.size() <= q) {
                                expected.add(result);
                            }
                            if (result.startsWith("error: ") || !result.equals(expected.getQuick(q))) {
                                failures.put("config=").put(config).put(" [parallelGroupBy=").put(isParallelGroupBy)
                                        .put(", parallelFilter=").put(isParallelFilter).put(", jit=").put(isJit)
                                        .put(", parallelTopK=").put(isParallelTopK).put(", encodedSort=").put(isEncodedSort).put("] sql=").put(sql).put('\n')
                                        .put(result).put('\n');
                            }
                        }
                    }
                }
            } finally {
                context.setParallelGroupByEnabled(configuration.isSqlParallelGroupByEnabled());
                context.setParallelFilterEnabled(configuration.isSqlParallelFilterEnabled());
                context.setParallelReadParquetEnabled(configuration.isSqlParallelReadParquetEnabled());
                context.setParallelTopKEnabled(configuration.isSqlParallelTopKEnabled());
                context.setJitMode(configuration.getSqlJitMode());
            }
            Assert.assertTrue(runs > 0);
            Assert.assertEquals("", failures.toString());
        });
    }

    private static void addQueries(ObjList<String> queries, String[] subqueries, String prefix, String suffix) {
        for (String subquery : subqueries) {
            queries.add(prefix + subquery + suffix);
        }
    }

    private static void createTable(String name, String symbolDefinition) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, " + symbolDefinition
                + ", k INT, v DOUBLE, l LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO " + name + """
                 SELECT
                    '2024-01-01'::TIMESTAMP + (x - 1) * 10_800_000_000,
                    CASE WHEN x % 9 = 0 THEN NULL WHEN x % 4 = 0 THEN 'A' WHEN x % 4 = 1 THEN 'B' WHEN x % 4 = 2 THEN 'C' ELSE 'D' END,
                    ((x * 7) % 10)::INT,
                    (x * 3 % 11) / 2.0,
                    (x * 13) % 37
                FROM long_sequence(32)
                """);
    }
}
