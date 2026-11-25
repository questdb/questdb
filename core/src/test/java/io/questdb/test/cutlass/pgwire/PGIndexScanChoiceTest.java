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


package io.questdb.test.cutlass.pgwire;

import io.questdb.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * The Index Or Scan choice of a GROUP BY over a POSTING-indexed symbol, as a prepared statement
 * executed twice with different binds: each execution estimates its own key, a 1% key opens the
 * index plan and a 20% key the scan, and both return the rows.
 */
public class PGIndexScanChoiceTest extends BasePGTest {

    @Test
    public void testPreparedStatementFollowsEachBind() throws Exception {
        // the parallel GROUP BY, and so the choice, runs only with shared query workers
        sharedQueryWorkerCount = 2;
        try {
            assertWithPgServer(CONN_AWARE_ALL, (connection, _, _, _) -> {
                // 257 hourly partitions of 100 rows: f is 20 of every 100 rows, k7 one
                execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING, a LONG) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL");
                execute("INSERT INTO t SELECT ((x - 1) * 36_000_000L)::TIMESTAMP, "
                        + "CASE WHEN (x - 1) % 5 = 0 THEN 'f' ELSE 'k' || ((x - 1) % 100) END, x FROM long_sequence(25_700)");
                try (
                        PreparedStatement query = connection.prepareStatement("SELECT count() n, sum(a) s FROM t WHERE s = ?");
                        PreparedStatement explain = connection.prepareStatement("EXPLAIN SELECT count() n, sum(a) s FROM t WHERE s = ?")
                ) {
                    for (int round = 0; round < 2; round++) {
                        // k7: x - 1 = 100h + 7, so x = 100h + 8, summed over h = 0..256
                        query.setString(1, "k7");
                        assertQueryResult(query, "n[BIGINT],s[BIGINT]\n257," + (100L * 256 * 257 / 2 + 8 * 257) + "\n");
                        explain.setString(1, "k7");
                        assertPlanContains(explain, "selected: 257", "opened: index");
                        // f: x - 1 = 5i, so x = 5i + 1, summed over i = 0..5139
                        query.setString(1, "f");
                        assertQueryResult(query, "n[BIGINT],s[BIGINT]\n5140," + (5L * 5139 * 5140 / 2 + 5140) + "\n");
                        explain.setString(1, "f");
                        assertPlanContains(explain, "selected: 5140", "opened: scan");
                    }
                }
            }, () -> {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_INDEX_GROUPBY_ENABLED, "true");
                setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_CHOICE_ENABLED, "true");
            });
        } finally {
            sharedQueryWorkerCount = 0;
        }
    }

    private void assertPlanContains(PreparedStatement statement, String... fragments) throws Exception {
        final StringBuilder plan = new StringBuilder();
        try (ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                plan.append(rs.getString(1)).append('\n');
            }
        }
        for (String fragment : fragments) {
            Assert.assertTrue(plan.toString(), plan.indexOf(fragment) >= 0);
        }
    }

    private void assertQueryResult(PreparedStatement statement, String expected) throws Exception {
        sink.clear();
        try (ResultSet rs = statement.executeQuery()) {
            assertResultSet(expected, sink, rs);
        }
    }
}
