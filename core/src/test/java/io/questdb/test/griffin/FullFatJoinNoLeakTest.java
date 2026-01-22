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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FullFatJoinNoLeakTest extends AbstractCairoTest {

    @Test
    public void testAsOfJoinNoLeak() throws Exception {
        testJoinThrowsLimitOverflowException(
                "SELECT \n" +
                        "    b.timebid timebid,\n" +
                        "    a.timeask timeask, \n" +
                        "    b.b b, \n" +
                        "    a.a a\n" +
                        "FROM (select b.bid b, b.ts timebid from bids b) b \n" +
                        "    ASOF JOIN\n" +
                        "(select a.ask a, a.ts timeask from asks a) a\n" +
                        "WHERE (b.timebid != a.timeask)"
        );
    }

    @Test
    public void testLtJoinNoLeak() throws Exception {
        testJoinThrowsLimitOverflowException(
                "SELECT \n" +
                        "    b.timebid timebid,\n" +
                        "    a.timeask timeask, \n" +
                        "    b.b b, \n" +
                        "    a.a a\n" +
                        "FROM (select b.bid b, b.ts timebid from bids b) b \n" +
                        "    LT JOIN\n" +
                        "(select a.ask a, a.ts timeask from asks a) a\n" +
                        "WHERE (b.timebid != a.timeask)"
        );
    }

    private void createTablesToJoin() throws SqlException {
        // ASKS
        execute("create table asks (ask int, ts timestamp) timestamp(ts) partition by none");
        execute("insert into asks values(100, 0)");
        execute("insert into asks values(101, 2);");
        execute("insert into asks values(102, 4);");

        // BIDS
        execute("create table bids (bid int, ts timestamp) timestamp(ts) partition by none");
        execute("insert into bids values(101, 1);");
        execute("insert into bids values(102, 3);");
        execute("insert into bids values(103, 5);");
    }

    private void testJoinThrowsLimitOverflowException(String sql) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JOIN_METADATA_PAGE_SIZE, 10);
        node1.setProperty(PropertyKey.CAIRO_SQL_JOIN_METADATA_MAX_RESIZES, 0);

        assertMemoryLeak(() -> {
            try {
                createTablesToJoin();
                assertExceptionNoLeakCheck(sql, sqlExecutionContext, true);
            } catch (LimitOverflowException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "limit of 0 resizes exceeded in FastMap");
                Assert.assertFalse(ex.isCritical());
            }
        });
    }

    @Test
    public void testLtJoinWithTimestampInOnClauseThrowsSqlException() throws Exception {
        assertMemoryLeak(() -> {
            execute("DROP TABLE IF EXISTS t1");
            execute("DROP TABLE IF EXISTS t2");

            execute("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL, val INT) timestamp(ts) partition by DAY");
            execute("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL INDEX, val INT) timestamp(ts) partition by DAY");

            execute("INSERT INTO t1 VALUES ('2024-01-01T10:00:00.000000Z', 1, 'A', 10)");
            execute("INSERT INTO t2 VALUES ('2024-01-01T10:00:00.000000Z', 2, 'A', 20)");

            try {
                execute(
                        "SELECT t1.ts, sub.isum FROM t1 LT JOIN (" +
                                "  SELECT ts, sum(i) as isum, s FROM t2 SAMPLE BY 1us ALIGN TO CALENDAR" +
                                ") sub ON (t1.s = sub.s) AND (t1.ts = sub.ts)"
                );
                Assert.fail("Expected SqlException due to timestamp in join key");
            } catch (SqlException e) {
                Assert.assertTrue(
                        "Expected error mentioning timestamp, got: " + e.getMessage(),
                        e.getMessage().toLowerCase().contains("timestamp")
                );
            }
        });
    }