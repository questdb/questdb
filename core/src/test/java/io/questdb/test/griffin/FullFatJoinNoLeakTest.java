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
}
