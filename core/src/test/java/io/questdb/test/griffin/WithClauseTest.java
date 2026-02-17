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

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class WithClauseTest extends AbstractCairoTest {

    @Test
    public void testWithAliasOverridingTable1() throws Exception {
        assertMemoryLeak(() -> assertQuery("address\tbalance\n",
                "WITH balance as ( SELECT * FROM balance WHERE address = 1 ) " +
                        "SELECT * FROM balance ",
                "CREATE TABLE balance (\n" +
                        "  address LONG,\n" +
                        "  balance DOUBLE\n" +
                        ");",
                null,
                "insert into balance values ( 1, 1.0 ), (2, 2.0);",
                "address\tbalance\n1\t1.0\n",
                true));
    }

    @Test
    public void testWithAliasOverridingTable2() throws Exception {
        assertMemoryLeak(() -> assertQuery("address\tbalance\n",
                "WITH balance as ( SELECT * FROM balance WHERE address = 1 ), \n" +
                        "     balance_other as ( SELECT * FROM balance )\n" +
                        "SELECT * FROM balance ",
                "CREATE TABLE balance (\n" +
                        "  address LONG,\n" +
                        "  balance DOUBLE\n" +
                        ");",
                null,
                "insert into balance values ( 1, 1.0 ), (2, 2.0);",
                "address\tbalance\n1\t1.0\n",
                true));
    }

    @Test
    public void testWithAliasOverridingTable3() throws Exception {
        assertMemoryLeak(() -> assertQuery("address\tbalance\n", "WITH balance as ( SELECT * FROM balance WHERE address = 1 ) \n" +
                "SELECT * FROM ( " +
                "WITH balance_other AS ( SELECT * FROM balance )\n" +
                "SELECT * FROM balance " +
                " ) ORDER BY 1 ", "CREATE TABLE balance (\n" +
                "  address LONG,\n" +
                "  balance DOUBLE\n" +
                ");", null, "insert into balance values ( 1, 1.0 ), (2, 2.0);", "address\tbalance\n1\t1.0\n", true, false, false));
    }

    @Test
    public void testWithAliasOverridingTable4() throws Exception {
        assertMemoryLeak(() -> {//to force 2nd balance with clause parsing
            assertQuery("address\tbalance\taddress1\tbalance1\n", "WITH balance2 as ( SELECT * FROM balance WHERE address = 2 ) " +
                            "SELECT * FROM (" +
                            "(" +
                            "WITH balance as (select * from balance where address = 1) " +
                            "SELECT b1.*, b2.* " +
                            "FROM balance b1 " +
                            "JOIN balance2 b2 on b1.address = b2.address " +
                            "JOIN balance b3 on b1.address = b3.address " +//to force 2nd balance with clause parsing
                            ") UNION ALL  " +
                            "SELECT * " +
                            "FROM balance b1 " +
                            "JOIN balance2 b2 on b1.address = b2.address " +
                            ")", "CREATE TABLE balance (\n" +
                            "  address LONG,\n" +
                            "  balance DOUBLE\n" +
                            ");",
                    null,
                    "insert into balance values ( 1, 1.0 ), (2, 2.0);",
                    "address\tbalance\taddress1\tbalance1\n2\t2.0\t2\t2.0\n",
                    false,
                    false,
                    false
            );
        });
    }

    @Test
    public void testWithLatestByFilterGroup() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table contact_events2 as (\n" +
                    "  select cast(x as SYMBOL) _id,\n" +
                    "    rnd_symbol('c1', 'c2', 'c3', 'c4') contactid, \n" +
                    "    CAST(x as Timestamp) timestamp, \n" +
                    "    rnd_symbol('g1', 'g2', 'g3', 'g4') groupId \n" +
                    "from long_sequence(500)) \n" +
                    "timestamp(timestamp)");

            // this is deliberately shuffled column in select to check that correct metadata is used on filtering
            // latest by queries
            TestUtils.printSql(
                    engine,
                    sqlExecutionContext,
                    "select groupId, _id, contactid, timestamp, _id from contact_events2 where groupId = 'g1' latest on timestamp partition by _id order by timestamp",
                    sink
            );
            String expected = sink.toString();
            Assert.assertTrue(expected.length() > 100);

            assertQueryNoLeakCheck(expected,
                    "with eventlist as (\n" +
                            "    select * from contact_events2 where groupId = 'g1' latest on timestamp partition by _id order by timestamp\n" +
                            ")\n" +
                            "select groupId, _id, contactid, timestamp, _id from eventlist where groupId = 'g1' \n",
                    "timestamp", true, false, true);
        });
    }

    @Test
    public void testWithSelectTwoWheres() throws Exception {
        assertException("with example as (select * from long_sequence(1))\n" +
                        "select * from example where true where false;",
                82,
                "unexpected token [where]"
        );
    }
}
