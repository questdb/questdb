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
import org.junit.Test;

public class SimulatedDeleteTest extends AbstractCairoTest {

    @Test
    public void testNotSelectDeleted() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances (cust_id int, balance_ccy symbol, balance double, inactive boolean, timestamp timestamp) timestamp(timestamp);");

            execute("insert into balances (cust_id, balance_ccy, balance, timestamp) values (1, 'USD', 1500.00, 6000000001);");
            execute("insert into balances (cust_id, balance_ccy, balance, timestamp) values (1, 'EUR', 650.50, 6000000002);");
            execute("insert into balances (cust_id, balance_ccy, balance, timestamp) values (2, 'USD', 900.75, 6000000003);");
            execute("insert into balances (cust_id, balance_ccy, balance, timestamp) values (2, 'EUR', 880.20, 6000000004);");
            execute("insert into balances (cust_id, balance_ccy, inactive, timestamp) values (1, 'USD', true, 6000000006);");

            assertSql(
                    "cust_id\tbalance_ccy\tbalance\tinactive\ttimestamp\n" +
                            "1\tEUR\t650.5\tfalse\t1970-01-01T01:40:00.000002Z\n", "(select * from balances where cust_id=1 latest on timestamp partition by balance_ccy) where not inactive;"
            );
        });
    }

    @Test
    public void testNotSelectDeletedByLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table state_table(time timestamp, id int, state symbol) timestamp(time);");

            execute("insert into state_table values(systimestamp(), 12345, 'OFF');");
            execute("insert into state_table values(systimestamp(), 12345, 'OFF');");
            execute("insert into state_table values(systimestamp(), 12345, 'OFF');");
            execute("insert into state_table values(systimestamp(), 12345, 'OFF');");
            execute("insert into state_table values(systimestamp(), 12345, 'ON');");

            TestUtils.assertSql(
                    engine,
                    sqlExecutionContext,
                    "(select state from state_table latest on time partition by state limit -1) where state != 'ON';",
                    sink,
                    "state\n"
            );
        });
    }
}
