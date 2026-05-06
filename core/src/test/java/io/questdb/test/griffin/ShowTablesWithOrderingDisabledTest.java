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
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

public class ShowTablesWithOrderingDisabledTest extends AbstractCairoTest {

    @Before
    public void beforeAll() {
        node1.setProperty(PropertyKey.CAIRO_METADATA_CACHE_SNAPSHOT_ORDERED, false);
    }

    @Test
    public void testShowTablesReturnsUnorderedList() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table deposits(account_no int, currency symbol, amount double)");
            execute("create table balances(account_no int, currency symbol, amount double)");
            execute("create table accounts(account_no int, currency symbol)");
            execute("create table card_payments(account_from_no int, account_to_no int, currency symbol, amount double)");
            assertSql("table_name\ncard_payments\naccounts\nbalances\ndeposits\n", "SHOW TABLES");
        });
    }

    @Test
    public void testShowTablesWithFunctionReturnsUnorderedList() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table deposits(account_no int, currency symbol, amount double)");
            execute("create table balances(account_no int, currency symbol, amount double)");
            execute("create table accounts(account_no int, currency symbol)");
            execute("create table card_payments(account_from_no int, account_to_no int, currency symbol, amount double)");
            assertSql("table_name\ncard_payments\naccounts\nbalances\ndeposits\n", "select * from all_tables()");
        });
    }

    @Test
    public void testTablesUnorderedAfterDropAndCreate() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table deposits(account_no int, currency symbol, amount double)");
            execute("create table balances(account_no int, currency symbol, amount double)");
            execute("create table accounts(account_no int, currency symbol)");
            execute("create table card_payments(account_from_no int, account_to_no int, currency symbol, amount double)");
            execute("drop table balances");
            execute("create table businesses(name symbol)");
            execute("create table balances2(account_no int, currency symbol, amount double)");
            assertSql("table_name\nbalances2\nbusinesses\ncard_payments\naccounts\ndeposits\n", "select * from all_tables()");
        });
    }

    @Test
    public void testTablesUnorderedAfterRename() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table deposits(account_no int, currency symbol, amount double)");
            execute("create table balances(account_no int, currency symbol, amount double)");
            execute("create table accounts(account_no int, currency symbol)");
            execute("create table card_payments(account_from_no int, account_to_no int, currency symbol, amount double)");
            execute("rename table balances to statement_balances");
            execute("create table businesses(name symbol)");
            execute("create table balances2(account_no int, currency symbol, amount double)");
            assertSql("table_name\nbalances2\nbusinesses\nstatement_balances\ncard_payments\naccounts\ndeposits\n", "select * from all_tables()");
        });
    }
}
