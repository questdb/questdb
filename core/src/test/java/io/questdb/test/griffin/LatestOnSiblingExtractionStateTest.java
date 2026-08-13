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
 * One WhereClauseParser instance serves every scan at a given generation depth, so union branches,
 * join slaves and sibling sub-queries extract through it one after another. Its node lists only
 * reset on entry to a <em>nested</em> extract(), not between siblings, so a branch that consumed a
 * NOT-EQUALS predicate on an indexed SYMBOL used to leave that node behind for the next branch to
 * adopt as its own. The LATEST ON planner invariant check reads those lists, and a borrowed node
 * made it reject queries whose own WHERE clause it had extracted nothing from.
 */
public class LatestOnSiblingExtractionStateTest extends AbstractCairoTest {

    @Test
    public void testJoinAgainstLatestOnSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT a.account_id, a.updated_at FROM orders a " +
                    "JOIN (SELECT account_id, updated_at FROM events " +
                    "      WHERE flag = true LATEST ON updated_at PARTITION BY k1, k2) b " +
                    "ON a.account_id = b.account_id " +
                    "WHERE a.account_id != 'acct-C'")
                    // the empty result is the same before and after the fix, so pin the two
                    // preconditions that make this shape exercise the leak at all: the master scan
                    // must extract the predicate, and orders must be the master
                    .withPlanContaining("account_id not in ['acct-C']", "Hash Join")
                    .timestamp("updated_at")
                    .noRandomAccess()
                    .returns("""
                            account_id\tupdated_at
                            """);
        });
    }

    /**
     * The leftover node used to be adopted even by a branch whose LATEST ON key is a single SYMBOL,
     * which is the shape the planner invariant check is meant to let through.
     */
    @Test
    public void testSingleSymbolKeyBranchAfterExcludedKey() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT account_id, updated_at FROM orders " +
                    "WHERE account_id != 'acct-C' LATEST ON updated_at PARTITION BY account_id " +
                    "UNION ALL " +
                    "SELECT account_id, updated_at FROM events " +
                    "WHERE flag = true LATEST ON updated_at PARTITION BY k1, k2")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            account_id\tupdated_at
                            acct-A\t2026-01-01T00:00:00.000000Z
                            acct-B\t2026-01-01T00:00:02.000000Z
                            """);
        });
    }

    /**
     * A non-SYMBOL LATEST ON key blocks key extraction just as several keys do, so it inherits the
     * leftover node the same way.
     */
    @Test
    public void testUnionWithNonSymbolLatestOnKey() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT account_id, updated_at FROM orders WHERE account_id != 'acct-C' " +
                    "UNION ALL " +
                    "SELECT account_id, updated_at FROM events " +
                    "WHERE flag = true LATEST ON updated_at PARTITION BY flag")
                    .noRandomAccess()
                    .returns("""
                            account_id\tupdated_at
                            acct-A\t2026-01-01T00:00:00.000000Z
                            acct-B\t2026-01-01T00:00:02.000000Z
                            """);
        });
    }

    /**
     * A lone NOT-EQUALS predicate is the whole WHERE clause, so extract0() takes its single-node
     * fast path, which hands the node to the model without emptying the list behind it.
     */
    @Test
    public void testUnionWithNotEqualsOnIndexedSymbol() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT account_id, updated_at FROM orders WHERE account_id != 'acct-C' " +
                    "UNION ALL " +
                    "SELECT account_id, updated_at FROM events " +
                    "WHERE flag = true LATEST ON updated_at PARTITION BY k1, k2")
                    .withPlanContaining("account_id not in ['acct-C']", "filter: flag=true")
                    .noRandomAccess()
                    .returns("""
                            account_id\tupdated_at
                            acct-A\t2026-01-01T00:00:00.000000Z
                            acct-B\t2026-01-01T00:00:02.000000Z
                            """);
        });
    }

    /**
     * The excluded value still has to reach the second branch's filter when that branch is the one
     * that extracted it, which is what stops the fix from over-clearing.
     */
    @Test
    public void testUnionWithNotEqualsOnLatestOnBranchItself() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT account_id, updated_at FROM events " +
                    "WHERE flag = true LATEST ON updated_at PARTITION BY k1, k2 " +
                    "UNION ALL " +
                    "SELECT account_id, updated_at FROM orders WHERE account_id != 'acct-C'")
                    .noRandomAccess()
                    .returns("""
                            account_id\tupdated_at
                            acct-B\t2026-01-01T00:00:02.000000Z
                            acct-A\t2026-01-01T00:00:00.000000Z
                            """);
        });
    }

    private static void createTables() throws Exception {
        execute("CREATE TABLE orders (" +
                "account_id SYMBOL INDEX, " +
                "updated_at TIMESTAMP" +
                ") TIMESTAMP(updated_at) PARTITION BY DAY");
        execute("INSERT INTO orders VALUES " +
                "('acct-A', '2026-01-01T00:00:00.000000Z')," +
                "('acct-C', '2026-01-01T00:00:01.000000Z')");
        execute("CREATE TABLE events (" +
                "account_id SYMBOL, " +
                "k1 SYMBOL, " +
                "k2 SYMBOL, " +
                "flag BOOLEAN, " +
                "updated_at TIMESTAMP" +
                ") TIMESTAMP(updated_at) PARTITION BY DAY");
        execute("INSERT INTO events VALUES " +
                "('acct-B', 'x', 'y', true, '2026-01-01T00:00:02.000000Z')," +
                "('acct-D', 'x', 'y', false, '2026-01-01T00:00:03.000000Z')");
    }
}
