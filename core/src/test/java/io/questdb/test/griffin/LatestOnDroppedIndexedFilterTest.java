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
import io.questdb.test.TestTimestampType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * A WHERE predicate on an indexed SYMBOL column used to be lifted out of the residual filter into
 * IntrinsicModel.keyColumn on the assumption that an index-backed cursor would consume it. When the
 * LATEST ON key column is not a SYMBOL, the generated cursor (LatestByAllFiltered /
 * LatestByAllSymbolsFiltered) filters with the residual filter alone, so the extracted predicate was
 * consumed by nobody and disappeared from the query - silently returning rows that do not satisfy
 * the WHERE clause. Adding an index to the predicate column was enough to flip a correct query to a
 * wrong one.
 */
@RunWith(Parameterized.class)
public class LatestOnDroppedIndexedFilterTest extends AbstractCairoTest {

    private static final String ACCT_A_LATEST =
            "account_id\torder_id\tvisible_in_history\tupdated_at\n" +
                    "acct-A\to1\ttrue\t2026-01-01T00:00:00.000000Z\n" +
                    "acct-A\to2\ttrue\t2026-01-01T00:00:02.000000Z\n";

    private final TestTimestampType timestampType;

    public LatestOnDroppedIndexedFilterTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testBindVariableIsApplied() throws Exception {
        assertMemoryLeak(() -> {
            createRepro();
            bindVariableService.clear();
            bindVariableService.setStr(0, "acct-A");
            assertQuery("select * from latest_on_repro " +
                    "where account_id = $1 and visible_in_history = true " +
                    "latest on updated_at partition by order_id order by updated_at")
                    .timestamp("updated_at")
                    .expectSize()
                    .returns(expected(ACCT_A_LATEST));
        });
    }

    @Test
    public void testEqIsApplied() throws Exception {
        assertMemoryLeak(() -> {
            createRepro();
            assertQuery("select * from latest_on_repro " +
                    "where account_id = 'acct-A' and visible_in_history = true " +
                    "latest on updated_at partition by order_id order by updated_at")
                    .withPlanContaining("account_id='acct-A'", "visible_in_history=true")
                    .timestamp("updated_at")
                    .expectSize()
                    .returns(expected(ACCT_A_LATEST));
        });
    }

    @Test
    public void testEqNoMatchIsApplied() throws Exception {
        assertMemoryLeak(() -> {
            createRepro();
            assertQuery("select * from latest_on_repro " +
                    "where account_id = 'ACCOUNT-DOES-NOT-EXIST' and visible_in_history = true " +
                    "latest on updated_at partition by order_id")
                    .timestamp("updated_at")
                    .expectSize()
                    .returns(expected("account_id\torder_id\tvisible_in_history\tupdated_at\n"));
        });
    }

    @Test
    public void testInIsApplied() throws Exception {
        assertMemoryLeak(() -> {
            createRepro();
            assertQuery("select * from latest_on_repro " +
                    "where account_id in ('acct-A') and visible_in_history = true " +
                    "latest on updated_at partition by order_id order by updated_at")
                    .timestamp("updated_at")
                    .expectSize()
                    .returns(expected(ACCT_A_LATEST));
        });
    }

    @Test
    public void testInSubQueryIsApplied() throws Exception {
        assertMemoryLeak(() -> {
            createRepro();
            assertQuery("select * from latest_on_repro " +
                    "where account_id in (select 'acct-A'::symbol) and visible_in_history = true " +
                    "latest on updated_at partition by order_id order by updated_at")
                    .timestamp("updated_at")
                    .expectSize()
                    .returns(expected(ACCT_A_LATEST));
        });
    }

    /**
     * Interval extraction and key extraction share one extract() call. Check the predicate still
     * survives when the timestamp bound is lifted into an IntervalPartitionFrameCursorFactory,
     * across more than one partition.
     */
    @Test
    public void testIntervalPlusPredicateIsApplied() throws Exception {
        assertMemoryLeak(() -> {
            createRepro();
            assertQuery("select * from latest_on_repro4 " +
                    "where account_id = 'acct-A' and updated_at in '2026-01-02' " +
                    "latest on updated_at partition by order_id order by updated_at")
                    .timestamp("updated_at")
                    .expectSize()
                    .returns(expected("account_id\torder_id\tvisible_in_history\tupdated_at\n" +
                            "acct-A\to1\ttrue\t2026-01-02T00:00:00.000000Z\n"));
        });
    }

    /**
     * The indexed predicate is the <em>only</em> predicate, so extraction used to empty the residual
     * filter completely - the plainest form of the bug and the likeliest real-world shape.
     */
    @Test
    public void testLonePredicateIsApplied() throws Exception {
        assertMemoryLeak(() -> {
            createRepro();
            assertQuery("select * from latest_on_repro " +
                    "where account_id = 'acct-A' " +
                    "latest on updated_at partition by order_id order by updated_at")
                    .timestamp("updated_at")
                    .expectSize()
                    .returns(expected(ACCT_A_LATEST));
        });
    }

    @Test
    public void testMultiColumnLatestByIsApplied() throws Exception {
        assertMemoryLeak(() -> {
            createRepro();
            assertQuery("select * from latest_on_repro " +
                    "where account_id = 'acct-A' and visible_in_history = true " +
                    "latest on updated_at partition by order_id, account_id order by updated_at")
                    .timestamp("updated_at")
                    .expectSize()
                    .returns(expected(ACCT_A_LATEST));
        });
    }

    @Test
    public void testNoIndexHintIsApplied() throws Exception {
        assertMemoryLeak(() -> {
            createRepro();
            assertQuery("select /*+ no_index(latest_on_repro account_id) */ * from latest_on_repro " +
                    "where account_id = 'acct-A' and visible_in_history = true " +
                    "latest on updated_at partition by order_id order by updated_at")
                    .timestamp("updated_at")
                    .expectSize()
                    .returns(expected(ACCT_A_LATEST));
        });
    }

    /**
     * The branch is selected by {@code !isSymbol(key)}, so every non-SYMBOL key type shares it.
     * VARCHAR is what the field report hit; check a fixed-width key too.
     */
    @Test
    public void testNonVarcharKeyIsApplied() throws Exception {
        assertMemoryLeak(() -> {
            createRepro();
            assertQuery("select * from latest_on_repro3 " +
                    "where account_id = 'acct-A' and visible_in_history = true " +
                    "latest on updated_at partition by order_num order by updated_at")
                    .timestamp("updated_at")
                    .expectSize()
                    .returns(expected("account_id\torder_num\tvisible_in_history\tupdated_at\n" +
                            "acct-A\t1\ttrue\t2026-01-01T00:00:00.000000Z\n" +
                            "acct-A\t2\ttrue\t2026-01-01T00:00:02.000000Z\n"));
        });
    }

    @Test
    public void testNotEqIsApplied() throws Exception {
        assertMemoryLeak(() -> {
            createRepro();
            assertQuery("select * from latest_on_repro " +
                    "where account_id != 'acct-C' and visible_in_history = true " +
                    "latest on updated_at partition by order_id order by updated_at")
                    .timestamp("updated_at")
                    .expectSize()
                    .returns(expected("account_id\torder_id\tvisible_in_history\tupdated_at\n" +
                            "acct-B\to1\ttrue\t2026-01-01T00:00:01.000000Z\n" +
                            "acct-A\to2\ttrue\t2026-01-01T00:00:02.000000Z\n" +
                            "acct-B\to3\ttrue\t2026-01-01T00:00:03.000000Z\n"));
        });
    }

    @Test
    public void testNotInIsApplied() throws Exception {
        assertMemoryLeak(() -> {
            createRepro();
            assertQuery("select * from latest_on_repro " +
                    "where account_id not in ('acct-B', 'acct-C') and visible_in_history = true " +
                    "latest on updated_at partition by order_id order by updated_at")
                    .timestamp("updated_at")
                    .expectSize()
                    .returns(expected(ACCT_A_LATEST));
        });
    }

    /**
     * The indexed-key shape that the extraction exists to serve: LATEST ON a SYMBOL key with a
     * predicate on that same key. Extraction must still happen here, so the plan keeps using the
     * index-backed cursor rather than degrading to a filtered full scan.
     */
    @Test
    public void testSymbolKeyPredicateStillUsesIndex() throws Exception {
        assertMemoryLeak(() -> {
            createRepro();
            assertQuery("select * from latest_on_repro " +
                    "where account_id = 'acct-A' " +
                    "latest on updated_at partition by account_id")
                    .withPlanContaining("Index backward scan")
                    .timestamp("updated_at")
                    .returns(expected("account_id\torder_id\tvisible_in_history\tupdated_at\n" +
                            "acct-A\to2\ttrue\t2026-01-01T00:00:02.000000Z\n"));
        });
    }

    /**
     * A SYMBOL LATEST ON key with the predicate on a <em>different</em>, indexed column: the
     * predicate is not the preferred key, so it stays in the filter and was always correct.
     */
    @Test
    public void testSymbolKeyWithForeignIndexedPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createRepro();
            assertQuery("select * from latest_on_repro2 " +
                    "where account_id = 'acct-A' and visible_in_history = true " +
                    "latest on updated_at partition by status order by updated_at")
                    .timestamp("updated_at")
                    .expectSize()
                    .returns(expected("account_id\torder_id\tvisible_in_history\tstatus\tupdated_at\n" +
                            "acct-A\to2\ttrue\tNEW\t2026-01-01T00:00:02.000000Z\n"));
        });
    }

    /**
     * Expected output is written in MICRO form; NANO renders three more fractional digits.
     */
    private String expected(String micros) {
        return timestampType == TestTimestampType.NANO ? micros.replace(".000000Z", ".000000000Z") : micros;
    }

    private void createRepro() throws Exception {
        execute("create table latest_on_repro (" +
                "account_id symbol index, " +
                "order_id varchar, " +
                "visible_in_history boolean, " +
                "updated_at " + timestampType.getTypeName() +
                ") timestamp(updated_at) partition by day wal");
        execute("insert into latest_on_repro values " +
                "('acct-A','o1',true, '2026-01-01T00:00:00.000000Z')," +
                "('acct-B','o1',true, '2026-01-01T00:00:01.000000Z')," +
                "('acct-A','o2',true, '2026-01-01T00:00:02.000000Z')," +
                "('acct-B','o3',true, '2026-01-01T00:00:03.000000Z')," +
                "('acct-C','o4',true, '2026-01-01T00:00:04.000000Z')," +
                "('acct-C','o4',false,'2026-01-01T00:00:05.000000Z')");
        drainWalQueue();

        // same data with a SYMBOL column available as a LATEST ON key
        execute("create table latest_on_repro2 (" +
                "account_id symbol index, " +
                "order_id varchar, " +
                "visible_in_history boolean, " +
                "status symbol, " +
                "updated_at " + timestampType.getTypeName() +
                ") timestamp(updated_at) partition by day wal");
        execute("insert into latest_on_repro2 values " +
                "('acct-A','o1',true, 'NEW','2026-01-01T00:00:00.000000Z')," +
                "('acct-B','o1',true, 'NEW','2026-01-01T00:00:01.000000Z')," +
                "('acct-A','o2',true, 'NEW','2026-01-01T00:00:02.000000Z')," +
                "('acct-B','o3',true, 'NEW','2026-01-01T00:00:03.000000Z')," +
                "('acct-C','o4',true, 'NEW','2026-01-01T00:00:04.000000Z')," +
                "('acct-C','o4',false,'NEW','2026-01-01T00:00:05.000000Z')");
        drainWalQueue();

        // rows spanning two partitions, for the interval-plus-predicate case
        execute("create table latest_on_repro4 (" +
                "account_id symbol index, " +
                "order_id varchar, " +
                "visible_in_history boolean, " +
                "updated_at " + timestampType.getTypeName() +
                ") timestamp(updated_at) partition by day wal");
        execute("insert into latest_on_repro4 values " +
                "('acct-A','o1',true,'2026-01-01T00:00:00.000000Z')," +
                "('acct-B','o1',true,'2026-01-01T00:00:01.000000Z')," +
                "('acct-A','o1',true,'2026-01-02T00:00:00.000000Z')," +
                "('acct-B','o1',true,'2026-01-02T00:00:01.000000Z')," +
                "('acct-A','o2',true,'2026-01-03T00:00:00.000000Z')");
        drainWalQueue();

        // a fixed-width (non-VARCHAR) non-SYMBOL LATEST ON key
        execute("create table latest_on_repro3 (" +
                "account_id symbol index, " +
                "order_num long, " +
                "visible_in_history boolean, " +
                "updated_at " + timestampType.getTypeName() +
                ") timestamp(updated_at) partition by day wal");
        execute("insert into latest_on_repro3 values " +
                "('acct-A',1,true, '2026-01-01T00:00:00.000000Z')," +
                "('acct-B',1,true, '2026-01-01T00:00:01.000000Z')," +
                "('acct-A',2,true, '2026-01-01T00:00:02.000000Z')," +
                "('acct-B',3,true, '2026-01-01T00:00:03.000000Z')," +
                "('acct-C',4,true, '2026-01-01T00:00:04.000000Z')," +
                "('acct-C',4,false,'2026-01-01T00:00:05.000000Z')");
        drainWalQueue();
    }
}
