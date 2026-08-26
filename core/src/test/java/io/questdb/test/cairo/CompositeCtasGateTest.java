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

package io.questdb.test.cairo;

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Regression lock for the CREATE TABLE AS SELECT composite gate.
 * <p>
 * MEASURED 2026-08-26 with the gate lifted, and the failure mode is a SILENT one, not the resolver
 * error the gate's comment used to predict. Nothing throws: the composite dimension is dropped and a
 * PLAIN table is created --
 * <pre>
 * CREATE TABLE c AS (SELECT * FROM src) TIMESTAMP(ts) PARTITION BY DAY, exch WAL
 *   SHOW CREATE TABLE c -> ... PARTITION BY DAY;     the ", exch" is gone
 *   on disk             -> c~2/2023-01-01/exch.d     flat day dir, no cell directories
 * </pre>
 * The user asks for composite partitioning and gets plain, with no error anywhere. That is the same
 * silent-wrong-DDL class as the enterprise CREATE TABLE path which dropped getPartitionSpec().
 * <p>
 * So this suite asserts BOTH halves: the statement is refused, AND no silently-plain table is left
 * behind. Asserting only the refusal would still pass if a future change created the table first and
 * threw afterwards.
 */
public class CompositeCtasGateTest extends AbstractCairoTest {

    @Test(timeout = 60_000)
    public void testCtasWithCompositeDimensionIsRefusedAndCreatesNothing() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO src VALUES ('2023-01-01T01:00:00.000000Z','E0',1.0)");
            drainWalQueue();

            try {
                execute("CREATE TABLE c AS (SELECT * FROM src) TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
                Assert.fail("composite CTAS must be refused");
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(),
                        "composite partitioning is not yet supported with CREATE TABLE AS SELECT");
            }

            // the load-bearing half: nothing was created, silently plain or otherwise
            assertQuery("SELECT count() FROM tables() WHERE table_name = 'c'")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n0\n");
        });
    }

    /**
     * POSITIVE CONTROL: a CTAS with no composite dimension is unaffected, so the guard cannot be
     * passing simply by refusing every CTAS.
     */
    @Test(timeout = 60_000)
    public void testPlainCtasStillWorks() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO src VALUES ('2023-01-01T01:00:00.000000Z','E0',1.0)");
            drainWalQueue();

            execute("CREATE TABLE c AS (SELECT * FROM src) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            assertQuery("SELECT count() FROM c").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
        });
    }
}
