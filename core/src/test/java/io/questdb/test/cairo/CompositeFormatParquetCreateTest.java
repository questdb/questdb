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
import org.junit.Assert;
import org.junit.Test;

/**
 * {@code FORMAT PARQUET} at CREATE on a composite table.
 * <p>
 * Composite tables do not support FORMAT PARQUET: partitions on such a table are BORN parquet, and the
 * composite ingestion path throws as soon as a cell resolves to a parquet partition (the cellKey-aware
 * {@code setPathForNativePartition} has no parquet counterpart). That refusal is correct.
 * <p>
 * The question this class asks is WHERE the refusal happens. A gate that fires per-cell during
 * ingestion, rather than at CREATE, accepts the DDL and hands back a table that cannot take a single
 * row -- and on a WAL table an apply-time throw SUSPENDS the table, so the failure arrives detached
 * from the statement that caused it.
 * <p>
 * MEASURED: it is already correct. CREATE refuses with
 * {@code SqlException [111] composite partitioning does not yet support FORMAT PARQUET [table=c]},
 * carrying a statement position, and the writer-side guard remains underneath as defence in depth for
 * non-SQL paths. These tests are therefore REGRESSION COVER for a gate that was previously untested at
 * this entry point, not a fix.
 * <p>
 * The sibling ALTER entry point -- {@code ALTER TABLE c SET FORMAT PARQUET} -- is covered by
 * {@code CompositeEarliestRefusalTest}, which was written when that path DID accept the statement and
 * suspend the table on the next commit. CREATE was the one of the two that had no test.
 */
public class CompositeFormatParquetCreateTest extends AbstractCairoTest {

    /**
     * A composite table declared FORMAT PARQUET must be refused by the CREATE itself.
     * <p>
     * Asserted through the error surfacing at DDL time. If this instead succeeds and the table only
     * fails later, the user gets a table that looks created and works for zero rows.
     */
    @Test(timeout = 60_000)
    public void testCreateCompositeWithFormatParquetIsRefusedAtCreate() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                        + "PARTITION BY DAY, exch LAYOUT PLAIN FORMAT PARQUET WAL");
                Assert.fail("CREATE must refuse FORMAT PARQUET on a composite table rather than accept the "
                        + "DDL and fail on the first insert");
            } catch (Exception expected) {
                final String message = String.valueOf(expected.getMessage());
                // The exact message, not merely "contains FORMAT PARQUET": a syntax error mentioning
                // the keyword would satisfy the looser check while proving nothing about the gate.
                Assert.assertTrue(
                        "the refusal must be the composite gate, naming the unsupported feature. Actual: " + message,
                        message.contains("composite partitioning does not yet support FORMAT PARQUET"));
            }
        });
    }

    /**
     * The plain twin must be unaffected: FORMAT PARQUET without dimensions is a supported feature and
     * must keep working. Without this the test above could be satisfied by refusing FORMAT PARQUET
     * everywhere.
     */
    @Test(timeout = 60_000)
    public void testFormatParquetStillWorksOnAPlainTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY FORMAT PARQUET WAL");
            execute("INSERT INTO p VALUES ('2023-01-01T01:00:00.000000Z','E0',1.0)");
            drainWalQueue();
            printSql("SELECT count() FROM p");
            Assert.assertEquals("count\n1\n", sink.toString());
        });
    }
}
