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

import org.junit.Test;

/**
 * Parquet-to-native conversion on a composite table.
 * <p>
 * The IMMEDIATE form ({@code doCommit = true}) already works per cell:
 * {@code convertPartitionParquetToNative} routes to
 * {@code convertCompositePartitionParquetToNative}. What is refused is the DEFERRED form
 * ({@code doCommit = false}), whose caller batches several conversions and commits them together via
 * {@code commitPendingParquetToNativeConversions} -- still cell-blind, deleting partition directories
 * through the cellKey-0 {@code safeDeletePartitionDir} and reopening "the" partition for a day.
 * <p>
 * The reachable symptom is an ALTER COLUMN TYPE on a composite table that holds a parquet partition:
 * the column conversion has to bring those partitions back to native first, and it batches them.
 */
public class CompositeParquetToNativeTest extends AbstractCompositeTwinTest {

    /**
     * CONVERT PARTITION TO NATIVE -- the immediate form, expected to work already. Present as the
     * control: it keeps the deferred-form test below honest about WHICH form is unsupported.
     */
    @Test(timeout = 120_000)
    public void testConvertPartitionBackToNative() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedTwoDays();

            execute("ALTER TABLE c CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            drainWalQueue();
            engine.releaseInactive();
            assertTwinEqual("");

            execute("ALTER TABLE c CONVERT PARTITION TO NATIVE LIST '2023-01-01'");
            execute("ALTER TABLE p CONVERT PARTITION TO NATIVE LIST '2023-01-01'");
            drainWalQueue();
            engine.releaseInactive();
            assertTwinEqual("");
            assertTwinEqual(" WHERE exch = 'E0'");
        });
    }

    /**
     * ALTER COLUMN TYPE over a table holding a PARQUET partition: the deferred form.
     */
    @Test(timeout = 120_000)
    public void testAlterColumnTypeOverAParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedTwoDays();

            execute("ALTER TABLE c CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            drainWalQueue();
            engine.releaseInactive();
            assertTwinEqual("");

            // TARGET SYMBOL specifically. A DOUBLE -> FLOAT change is lazy-decode compatible, so
            // ConvertOperatorImpl takes its "parquet storage is compatible" branch and never queues a
            // deferred conversion at all -- the first version of this test did exactly that and passed
            // without reaching the refusal. isTargetSymbol is the unconditional trigger.
            execute("ALTER TABLE c ALTER COLUMN px TYPE SYMBOL");
            execute("ALTER TABLE p ALTER COLUMN px TYPE SYMBOL");
            drainWalQueue();
            engine.releaseInactive();

            assertTwinEqual("");
            assertTwinEqual(" WHERE exch = 'E1'");
        });
    }

    private void seedTwoDays() throws Exception {
        insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0),"
                + "('2023-01-01T02:00:00.000000Z','E1',2.0),"
                + "('2023-01-02T01:00:00.000000Z','E0',3.0),"
                + "('2023-01-02T02:00:00.000000Z','E1',4.0)");
        drainWalQueue();
        engine.releaseInactive();
    }
}
