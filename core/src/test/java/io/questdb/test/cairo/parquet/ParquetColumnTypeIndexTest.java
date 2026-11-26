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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class ParquetColumnTypeIndexTest extends AbstractCairoTest {
    private final String indexDefinition;
    private final boolean isMergeAppend;

    public ParquetColumnTypeIndexTest(String indexDefinition, boolean isMergeAppend) {
        this.indexDefinition = indexDefinition;
        this.isMergeAppend = isMergeAppend;
    }

    @Parameterized.Parameters(name = "index={0}, mergeAppend={1}")
    public static Collection<Object[]> data() {
        Collection<Object[]> parameters = new ArrayList<>();
        for (String index : List.of("POSTING", "POSTING INCLUDE (marker)", "BITMAP")) {
            for (boolean isMergeAppend : new boolean[]{false, true}) {
                parameters.add(new Object[]{index, isMergeAppend});
            }
        }
        return parameters;
    }

    @Test
    public void testAlterColumnTypeKeepsIndexes() throws Exception {
        testAlterColumnTypeKeepsIndexes(false, true);
    }

    @Test
    public void testAlterColumnTypeKeepsIndexesAfterParquetO3() throws Exception {
        testAlterColumnTypeKeepsIndexes(true, true);
    }

    @Test
    public void testAlterColumnTypeKeepsIndexesOfHistoricalPartition() throws Exception {
        testAlterColumnTypeKeepsIndexes(false, false);
    }

    private void assertIndexes(int expectedA, int expectedNull, int expectedRows) throws Exception {
        assertQuery("SELECT count() FROM t").noRandomAccess().expectSize().returns("count\n" + expectedRows + "\n");
        for (String predicate : List.of("= 'a'", "= 'b'", "IS NULL")) {
            final int expected = predicate.equals("IS NULL") ? expectedNull : expectedA;
            assertQuery("SELECT count() FROM t WHERE s " + predicate)
                    .noRandomAccess().expectSize().returns("count\n" + expected + "\n");
            assertQuery("SELECT count() FROM t WHERE s::VARCHAR " + predicate)
                    .noRandomAccess().expectSize().returns("count\n" + expected + "\n");
            assertSqlCursors(
                    "SELECT w, ts, marker, s FROM t WHERE s::VARCHAR " + predicate + " ORDER BY ts",
                    "SELECT w, ts, marker, s FROM t WHERE s " + predicate + " ORDER BY ts"
            );
        }
    }

    private void convertToParquet(TableToken token) throws Exception {
        execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
        assertQuery("SELECT count() FROM table_partitions('t') WHERE isParquet")
                .noRandomAccess().expectSize().returns("count\n1\n");
    }

    private void testAlterColumnTypeKeepsIndexes(boolean hasParquetO3, boolean isLastPartition) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, isMergeAppend);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (w INT, ts TIMESTAMP, marker LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT x::INT, timestamp_sequence('2024-01-01', 100_000), x FROM long_sequence(150_000)");
            drainWalQueue();
            // The symbol column's top falls inside the second parquet row group.
            execute("ALTER TABLE t ADD COLUMN s SYMBOL");
            execute("ALTER TABLE t ALTER COLUMN s ADD INDEX TYPE " + indexDefinition);
            execute("""
                    INSERT INTO t SELECT 1, timestamp_sequence('2024-01-01T05:00:00', 1_000_000), x,
                        CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END FROM long_sequence(100)
                    """);
            drainWalQueue();
            final TableToken token = engine.verifyTableName("t");
            convertToParquet(token);
            if (hasParquetO3) {
                execute("""
                        INSERT INTO t SELECT 2, timestamp_sequence('2024-01-01T04:30:00', 1_000_000), x,
                            CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END FROM long_sequence(100)
                        """);
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            }
            if (!isLastPartition) {
                execute("INSERT INTO t VALUES (3, '2024-01-02', 3, NULL)");
                drainWalQueue();
            }
            final int expectedA = hasParquetO3 ? 100 : 50;
            final int expectedNull = isLastPartition ? 150_000 : 150_001;
            final int expectedRows = expectedNull + 2 * expectedA;
            assertIndexes(expectedA, expectedNull, expectedRows);
            final long txnBefore;
            try (TableReader reader = getReader(token)) {
                txnBefore = reader.getTxn();
            }

            execute("ALTER TABLE t ALTER COLUMN w TYPE SYMBOL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            try (TableReader reader = getReader(token)) {
                Assert.assertEquals("ALTER must publish the decoded partition and metadata together", txnBefore + 1, reader.getTxn());
            }
            assertQuery("SELECT count() FROM table_partitions('t') WHERE isParquet")
                    .noRandomAccess().expectSize().returns("count\n0\n");
            assertIndexes(expectedA, expectedNull, expectedRows);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertIndexes(expectedA, expectedNull, expectedRows);

            // A damaged posting index used to fail this conversion and suspend the table.
            convertToParquet(token);
            assertIndexes(expectedA, expectedNull, expectedRows);
            execute("INSERT INTO t VALUES ('99', '2024-01-01T06:00:00', 99, 'a'), ('99', '2024-01-01T06:00:01', 99, 'b')");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertIndexes(expectedA + 1, expectedNull, expectedRows + 2);
        });
    }
}
