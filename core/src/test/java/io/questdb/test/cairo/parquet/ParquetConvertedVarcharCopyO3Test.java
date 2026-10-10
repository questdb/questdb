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
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the column identity that {@code O3PartitionJob.copyO3ToRowGroup()} writes into the
 * parquet descriptor it hands to the Rust updater over JNI.
 * <p>
 * {@code ALTER COLUMN TYPE} re-keys a column: {@code TableColumnMetadata} keeps the parquet
 * field id in {@code getOriginalWriterIndex()} while {@code getWriterIndex()} moves on to a
 * fresh id. Every parquet field id on disk comes from the original index -- see
 * {@code TableUtils.produceParquetForNativePartition()} and the target schema
 * {@code O3PartitionJob.processParquetPartition()} builds. A descriptor naming the current
 * writer index instead addresses a field that no parquet file carries.
 * <p>
 * The visible consequence is the persisted VARCHAR {@code ascii} flag. The Rust updater keys
 * its all-ASCII tracker by parquet field id ({@code parquet_write/update.rs},
 * {@code track_new_data_ascii} and {@code ParquetUpdater::end}); an unknown id hides the
 * inserted non-ASCII bytes from the tracker, so the file keeps {@code ascii:true} and the
 * reader then stamps HEADER_FLAG_ASCII on values that are not ASCII. {@code length()} counts
 * bytes instead of characters and the UTF-16 cast yields two replacement units.
 * <p>
 * The native control table runs the identical sequence without the parquet conversion, so a
 * divergence between the two isolates the parquet write path.
 */
public class ParquetConvertedVarcharCopyO3Test extends AbstractCairoTest {

    // 'e' with acute accent: one character, two UTF-8 bytes.
    private static final String EXPECTED_O3_ROW = """
            v\tlen\tbytes
            é\t1\t2
            """;

    @Test
    public void testCopyO3AfterLastRowGroupKeepsAsciiMetadata() throws Exception {
        // O3 data past the last row group maximum: the new row group lands at the end.
        assertConvertedVarcharSurvivesCopyO3(true, "2024-01-01T23:00:00.000000Z");
    }

    @Test
    public void testCopyO3BeforeFirstRowGroupKeepsAsciiMetadata() throws Exception {
        // O3 data ahead of the first row group minimum: the new row group is prepended.
        assertConvertedVarcharSurvivesCopyO3(true, "2024-01-01T00:00:00.000000Z");
    }

    @Test
    public void testCopyO3BeforeFirstRowGroupKeepsAsciiMetadataBypassWal() throws Exception {
        // Same path driven by the non-WAL writer instead of ApplyWal2TableJob.
        assertConvertedVarcharSurvivesCopyO3(false, "2024-01-01T00:00:00.000000Z");
    }

    /**
     * Builds an all-ASCII STRING column, converts it to VARCHAR (which re-keys the column),
     * converts its historical partition to parquet, then inserts a non-ASCII value at
     * {@code o3Timestamp} so the O3 merge strategy picks COPY_O3. Asserts that both the
     * parquet table and the native control return the value as one character.
     */
    private void assertConvertedVarcharSurvivesCopyO3(boolean wal, String o3Timestamp) throws Exception {
        // 16 rows over a row group size of 8 give the converted partition exactly two row
        // groups, so an insert outside their bounds becomes a COPY_O3 action rather than
        // a MERGE of an existing row group.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        assertMemoryLeak(() -> {
            for (String table : new String[]{"pq", "nat"}) {
                execute("CREATE TABLE " + table + " (ts TIMESTAMP, v STRING) TIMESTAMP(ts) PARTITION BY DAY " + (wal ? "WAL" : "BYPASS WAL"));
                execute("INSERT INTO " + table + " SELECT timestamp_sequence('2024-01-01T06:00:00.000000Z', 60_000_000L), 'ascii_' || x FROM long_sequence(16)");
                // Keeps 2024-01-01 out of the active partition so it can be converted.
                execute("INSERT INTO " + table + " VALUES ('2024-01-02T06:00:00.000000Z', 'ascii_active')");
                drainWalQueue();
                execute("ALTER TABLE " + table + " ALTER COLUMN v TYPE VARCHAR");
                drainWalQueue();
            }
            // The column is all-ASCII at conversion time, so the file records ascii:true for v.
            execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            Assert.assertEquals(2, assertConvertedParquetPartition());

            for (String table : new String[]{"pq", "nat"}) {
                execute("INSERT INTO " + table + " VALUES ('" + o3Timestamp + "', 'é')");
            }
            drainWalQueue();

            // A third row group beside the two untouched ones is the COPY_O3 signature:
            // MERGE would have rewritten an existing row group instead of adding one.
            Assert.assertEquals(3, assertConvertedParquetPartition());

            assertQuery("SELECT v, length(v) len, length_bytes(v) bytes FROM nat WHERE ts = '" + o3Timestamp + "'")
                    .noLeakCheck()
                    .returns(EXPECTED_O3_ROW);
            assertQuery("SELECT v, length(v) len, length_bytes(v) bytes FROM pq WHERE ts = '" + o3Timestamp + "'")
                    .noLeakCheck()
                    .returns(EXPECTED_O3_ROW);
        });
    }

    /**
     * Asserts that partition 0 of {@code pq} is a parquet partition whose {@code v} column is
     * type-converted (original and current writer indexes diverge) and whose parquet field id
     * is the original index. Returns the partition's row group count.
     */
    private int assertConvertedParquetPartition() {
        try (TableReader reader = engine.getReader("pq")) {
            TableReaderMetadata metadata = reader.getMetadata();
            TableColumnMetadata columnMetadata = metadata.getColumnMetadata(metadata.getColumnIndex("v"));
            Assert.assertEquals(PartitionFormat.PARQUET, reader.getPartitionFormat(0));
            Assert.assertEquals(1, columnMetadata.getOriginalWriterIndex());
            Assert.assertEquals(2, columnMetadata.getWriterIndex());
            reader.openPartition(0);
            Assert.assertEquals(1, reader.getAndInitParquetPartitionDecoder(0).metadata().getColumnId(1));
            return reader.getAndInitParquetPartitionDecoder(0).metadata().getRowGroupCount();
        }
    }
}
