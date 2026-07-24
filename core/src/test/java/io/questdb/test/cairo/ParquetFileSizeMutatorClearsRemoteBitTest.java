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

package io.questdb.test.cairo;

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

/**
 * Data-rewrite primitives that refresh the parquet file size must clear bit 63
 * (REMOTE); format-only transitions preserve it.
 */
public class ParquetFileSizeMutatorClearsRemoteBitTest extends AbstractCairoTest {

    @Test
    public void testGetPartitionParquetFileSizeMasksReservedFlagBits() throws Exception {
        // Bits 56..62 are reserved for flags; only bits 0..55 are the value. A raw word with the
        // reserved region set must read back as the low-56-bit value, never with the flag bits.
        TestUtils.assertMemoryLeak(() -> withTxWriter("mutReserved", (tw, ts) -> {
            // setPartitionParquet writes the slot; plant reserved bits 56..62 over a 4096 value.
            tw.setPartitionParquet(ts, (0x7FL << 56) | 4096L);

            Assert.assertEquals("reserved flag bits must be masked off the value",
                    4096L, tw.getPartitionParquetFileSize(0));
            Assert.assertFalse("reserved bits 56..62 are distinct from REMOTE (bit 63)",
                    tw.isPartitionRemote(0));
        }));
    }

    @Test
    public void testSetPartitionParquetFileSizeClearsRemoteBit() throws Exception {
        // setPartitionParquetFileSize is a data-rewrite mutator: set REMOTE, call
        // it with a different size, assert REMOTE is cleared and the masked size
        // matches the new value.
        TestUtils.assertMemoryLeak(() -> withTxWriter("mutPreserve", (tw, ts) -> {
            tw.setPartitionParquet(ts, 4096L);
            tw.setPartitionRemote(0, true);
            Assert.assertTrue(tw.isPartitionRemote(0));
            Assert.assertEquals(4096L, tw.getPartitionParquetFileSize(0));

            tw.setPartitionParquetFileSize(0, 8192L);

            Assert.assertFalse("setPartitionParquetFileSize must clear REMOTE",
                    tw.isPartitionRemote(0));
            Assert.assertEquals("setPartitionParquetFileSize must overwrite the size",
                    8192L, tw.getPartitionParquetFileSize(0));
        }));
    }

    @Test
    public void testSetPartitionParquetPreservesRemoteBit() throws Exception {
        // setPartitionParquet is a format/materialization transition: the same rows are represented
        // as parquet, so REMOTE survives.
        TestUtils.assertMemoryLeak(() -> withTxWriter("mutFormat", (tw, ts) -> {
            tw.setPartitionParquet(ts, 4096L);
            tw.setPartitionRemote(0, true);
            Assert.assertTrue(tw.isPartitionRemote(0));

            // Call it again with a different size — stand-in for a
            // rewrite path that re-publishes the slot.
            tw.setPartitionParquet(ts, 16_384L);

            Assert.assertTrue("setPartitionParquet must preserve REMOTE on format rewrite",
                    tw.isPartitionRemote(0));
            Assert.assertEquals("size must equal the new fileLength",
                    16_384L, tw.getPartitionParquetFileSize(0));
        }));
    }

    private static void withTxWriter(String tableName, TxWriterAction action) throws Exception {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        final TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
        model.timestamp();
        AbstractCairoTest.create(model);
        try (Path path = new Path()) {
            final TableToken tableToken = engine.verifyTableName(tableName);
            path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
            try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                final long ts = 0;
                tw.updatePartitionSizeByTimestamp(ts, 1);
                action.run(tw, ts);
            }
        }
    }

    @FunctionalInterface
    private interface TxWriterAction {
        void run(TxWriter tw, long partitionTimestamp) throws Exception;
    }
}
