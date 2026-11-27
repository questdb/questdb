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

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A pooled reader keeps the index reader objects of a partition it closes. A column dropped while the partition is
 * closed moves the next columns into other slots, and the partition opens again with the reader objects of the slots.
 */
public class TableReaderIndexReaderSlotTest extends AbstractCairoTest {

    @Test
    public void testBitmapColumnTakesSlotOfPostingColumnWhilePartitionClosed() throws Exception {
        // a rewrite of the partition gives it a new directory, so that the pooled reader closes it
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        x LONG,
                        p SYMBOL INDEX TYPE POSTING,
                        b SYMBOL INDEX TYPE BITMAP,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t VALUES
                        (1, 'p1', 'b1', '2024-01-01T10:00:00.000000Z'),
                        (2, 'p2', 'b2', '2024-01-02T10:00:00.000000Z')
                    """);
            drainWalQueue();

            // the pooled reader opens the index readers of both columns in the first partition
            try (TableReader reader = getReader("t")) {
                reader.openPartition(0);
                Assert.assertNotNull(reader.getIndexReader(0, reader.getMetadata().getColumnIndex("p"), IndexReader.DIR_FORWARD));
                Assert.assertNotNull(reader.getIndexReader(0, reader.getMetadata().getColumnIndex("b"), IndexReader.DIR_FORWARD));
            }

            // an out-of-order row rewrites the first partition, and x goes, so p and b move one slot left
            execute("INSERT INTO t VALUES (3, 'p3', 'b3', '2024-01-01T09:00:00.000000Z')");
            execute("ALTER TABLE t DROP COLUMN x");
            drainWalQueue();

            // the index scan of b runs on the pooled reader
            assertQuery("SELECT p, b, ts FROM t WHERE b = 'b3'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("p\tb\tts\np3\tb3\t2024-01-01T09:00:00.000000Z\n");
        });
    }
}
