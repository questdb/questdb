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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A POSTING index that INCLUDEs a column added after older partitions were written. Those partitions have no file
 * for the column. A commit that touches such a partition without writing a row into it still seals the index there.
 */
public class CoveringIndexAddedColumnSealTest extends AbstractCairoTest {

    @Test
    public void testEmptyReplaceRangeInPartitionThatPredatesCoveredColumn() throws Exception {
        // merge-append publishes the covering index on its own path; production has it off
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                        ('2022-02-25T01:00:00.000000Z', 'a', 1),
                        ('2022-02-25T10:00:00.000000Z', 'b', 2),
                        ('2022-02-26T01:00:00.000000Z', 'a', 3)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t ADD COLUMN var_top VARCHAR");
            // VARCHAR returns from its aux read, so only a fixed-size column reaches the data-file read
            execute("ALTER TABLE t ADD COLUMN d DOUBLE");
            execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (var_top, d)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("t");
            try (TableReader reader = engine.getReader(token)) {
                final int varTop = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("var_top"));
                Assert.assertEquals(
                        "precondition: the older partition must not have the covered column",
                        -1,
                        reader.getColumnVersionReader().getColumnTop(MicrosTimestampDriver.floor("2022-02-25"), varTop)
                );
            }

            // Zero rows, and the range holds none of the partition's rows: nothing is written to 2022-02-25. A range
            // that removes rows rewrites the partition instead and does not fail.
            try (WalWriter w = engine.getWalWriter(token)) {
                w.commitWithParams(
                        MicrosTimestampDriver.floor("2022-02-25T04:00:00.000000Z"),
                        MicrosTimestampDriver.floor("2022-02-25T05:00:00.000000Z"),
                        WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                );
            }
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("suspended\nfalse\n");
            assertQuery("SELECT ts, sym, v, var_top, d FROM t WHERE sym = 'b'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\tsym\tv\tvar_top\td\n2022-02-25T10:00:00.000000Z\tb\t2\t\tnull\n");
        });
    }
}
