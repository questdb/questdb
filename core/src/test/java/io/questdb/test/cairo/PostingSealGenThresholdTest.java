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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Covers {@link io.questdb.cairo.TableWriter#sealPostingIndexesForLastPartitionFastLag()}
 * sealing covering POSTING indexes once unsealed gen count crosses
 * {@code cairo.posting.seal.gen.threshold}.
 *
 * <p>Pure in-order WAL ingestion to a single partition routes through fast-lag.
 * Without this gating, covering posting accumulates one gen per commit because
 * the fast-lag path historically exempted covering (waiting for the next O3
 * commit). For workloads that never produce O3, gen count grew without bound
 * and per-key cursor probes scaled linearly with it.
 */
public class PostingSealGenThresholdTest extends AbstractCairoTest {

    @Test
    public void testHighThresholdLetsGensAccumulate() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_SEAL_GEN_THRESHOLD, Integer.MAX_VALUE);
        assertMemoryLeak(() -> {
            createCoveringTable("t_high");
            int commits = 8;
            for (int i = 0; i < commits; i++) {
                execute("INSERT INTO t_high VALUES ('2024-01-01T00:00:0" + i + ".000000Z', 'A', " + i + ".0)");
                drainWalQueue();
            }
            assertEquals(
                    "MAX_VALUE threshold disables auto-seal; gen per commit stays unsealed",
                    commits,
                    readGenCount("t_high")
            );
        });
    }

    @Test
    public void testThresholdCapsGenCount() throws Exception {
        // sealIfMultiGen seals when genCount > threshold. With threshold=4 and
        // 10 in-order commits, the writer cycles 1..5 -> seal -> 1..5 -> seal,
        // so genCount never exceeds 5.
        node1.setProperty(PropertyKey.CAIRO_POSTING_SEAL_GEN_THRESHOLD, 4);
        assertMemoryLeak(() -> {
            createCoveringTable("t_cap");
            int commits = 10;
            for (int i = 0; i < commits; i++) {
                execute("INSERT INTO t_cap VALUES ('2024-01-01T00:00:0" + i + ".000000Z', 'A', " + i + ".0)");
                drainWalQueue();
            }
            int genCount = readGenCount("t_cap");
            assertTrue(
                    "threshold=4 must cap genCount at <= 5 after " + commits
                            + " commits, got " + genCount,
                    genCount <= 5
            );
            assertTrue(
                    "threshold=4 must allow some accumulation between seals, got " + genCount,
                    genCount >= 1
            );
        });
    }

    @Test
    public void testThresholdZeroSealsEveryCommit() throws Exception {
        // sealIfMultiGen seals when genCount > threshold. threshold=0 means
        // every commit that produces a gen triggers a seal, so the next read
        // always sees a single dense gen.
        node1.setProperty(PropertyKey.CAIRO_POSTING_SEAL_GEN_THRESHOLD, 0);
        assertMemoryLeak(() -> {
            createCoveringTable("t_zero");
            int commits = 6;
            for (int i = 0; i < commits; i++) {
                execute("INSERT INTO t_zero VALUES ('2024-01-01T00:00:0" + i + ".000000Z', 'A', " + i + ".0)");
                drainWalQueue();
            }
            assertEquals(
                    "threshold=0 seals after every commit; reader sees one dense gen",
                    1,
                    readGenCount("t_zero")
            );
        });
    }

    private void createCoveringTable(String name) throws Exception {
        execute("CREATE TABLE " + name + " ("
                + "ts TIMESTAMP, "
                + "sym SYMBOL INDEX TYPE POSTING INCLUDE (price), "
                + "price DOUBLE"
                + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private int readGenCount(String tableName) {
        try (TableReader r = engine.getReader(tableName);
             Path path = new Path()) {
            long partitionTs = r.getPartitionTimestampByIndex(0);
            long partitionNameTxn = r.getTxFile().getPartitionNameTxn(0);
            path.of(configuration.getDbRoot()).concat(r.getTableToken().getDirName());
            TableUtils.setPathForNativePartition(
                    path,
                    ColumnType.TIMESTAMP_MICRO,
                    PartitionBy.DAY,
                    partitionTs,
                    partitionNameTxn
            );
            try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                    configuration, path, "sym", COLUMN_NAME_TXN_NONE, partitionTs, 0,
                    r.getMetadata(), r.getColumnVersionReader(), partitionTs)) {
                return reader.getGenCount();
            }
        }
    }
}
