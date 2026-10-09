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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A merge-append plan grows each column file once, up front, to the extent the whole plan can reach, and that extent
 * is an upper bound: a dedup merge that drops rows writes less than it reserved. The column frames the plan writes
 * through give back what nobody wrote when they close, so the partition does not carry the over-estimate on disk
 * until something rewrites it.
 * <p>
 * The fixture is a dedup merge whose every incoming row replaces one the piece holds, with a different value: the
 * plan reserves the piece's rows plus the incoming ones, and writes only the piece's rows. 20,000 rows of the
 * narrowest column are 160,000 bytes, more than a 64KiB page on Windows, so the over-estimate cannot hide in the
 * page-rounded slack of the last allocation.
 */
public class CompositeColumnFileTrimTest extends AbstractCairoTest {
    private static final String REPLAY = "INSERT INTO x SELECT x + 100_000, 'abc', timestamp_sequence('2024-01-01T01:00:00', 1_000_000L) FROM long_sequence(20_000)";

    @Test
    public void testDedupMergeTrimsUnwrittenReservation() throws Exception {
        assertMemoryLeak(() -> assertTrimmedAfterDedupMerge(false));
    }

    @Test
    public void testDedupMergeTrimsUnwrittenReservationWithMixedIo() throws Exception {
        assertMemoryLeak(() -> assertTrimmedAfterDedupMerge(true));
    }

    /**
     * Windows refuses to shorten a file while any view of it is mapped, and a reader of the partition maps its column
     * files. The trim is best-effort: the refused bytes stay, exactly as they did before files were trimmed, and the
     * close and the commits after it go on.
     */
    @Test
    public void testRefusedTrimLeavesTheFileUsable() throws Exception {
        final WindowsMappedTruncateFacade ff = new WindowsMappedTruncateFacade();
        assertMemoryLeak(ff, () -> {
            engine.resetFrameFactory();
            createDedupDay();
            try (TableReader reader = engine.getReader("x")) {
                reader.openPartition(0);
                execute(REPLAY);
                drainWalQueue();
                engine.releaseAllWriters();
                Assert.assertTrue("no trim was refused under a mapped reader", ff.getRefusedTruncateCount() > 0);
            }
            final TableToken token = engine.verifyTableName("x");
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            execute(REPLAY.replace("x + 100_000", "x + 200_000"));
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT count() c, sum(v) s, count_distinct(s) d FROM x")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\ts\td\n40001\t4728020000\t1\n");
        });
    }

    private static void assertFileLength(FilesFacade ff, Path partitionPath, String fileName, long liveBytes) {
        final int plen = partitionPath.size();
        try {
            final long length = ff.length(partitionPath.concat(fileName).$());
            Assert.assertTrue(fileName + " is shorter than its rows [length=" + length + ", liveBytes=" + liveBytes + ']',
                    length >= liveBytes);
            Assert.assertEquals(fileName + " kept bytes nobody wrote [liveBytes=" + liveBytes + ']',
                    Files.ceilPageSize(liveBytes), length);
        } finally {
            partitionPath.trimTo(plen);
        }
    }

    private void assertTrimmedAfterDedupMerge(boolean mixedIo) throws Exception {
        node1.setProperty(PropertyKey.DEBUG_CAIRO_ALLOW_MIXED_IO, mixedIo);
        createDedupDay();
        execute(REPLAY);
        drainWalQueue();
        // Closes the writer, and with it every frame it kept open across commits.
        engine.releaseAllWriters();

        final TableToken token = engine.verifyTableName("x");
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
        try (TableReader reader = engine.getReader(token); Path path = new Path()) {
            // The fixture, not the fix: a composite day whose plan merged in place.
            Assert.assertTrue(reader.getTxFile().isPartitionComposite(0));
            final long e = reader.getGeometry().getE(0);
            Assert.assertTrue("the merge did not write at the tail [e=" + e + ']', e > 40_000);
            path.of(configuration.getDbRoot()).concat(token.getDirName());
            TableUtils.setPathForNativePartition(
                    path,
                    reader.getMetadata().getTimestampType(),
                    reader.getPartitionedBy(),
                    reader.getPartitionTimestampByIndex(0),
                    reader.getTxFile().getPartitionNameTxn(0)
            );
            final FilesFacade ff = configuration.getFilesFacade();
            assertFileLength(ff, path, "v.d", e * Long.BYTES);
            assertFileLength(ff, path, "ts.d", e * Long.BYTES);
            // STRING: an N+1 aux vector of 8-byte offsets, and 4 + 2 * 3 data bytes per 'abc'.
            assertFileLength(ff, path, "s.i", (e + 1) * Long.BYTES);
            assertFileLength(ff, path, "s.d", e * 10);
        }
        assertQuery("SELECT count() c, sum(v) s, count_distinct(s) d FROM x")
                .noRandomAccess()
                .expectSize()
                .returns("c\ts\td\n40001\t2728020000\t1\n");
    }

    private static void createDedupDay() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1G");
        // The merge leaves as many dead rows as live ones; nothing may fold them away before the files are looked at.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "10.0");
        execute("CREATE TABLE x (v LONG, s STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
        execute("INSERT INTO x SELECT x, 'abc', timestamp_sequence('2024-01-01', 1_000_000L) FROM long_sequence(40_000)");
        execute("INSERT INTO x VALUES (0, 'abc', '2024-01-03')");
        drainWalQueue();
    }
}
