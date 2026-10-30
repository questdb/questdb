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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A WAL dedup commit that sorts against rows held in LAG maps the LAG rows of every dedup key in
 * TableWriter.deduplicateSortedIndex() and records the mappings in a dedup column block, a block O3 partition
 * tasks also take. This test fails such a mapping in a block an O3 partition task left -1 sentinels in, and
 * checks that the commit fails with the mapping error (issue #8067).
 */
public class WalDedupStaleSlotTest extends AbstractCairoTest {
    // The largest OS page (the Windows mmap granularity) is a multiple of every smaller one, so this append page
    // gives every OS the same rows per page, and a run on one OS runs the row counts of all of them.
    private static final long APPEND_PAGE_SIZE = Math.max(64 * 1024, Files.PAGE_SIZE);
    // Rows of an 8-byte column per append page.
    private static final long ROWS_PER_PAGE = APPEND_PAGE_SIZE / Long.BYTES;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // LAG rows that span append pages need a fresh mapping, which the test fails
        node1.setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, APPEND_PAGE_SIZE);
        // TableWriter parks a transaction in LAG only while LAG stays within multiplier x max uncommitted rows. The test
        // parks 3 append pages of rows there, past the 20 x 1,000 rows of the test defaults, so pin both factors.
        node1.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_000);
        node1.setProperty(PropertyKey.CAIRO_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER, 100);
        // merge-append tables take no LAG
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
        // one WAL transaction per apply job run
        node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);
    }

    @Test
    public void testFailedLagMapKeepsItsError() throws Exception {
        final Set<Long> keyFds = ConcurrentHashMap.newKeySet();
        final AtomicBoolean failKeyMap = new AtomicBoolean();
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean close(long fd) {
                keyFds.remove(fd);
                return super.close(fd);
            }

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (flags == Files.MAP_RO && memoryTag == MemoryTag.MMAP_TABLE_WRITER && keyFds.contains(fd) && failKeyMap.compareAndSet(true, false)) {
                    return FilesFacade.MAP_FAILED;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                // k is added by ALTER TABLE, so its partition files carry a column name txn: k.d.<txn>
                if (fd > -1 && Utf8s.stringFromUtf8Bytes(name).contains(Files.SEPARATOR + "k.d.")) {
                    keyFds.add(fd);
                }
                return fd;
            }
        };
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t (v, ts) SELECT x::INT, timestamp_sequence('2024-01-01', 1_000) FROM long_sequence(10)");
            drainWalQueue();
            // k is all column top in 2024-01-01
            execute("ALTER TABLE t ADD COLUMN k LONG");
            drainWalQueue();
            execute("ALTER TABLE t DEDUP ENABLE UPSERT KEYS(ts, k)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("t");

            insert("2024-01-02T00:00:00", 10);
            applyOneWalTxn(token);
            // The O3 merge into 2024-01-01 leaves -1 sentinels for k in its partition task's dedup column block.
            insert("2024-01-01T00:00:00.000500", 5);
            applyOneWalTxn(token);
            // This commit's dedup takes the next block and clears the block list.
            insert("2024-01-02T01:00:00", 10);
            applyOneWalTxn(token);
            // The first transaction stays in LAG. The second dedups against it, gets the block with the
            // sentinels, and fails to map the LAG rows of k.
            insert("2024-01-02T02:00:00", 3 * ROWS_PER_PAGE);
            insert("2024-01-02T02:00:00.000500", 10);
            applyOneWalTxn(token);
            failKeyMap.set(true);
            applyOneWalTxn(token);

            Assert.assertFalse("the commit did not map the LAG rows of k", failKeyMap.get());
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
            TestUtils.assertContains(engine.getTableSequencerAPI().getTxnTracker(token).getErrorMessage(), "could not mmap");
        });
    }

    private static void applyOneWalTxn(TableToken token) {
        engine.getTableSequencerAPI().getTxnTracker(token).getMemPressureControl().setMaxBlockRowCount(1);
        try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
            walApplyJob.run();
        }
    }

    private static void insert(String fromTimestamp, long rowCount) throws Exception {
        execute("INSERT INTO t (v, k, ts) SELECT x::INT, x, timestamp_sequence('" + fromTimestamp + "', 1_000) FROM long_sequence(" + rowCount + ")");
    }
}
