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

import io.questdb.cairo.idx.PostingIndexChainEntry;
import io.questdb.cairo.idx.PostingIndexChainWriter;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Tests for {@link PostingIndexWriter#diffAgainstChainHead}. Verifies the
 * primitive correctly evicts stale (key, rowId) entries by validating each
 * existing chain entry against the column .d file.
 */
public class PostingIndexDiffTest extends AbstractCairoTest {

    /**
     * Happy path: chain has both an OLD (key, rowId) pair from a pre-O3
     * write and a NEW (key, rowId) pair from an O3 commit that rewrote the
     * same rowId with a different symbol. The .d file reflects the post-O3
     * state. diffAgainstChainHead must keep the NEW pair and evict the OLD.
     */
    @Test
    public void testDiffEvictsStaleEntryKeepsNewEntry() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "diff_evicts_stale";
                int plen = path.size();
                FilesFacade ff = configuration.getFilesFacade();

                // Build a synthetic column .d file at <db>/sym.d. The diff's
                // validator reads 4-byte ints from this file at rowId * 4. We
                // place 4 rows: symbols [1, 1, 1, 5]. toIndexKey adds 1 for
                // non-negative, so the expected keys per rowId are [2, 2, 2, 6].
                long dataFd;
                try (Path dpath = new Path().of(configuration.getDbRoot()).concat("sym.d")) {
                    LPSZ dFilePath = dpath.$();
                    long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
                    try {
                        Unsafe.putInt(buf, 1);
                        Unsafe.putInt(buf + 4, 1);
                        Unsafe.putInt(buf + 8, 1);
                        Unsafe.putInt(buf + 12, 5);
                        long writeFd = ff.openRW(dFilePath, configuration.getWriterFileOpenOpts());
                        Assert.assertTrue("could not create test .d file", writeFd > 0);
                        long written = ff.write(writeFd, buf, 16, 0);
                        Assert.assertEquals(16, written);
                        ff.close(writeFd);
                    } finally {
                        Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
                    }
                    dataFd = ff.openRO(dFilePath);
                    Assert.assertTrue("could not open test .d file", dataFd > 0);
                }

                try {
                    // Build a chain with both stale and fresh entries.
                    // Pre-O3: every rowId 0..3 was symbol 1, so chain
                    // had key=2 -> {0,1,2,3}.
                    // Post-O3: rowId 3 rewritten to symbol 5; O3CopyJob
                    // added (key=6, rowId=3) but did NOT evict (key=2, rowId=3).
                    // The chain at this moment has BOTH entries for rowId=3.
                    try (PostingIndexWriter writer = new PostingIndexWriter(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)) {
                        writer.setNextTxnAtSeal(10L);
                        writer.add(2, 0);
                        writer.add(2, 1);
                        writer.add(2, 2);
                        writer.add(2, 3);
                        writer.add(6, 3);
                        writer.setMaxValue(3);
                        writer.commit();
                        writer.setNextTxnAtSeal(10L);
                        writer.seal();
                    }

                    // Reopen and run the diff. partitionSize=4, columnTop=0.
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                        writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, /* init */ false);
                        writer.setNextTxnAtSeal(20L);
                        writer.diffAgainstChainHead(ff, dataFd, /* columnTop */ 0, /* partitionSize */ 4);

                        // Cursor for key=2 must yield exactly {0, 1, 2} (rowId=3 evicted).
                        RowCursor key2 = writer.getCursor(2);
                        long[] key2Rows = drain(key2);
                        Assert.assertArrayEquals(
                                "key=2 must keep rowIds 0,1,2 and have rowId=3 evicted as stale",
                                new long[]{0, 1, 2}, key2Rows
                        );

                        // Cursor for key=6 must yield exactly {3} (the post-O3 entry).
                        RowCursor key6 = writer.getCursor(6);
                        long[] key6Rows = drain(key6);
                        Assert.assertArrayEquals(
                                "key=6 must retain rowId=3 (matches .d, post-O3 state)",
                                new long[]{3}, key6Rows
                        );
                    }

                    // Verify on disk: chain has two entries (OLD prev + NEW head).
                    LPSZ keyFile = PostingIndexUtils.keyFileName(
                            path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                    long fileSize = ff.length(keyFile);
                    try (MemoryCMARWImpl mem = new MemoryCMARWImpl(
                            ff, keyFile, ff.getPageSize(), fileSize,
                            MemoryTag.MMAP_DEFAULT, /* opts */ 0)) {
                        PostingIndexChainWriter chain = new PostingIndexChainWriter();
                        chain.openExisting(mem);
                        Assert.assertTrue("chain must have a head", chain.hasHead());
                        Assert.assertTrue(
                                "diff must append a NEW chain entry (sealTxn rotated). "
                                        + "Got entryCount=" + chain.getEntryCount(),
                                chain.getEntryCount() >= 2
                        );
                        PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
                        chain.loadHeadEntry(mem, head);
                        Assert.assertEquals(
                                "head entry must be a single dense gen", 1, head.genCount
                        );
                        Assert.assertEquals(
                                "head txnAtSeal must reflect the diff's setNextTxnAtSeal",
                                20L, head.txnAtSeal
                        );
                    }
                } finally {
                    ff.close(dataFd);
                }
            }
        });
    }

    private static long[] drain(RowCursor cursor) {
        long[] out = new long[16];
        int n = 0;
        while (cursor.hasNext()) {
            if (n == out.length) {
                long[] grown = new long[out.length * 2];
                System.arraycopy(out, 0, grown, 0, n);
                out = grown;
            }
            out[n++] = cursor.next();
        }
        long[] result = new long[n];
        System.arraycopy(out, 0, result, 0, n);
        return result;
    }
}
