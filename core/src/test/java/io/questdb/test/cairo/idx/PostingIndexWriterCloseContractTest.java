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

package io.questdb.test.cairo.idx;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Pins the cleanup contract of {@link PostingIndexWriter#close()}: it runs from
 * {@code TableWriter.doClose} through {@code Misc.freeObjList(indexers)}, ahead of
 * ~25 further {@code Misc.free(...)} calls and {@code releaseLock(...)}, and neither
 * {@code Misc.freeObjList} nor {@code SymbolColumnIndexer.close()} guards its element.
 * A throw out of {@code close()} therefore leaks the writer's native memory and strands
 * the table lock. The .pk trim inside {@code close()} reads and validates the chain
 * header, so a damaged header must degrade to a logged no-trim, not to a throw.
 */
public class PostingIndexWriterCloseContractTest extends AbstractCairoTest {

    @Test
    public void testCloseDoesNotTruncateWhenHeaderIsUnreadable() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_close_contract";
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final PostingIndexWriter writer = new PostingIndexWriter(configuration);
                try {
                    writer.setO3PathContext(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 42L);
                    writer.openFromO3Context(/* isInit */ true);
                    for (int i = 0; i < 64; i++) {
                        writer.add(i & 7, i);
                    }
                    writer.setMaxValue(63);
                    writer.commit();

                    final LPSZ keyFile = PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                    corruptHeaderFormatVersion(ff, keyFile);
                    final long lengthBeforeClose = ff.length(keyFile);
                    Assert.assertTrue(
                            "the published .pk must extend past the header window",
                            lengthBeforeClose > PostingIndexUtils.KEY_FILE_RESERVED
                    );

                    // The contract under test: close() is a cleanup method and must not throw.
                    writer.close();

                    Assert.assertEquals(
                            "an unreadable header must degrade to leaving the .pk untouched, never to a trim "
                                    + "computed from this instance's stale cached high-water",
                            lengthBeforeClose,
                            ff.length(keyFile)
                    );
                } finally {
                    writer.close();
                }
            }
        });
    }

    @Test
    public void testTableWriterCloseSurvivesUnreadablePostingHeader() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE posting_close_lock (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO posting_close_lock
                    SELECT dateadd('s', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), rnd_symbol('A', 'B', 'C')
                    FROM long_sequence(10_000)
                    """);
            engine.releaseInactive();

            final TableToken token = engine.verifyTableName("posting_close_lock");
            final long expectedRowCount;
            final long partitionTimestamp;
            final long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                expectedRowCount = reader.size();
                final int last = reader.getTxFile().getPartitionCount() - 1;
                partitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(last);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(last);
            }
            engine.releaseInactive();

            final FilesFacade ff = configuration.getFilesFacade();
            final long[] savedFormatVersions;
            // An off-pool writer owns the table lock outright, so a throw out of doClose
            // strands that lock instead of parking a distressed writer in the pool.
            final TableWriter writer = newOffPoolWriter(configuration, "posting_close_lock");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForNativePartition(
                        path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTimestamp, partitionNameTxn);
                final LPSZ keyFile = PostingIndexUtils.keyFileName(path, "sym", COLUMN_NAME_TXN_NONE);
                Assert.assertTrue("the live partition must carry a .pk, path=" + keyFile, ff.length(keyFile) > 0);
                savedFormatVersions = corruptHeaderFormatVersion(ff, keyFile);

                // The contract under test: close() must keep freeing and release the lock.
                writer.close();

                restoreHeaderFormatVersion(ff, keyFile, savedFormatVersions);
            } catch (Throwable th) {
                writer.close();
                throw th;
            }

            // Proves the table lock was released: a fresh off-pool writer can take it.
            try (TableWriter reopened = newOffPoolWriter(configuration, "posting_close_lock")) {
                Assert.assertEquals(expectedRowCount, reopened.size());
            }
        });
    }

    /**
     * Writes an unsupported format version into both header pages of a live .pk and
     * returns the replaced values. Both pages are stamped so the corruption lands on
     * whichever page {@code readUnderSeqlock} picks. The mapping is shared, so a
     * writer holding the same file sees the change.
     */
    private static long[] corruptHeaderFormatVersion(FilesFacade ff, LPSZ keyFile) {
        final long fileSize = ff.length(keyFile);
        final MemoryCMARWImpl mem = new MemoryCMARWImpl(
                ff, keyFile, ff.getPageSize(), fileSize, MemoryTag.MMAP_DEFAULT, /* opts */ 0);
        try {
            final long[] saved = new long[]{
                    mem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_FORMAT_VERSION),
                    mem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_FORMAT_VERSION)
            };
            mem.putLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_FORMAT_VERSION, 99L);
            mem.putLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_FORMAT_VERSION, 99L);
            return saved;
        } finally {
            // close(false): this helper must not resize the file underneath the live writer.
            mem.close(false);
        }
    }

    private static void restoreHeaderFormatVersion(FilesFacade ff, LPSZ keyFile, long[] saved) {
        final long fileSize = ff.length(keyFile);
        final MemoryCMARWImpl mem = new MemoryCMARWImpl(
                ff, keyFile, ff.getPageSize(), fileSize, MemoryTag.MMAP_DEFAULT, /* opts */ 0);
        try {
            mem.putLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_FORMAT_VERSION, saved[0]);
            mem.putLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.V2_HEADER_OFFSET_FORMAT_VERSION, saved[1]);
        } finally {
            mem.close(false);
        }
    }
}
