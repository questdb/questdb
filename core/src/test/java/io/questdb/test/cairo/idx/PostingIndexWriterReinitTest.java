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

import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

public class PostingIndexWriterReinitTest extends AbstractCairoTest {
    private static final int KEY_COUNT = 4;
    private static final String NAME = "s";

    @Test
    public void testInitOfNewIndexStartsAtSealTxnZero() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path, NAME, COLUMN_NAME_TXN_NONE, true);
                    write(writer, 0, 8);
                }
                Assert.assertEquals(0, headSealTxn(path, plen));
                assertRows(path, plen, 0, 8);
            }
        });
    }

    @Test
    public void testInitOverUnreadableKeyFileStartsAtSealTxnZero() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, NAME, COLUMN_NAME_TXN_NONE)) {
                    write(writer, 0, 8);
                    writer.seal();
                    write(writer, 8, 16);
                    writer.seal();
                }
                final FilesFacade ff = configuration.getFilesFacade();
                final long fd = ff.openRW(PostingIndexUtils.keyFileName(path.trimTo(plen), NAME, COLUMN_NAME_TXN_NONE), configuration.getWriterFileOpenOpts());
                try {
                    Assert.assertTrue(ff.truncate(fd, 64));
                } finally {
                    ff.close(fd);
                }

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), NAME, COLUMN_NAME_TXN_NONE, true);
                    write(writer, 100, 104);
                }
                Assert.assertEquals(0, headSealTxn(path, plen));
                assertRows(path, plen, 100, 104);
            }
        });
    }

    @Test
    public void testInitStartsPastSealTxnsOfReplacedIndex() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                final long replacedSealTxn;
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, NAME, COLUMN_NAME_TXN_NONE)) {
                    write(writer, 0, 8);
                    writer.seal();
                    write(writer, 8, 16);
                    writer.seal();
                    replacedSealTxn = headSealTxn(path, plen);
                }
                Assert.assertTrue("fixture: the replaced index must have sealed", replacedSealTxn > 0);

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), NAME, COLUMN_NAME_TXN_NONE, true);
                    write(writer, 100, 104);
                }
                Assert.assertEquals(replacedSealTxn + 1, headSealTxn(path, plen));
                assertRows(path, plen, 100, 104);

                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), NAME, COLUMN_NAME_TXN_NONE, false);
                    write(writer, 104, 108);
                    writer.seal();
                }
                Assert.assertEquals(replacedSealTxn + 2, headSealTxn(path, plen));
                assertRows(path, plen, 100, 108);
            }
        });
    }

    private static void assertRows(Path path, int plen, int rowLo, int rowHi) {
        try (PostingIndexFwdReader reader = new PostingIndexFwdReader(configuration, path.trimTo(plen), NAME, COLUMN_NAME_TXN_NONE, -1, 0)) {
            int rowCount = 0;
            for (int key = 0; key < KEY_COUNT; key++) {
                final RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE);
                try {
                    long expectedRow = rowLo + Math.floorMod(key - rowLo, KEY_COUNT);
                    while (cursor.hasNext()) {
                        Assert.assertEquals("key " + key, expectedRow, cursor.next());
                        expectedRow += KEY_COUNT;
                        rowCount++;
                    }
                    Assert.assertTrue("key " + key + " ends at " + expectedRow, expectedRow >= rowHi);
                } finally {
                    Misc.free(cursor);
                }
            }
            Assert.assertEquals(rowHi - rowLo, rowCount);
        }
    }

    private static long headSealTxn(Path path, int plen) {
        return PostingIndexUtils.readSealTxnFromKeyFile(
                configuration.getFilesFacade(),
                PostingIndexUtils.keyFileName(path.trimTo(plen), NAME, COLUMN_NAME_TXN_NONE)
        );
    }

    private static void write(PostingIndexWriter writer, int rowLo, int rowHi) {
        for (int row = rowLo; row < rowHi; row++) {
            writer.add(row % KEY_COUNT, row);
        }
        writer.setMaxValue(rowHi - 1);
        writer.commit();
    }
}
