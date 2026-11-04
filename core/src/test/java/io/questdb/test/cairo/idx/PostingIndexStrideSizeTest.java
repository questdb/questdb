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

import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

public class PostingIndexStrideSizeTest extends AbstractCairoTest {
    @Test
    public void testAlignedFlatSealAndIncrementalSeal() throws Exception {
        assertSealRoundTrip(1, 4, 1.0, 16, 16);
    }

    @Test
    public void testDeltaSealAndIncrementalSeal() throws Exception {
        assertSealRoundTrip(256, 1, 0.0, 0, 0);
    }

    @Test
    public void testNaturalFlatSealAndIncrementalSeal() throws Exception {
        assertSealRoundTrip(1, 4, 0.0, 10, 11);
    }

    private void assertSealRoundTrip(int rowsPerKey, int rowStep, double alignmentThreshold, int initialWidth, int finalWidth) throws Exception {
        assertMemoryLeak(() -> {
            DefaultCairoConfiguration cfg = new DefaultCairoConfiguration(root) {
                @Override
                public double getPostingIndexAlignedBitWidthThreshold() {
                    return alignmentThreshold;
                }
            };
            try (Path path = new Path().of(root)) {
                int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(cfg, path, "s", COLUMN_NAME_TXN_NONE)) {
                    // Two strides let the second seal rebuild only dirty stride 0.
                    for (int row = 0; row < rowsPerKey; row++) {
                        for (int key = 0; key < 300; key++) {
                            writer.add(key, ((long) row * 300 + key) * rowStep);
                        }
                    }
                    writer.commit();
                    writer.seal();
                    Assert.assertFalse(writer.isLastSealStreamingForTesting());
                    Assert.assertFalse(writer.isLastSealIncrementalForTesting());
                    assertRows(cfg, path.trimTo(plen), rowsPerKey, rowStep, false, initialWidth);
                    writer.add(0, (long) rowsPerKey * 300 * rowStep);
                    writer.commit();
                    writer.seal();
                    Assert.assertTrue(writer.isLastSealIncrementalForTesting());
                    assertRows(cfg, path.trimTo(plen), rowsPerKey, rowStep, true, finalWidth);
                }
            }
        });
    }

    private void assertRows(DefaultCairoConfiguration cfg, Path path, int rowsPerKey, int rowStep, boolean hasExtraRow, int expectedWidth) {
        try (PostingIndexFwdReader reader = new PostingIndexFwdReader(cfg, path, "s", COLUMN_NAME_TXN_NONE, 0, 0)) {
            // Sealing writes the single dense generation at offset zero.
            long genAddr = reader.getValueBaseAddress();
            long strideAddr = genAddr + PostingIndexUtils.strideIndexSize(300) + Unsafe.getLong(genAddr);
            Assert.assertEquals(expectedWidth == 0 ? PostingIndexUtils.STRIDE_MODE_DELTA : PostingIndexUtils.STRIDE_MODE_FLAT,
                    Unsafe.getByte(strideAddr));
            if (expectedWidth != 0) {
                Assert.assertEquals(expectedWidth, Unsafe.getByte(strideAddr + 1));
            }
            for (int key = 0; key < 300; key++) {
                try (RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE)) {
                    for (int row = 0; row < rowsPerKey; row++) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(((long) row * 300 + key) * rowStep, cursor.next());
                    }
                    if (hasExtraRow && key == 0) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals((long) rowsPerKey * 300 * rowStep, cursor.next());
                    }
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        }
    }
}
