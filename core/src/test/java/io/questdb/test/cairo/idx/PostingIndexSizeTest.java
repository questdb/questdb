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

import io.questdb.cairo.idx.PostingIndexUtils;
import org.junit.Assert;
import org.junit.Test;

public class PostingIndexSizeTest {
    @Test
    public void testFlatSelectionAtHeaderBoundary() {
        int header = PostingIndexUtils.strideFlatHeaderSize(256);
        Assert.assertEquals(1040, header);
        Assert.assertEquals(30, PostingIndexUtils.selectFlatBitWidth(572_662_028, 30, 30, header, 3_000_000_000L));
        Assert.assertEquals(0, PostingIndexUtils.selectFlatBitWidth(572_662_029, 30, 30, header, 3_000_000_000L));
        Assert.assertEquals(0, PostingIndexUtils.selectFlatBitWidth(572_662_030, 30, 30, header, 3_000_000_000L));
        // Byte-aligned packing also covers an exact Integer.MAX_VALUE total.
        long limit = Integer.MAX_VALUE - header;
        Assert.assertEquals(8, PostingIndexUtils.selectFlatBitWidth(limit - 1, 8, 8, header, 3_000_000_000L));
        Assert.assertEquals(8, PostingIndexUtils.selectFlatBitWidth(limit, 8, 8, header, 3_000_000_000L));
        Assert.assertEquals(0, PostingIndexUtils.selectFlatBitWidth(limit + 1, 8, 8, header, 3_000_000_000L));
    }

    @Test
    public void testFlatSelectionPreservesAlignedPreference() {
        int header = PostingIndexUtils.strideFlatHeaderSize(256);
        Assert.assertEquals(32, PostingIndexUtils.selectFlatBitWidth(100, 30, 32, header, 1441));
        Assert.assertEquals(30, PostingIndexUtils.selectFlatBitWidth(100, 30, 32, header, 1440));
        Assert.assertEquals(0, PostingIndexUtils.selectFlatBitWidth(100, 30, 32, header, 1415));
        // Natural fits, aligned exceeds the int limit including its header.
        Assert.assertEquals(30, PostingIndexUtils.selectFlatBitWidth(550_000_000, 30, 32, header, 3_000_000_000L));
    }

    @Test
    public void testFlatSelectionRejectsOversizedDataAndCounts() {
        int header = PostingIndexUtils.strideFlatHeaderSize(256);
        Assert.assertEquals(0, PostingIndexUtils.selectFlatBitWidth(572_662_306, 30, 32, header, 3_000_000_000L));
        Assert.assertEquals(0, PostingIndexUtils.selectFlatBitWidth(576_823_200, 30, 32, header, 3_000_000_000L));
        Assert.assertEquals(0, PostingIndexUtils.selectFlatBitWidth((long) Integer.MAX_VALUE + 1, 1, 1, header, 3_000_000_000L));
    }
}
