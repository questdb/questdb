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

package io.questdb.test.griffin.engine.ops;

import io.questdb.cairo.IndexType;
import io.questdb.griffin.engine.ops.AlterOperation;
import org.junit.Assert;
import org.junit.Test;

/**
 * Round-trip and cross-version compatibility tests for the ALTER ADD_COLUMN /
 * CHANGE_COLUMN_TYPE flags long. The v2 layout (this branch onwards) carries
 * a format-marker bit so a future widening of the index-type mask cannot
 * silently re-interpret in-flight WAL payloads written under an earlier
 * layout.
 */
public class AlterOperationFlagsTest {

    private static final long V1_BIT_DEDUP_KEY = 0x02L;
    private static final long V1_BIT_INDEXED = 0x01L;
    private static final long V2_FORMAT_MARKER = 1L << 63;

    @Test
    public void testRoundTripAllIndexTypes() {
        for (byte type : new byte[]{
                IndexType.NONE, IndexType.BITMAP,
                IndexType.POSTING, IndexType.POSTING_DELTA, IndexType.POSTING_EF
        }) {
            for (boolean dedup : new boolean[]{false, true}) {
                long flags = AlterOperation.getFlags(type, dedup);
                Assert.assertEquals("indexType round-trip failed for " + type + "/" + dedup,
                        type, AlterOperation.decodeIndexType(flags));
                Assert.assertEquals("dedup round-trip failed for " + type + "/" + dedup,
                        dedup, AlterOperation.decodeIsDedupKey(flags));
            }
        }
    }

    @Test
    public void testV1NotIndexedNotDedup() {
        // Pre-POSTING master payload: flags=0x0. Decodes as NONE / not-dedup.
        long flags = 0x0L;
        Assert.assertEquals(IndexType.NONE, AlterOperation.decodeIndexType(flags));
        Assert.assertFalse(AlterOperation.decodeIsDedupKey(flags));
    }

    @Test
    public void testV1IndexedNotDedup() {
        // Pre-POSTING master payload: flags=0x1 (BIT_INDEXED). Must decode
        // as BITMAP because that was the only index type that existed.
        long flags = V1_BIT_INDEXED;
        Assert.assertEquals(IndexType.BITMAP, AlterOperation.decodeIndexType(flags));
        Assert.assertFalse(AlterOperation.decodeIsDedupKey(flags));
    }

    @Test
    public void testV1DedupOnly() {
        // Pre-POSTING master payload: flags=0x2 (BIT_DEDUP_KEY). Decodes as
        // NONE + dedup. Not producible from SQL today, but the format must
        // still round-trip for any synthetic / future caller.
        long flags = V1_BIT_DEDUP_KEY;
        Assert.assertEquals(IndexType.NONE, AlterOperation.decodeIndexType(flags));
        Assert.assertTrue(AlterOperation.decodeIsDedupKey(flags));
    }

    @Test
    public void testV1IndexedAndDedup() {
        // Pre-POSTING master payload: flags=0x3. Decodes as BITMAP + dedup.
        long flags = V1_BIT_INDEXED | V1_BIT_DEDUP_KEY;
        Assert.assertEquals(IndexType.BITMAP, AlterOperation.decodeIndexType(flags));
        Assert.assertTrue(AlterOperation.decodeIsDedupKey(flags));
    }

    @Test
    public void testV2MarkerSetByGetFlags() {
        // Every payload written by this branch must carry the v2 format bit
        // so a future reader can distinguish layouts and pick the right
        // decoder. The least bit pattern (NONE, no dedup) still has the
        // marker set.
        long flags = AlterOperation.getFlags(IndexType.NONE, false);
        Assert.assertEquals("v2 marker bit must be set on every getFlags() result",
                V2_FORMAT_MARKER, flags & V2_FORMAT_MARKER);
    }

    @Test
    public void testV2DedupBitDoesNotCollideWithV1() {
        // Bit 3 (the v2 dedup bit) was unused in v1, so a v1 payload could
        // never accidentally set it. Conversely, a v2 payload with dedup=true
        // must not also carry the v1 dedup bit (0x2) — otherwise a master
        // reader would see two conflicting signals.
        long v2Flags = AlterOperation.getFlags(IndexType.BITMAP, true);
        Assert.assertEquals("v2 dedup bit (0x8) expected", 0x8L, v2Flags & 0x8L);
        Assert.assertEquals("v1 dedup bit (0x2) must not be set in v2 payload",
                0L, v2Flags & V1_BIT_DEDUP_KEY);
    }
}
