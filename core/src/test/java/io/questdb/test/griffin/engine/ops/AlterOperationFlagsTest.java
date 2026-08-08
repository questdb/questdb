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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.MemoryTag;
import io.questdb.test.tools.TestUtils;
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
    public void testSetTableFormatRoundTrip() throws Exception {
        // Opcode 27 (SET_TABLE_FORMAT) carries a single long in extraInfo: the
        // target table format (TABLE_FORMAT_NATIVE / TABLE_FORMAT_PARQUET).
        // The WAL sequencer relies on serializeBody/deserializeBody (via
        // BinaryAlterSerializer) round-tripping the command opcode and the
        // tableFormat payload byte-for-byte, otherwise apply() would silently
        // hand setMetaTableFormat() a wrong value on the apply side. This test
        // exercises the same path that the WAL sequencer uses, plus a second
        // serialize from the deserialized op to catch any asymmetry between
        // the write and read sides.
        TestUtils.assertMemoryLeak(() -> {
            final TableToken tableToken = new TableToken(
                    "fmt_table", "fmt_table~1", null, 17, false, false, false
            );
            final int tableNamePosition = 42;
            final int tableFormat = TableUtils.TABLE_FORMAT_PARQUET;

            AlterOperationBuilder builder = new AlterOperationBuilder();
            AlterOperation src = builder
                    .ofSetTableFormat(tableNamePosition, tableToken, tableToken.getTableId(), tableFormat)
                    .build();
            try (
                    MemoryCARW sink1 = new MemoryCARWImpl(256, 1, MemoryTag.NATIVE_DEFAULT);
                    MemoryCARW sink2 = new MemoryCARWImpl(256, 1, MemoryTag.NATIVE_DEFAULT)
            ) {
                src.serializeBody(sink1);
                long size1 = sink1.getAppendOffset();

                // Serialized layout (see AlterOperation.serializeBody):
                //   short command           @ offset 0
                //   int   tableNamePosition @ offset 2
                //   int   longSize          @ offset 6
                //   long  extraInfo[0..n)   @ offset 10
                //   int   strSize           after the longs
                //   ... strings ...
                Assert.assertEquals(AlterOperation.SET_TABLE_FORMAT, sink1.getShort(0));
                Assert.assertEquals(tableNamePosition, sink1.getInt(2));
                Assert.assertEquals("SET_TABLE_FORMAT must serialize exactly one long",
                        1, sink1.getInt(6));
                Assert.assertEquals("tableFormat long must survive serialize",
                        tableFormat, sink1.getLong(10));
                Assert.assertEquals("SET_TABLE_FORMAT must serialize zero strings",
                        0, sink1.getInt(18));

                AlterOperation dst = new AlterOperation();
                dst.deserializeBody(sink1, 0L, size1);

                Assert.assertEquals("opcode must round-trip",
                        AlterOperation.SET_TABLE_FORMAT, dst.getCommand());
                Assert.assertEquals("tableNamePosition must round-trip",
                        tableNamePosition, dst.getTableNamePosition());

                // Re-serialize the deserialized op and assert byte-for-byte
                // equality with the original. This catches any asymmetry
                // between serializeBody and deserializeBody that would
                // otherwise corrupt downstream WAL replay.
                dst.serializeBody(sink2);
                long size2 = sink2.getAppendOffset();
                Assert.assertEquals("re-serialized payload must match original size",
                        size1, size2);
                for (long i = 0; i < size1; i++) {
                    Assert.assertEquals("re-serialized payload must match original byte at offset " + i,
                            sink1.getByte(i), sink2.getByte(i));
                }
            }
        });
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
