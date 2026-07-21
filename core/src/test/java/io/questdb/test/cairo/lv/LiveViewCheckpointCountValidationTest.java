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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.lv.LiveViewFunctionSnapshot;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.lv.LiveViewStatePageReader;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A checkpoint payload that passed the ring/head CRC can still carry a structurally corrupt
 * partition count (a deliberately re-CRCed file, or a buggy writer). A negative count would
 * clear state then zero-iterate, and a header-only payload crafted to match the length check
 * would restore empty state silently. The restore paths must reject the count before touching
 * live state.
 */
public class LiveViewCheckpointCountValidationTest extends AbstractCairoTest {

    // The anchor map is empty in these tests, so the sink is never invoked.
    private static final RecordSink NOOP_SINK = new RecordSink() {
        @Override
        public void copy(Record r, RecordSinkSPI w) {
        }

        @Override
        public void setFunctions(ObjList<Function> keyFunctions) {
        }
    };

    @Test
    public void testAnchorSnapshotRejectsNegativeCount() throws Exception {
        assertMemoryLeak(() -> {
            final Map anchorMap = MapFactory.createUnorderedMap(
                    configuration, new SingleColumnType(ColumnType.LONG), LiveViewWindow.anchorMapValueTypes());
            // LiveViewWindow.close() frees the anchor map; the anchor Function and the
            // (empty) window-functions list are owned upstream, so they are not freed here.
            try (
                    LiveViewWindow window = new LiveViewWindow(
                            configuration, "w", LongConstant.NULL, ColumnType.LONG,
                            new SingleColumnType(ColumnType.LONG), anchorMap, NOOP_SINK, new ObjList<>(), false, null, null);
                    MemoryCARWImpl buf = new MemoryCARWImpl(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
            ) {
                // A valid empty-map snapshot: prelude + partitionCount(0), no entries.
                window.snapshot(buf);
                final long len = buf.getAppendOffset();
                window.restore(buf); // sanity: a well-formed count restores cleanly

                // The empty map wrote no entries, so the trailing long is the partition count.
                buf.jumpTo(len - Long.BYTES);
                buf.putLong(-1L);
                try {
                    window.restore(buf);
                    Assert.fail("negative anchor partition count must be rejected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "negative partition count");
                }
            }
        });
    }

    @Test
    public void testFunctionSnapshotRejectsNegativeAndNonUnitScalarCount() throws Exception {
        assertMemoryLeak(() -> {
            try (MemoryCARWImpl buf = new MemoryCARWImpl(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                final ScalarStub f = new ScalarStub();
                // write() emits: keyColumnCount(int)=0, partitionCount(long)=1, then one state long.
                LiveViewFunctionSnapshot.write(buf, f);
                final long len = buf.getAppendOffset();
                LiveViewFunctionSnapshot.restore(buf, 0, len, f, 0); // sanity: count=1 restores cleanly

                // The partition count is the long immediately after the 4-byte key-column count.
                buf.jumpTo(Integer.BYTES);
                buf.putLong(-1L);
                try {
                    LiveViewFunctionSnapshot.restore(buf, 0, len, f, 0);
                    Assert.fail("negative function-snapshot partition count must be rejected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "negative partition count");
                }

                // A scalar snapshot must carry exactly one partition; any other (non-negative)
                // value would be ignored by the count-agnostic scalar restore.
                buf.jumpTo(Integer.BYTES);
                buf.putLong(2L);
                try {
                    LiveViewFunctionSnapshot.restore(buf, 0, len, f, 0);
                    Assert.fail("scalar function-snapshot count != 1 must be rejected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "scalar partition count must be 1");
                }
                f.close();
            }
        });
    }

    @Test
    public void testAnchorSnapshotRejectsCountExceedingPayload() throws Exception {
        // m9: an oversized (but positive, CRC-valid) partition count must be rejected up front,
        // before the read loop drives an out-of-bounds / long-running read that only the final
        // length check would catch. Uses the 3-arg restore so a real payloadLength is known -
        // the 1-arg overload passes Long.MAX_VALUE and deliberately skips the bound. The empty
        // snapshot leaves no room for entries, so any positive count exceeds; 5 stays inside the
        // buffer so the pre-fix path reaches the length-mismatch check rather than reading OOB.
        assertMemoryLeak(() -> {
            final Map anchorMap = MapFactory.createUnorderedMap(
                    configuration, new SingleColumnType(ColumnType.LONG), LiveViewWindow.anchorMapValueTypes());
            try (
                    LiveViewWindow window = new LiveViewWindow(
                            configuration, "w", LongConstant.NULL, ColumnType.LONG,
                            new SingleColumnType(ColumnType.LONG), anchorMap, NOOP_SINK, new ObjList<>(), false, null, null);
                    MemoryCARWImpl buf = new MemoryCARWImpl(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
            ) {
                window.snapshot(buf);
                final long len = buf.getAppendOffset();
                window.restore(buf, 0, len); // sanity: a well-formed count restores cleanly

                // The empty map wrote no entries, so the trailing long is the partition count.
                buf.jumpTo(len - Long.BYTES);
                buf.putLong(5L);
                try {
                    window.restore(buf, 0, len);
                    Assert.fail("oversized anchor partition count must be rejected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "partition count exceeds payload");
                }
            }
        });
    }

    @Test
    public void testFunctionSnapshotRejectsCountExceedingPayload() throws Exception {
        // m9: an oversized (but positive, CRC-valid) partition count must be rejected up front,
        // before onCheckpointRestoreBegin mutates state - the guard runs ahead of the scalar
        // count != 1 check, so a scalar stub still exercises it (pre-fix, the scalar check
        // rejects with a different message; the guard is what makes the map path safe too).
        assertMemoryLeak(() -> {
            try (MemoryCARWImpl buf = new MemoryCARWImpl(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                final ScalarStub f = new ScalarStub();
                LiveViewFunctionSnapshot.write(buf, f);
                final long len = buf.getAppendOffset();
                LiveViewFunctionSnapshot.restore(buf, 0, len, f, 0); // sanity: count=1 restores cleanly

                // The partition count is the long immediately after the 4-byte key-column count.
                buf.jumpTo(Integer.BYTES);
                buf.putLong(Long.MAX_VALUE);
                try {
                    LiveViewFunctionSnapshot.restore(buf, 0, len, f, 0);
                    Assert.fail("oversized function-snapshot partition count must be rejected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "partition count exceeds payload");
                }
                f.close();
            }
        });
    }

    @Test
    public void testFunctionStatePageRejectsCorruptLengthAndDecoderOverread() throws Exception {
        assertMemoryLeak(() -> {
            try (MemoryCARWImpl buf = new MemoryCARWImpl(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                final ScalarStub f = new ScalarStub();
                LiveViewFunctionSnapshot.write(buf, f);
                final long payloadLength = buf.getAppendOffset();

                // key column count (4) + partition count (8), then the exact state-page length.
                final long pageLengthOffset = Integer.BYTES + Long.BYTES;
                buf.jumpTo(pageLengthOffset);
                buf.putLong(Long.BYTES + 1L);
                try {
                    LiveViewFunctionSnapshot.restore(buf, 0, payloadLength, f, 0);
                    Assert.fail("state page extending past the function payload must be rejected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "state page exceeds payload");
                }
                Assert.assertEquals(0, f.restoreBeginCount);

                buf.jumpTo(pageLengthOffset);
                buf.putLong(Long.BYTES);
                f.overread = true;
                // Place readable bytes immediately after the declared payload. The page cursor
                // must still reject the buggy decoder instead of consuming this adjacent value.
                buf.jumpTo(payloadLength);
                buf.putLong(0x1122_3344_5566_7788L);
                try {
                    LiveViewFunctionSnapshot.restore(buf, 0, payloadLength, f, 0);
                    Assert.fail("function decoder must not read past its state page");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "state page read out of bounds");
                }
                f.close();
            }
        });
    }

    // A scalar (no-map) window function that round-trips a single state long, enough to drive
    // LiveViewFunctionSnapshot.write / restore without a real partition map.
    private static class ScalarStub extends BaseWindowFunction {
        private boolean overread;
        private int restoreBeginCount;

        private ScalarStub() {
            super(null);
        }

        @Override
        public String getName() {
            return "scalar";
        }

        @Override
        public void onCheckpointRestoreBegin() {
            restoreBeginCount++;
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }

        @Override
        public long restoreCheckpointState(LiveViewStatePageReader source, long offset, MapValue value, int formatVersion) {
            source.getLong(offset);
            if (overread) {
                source.getByte(offset + Long.BYTES);
            }
            return offset + Long.BYTES;
        }

        @Override
        public void freezeCheckpointState(LiveViewStatePageWriter sink, MapValue value) {
            sink.putLong(42L);
        }
    }
}
