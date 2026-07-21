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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointScratchOverlay;
import io.questdb.cairo.lv.LiveViewStatePageReader;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The scratch overlay a repair with a finite convergence boundary takes over
 * the published window state before replaying on top of it.
 * <p>
 * Its contract is narrow but load-bearing: whatever the replay does to the live
 * function instances and the anchor map, the state the repair entered with has to
 * come back. A silent failure here does not corrupt the durable output the repair
 * just wrote - it corrupts every row the view emits afterwards, because the in-order
 * path keeps accumulating on whatever the repair left behind.
 */
public class LiveViewCheckpointScratchOverlayTest extends AbstractCairoTest {
    // Projects the anchor window's single LONG partition key straight off the record.
    private static final RecordSink KEY_SINK = new RecordSink() {
        @Override
        public void copy(Record r, RecordSinkSPI w) {
            w.putLong(r.getLong(0));
        }

        @Override
        public void setFunctions(ObjList<Function> keyFunctions) {
        }
    };
    // The view is unanchored, so the overlay carries function state alone.
    private static final LiveViewWindow NO_ANCHOR = null;

    @Test
    public void testAnchorMapSurvivesAReplayThatWipedIt() throws Exception {
        // A localized repair over an anchored view clears the anchor map before
        // replaying, so the first row of each partition in the replayed segment resets
        // the functions on it. The frontier the runtime entered with therefore exists
        // nowhere else, and only the overlay puts it back. The follow-up row is what
        // proves the anchor VALUES came back and not merely the keys: with the same
        // anchor it must not reset, which is exactly what a garbled or lost value would
        // do.
        assertMemoryLeak(() -> {
            final ResetCountingStub function = new ResetCountingStub();
            final ObjList<WindowFunction> functions = functions(function);
            final KeyRecord record = new KeyRecord();
            try (
                    LiveViewWindow window = anchorWindow(functions);
                    LiveViewCheckpointScratchOverlay overlay = new LiveViewCheckpointScratchOverlay()
            ) {
                record.key = 1;
                window.processRow(record);
                record.key = 2;
                window.processRow(record);
                Assert.assertEquals(2, window.getAnchorMapSize());
                Assert.assertEquals(2, function.resets);

                overlay.capture(functions, window);
                window.toTop();
                Assert.assertEquals(0, window.getAnchorMapSize());

                overlay.restore(functions, window);
                Assert.assertEquals(2, window.getAnchorMapSize());

                record.key = 1;
                window.processRow(record);
                Assert.assertEquals("a restored anchor value must not re-fire the reset", 2, function.resets);
            }
        });
    }

    @Test
    public void testCaptureDiscardsAnEarlierUnrestoredCapture() throws Exception {
        // A repair that failed between capture and restore leaves state behind. The
        // next one must not put it back over a runtime it never described.
        assertMemoryLeak(() -> {
            final StateStub f = new StateStub(7L);
            final ObjList<WindowFunction> functions = functions(f);
            try (LiveViewCheckpointScratchOverlay overlay = new LiveViewCheckpointScratchOverlay()) {
                overlay.capture(functions, NO_ANCHOR);
                f.state = 8L;
                overlay.capture(functions, NO_ANCHOR);
                f.state = 9L;
                overlay.restore(functions, NO_ANCHOR);
                Assert.assertEquals(8L, f.state);
            }
        });
    }

    @Test
    public void testRestoreRejectsAFunctionListThatGrewACapableFunction() throws Exception {
        // Alignment between the two passes is positional. A list that gained a
        // capable function would shift every frame by one and restore one function's
        // bytes into another, so the reconciliation has to reject it outright.
        assertMemoryLeak(() -> {
            final StateStub captured = new StateStub(1L);
            final StateStub added = new StateStub(2L);
            try (LiveViewCheckpointScratchOverlay overlay = new LiveViewCheckpointScratchOverlay()) {
                overlay.capture(functions(captured), NO_ANCHOR);
                try {
                    overlay.restore(functions(captured, added), NO_ANCHOR);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "overlay is short of captured functions");
                }
            }
        });
    }

    @Test
    public void testRestoreRejectsAFunctionListThatLostACapableFunction() throws Exception {
        assertMemoryLeak(() -> {
            final StateStub first = new StateStub(1L);
            final StateStub second = new StateStub(2L);
            try (LiveViewCheckpointScratchOverlay overlay = new LiveViewCheckpointScratchOverlay()) {
                overlay.capture(functions(first, second), NO_ANCHOR);
                try {
                    overlay.restore(functions(first), NO_ANCHOR);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "overlay holds unclaimed function state");
                }
            }
        });
    }

    @Test
    public void testRestoreRejectsAnAnchorArmThatDoesNotMatchTheRuntime() throws Exception {
        // The anchor payload is optional, so a capture and a restore that disagree about
        // whether it is there would silently leave the anchor map holding whatever the
        // replay rebuilt. Reconcile it the same way the function frames are reconciled.
        assertMemoryLeak(() -> {
            final ObjList<WindowFunction> functions = functions(new ResetCountingStub());
            try (
                    LiveViewWindow window = anchorWindow(functions);
                    LiveViewCheckpointScratchOverlay overlay = new LiveViewCheckpointScratchOverlay()
            ) {
                overlay.capture(functions, NO_ANCHOR);
                try {
                    overlay.restore(functions, window);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "anchor state does not match the runtime");
                }

                overlay.capture(functions, window);
                try {
                    overlay.restore(functions, NO_ANCHOR);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "anchor state does not match the runtime");
                }
            }
        });
    }

    @Test
    public void testRestoreRejectsAnEmptyOverlay() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<WindowFunction> functions = functions(new StateStub(1L));
            try (LiveViewCheckpointScratchOverlay overlay = new LiveViewCheckpointScratchOverlay()) {
                try {
                    overlay.restore(functions, NO_ANCHOR);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "overlay holds no captured state");
                }
            }
        });
    }

    @Test
    public void testRoundTripPutsEveryCapableFunctionBack() throws Exception {
        // The whole point: the replay mutates the live instances, and the overlay
        // undoes exactly that. The uncapable function in the middle proves both passes
        // apply the same filter - a skip on one side only would misalign the frames
        // and restore the wrong bytes into the wrong function.
        assertMemoryLeak(() -> {
            final StateStub first = new StateStub(11L);
            final IncapableStub incapable = new IncapableStub();
            final StateStub second = new StateStub(22L);
            final ObjList<WindowFunction> functions = functions(first, incapable, second);
            try (LiveViewCheckpointScratchOverlay overlay = new LiveViewCheckpointScratchOverlay()) {
                Assert.assertFalse(overlay.isCaptured());
                overlay.capture(functions, NO_ANCHOR);
                Assert.assertTrue(overlay.isCaptured());

                first.state = -1L;
                second.state = -2L;

                overlay.restore(functions, NO_ANCHOR);
                Assert.assertEquals(11L, first.state);
                Assert.assertEquals(22L, second.state);
                // Each restored function was told its state was about to be replaced.
                Assert.assertEquals(1, first.restoreBeginCount);
                Assert.assertEquals(1, second.restoreBeginCount);
                Assert.assertEquals(0, incapable.restoreBeginCount);
                // The buffer is released with the state it held: it is as large as the
                // whole window state, and the next repair allocates its own.
                Assert.assertFalse(overlay.isCaptured());
            }
        });
    }

    /**
     * An anchored window over a single LONG partition key, with a constant anchor
     * expression so every row of the same key sits in the same segment. Enough to drive
     * the overlay's anchor arm without a compiled live view: the map is keyed and
     * populated by {@code processRow}, which is the only thing the overlay reads.
     */
    private LiveViewWindow anchorWindow(ObjList<WindowFunction> functions) {
        final SingleColumnType keyTypes = new SingleColumnType(ColumnType.LONG);
        return new LiveViewWindow(
                configuration,
                "w",
                new LongConstant(1_000L),
                ColumnType.LONG,
                keyTypes,
                MapFactory.createUnorderedMap(configuration, keyTypes, LiveViewWindow.anchorMapValueTypes()),
                KEY_SINK,
                functions,
                false,
                null,
                null
        );
    }

    private static ObjList<WindowFunction> functions(WindowFunction... fns) {
        final ObjList<WindowFunction> list = new ObjList<>();
        for (WindowFunction f : fns) {
            list.add(f);
        }
        return list;
    }

    /** A function outside the checkpoint pipeline: neither pass may carry it. */
    private static class IncapableStub extends BaseWindowFunction {
        private int restoreBeginCount;

        private IncapableStub() {
            super(null);
        }

        @Override
        public String getName() {
            return "incapable";
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public void onCheckpointRestoreBegin() {
            restoreBeginCount++;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }
    }

    /** A record whose only column is the anchor window's LONG partition key. */
    private static class KeyRecord implements Record {
        private long key;

        @Override
        public long getLong(int col) {
            return key;
        }
    }

    /**
     * A function with no checkpoint state that counts the anchor's reset dispatches,
     * which is how the tests observe whether a restored anchor value was believed.
     */
    private static class ResetCountingStub extends BaseWindowFunction {
        private int resets;

        private ResetCountingStub() {
            super(null);
        }

        @Override
        public String getName() {
            return "reset-counting";
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }

        @Override
        public void resetPartition(Record record) {
            resets++;
        }
    }

    /**
     * A scalar (no-map) window function whose whole state is one long, enough to
     * drive the overlay's freeze/restore round trip without a real partition map.
     */
    private static class StateStub extends BaseWindowFunction {
        private int restoreBeginCount;
        private long state;

        private StateStub(long state) {
            super(null);
            this.state = state;
        }

        @Override
        public void freezeCheckpointState(LiveViewStatePageWriter sink, MapValue value) {
            sink.putLong(state);
        }

        @Override
        public String getName() {
            return "state";
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public void onCheckpointRestoreBegin() {
            restoreBeginCount++;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }

        @Override
        public long restoreCheckpointState(LiveViewStatePageReader source, long offset, MapValue value, int formatVersion) {
            state = source.getLong(offset);
            return offset + Long.BYTES;
        }

        @Override
        public boolean supportsCheckpointState() {
            return true;
        }
    }
}
