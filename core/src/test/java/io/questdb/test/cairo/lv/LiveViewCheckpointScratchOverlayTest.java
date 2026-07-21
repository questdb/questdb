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
import io.questdb.cairo.lv.LiveViewCheckpointScratchOverlay;
import io.questdb.cairo.lv.LiveViewStatePageReader;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The scratch overlay a repair with a finite convergence boundary takes over the
 * published window state before replaying on top of it (design section 12.4).
 * <p>
 * Its contract is narrow but load-bearing: whatever the replay does to the live
 * function instances, the state the repair entered with has to come back. A silent
 * failure here does not corrupt the durable output the repair just wrote - it
 * corrupts every row the view emits afterwards, because the in-order path keeps
 * accumulating on whatever the repair left behind.
 */
public class LiveViewCheckpointScratchOverlayTest extends AbstractCairoTest {

    @Test
    public void testCaptureDiscardsAnEarlierUnrestoredCapture() throws Exception {
        // A repair that failed between capture and restore leaves state behind. The
        // next one must not put it back over a runtime it never described.
        assertMemoryLeak(() -> {
            final StateStub f = new StateStub(7L);
            final ObjList<WindowFunction> functions = functions(f);
            try (LiveViewCheckpointScratchOverlay overlay = new LiveViewCheckpointScratchOverlay()) {
                overlay.capture(functions);
                f.state = 8L;
                overlay.capture(functions);
                f.state = 9L;
                overlay.restore(functions);
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
                overlay.capture(functions(captured));
                try {
                    overlay.restore(functions(captured, added));
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
                overlay.capture(functions(first, second));
                try {
                    overlay.restore(functions(first));
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "overlay holds unclaimed function state");
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
                    overlay.restore(functions);
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
                overlay.capture(functions);
                Assert.assertTrue(overlay.isCaptured());

                first.state = -1L;
                second.state = -2L;

                overlay.restore(functions);
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
