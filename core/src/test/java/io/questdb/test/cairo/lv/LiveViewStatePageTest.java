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
import io.questdb.cairo.lv.LiveViewStatePageReader;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.MemoryTag;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

public class LiveViewStatePageTest {

    @Test
    public void testReaderCannotReachAdjacentBytes() {
        try (MemoryCARWImpl mem = new MemoryCARWImpl(128, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putLong(0x1122_3344_5566_7788L);
            mem.putLong(0x7f7e_7d7c_7b7a_7978L);
            final LiveViewStatePageReader reader = new LiveViewStatePageReader().of(mem, 0, Long.BYTES);
            Assert.assertEquals(0x1122_3344_5566_7788L, reader.getLong(0));
            assertCorrupt(() -> reader.getByte(Long.BYTES), "state page read out of bounds");
            assertCorrupt(() -> reader.getLong(1), "state page read out of bounds");
            assertCorrupt(() -> reader.getLong(-1), "state page read out of bounds");
        }
    }

    @Test
    public void testReaderRejectsMalformedPageReference() {
        try (MemoryCARWImpl mem = new MemoryCARWImpl(64, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putLong(42);
            final LiveViewStatePageReader reader = new LiveViewStatePageReader();
            assertCorrupt(() -> reader.of(mem, -1, 1), "state page reference out of bounds");
            assertCorrupt(() -> reader.of(mem, 0, -1), "state page reference out of bounds");
            assertCorrupt(() -> reader.of(mem, Long.MAX_VALUE, 1), "state page reference out of bounds");
            assertCorrupt(() -> reader.of(mem, 4, Long.MAX_VALUE), "state page reference out of bounds");
        }
    }

    @Test
    public void testReaderRejectsMalformedStringLength() {
        try (MemoryCARWImpl mem = new MemoryCARWImpl(128, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            // Both malformed-length shapes must classify as corruption so the restore
            // fallback can isolate the damage to one root version.
            mem.putInt(-2);
            mem.putInt(1_000);
            final LiveViewStatePageReader reader = new LiveViewStatePageReader().of(mem, 0, 2 * Integer.BYTES);
            assertCorrupt(() -> reader.getStrA(0), "state page string length invalid");
            assertCorrupt(() -> reader.getStrA(Integer.BYTES), "state page read out of bounds");
        }
    }

    @Test
    public void testUnopenedReaderIsNotClassifiedAsStorageCorruption() {
        // A reader that was never opened is a caller defect, not stored corruption.
        // Classifying it as LV_CHECKPOINT_TIMELINE_INVALID would let the restore
        // predecessor fallback swallow it as a bad root and mask the defect.
        final LiveViewStatePageReader reader = new LiveViewStatePageReader();
        assertCallerDefect(() -> reader.getLong(0), "state page reader is not open");
        assertCallerDefect(reader::size, "state page reader is not open");
    }

    @Test
    public void testWindowFunctionContractDoesNotExposeRawCheckpointMemory() {
        for (Method method : WindowFunction.class.getDeclaredMethods()) {
            Assert.assertNotEquals("snapshotPartitionState", method.getName());
            Assert.assertNotEquals("restorePartitionState", method.getName());
            if (method.getName().contains("CheckpointState")) {
                for (Class<?> parameterType : method.getParameterTypes()) {
                    Assert.assertNotEquals(MemoryA.class, parameterType);
                    Assert.assertNotEquals(MemoryR.class, parameterType);
                }
            }
        }
    }

    @Test
    public void testWriterEnforcesPageLimitWithoutOverwritingPrefix() {
        try (MemoryCARWImpl mem = new MemoryCARWImpl(64, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            mem.putInt(0x1234_5678);
            final LiveViewStatePageWriter writer = new LiveViewStatePageWriter().of(mem, Long.BYTES);
            writer.putLong(42);
            Assert.assertEquals(Long.BYTES, writer.size());
            Assert.assertEquals(Integer.BYTES, writer.getPageStart());
            assertCallerDefect(() -> writer.putByte((byte) 1), "state page exceeds size limit");
            Assert.assertEquals(0x1234_5678, mem.getInt(0));
            Assert.assertEquals(42, mem.getLong(Integer.BYTES));
        }
    }

    /**
     * Asserts a rejection that a caller's own defect caused, never stored corruption.
     * It must stay off {@link CairoException#LV_CHECKPOINT_TIMELINE_INVALID}, which
     * the restore fallback treats as "skip this root and try its predecessor".
     */
    private static void assertCallerDefect(Runnable action, CharSequence message) {
        try {
            action.run();
            Assert.fail("expected checkpoint state page rejection");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), message);
            Assert.assertEquals(
                    "a caller defect must not be classified as recoverable checkpoint corruption",
                    0,
                    e.getErrno()
            );
        }
    }

    /**
     * Asserts a rejection that a malformed checkpoint payload caused. It must carry
     * {@link CairoException#LV_CHECKPOINT_TIMELINE_INVALID}, or
     * {@code LiveViewCheckpointTimelineStoreReader.restoreLatestCompatible} rethrows
     * instead of skipping the root and retrying its predecessor.
     */
    private static void assertCorrupt(Runnable action, CharSequence message) {
        try {
            action.run();
            Assert.fail("expected checkpoint state page rejection");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), message);
            Assert.assertEquals(
                    "a corrupt state page must classify as LV_CHECKPOINT_TIMELINE_INVALID"
                            + " so the restore predecessor fallback engages",
                    CairoException.LV_CHECKPOINT_TIMELINE_INVALID,
                    e.getErrno()
            );
        }
    }
}
