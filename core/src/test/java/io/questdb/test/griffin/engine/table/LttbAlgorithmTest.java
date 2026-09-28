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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.table.LttbAlgorithm;
import io.questdb.griffin.engine.table.SubsampleAlgorithm;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LttbAlgorithmTest extends AbstractCairoTest {

    @Test
    public void testGapTargetBufferFailsUnderQueryMemoryLimit() throws Exception {
        assertMemoryLeak(() -> {
            final long segmentBufferBytes = 64L * Long.BYTES;
            final long bufferBytes = 2L * SubsampleAlgorithm.ENTRY_SIZE;
            // Keep input and output allocations off the tracker so neither can mask missing
            // target-buffer accounting with a later query-memory breach.
            final long buffer = Unsafe.malloc(bufferBytes, MemoryTag.NATIVE_DEFAULT);
            try (
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(segmentBufferBytes);
                    DirectLongList selected = new DirectLongList(2, MemoryTag.NATIVE_DEFAULT)
            ) {
                final LttbAlgorithm algorithm = new LttbAlgorithm(1);
                try {
                    // Two one-point segments fit the initial segment list without growing it.
                    Unsafe.putLong(buffer, 0);
                    Unsafe.putDouble(buffer + Long.BYTES, 1);
                    Unsafe.putLong(buffer + SubsampleAlgorithm.ENTRY_SIZE, 2);
                    Unsafe.putDouble(buffer + SubsampleAlgorithm.ENTRY_SIZE + Long.BYTES, 2);
                    algorithm.setMemoryTracker(tracker);

                    // The 512-byte segment allocation fits exactly; only the target allocation
                    // can breach. Without its tracker binding, selection succeeds instead.
                    final CairoException e = Assert.assertThrows(CairoException.class, () -> algorithm.select(
                            buffer, 2, 2, false, selected, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER
                    ));
                    Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                    Assert.assertEquals("only the segment buffer should be charged", segmentBufferBytes, tracker.getUsed());
                    Assert.assertEquals("target allocation must fail before selection", 0, selected.size());
                } finally {
                    algorithm.close();
                }
                Assert.assertEquals("close must release the segment charge", 0, tracker.getUsed());
            } finally {
                Unsafe.free(buffer, bufferBytes, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }
}
