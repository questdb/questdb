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

import io.questdb.griffin.engine.table.AsyncFilterContext;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class AsyncFilterContextTest extends AbstractCairoTest {

    @Test
    public void testClearShrinksGrownRowIdLists() throws Exception {
        // A parallel GROUP BY/TOP K/join keeps its AsyncFilterContext alive while the
        // factory is cached or idle. clear() must hand back the row-id lists, which a
        // JIT filter grows to a full page frame (up to cairo.sql.page.frame.max.rows
        // longs = 8 MB each), otherwise an idle factory pins peak-sized NATIVE_OFFLOAD
        // buffers until eviction.
        assertMemoryLeak(() -> {
            final int slotCount = 4;
            final long initialCapacity = configuration.getPageFrameReduceRowIdListCapacity();
            final long grownCapacity = 1_000_000; // a full default page frame: 8 MB per list

            AsyncFilterContext ctx = new AsyncFilterContext(
                    configuration,
                    null, // no compiled filter -> only the row-id lists are allocated
                    null,
                    null,
                    null,
                    null,
                    null,
                    slotCount,
                    0,
                    Long.MAX_VALUE,
                    Long.MAX_VALUE
            );
            try {
                final long memAtInitial = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);

                // Grow the owner plus every per-worker row-id list to a full frame.
                ctx.getFilteredRows(-1).setCapacity(grownCapacity);
                for (int i = 0; i < slotCount; i++) {
                    ctx.getFilteredRows(i).setCapacity(grownCapacity);
                }

                final long memGrown = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);
                final long expectedGrowthBytes = (1 + slotCount) * (grownCapacity - initialCapacity) * Long.BYTES;
                Assert.assertTrue(
                        "row-id lists should have grown by ~" + expectedGrowthBytes + " bytes, grew by " + (memGrown - memAtInitial),
                        memGrown - memAtInitial >= expectedGrowthBytes
                );

                // The fix: clear() shrinks the lists back to their initial capacity.
                ctx.clear();

                Assert.assertEquals(initialCapacity, ctx.getFilteredRows(-1).getCapacity());
                for (int i = 0; i < slotCount; i++) {
                    Assert.assertEquals(initialCapacity, ctx.getFilteredRows(i).getCapacity());
                }

                final long memCleared = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_OFFLOAD);
                Assert.assertTrue(
                        "clear() should release the grown row-id memory, still holding " + (memCleared - memAtInitial) + " extra bytes",
                        memCleared <= memAtInitial
                );
            } finally {
                Misc.free(ctx);
            }
        });
    }
}
