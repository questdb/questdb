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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CompositeWalLagBuffer;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link CompositeWalLagBuffer} -- composite-partitioning Plan #5 (cell-aware WAL-LAG
 * batching), Task 2. This is a pure storage-component test: no {@code TableWriter}, no SQL, no engine
 * -- just the buffer's own append/growth/read-back/clear/close/reject-var-len contract, which is the
 * entire surface Task 3 will later drive.
 */
public class CompositeWalLagBufferTest extends AbstractTest {

    // ts TIMESTAMP, exch SYMBOL (int-encoded, 4 bytes), px DOUBLE, n LONG -- the brief's example set.
    private static final int COL_EXCH = 1;
    private static final int COL_N = 3;
    private static final int COL_PX = 2;
    private static final int COL_TS = 0;

    @Test
    public void testAppendAccumulatesAndReadsBackValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            IntList types = fourColumnTypes();
            CompositeWalLagBuffer buf = new CompositeWalLagBuffer(types);
            ObjList<MemoryCARW> sources = new ObjList<>();
            try {
                long[] ts = {1000, 2000, 3000, 4000, 5000};
                int[] exch = {0, 1, 2, 1, 0};
                double[] px = {1.5, 2.5, 3.5, 4.5, 5.5};
                long[] n = {10, 20, 30, 40, 50};
                ObjList<MemoryCR> src = fourColumnSource(sources, ts, exch, px, n);

                buf.append(src, 0, 5);

                Assert.assertEquals(5, buf.getRowCount());
                assertLongColumn(buf, COL_TS, ts);
                assertIntColumn(buf, COL_EXCH, exch);
                assertDoubleColumn(buf, COL_PX, px);
                assertLongColumn(buf, COL_N, n);
            } finally {
                buf.close();
                Misc.freeObjListAndKeepObjects(sources);
            }
        });
    }

    @Test
    public void testAppendPartialRangeCopiesOnlyRequestedRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            IntList types = new IntList();
            types.add(ColumnType.LONG);
            CompositeWalLagBuffer buf = new CompositeWalLagBuffer(types);
            ObjList<MemoryCARW> sources = new ObjList<>();
            try {
                MemoryCARW srcMem = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
                sources.add(srcMem);
                for (long v = 0; v < 10; v++) {
                    srcMem.putLong(v);
                }
                ObjList<MemoryCR> src = new ObjList<>();
                src.add(srcMem);

                // only rows [3, 7) should land in the buffer
                buf.append(src, 3, 7);

                Assert.assertEquals(4, buf.getRowCount());
                long addr = buf.getColumnAddress(0);
                for (int i = 0; i < 4; i++) {
                    Assert.assertEquals(3 + i, Unsafe.getLong(addr + (long) i * 8));
                }
            } finally {
                buf.close();
                Misc.freeObjListAndKeepObjects(sources);
            }
        });
    }

    @Test
    public void testMultipleAppendsFromDifferentSourcesPreserveOrder() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            IntList types = fourColumnTypes();
            CompositeWalLagBuffer buf = new CompositeWalLagBuffer(types);
            ObjList<MemoryCARW> sources = new ObjList<>();
            try {
                long[] tsA = {1000, 2000, 3000, 4000, 5000};
                int[] exchA = {0, 1, 2, 1, 0};
                double[] pxA = {1.5, 2.5, 3.5, 4.5, 5.5};
                long[] nA = {10, 20, 30, 40, 50};
                ObjList<MemoryCR> srcA = fourColumnSource(sources, tsA, exchA, pxA, nA);

                long[] tsB = {6000, 7000, 8000};
                int[] exchB = {2, 0, 1};
                double[] pxB = {6.5, 7.5, 8.5};
                long[] nB = {60, 70, 80};
                ObjList<MemoryCR> srcB = fourColumnSource(sources, tsB, exchB, pxB, nB);

                buf.append(srcA, 0, 5);
                Assert.assertEquals(5, buf.getRowCount());
                buf.append(srcB, 0, 3);
                Assert.assertEquals(8, buf.getRowCount());

                assertLongColumn(buf, COL_TS, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000);
                assertIntColumn(buf, COL_EXCH, 0, 1, 2, 1, 0, 2, 0, 1);
                assertDoubleColumn(buf, COL_PX, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5);
                assertLongColumn(buf, COL_N, 10, 20, 30, 40, 50, 60, 70, 80);
            } finally {
                buf.close();
                Misc.freeObjListAndKeepObjects(sources);
            }
        });
    }

    @Test
    public void testGrowthAcrossCapacityBoundaryPreservesEarlierRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            IntList types = new IntList();
            types.add(ColumnType.LONG);
            // 16 bytes => 2 rows/page; 500 single-row appends forces many reallocations.
            CompositeWalLagBuffer buf = new CompositeWalLagBuffer(types, 16);
            ObjList<MemoryCARW> sources = new ObjList<>();
            try {
                final int total = 500;
                MemoryCARW srcMem = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
                sources.add(srcMem);
                for (long v = 0; v < total; v++) {
                    srcMem.putLong(v);
                }
                ObjList<MemoryCR> src = new ObjList<>();
                src.add(srcMem);

                for (long i = 0; i < total; i++) {
                    buf.append(src, i, i + 1);
                    // re-fetch address on every check -- must never be cached across an append, since
                    // the region's base address can move on growth.
                    Assert.assertEquals(i + 1, buf.getRowCount());
                }

                long addr = buf.getColumnAddress(0);
                for (long i = 0; i < total; i++) {
                    Assert.assertEquals("row " + i, i, Unsafe.getLong(addr + i * 8));
                }
            } finally {
                buf.close();
                Misc.freeObjListAndKeepObjects(sources);
            }
        });
    }

    @Test
    public void testClearResetsRowCountAndAllowsReuse() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            IntList types = new IntList();
            types.add(ColumnType.LONG);
            CompositeWalLagBuffer buf = new CompositeWalLagBuffer(types);
            ObjList<MemoryCARW> sources = new ObjList<>();
            try {
                MemoryCARW srcMem = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
                sources.add(srcMem);
                for (long v = 0; v < 20; v++) {
                    srcMem.putLong(v * 10);
                }
                ObjList<MemoryCR> src = new ObjList<>();
                src.add(srcMem);

                buf.append(src, 0, 10);
                Assert.assertEquals(10, buf.getRowCount());

                buf.clear();
                Assert.assertEquals(0, buf.getRowCount());

                // reuse: append fresh rows post-clear and confirm only the new content is visible
                buf.append(src, 10, 20);
                Assert.assertEquals(10, buf.getRowCount());
                long addr = buf.getColumnAddress(0);
                for (int i = 0; i < 10; i++) {
                    Assert.assertEquals((10 + i) * 10L, Unsafe.getLong(addr + (long) i * 8));
                }
            } finally {
                buf.close();
                Misc.freeObjListAndKeepObjects(sources);
            }
        });
    }

    @Test
    public void testCloseFreesNativeMemoryWithNoLeak() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long used = Unsafe.getMemUsed();

            IntList types = fourColumnTypes();
            CompositeWalLagBuffer buf = new CompositeWalLagBuffer(types);
            ObjList<MemoryCARW> sources = new ObjList<>();
            MemoryCARW srcMem = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
            sources.add(srcMem);
            ObjList<MemoryCR> src = new ObjList<>();
            src.add(srcMem);
            src.add(srcMem);
            src.add(srcMem);
            src.add(srcMem);
            for (int i = 0; i < 5; i++) {
                srcMem.putLong(i);
            }
            buf.append(src, 0, 5);
            Assert.assertEquals(5, buf.getRowCount());
            Assert.assertTrue(Unsafe.getMemUsed() > used);

            buf.close();
            // idempotent -- a second close must not double-free or throw
            buf.close();
            Misc.freeObjListAndKeepObjects(sources);

            Assert.assertEquals(used, Unsafe.getMemUsed());
        });
    }

    @Test
    public void testVarSizeColumnRejectedInConstructor() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int[] varSizeTypes = {ColumnType.VARCHAR, ColumnType.STRING, ColumnType.BINARY, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1)};
            for (int varType : varSizeTypes) {
                IntList types = new IntList();
                types.add(ColumnType.TIMESTAMP);
                types.add(varType);
                try {
                    new CompositeWalLagBuffer(types);
                    Assert.fail("expected UnsupportedOperationException for " + ColumnType.nameOf(varType));
                } catch (UnsupportedOperationException expected) {
                    TestUtils.assertContains(expected.getMessage(), ColumnType.nameOf(varType));
                }
            }
        });
    }

    private static void assertDoubleColumn(CompositeWalLagBuffer buf, int col, double... expected) {
        long addr = buf.getColumnAddress(col);
        Assert.assertEquals((long) expected.length << 3, buf.getColumnSize(col));
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(expected[i], Unsafe.getDouble(addr + ((long) i << 3)), 0.0);
        }
    }

    private static void assertIntColumn(CompositeWalLagBuffer buf, int col, int... expected) {
        long addr = buf.getColumnAddress(col);
        Assert.assertEquals((long) expected.length << 2, buf.getColumnSize(col));
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(expected[i], Unsafe.getInt(addr + ((long) i << 2)));
        }
    }

    private static void assertLongColumn(CompositeWalLagBuffer buf, int col, long... expected) {
        long addr = buf.getColumnAddress(col);
        Assert.assertEquals((long) expected.length << 3, buf.getColumnSize(col));
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(expected[i], Unsafe.getLong(addr + ((long) i << 3)));
        }
    }

    private static IntList fourColumnTypes() {
        IntList types = new IntList();
        types.add(ColumnType.TIMESTAMP); // ts
        types.add(ColumnType.SYMBOL);     // exch (int-encoded, 4 bytes)
        types.add(ColumnType.DOUBLE);     // px
        types.add(ColumnType.LONG);       // n
        return types;
    }

    /**
     * Builds one synthetic 4-column (ts/exch/px/n) source batch as a dense {@code MemoryCR} list ready
     * to pass to {@link CompositeWalLagBuffer#append}. The backing {@code MemoryCARW} instances are
     * registered into {@code sources} for the caller to free.
     */
    private static ObjList<MemoryCR> fourColumnSource(
            ObjList<MemoryCARW> sources, long[] ts, int[] exch, double[] px, long[] n
    ) {
        MemoryCARW tsMem = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
        MemoryCARW exchMem = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
        MemoryCARW pxMem = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
        MemoryCARW nMem = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
        sources.add(tsMem);
        sources.add(exchMem);
        sources.add(pxMem);
        sources.add(nMem);

        for (long v : ts) {
            tsMem.putLong(v);
        }
        for (int v : exch) {
            exchMem.putInt(v);
        }
        for (double v : px) {
            pxMem.putDouble(v);
        }
        for (long v : n) {
            nMem.putLong(v);
        }

        ObjList<MemoryCR> src = new ObjList<>();
        src.add(tsMem);
        src.add(exchMem);
        src.add(pxMem);
        src.add(nMem);
        return src;
    }
}
