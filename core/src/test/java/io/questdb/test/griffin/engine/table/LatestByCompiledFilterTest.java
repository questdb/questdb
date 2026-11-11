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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.table.LatestByCompiledFilter;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

public class LatestByCompiledFilterTest extends AbstractTest {
    @Test
    public void testEligibilityRecheckedAfterErrorAndReopen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            final TestMemory memory = new TestMemory(17);
            final CapturingFilter compiled = new CapturingFilter(17);
            try (
                    TestCache cache = new TestCache(17);
                    TestPool pool = new TestPool(configuration, memory);
                    LatestByCompiledFilter filter = new LatestByCompiledFilter(configuration, BooleanConstant.TRUE,
                            compiled, new ObjList<>(), IntList.createWithValues(16))
            ) {
                filter.init(null, null);
                memory.isThrowOnCastCheck = true;
                try {
                    LatestByCompiledFilter.apply(filter, pool, cache, 0, 0, 1);
                    Assert.fail("expected eligibility failure");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "injected eligibility failure");
                }
                compiled.expectedData.setQuick(16, dataBase(16));
                Assert.assertNotNull(LatestByCompiledFilter.apply(filter, pool, cache, 0, 0, 1));
                Assert.assertEquals(2, memory.castChecks);
                Assert.assertEquals(1, compiled.calls);
                for (int state = 0; state < 3; state++) {
                    filter.cursorClosed();
                    memory.isCast = state == 0;
                    memory.topIndex = state == 1 ? 16 : -1;
                    filter.init(null, null);
                    if (state < 2) {
                        Assert.assertNull(LatestByCompiledFilter.apply(filter, pool, cache, 0, 0, 1));
                    } else {
                        Assert.assertNotNull(LatestByCompiledFilter.apply(filter, pool, cache, 0, 0, 1));
                    }
                }
                Assert.assertEquals(5, memory.castChecks);
                Assert.assertEquals(5, pool.releases);
                Assert.assertEquals(2, compiled.calls);
            }
        });
    }

    @Test
    public void testPreparationScalesWithPredicateColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            for (int columnCount : new int[]{3, 515}) {
                final int last = columnCount - 1;
                final TestMemory memory = new TestMemory(columnCount);
                memory.inlineIndex = last;
                final CapturingFilter compiled = new CapturingFilter(columnCount);
                compiled.rowCount = 2048;
                try (
                        TestCache cache = new TestCache(columnCount);
                        TestPool pool = new TestPool(configuration, memory);
                        LatestByCompiledFilter filter = new LatestByCompiledFilter(configuration, BooleanConstant.TRUE,
                                compiled, new ObjList<>(), IntList.createWithValues(last, 0))
                ) {
                    cache.types.setQuick(last, ColumnType.VARCHAR);
                    filter.init(null, null);
                    for (int batch = 0; batch < 32; batch++) {
                        long rowLo = batch * 2048L;
                        compiled.expectedData.setQuick(0, dataBase(0) + rowLo * 8);
                        compiled.expectedAux.setQuick(last, auxBase(last) + rowLo * 16);
                        Assert.assertNotNull(LatestByCompiledFilter.apply(filter, pool, cache, 0, rowLo, rowLo + 2048));
                    }
                    Assert.assertEquals(66, memory.dataReads);
                    Assert.assertEquals(33, memory.auxReads);
                    Assert.assertEquals(1, memory.castChecks);
                    Assert.assertEquals(0, memory.topChecks);
                    Assert.assertEquals(32, compiled.calls);
                    Assert.assertEquals(32, pool.releases);
                }
            }
        });
    }

    @Test
    public void testRetryAfterDataGrowthFailure() throws Exception {
        assertRetryAfterGrowthFailure(17, 0, true);
    }

    @Test
    public void testRetryAfterFinalAuxGrowthFailure() throws Exception {
        assertRetryAfterGrowthFailure(17, 16 * Long.BYTES, false);
    }

    @Test
    public void testRetryAfterFinalAuxGrowthFailureOnReopen() throws Exception {
        assertRetryAfterGrowthFailure(17, 16 * Long.BYTES, true);
    }

    @Test
    public void testRetryAfterNonFinalAuxGrowthFailure() throws Exception {
        assertRetryAfterGrowthFailure(18, 16 * Long.BYTES, true);
    }

    @Test
    public void testSparseAddressesUseFrameOriginAndVarSizeAuxOffsets() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            final TestMemory memory = new TestMemory(40);
            final CapturingFilter compiled = new CapturingFilter(40);
            try (
                    TestCache cache = new TestCache(40);
                    TestPool pool = new TestPool(configuration, memory);
                    LatestByCompiledFilter filter = new LatestByCompiledFilter(configuration, BooleanConstant.TRUE,
                            compiled, new ObjList<>(), IntList.createWithValues(33, 3, 18, 5, 16, 7, 11, 9))
            ) {
                cache.types.setQuick(3, ColumnType.BYTE);
                cache.types.setQuick(5, ColumnType.CHAR);
                cache.types.setQuick(7, ColumnType.INT);
                cache.types.setQuick(9, ColumnType.LONG);
                cache.types.setQuick(11, ColumnType.UUID);
                cache.types.setQuick(16, ColumnType.STRING);
                cache.types.setQuick(18, ColumnType.BINARY);
                cache.types.setQuick(33, ColumnType.VARCHAR);
                filter.init(null, null);
                for (long rowLo : new long[]{0, 1, 2048, 4096}) {
                    compiled.expectedData.setQuick(3, dataBase(3) + rowLo);
                    compiled.expectedData.setQuick(5, dataBase(5) + rowLo * 2);
                    compiled.expectedData.setQuick(7, dataBase(7) + rowLo * 4);
                    compiled.expectedData.setQuick(9, dataBase(9) + rowLo * 8);
                    compiled.expectedData.setQuick(11, dataBase(11) + rowLo * 16);
                    compiled.expectedData.setQuick(16, dataBase(16));
                    compiled.expectedData.setQuick(18, dataBase(18));
                    compiled.expectedData.setQuick(33, dataBase(33));
                    compiled.expectedAux.setQuick(16, auxBase(16) + rowLo * 8);
                    compiled.expectedAux.setQuick(18, auxBase(18) + rowLo * 8);
                    compiled.expectedAux.setQuick(33, auxBase(33) + rowLo * 16);
                    DirectLongList rows = LatestByCompiledFilter.apply(filter, pool, cache, 0, rowLo, rowLo + 1);
                    Assert.assertNotNull(rows);
                    Assert.assertEquals(1, rows.size());
                    Assert.assertEquals(0, rows.get(0));
                }
                Assert.assertEquals(4, pool.releases);
            }
        });
    }

    private static void assertRetryAfterGrowthFailure(int columnCount, long headroom, boolean isReopen) throws Exception {
        // The failing retry must stop at DirectLongList's bounds assertion, not write out of bounds.
        Assert.assertTrue(DirectLongList.class.desiredAssertionStatus());
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            Assert.assertEquals(16, configuration.getPageFrameReduceColumnListCapacity());
            final TestMemory memory = new TestMemory(columnCount);
            final CapturingFilter compiled = new CapturingFilter(columnCount);
            try (
                    TestCache cache = new TestCache(columnCount);
                    TestPool pool = new TestPool(configuration, memory);
                    LatestByCompiledFilter filter = new LatestByCompiledFilter(configuration, BooleanConstant.TRUE,
                            compiled, new ObjList<>(), IntList.createWithValues(columnCount - 1))
            ) {
                cache.types.setQuick(columnCount - 1, ColumnType.VARCHAR);
                filter.init(null, null);
                final long savedLimit = Unsafe.getRssMemLimit();
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + headroom);
                try {
                    LatestByCompiledFilter.apply(filter, pool, cache, 0, 0, 1);
                    Assert.fail("expected global RSS breach during pointer table growth");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "global RSS memory limit exceeded");
                } finally {
                    Unsafe.setRssMemLimit(savedLimit);
                }
                Assert.assertEquals(0, compiled.calls);
                Assert.assertEquals(1, pool.releases);
                final DirectLongList data = pointerList(filter, "dataAddresses");
                final DirectLongList aux = pointerList(filter, "auxAddresses");
                Assert.assertEquals(headroom == 0 ? 16 : 17, data.size());
                Assert.assertEquals(headroom == 0 ? 16 : 32, data.getCapacity());
                Assert.assertEquals(16, aux.size());
                Assert.assertEquals(16, aux.getCapacity());
                if (isReopen) {
                    filter.cursorClosed();
                    filter.init(null, null);
                }
                compiled.expectedData.setQuick(columnCount - 1, dataBase(columnCount - 1));
                compiled.expectedAux.setQuick(columnCount - 1, auxBase(columnCount - 1));
                Assert.assertNotNull(LatestByCompiledFilter.apply(filter, pool, cache, 0, 0, 1));
                Assert.assertEquals(columnCount, data.size());
                Assert.assertEquals(columnCount, aux.size());
                Assert.assertEquals(1, compiled.calls);
                Assert.assertEquals(2, pool.releases);
            }
        });
    }

    private static long auxBase(int columnIndex) {
        return 2_000_777L + columnIndex * 65_536L;
    }

    private static long dataBase(int columnIndex) {
        return 1_000_777L + columnIndex * 65_536L;
    }

    private static DirectLongList pointerList(LatestByCompiledFilter filter, String name) throws Exception {
        Field field = LatestByCompiledFilter.class.getDeclaredField(name);
        field.setAccessible(true);
        return (DirectLongList) field.get(filter);
    }

    // These tests inspect pointer tables; the fake frame addresses never reach native code.
    // LatestByJitTest covers actual native loads through the SQL engine.
    private static class CapturingFilter extends CompiledFilter {
        private final LongList expectedAux = new LongList();
        private final LongList expectedData = new LongList();
        private int calls;
        private long rowCount = 1;

        private CapturingFilter(int columnCount) {
            expectedAux.setPos(columnCount);
            expectedAux.fill(0, columnCount, 0);
            expectedData.setPos(columnCount);
            expectedData.fill(0, columnCount, 0);
        }

        @Override
        public long call(long dataAddress, long dataSize, long auxAddress, long varsAddress, long varsSize, long rowsAddress, long rowsCount) {
            Assert.assertEquals(expectedData.size(), dataSize);
            Assert.assertEquals(rowCount, rowsCount);
            Assert.assertEquals(0, varsSize);
            for (int i = 0; i < dataSize; i++) {
                Assert.assertEquals("data slot " + i, expectedData.getQuick(i), Unsafe.getLong(dataAddress + i * 8L));
                Assert.assertEquals("aux slot " + i, expectedAux.getQuick(i), Unsafe.getLong(auxAddress + i * 8L));
            }
            Unsafe.putLong(rowsAddress, 0);
            calls++;
            return 1;
        }
    }

    private static class TestCache extends PageFrameAddressCache {
        private final IntList types = new IntList();

        private TestCache(int columnCount) {
            for (int i = 0; i < columnCount; i++) {
                types.add(ColumnType.LONG);
            }
        }

        @Override
        public int getColumnCount() {
            return types.size();
        }

        @Override
        public IntList getColumnTypes() {
            return types;
        }
    }

    private static class TestMemory implements PageFrameMemory {
        private final int columnCount;
        private int auxReads;
        private int castChecks;
        private int dataReads;
        private int inlineIndex = -1;
        private boolean isCast;
        private boolean isThrowOnCastCheck;
        private int topChecks;
        private int topIndex = -1;

        private TestMemory(int columnCount) {
            this.columnCount = columnCount;
        }

        @Override
        public long getAuxPageAddress(int columnIndex) {
            auxReads++;
            return columnIndex == topIndex ? 0 : auxBase(columnIndex);
        }

        @Override
        public DirectLongList getAuxPageAddresses() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DirectLongList getAuxPageSizes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getColumnCount() {
            return columnCount;
        }

        @Override
        public int getColumnOffset() {
            return 0;
        }

        @Override
        public byte getFrameFormat() {
            return PartitionFormat.NATIVE;
        }

        @Override
        public int getFrameIndex() {
            return 0;
        }

        @Override
        public long getPageAddress(int columnIndex) {
            dataReads++;
            return columnIndex == topIndex || columnIndex == inlineIndex ? 0 : dataBase(columnIndex);
        }

        @Override
        public DirectLongList getPageAddresses() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPageSize(int columnIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DirectLongList getPageSizes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public PageFrameMemoryPool getPool() {
            return null;
        }

        @Override
        public long getRowIdOffset() {
            return 777;
        }

        @Override
        public int getSourceColumnType(int columnIndex) {
            return -1;
        }

        @Override
        public boolean hasColumnTops() {
            topChecks++;
            return true;
        }

        @Override
        public boolean hasColumnTypeCasts() {
            castChecks++;
            if (isThrowOnCastCheck) {
                isThrowOnCastCheck = false;
                throw CairoException.nonCritical().put("injected eligibility failure");
            }
            return isCast;
        }

        @Override
        public boolean populateRemainingColumns(IntHashSet filterColumnIndexes, DirectLongList filteredRows, boolean fillWithNulls) {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestPool extends PageFrameMemoryPool {
        private final TestMemory memory;
        private int releases;

        private TestPool(CairoConfiguration configuration, TestMemory memory) {
            super(configuration);
            this.memory = memory;
        }

        @Override
        public PageFrameMemory navigateTo(int frameIndex) {
            return memory;
        }

        @Override
        public void releaseFrameMemory() {
            releases++;
        }
    }
}
