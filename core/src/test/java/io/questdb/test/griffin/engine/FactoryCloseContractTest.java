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

package io.questdb.test.griffin.engine;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.PriorityMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRecordCursor;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.join.AsOfJoinLightNoKeyRecordCursorFactory;
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.join.AsyncWindowJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.HashJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.table.AsyncGroupByAtom;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

public class FactoryCloseContractTest extends AbstractCairoTest {

    @Test
    public void testAsyncGroupByFactoryCloseReachesAtomAfterBaseFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException baseFailure = new RuntimeException("base close");
            final RuntimeException atomFailure = new RuntimeException("atom function close");
            final RuntimeException recordFailure = new RuntimeException("record function close");
            final CloseTrackingFactory base = new CloseTrackingFactory(new GenericRecordMetadata(), baseFailure);
            final CloseTrackingBooleanFunction atomFunction = new CloseTrackingBooleanFunction(atomFailure);
            final CloseTrackingBooleanFunction recordFunction = new CloseTrackingBooleanFunction(recordFailure);
            final CloseTrackingUnorderedPageFrameSequence frameSequence = new CloseTrackingUnorderedPageFrameSequence(
                    atomFunction
            );
            final AsyncGroupByRecordCursorFactory factory = allocate(AsyncGroupByRecordCursorFactory.class);
            final ObjList<Function> recordFunctions = new ObjList<>();
            recordFunctions.add(recordFunction);
            setField(AsyncGroupByRecordCursorFactory.class, factory, "base", base);
            setField(AsyncGroupByRecordCursorFactory.class, factory, "frameSequence", frameSequence);
            setField(AsyncGroupByRecordCursorFactory.class, factory, "recordFunctions", recordFunctions);

            try {
                factory.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(baseFailure, e);
                Assert.assertArrayEquals(new Throwable[]{atomFailure, recordFailure}, e.getSuppressed());
            }
            factory.close();
            Assert.assertEquals(1, base.getCloseCount());
            Assert.assertEquals(1, frameSequence.closeCount);
            Assert.assertEquals(1, atomFunction.closeCount);
            Assert.assertEquals(1, recordFunction.closeCount);
        });
    }

    @Test
    public void testFilteredFactoryCloseContinuesAfterBaseFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException baseFailure = new RuntimeException("base close");
            final RuntimeException filterFailure = new RuntimeException("filter close");
            final CloseTrackingFactory base = new CloseTrackingFactory(new GenericRecordMetadata(), baseFailure);
            final CloseTrackingBooleanFunction filter = new CloseTrackingBooleanFunction(filterFailure);
            final FilteredRecordCursorFactory factory = new FilteredRecordCursorFactory(base, filter);

            try {
                factory.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(baseFailure, e);
                Assert.assertArrayEquals(new Throwable[]{filterFailure}, e.getSuppressed());
            }
            factory.close();
            Assert.assertEquals(1, base.getCloseCount());
            Assert.assertEquals(1, filter.closeCount);
        });
    }

    @Test
    public void testGroupByConstructorPreservesPrimaryAndAttemptsAllCleanup() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException firstCloseFailure = new RuntimeException("first function close");
            final RuntimeException secondCloseFailure = new RuntimeException("second function close");
            final RuntimeException baseCloseFailure = new RuntimeException("base close");
            final CloseTrackingBooleanFunction first = new CloseTrackingBooleanFunction(firstCloseFailure);
            final CloseTrackingBooleanFunction second = new CloseTrackingBooleanFunction(secondCloseFailure);
            final ObjList<Function> recordFunctions = new ObjList<>();
            recordFunctions.add(first);
            recordFunctions.add(second);
            final CloseTrackingFactory base = new CloseTrackingFactory(new GenericRecordMetadata(), baseCloseFailure);

            try {
                new GroupByRecordCursorFactory(
                        (BytecodeAssembler) null,
                        engine.getConfiguration(),
                        base,
                        new ListColumnFilter(),
                        new ArrayColumnTypes(),
                        new ArrayColumnTypes(),
                        new GenericRecordMetadata(),
                        new ObjList<>(),
                        new ObjList<>(),
                        recordFunctions,
                        null
                );
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertTrue(e instanceof NullPointerException);
                Assert.assertArrayEquals(new Throwable[]{firstCloseFailure}, e.getSuppressed());
                Assert.assertArrayEquals(new Throwable[]{secondCloseFailure, baseCloseFailure}, firstCloseFailure.getSuppressed());
            }
            Assert.assertEquals(1, first.closeCount);
            Assert.assertEquals(1, second.closeCount);
            Assert.assertEquals(1, base.getCloseCount());
        });
    }

    @Test
    public void testHashJoinConstructorPreservesPrimaryAndAttemptsAllCleanup() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException constructionFailure = new RuntimeException("post-record-chain metadata");
            final RuntimeException masterCloseFailure = new RuntimeException("master close");
            final RuntimeException slaveCloseFailure = new RuntimeException("slave close");
            final CloseTrackingFactory master = new CloseTrackingFactory(new GenericRecordMetadata(), masterCloseFailure);
            final ThrowingSecondMetadataFactory slave = new ThrowingSecondMetadataFactory(
                    new GenericRecordMetadata(),
                    slaveCloseFailure,
                    constructionFailure
            );
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.INT);
            final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.LONG);
            valueTypes.add(ColumnType.LONG);
            valueTypes.add(ColumnType.LONG);

            try {
                new HashJoinRecordCursorFactory(
                        engine.getConfiguration(),
                        joinMetadata(),
                        master,
                        slave,
                        keyTypes,
                        valueTypes,
                        null,
                        null,
                        null,
                        0,
                        new JoinContext(),
                        new int[]{0},
                        new int[]{0}
                );
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(constructionFailure, e);
                Assert.assertArrayEquals(new Throwable[]{masterCloseFailure}, e.getSuppressed());
                Assert.assertArrayEquals(new Throwable[]{slaveCloseFailure}, masterCloseFailure.getSuppressed());
            }
            Assert.assertEquals(1, master.getCloseCount());
            Assert.assertEquals(1, slave.getCloseCount());
        });
    }

    @Test
    public void testJoinFactoryCloseContinuesAfterMasterFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException masterFailure = new RuntimeException("master close");
            final RuntimeException slaveFailure = new RuntimeException("slave close");
            final CloseTrackingFactory master = new CloseTrackingFactory(timestampMetadata("master_ts"), masterFailure);
            final CloseTrackingFactory slave = new CloseTrackingFactory(timestampMetadata("slave_ts"), slaveFailure);
            final AsOfJoinLightNoKeyRecordCursorFactory factory = new AsOfJoinLightNoKeyRecordCursorFactory(
                    joinMetadata(),
                    master,
                    slave,
                    1,
                    Long.MAX_VALUE
            );

            try {
                factory.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(masterFailure, e);
                Assert.assertArrayEquals(new Throwable[]{slaveFailure}, e.getSuppressed());
            }
            factory.close();
            Assert.assertEquals(1, master.getCloseCount());
            Assert.assertEquals(1, slave.getCloseCount());
        });
    }

    @Test
    public void testOrderedPageFrameSequenceConstructorPreservesPrimary() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException constructionFailure = new RuntimeException("circuit breaker configuration");
            final RuntimeException clearFailure = new RuntimeException("atom clear");
            final RuntimeException closeFailure = new RuntimeException("atom close");
            final CloseTrackingAtom atom = new CloseTrackingAtom(clearFailure, closeFailure);
            final CairoConfiguration configuration = new CairoConfigurationWrapper(engine.getConfiguration()) {
                @Override
                public io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
                    throw constructionFailure;
                }
            };

            try {
                new PageFrameSequence<>(
                        engine,
                        configuration,
                        engine.getMessageBus(),
                        atom,
                        null,
                        null,
                        1,
                        (byte) 0
                );
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(constructionFailure, e);
                Assert.assertArrayEquals(new Throwable[]{clearFailure}, e.getSuppressed());
                Assert.assertArrayEquals(new Throwable[]{closeFailure}, clearFailure.getSuppressed());
            }
            Assert.assertEquals(1, atom.clearCount);
            Assert.assertEquals(1, atom.closeCount);
        });
    }

    @Test
    public void testOrderedWindowFactoryCloseContinuesAfterSequenceResetFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException masterFailure = new RuntimeException("master close");
            final RuntimeException clearFailure = new RuntimeException("atom clear");
            final RuntimeException atomCloseFailure = new RuntimeException("atom close");
            final RuntimeException metadataFailure = new RuntimeException("metadata close");
            final CloseTrackingFactory master = new CloseTrackingFactory(new GenericRecordMetadata(), masterFailure);
            final CloseTrackingFactory slave = new CloseTrackingFactory(new GenericRecordMetadata(), null);
            final CloseTrackingAtom atom = new CloseTrackingAtom(clearFailure, atomCloseFailure);
            final CloseTrackingPageFrameCursor frameCursor = new CloseTrackingPageFrameCursor();
            final PageFrameSequence<CloseTrackingAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    engine.getConfiguration(),
                    engine.getMessageBus(),
                    atom,
                    null,
                    null,
                    1,
                    (byte) 0
            );
            setField(PageFrameSequence.class, frameSequence, "frameCursor", frameCursor);
            final CloseTrackingJoinRecordMetadata metadata = new CloseTrackingJoinRecordMetadata(metadataFailure);
            final AsyncWindowJoinRecordCursorFactory factory = allocate(AsyncWindowJoinRecordCursorFactory.class);
            setField(AsyncWindowJoinRecordCursorFactory.class, factory, "masterFactory", master);
            setField(AsyncWindowJoinRecordCursorFactory.class, factory, "slaveFactory", slave);
            setField(AsyncWindowJoinRecordCursorFactory.class, factory, "frameSequence", frameSequence);
            setField(AsyncWindowJoinRecordCursorFactory.class, factory, "joinMetadata", metadata);

            try {
                factory.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(masterFailure, e);
                Assert.assertArrayEquals(new Throwable[]{clearFailure, metadataFailure}, e.getSuppressed());
                Assert.assertArrayEquals(new Throwable[]{atomCloseFailure}, clearFailure.getSuppressed());
            }
            factory.close();
            frameSequence.close();
            Assert.assertEquals(1, master.getCloseCount());
            Assert.assertEquals(1, slave.getCloseCount());
            Assert.assertEquals(1, frameCursor.closeCount);
            Assert.assertEquals(1, atom.clearCount);
            Assert.assertEquals(1, atom.closeCount);
            Assert.assertEquals(1, metadata.closeCount);
        });
    }

    @Test
    public void testProjectionFactoryConstructorRollsBackAdoptedOwners() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException constructionFailure = new RuntimeException("supports random access");
            final RuntimeException firstCloseFailure = new RuntimeException("first function close");
            final RuntimeException secondCloseFailure = new RuntimeException("second function close");
            final RuntimeException baseCloseFailure = new RuntimeException("base close");
            final CloseTrackingBooleanFunction first = new CloseTrackingBooleanFunction(firstCloseFailure, constructionFailure);
            final CloseTrackingBooleanFunction second = new CloseTrackingBooleanFunction(secondCloseFailure);
            final ObjList<Function> functions = new ObjList<>();
            functions.add(first);
            functions.add(second);
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            final CloseTrackingFactory base = new CloseTrackingFactory(metadata, baseCloseFailure);

            try {
                new VirtualRecordCursorFactory(
                        metadata,
                        new PriorityMetadata(0, metadata),
                        functions,
                        base,
                        0
                );
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(constructionFailure, e);
                Assert.assertArrayEquals(new Throwable[]{firstCloseFailure}, e.getSuppressed());
                Assert.assertArrayEquals(new Throwable[]{secondCloseFailure, baseCloseFailure}, firstCloseFailure.getSuppressed());
            }
            Assert.assertEquals(1, first.closeCount);
            Assert.assertEquals(1, second.closeCount);
            Assert.assertEquals(1, base.getCloseCount());
        });
    }

    @Test
    public void testProjectionFactoryCloseContinuesAfterFunctionFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException firstFailure = new RuntimeException("first function close");
            final RuntimeException secondFailure = new RuntimeException("second function close");
            final RuntimeException baseFailure = new RuntimeException("base close");
            final CloseTrackingBooleanFunction first = new CloseTrackingBooleanFunction(firstFailure);
            final CloseTrackingBooleanFunction second = new CloseTrackingBooleanFunction(secondFailure);
            final ObjList<Function> functions = new ObjList<>();
            functions.add(first);
            functions.add(second);
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            final CloseTrackingFactory base = new CloseTrackingFactory(metadata, baseFailure);
            final VirtualRecordCursorFactory factory = new VirtualRecordCursorFactory(
                    metadata,
                    new PriorityMetadata(0, metadata),
                    functions,
                    base,
                    0
            );

            try {
                factory.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(firstFailure, e);
                Assert.assertArrayEquals(new Throwable[]{secondFailure, baseFailure}, e.getSuppressed());
            }
            factory.close();
            Assert.assertEquals(1, first.closeCount);
            Assert.assertEquals(1, second.closeCount);
            Assert.assertEquals(1, base.getCloseCount());
        });
    }

    @Test
    public void testQueryProgressFactoryCloseContinuesAfterBaseFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException baseFailure = new RuntimeException("base close");
            final CloseTrackingPageFrameCursor pageFrameCursor = new CloseTrackingPageFrameCursor();
            final PageFrameFactory base = new PageFrameFactory(new GenericRecordMetadata(), baseFailure, pageFrameCursor);
            final QueryProgress factory = new QueryProgress(engine.getQueryRegistry(), "SELECT 1", base);
            Assert.assertNotNull(factory.getPageFrameCursor(sqlExecutionContext, RecordCursorFactory.SCAN_DIRECTION_FORWARD));

            try {
                factory.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(baseFailure, e);
            }
            factory.close();
            Assert.assertEquals(1, base.getCloseCount());
            Assert.assertEquals(1, pageFrameCursor.closeCount);
        });
    }

    private static <T> T allocate(Class<T> clazz) throws InstantiationException {
        return (T) Unsafe.getUnsafe().allocateInstance(clazz);
    }

    private static GenericRecordMetadata joinMetadata() {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("master_ts", ColumnType.TIMESTAMP));
        metadata.add(new TableColumnMetadata("slave_ts", ColumnType.TIMESTAMP));
        metadata.setTimestampIndex(0);
        return metadata;
    }

    private static void setField(Class<?> clazz, Object instance, String name, Object value) throws NoSuchFieldException {
        final Field field = clazz.getDeclaredField(name);
        Unsafe.getUnsafe().putObject(instance, Unsafe.getUnsafe().objectFieldOffset(field), value);
    }

    private static GenericRecordMetadata timestampMetadata(String name) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata(name, ColumnType.TIMESTAMP));
        metadata.setTimestampIndex(0);
        return metadata;
    }

    private static class CloseTrackingAtom implements StatefulAtom {
        private final RuntimeException clearFailure;
        private final RuntimeException closeFailure;
        private int clearCount;
        private int closeCount;

        private CloseTrackingAtom(RuntimeException clearFailure, RuntimeException closeFailure) {
            this.clearFailure = clearFailure;
            this.closeFailure = closeFailure;
        }

        @Override
        public void clear() {
            clearCount++;
            if (clearFailure != null) {
                throw clearFailure;
            }
        }

        @Override
        public void close() {
            closeCount++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static class CloseTrackingBooleanFunction extends BooleanFunction {
        private final RuntimeException closeFailure;
        private int closeCount;

        private final RuntimeException supportsRandomAccessFailure;

        private CloseTrackingBooleanFunction(RuntimeException closeFailure) {
            this(closeFailure, null);
        }

        private CloseTrackingBooleanFunction(RuntimeException closeFailure, RuntimeException supportsRandomAccessFailure) {
            this.closeFailure = closeFailure;
            this.supportsRandomAccessFailure = supportsRandomAccessFailure;
        }

        @Override
        public void close() {
            closeCount++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public boolean getBool(Record rec) {
            return true;
        }

        @Override
        public boolean supportsRandomAccess() {
            if (supportsRandomAccessFailure != null) {
                throw supportsRandomAccessFailure;
            }
            return true;
        }
    }

    private static class CloseTrackingFactory extends AbstractRecordCursorFactory {
        private final RuntimeException closeFailure;
        private int closeCount;

        private CloseTrackingFactory(RecordMetadata metadata, RuntimeException closeFailure) {
            super(metadata);
            this.closeFailure = closeFailure;
        }

        protected int getCloseCount() {
            return closeCount;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            return EmptyTableRecordCursor.INSTANCE;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return true;
        }

        @Override
        protected void _close() {
            closeCount++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static class ThrowingSecondMetadataFactory extends CloseTrackingFactory {
        private final RuntimeException metadataFailure;
        private int metadataCallCount;

        private ThrowingSecondMetadataFactory(
                RecordMetadata metadata,
                RuntimeException closeFailure,
                RuntimeException metadataFailure
        ) {
            super(metadata, closeFailure);
            this.metadataFailure = metadataFailure;
        }

        @Override
        public RecordMetadata getMetadata() {
            if (++metadataCallCount == 2) {
                throw metadataFailure;
            }
            return super.getMetadata();
        }
    }

    private class CloseTrackingUnorderedPageFrameSequence extends UnorderedPageFrameSequence<AsyncGroupByAtom> {
        private final CloseTrackingBooleanFunction atomFunction;
        private int closeCount;

        private CloseTrackingUnorderedPageFrameSequence(CloseTrackingBooleanFunction atomFunction) {
            super(engine, engine.getConfiguration(), engine.getMessageBus(), null, null, 1);
            this.atomFunction = atomFunction;
        }

        @Override
        public void close() {
            closeCount++;
            super.close();
            atomFunction.close();
        }
    }

    private static class CloseTrackingJoinRecordMetadata extends JoinRecordMetadata {
        private final RuntimeException closeFailure;
        private int closeCount;

        private CloseTrackingJoinRecordMetadata(RuntimeException closeFailure) {
            super(engine.getConfiguration(), 2);
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            closeCount++;
            super.close();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static class CloseTrackingPageFrameCursor implements PageFrameCursor {
        private int closeCount;

        @Override
        public void calculateSize(RecordCursor.Counter counter) {
        }

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public ColumnMapping getColumnMapping() {
            return null;
        }

        @Override
        public long getRemainingRowsInInterval() {
            return 0;
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return null;
        }

        @Override
        public boolean isExternal() {
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return null;
        }

        @Override
        public @Nullable PageFrame next(long skipTarget) {
            return null;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public boolean supportsSizeCalculation() {
            return true;
        }

        @Override
        public void toTop() {
        }
    }

    private static class PageFrameFactory extends CloseTrackingFactory {
        private final PageFrameCursor pageFrameCursor;

        private PageFrameFactory(
                RecordMetadata metadata,
                RuntimeException closeFailure,
                PageFrameCursor pageFrameCursor
        ) {
            super(metadata, closeFailure);
            this.pageFrameCursor = pageFrameCursor;
        }

        @Override
        public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) {
            return pageFrameCursor;
        }

        @Override
        public boolean supportsPageFrameCursor() {
            return true;
        }
    }
}
