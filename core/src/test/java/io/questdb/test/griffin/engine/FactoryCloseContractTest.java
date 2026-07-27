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
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PriorityMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRecordCursor;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.groupby.DistinctRecordCursorFactory;
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinLightNoKeyRecordCursorFactory;
import io.questdb.griffin.engine.join.AsyncWindowJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.HashJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.join.WindowJoinRecordCursorFactory;
import io.questdb.griffin.engine.orderby.SortedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByAtom;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHorizonJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByAllIndexedRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestByValueFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.PageFrameRecordCursor;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.griffin.engine.union.UnionAllRecordCursorFactory;
import io.questdb.griffin.model.JoinContext;
import io.questdb.jit.CompiledCountOnlyFilter;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.RostiAllocFacade;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class FactoryCloseContractTest extends AbstractCairoTest {

    @Test
    public void testAsyncFilteredFactoryDetachesAllOwnersBeforeBaseCallback() throws Exception {
        assertMemoryLeak(() -> {
            final AsyncFilteredRecordCursorFactory factory = allocate(AsyncFilteredRecordCursorFactory.class);
            final AtomicInteger callbackCount = new AtomicInteger();
            final CallbackCloseFactory base = new CallbackCloseFactory(() -> {
                callbackCount.incrementAndGet();
                assertFieldsNull(
                        AsyncFilteredRecordCursorFactory.class,
                        factory,
                        "base",
                        "cursor",
                        "filter",
                        "frameSequence",
                        "negativeLimitCursor",
                        "negativeLimitRows"
                );
            });
            final Object cursor = allocate(Class.forName("io.questdb.griffin.engine.table.AsyncFilteredRecordCursor"));
            final CloseTrackingBooleanFunction filter = new CloseTrackingBooleanFunction(null);
            final PageFrameSequence<CloseTrackingAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    engine.getConfiguration(),
                    engine.getMessageBus(),
                    new CloseTrackingAtom(null, null),
                    null,
                    null,
                    1,
                    (byte) 0
            );
            final Object negativeLimitCursor = allocate(Class.forName(
                    "io.questdb.griffin.engine.table.AsyncFilteredNegativeLimitRecordCursor"
            ));
            final DirectLongList negativeLimitRows = new DirectLongList(2, MemoryTag.NATIVE_OFFLOAD);
            setField(AsyncFilteredRecordCursorFactory.class, factory, "base", base);
            setField(AsyncFilteredRecordCursorFactory.class, factory, "cursor", cursor);
            setField(AsyncFilteredRecordCursorFactory.class, factory, "filter", filter);
            setField(AsyncFilteredRecordCursorFactory.class, factory, "frameSequence", frameSequence);
            setField(AsyncFilteredRecordCursorFactory.class, factory, "negativeLimitCursor", negativeLimitCursor);
            setField(AsyncFilteredRecordCursorFactory.class, factory, "negativeLimitRows", negativeLimitRows);

            factory.close();
            Assert.assertEquals(1, callbackCount.get());
            Assert.assertEquals(1, base.getCloseCount());
            Assert.assertEquals(1, filter.closeCount);
        });
    }

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
    public void testAsyncGroupByFactoryDetachesAllOwnersBeforeBaseCallback() throws Exception {
        assertMemoryLeak(() -> {
            final AsyncGroupByRecordCursorFactory factory = allocate(AsyncGroupByRecordCursorFactory.class);
            final AtomicInteger callbackCount = new AtomicInteger();
            final CallbackCloseFactory base = new CallbackCloseFactory(() -> {
                callbackCount.incrementAndGet();
                assertFieldsNull(
                        AsyncGroupByRecordCursorFactory.class,
                        factory,
                        "base",
                        "cursor",
                        "frameSequence",
                        "recordFunctions",
                        "sharedCursors",
                        "sharedRecordFunctions"
                );
            });
            final CloseTrackingUnorderedPageFrameSequence frameSequence = new CloseTrackingUnorderedPageFrameSequence(
                    new CloseTrackingBooleanFunction(null)
            );
            setField(AsyncGroupByRecordCursorFactory.class, factory, "base", base);
            setField(AsyncGroupByRecordCursorFactory.class, factory, "frameSequence", frameSequence);
            setField(AsyncGroupByRecordCursorFactory.class, factory, "recordFunctions", new ObjList<>());
            setField(AsyncGroupByRecordCursorFactory.class, factory, "sharedCursors", new ObjList<>());
            setField(AsyncGroupByRecordCursorFactory.class, factory, "sharedRecordFunctions", new ObjList<>());

            factory.close();
            Assert.assertEquals(1, callbackCount.get());
            Assert.assertEquals(1, base.getCloseCount());
            Assert.assertEquals(1, frameSequence.closeCount);
        });
    }

    @Test
    public void testAsyncHorizonFactoryDetachesAllOwnersBeforeMasterCallback() throws Exception {
        assertMemoryLeak(() -> {
            final AsyncHorizonJoinRecordCursorFactory factory = allocate(AsyncHorizonJoinRecordCursorFactory.class);
            final AtomicInteger callbackCount = new AtomicInteger();
            final CallbackCloseFactory master = new CallbackCloseFactory(() -> {
                callbackCount.incrementAndGet();
                assertFieldsNull(
                        AsyncHorizonJoinRecordCursorFactory.class,
                        factory,
                        "cursor",
                        "frameSequence",
                        "horizonJoinMetadata",
                        "masterFactory",
                        "recordFunctions",
                        "resources",
                        "slaveFactory"
                );
            });
            final CloseTrackingFactory slave = new CloseTrackingFactory(new GenericRecordMetadata(), null);
            final CloseTrackingJoinRecordMetadata metadata = new CloseTrackingJoinRecordMetadata(null);
            setField(AsyncHorizonJoinRecordCursorFactory.class, factory, "horizonJoinMetadata", metadata);
            setField(AsyncHorizonJoinRecordCursorFactory.class, factory, "masterFactory", master);
            setField(AsyncHorizonJoinRecordCursorFactory.class, factory, "recordFunctions", new ObjList<>());
            setField(AsyncHorizonJoinRecordCursorFactory.class, factory, "slaveFactory", slave);

            factory.close();
            Assert.assertEquals(1, callbackCount.get());
            Assert.assertEquals(1, master.getCloseCount());
            Assert.assertEquals(1, slave.getCloseCount());
            Assert.assertEquals(1, metadata.closeCount);
        });
    }

    @Test
    public void testAsyncJitFactoryDetachesAllOwnersBeforeBaseCallback() throws Exception {
        assertMemoryLeak(() -> {
            final AsyncJitFilteredRecordCursorFactory factory = allocate(AsyncJitFilteredRecordCursorFactory.class);
            final RuntimeException baseFailure = new RuntimeException("base close");
            final RuntimeException bindVarFailure = new RuntimeException("bind variable close");
            final RuntimeException filterFailure = new RuntimeException("filter close");
            final AtomicInteger callbackCount = new AtomicInteger();
            final CallbackCloseFactory base = new CallbackCloseFactory(() -> {
                callbackCount.incrementAndGet();
                assertFieldsNull(
                        AsyncJitFilteredRecordCursorFactory.class,
                        factory,
                        "base",
                        "bindVarFunctions",
                        "bindVarMemory",
                        "compiledCountOnlyFilter",
                        "compiledFilter",
                        "cursor",
                        "filter",
                        "frameSequence",
                        "negativeLimitCursor",
                        "negativeLimitRows"
                );
                throw baseFailure;
            });
            final ObjList<Function> bindVarFunctions = new ObjList<>();
            final CloseTrackingBooleanFunction bindVarFunction = new CloseTrackingBooleanFunction(bindVarFailure);
            bindVarFunctions.add(bindVarFunction);
            final MemoryCARW bindVarMemory = Vm.getCARWInstance(64, 1, MemoryTag.NATIVE_JIT);
            final CompiledCountOnlyFilter compiledCountOnlyFilter = new CompiledCountOnlyFilter();
            final CompiledFilter compiledFilter = new CompiledFilter();
            final Object cursor = allocate(Class.forName("io.questdb.griffin.engine.table.AsyncFilteredRecordCursor"));
            final CloseTrackingBooleanFunction filter = new CloseTrackingBooleanFunction(filterFailure);
            final PageFrameSequence<CloseTrackingAtom> frameSequence = new PageFrameSequence<>(
                    engine,
                    engine.getConfiguration(),
                    engine.getMessageBus(),
                    new CloseTrackingAtom(null, null),
                    null,
                    null,
                    1,
                    (byte) 0
            );
            final Object negativeLimitCursor = allocate(Class.forName(
                    "io.questdb.griffin.engine.table.AsyncFilteredNegativeLimitRecordCursor"
            ));
            final DirectLongList negativeLimitRows = new DirectLongList(2, MemoryTag.NATIVE_OFFLOAD);
            setField(AsyncJitFilteredRecordCursorFactory.class, factory, "base", base);
            setField(AsyncJitFilteredRecordCursorFactory.class, factory, "bindVarFunctions", bindVarFunctions);
            setField(AsyncJitFilteredRecordCursorFactory.class, factory, "bindVarMemory", bindVarMemory);
            setField(AsyncJitFilteredRecordCursorFactory.class, factory, "compiledCountOnlyFilter", compiledCountOnlyFilter);
            setField(AsyncJitFilteredRecordCursorFactory.class, factory, "compiledFilter", compiledFilter);
            setField(AsyncJitFilteredRecordCursorFactory.class, factory, "cursor", cursor);
            setField(AsyncJitFilteredRecordCursorFactory.class, factory, "filter", filter);
            setField(AsyncJitFilteredRecordCursorFactory.class, factory, "frameSequence", frameSequence);
            setField(AsyncJitFilteredRecordCursorFactory.class, factory, "negativeLimitCursor", negativeLimitCursor);
            setField(AsyncJitFilteredRecordCursorFactory.class, factory, "negativeLimitRows", negativeLimitRows);

            try {
                factory.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(baseFailure, e);
                Assert.assertArrayEquals(new Throwable[]{filterFailure, bindVarFailure}, e.getSuppressed());
            }
            factory.close();
            Assert.assertEquals(1, callbackCount.get());
            Assert.assertEquals(1, base.getCloseCount());
            Assert.assertEquals(1, bindVarFunction.closeCount);
            Assert.assertEquals(1, filter.closeCount);
        });
    }

    @Test
    public void testDistinctFactoryCloseReachesLimitFunctionsAfterBaseFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException baseFailure = new RuntimeException("base close");
            final CloseTrackingFactory base = new CloseTrackingFactory(new GenericRecordMetadata(), baseFailure);
            final CloseTrackingBooleanFunction limitLoFunction = new CloseTrackingBooleanFunction(null);
            final CloseTrackingBooleanFunction limitHiFunction = new CloseTrackingBooleanFunction(null);
            final DistinctRecordCursorFactory factory = allocate(DistinctRecordCursorFactory.class);
            setField(DistinctRecordCursorFactory.class, factory, "base", base);
            setField(DistinctRecordCursorFactory.class, factory, "limitLoFunction", limitLoFunction);
            setField(DistinctRecordCursorFactory.class, factory, "limitHiFunction", limitHiFunction);

            try {
                factory.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(baseFailure, e);
            }
            factory.close();
            Assert.assertEquals(1, base.getCloseCount());
            Assert.assertEquals(1, limitLoFunction.closeCount);
            Assert.assertEquals(1, limitHiFunction.closeCount);
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
    public void testGroupByFactoryDetachesAllOwnersBeforeFunctionCallback() throws Exception {
        assertMemoryLeak(() -> {
            final GroupByRecordCursorFactory factory = allocate(GroupByRecordCursorFactory.class);
            final AtomicInteger callbackCount = new AtomicInteger();
            final CallbackCloseFunction callbackFunction = new CallbackCloseFunction(() -> {
                callbackCount.incrementAndGet();
                assertFieldsNull(
                        GroupByRecordCursorFactory.class,
                        factory,
                        "base",
                        "cursor",
                        "groupByFunctions",
                        "keyFunctions",
                        "recordFunctions",
                        "sharedCursors",
                        "sharedRecordFunctions"
                );
            });
            final ObjList<Function> recordFunctions = new ObjList<>();
            recordFunctions.add(callbackFunction);
            setField(GroupByRecordCursorFactory.class, factory, "base", new CloseTrackingFactory(new GenericRecordMetadata(), null));
            setField(GroupByRecordCursorFactory.class, factory, "groupByFunctions", new ObjList<>());
            setField(GroupByRecordCursorFactory.class, factory, "keyFunctions", new ObjList<>());
            setField(GroupByRecordCursorFactory.class, factory, "recordFunctions", recordFunctions);
            setField(GroupByRecordCursorFactory.class, factory, "sharedCursors", new ObjList<>());
            setField(GroupByRecordCursorFactory.class, factory, "sharedRecordFunctions", new ObjList<>());

            factory.close();
            Assert.assertEquals(1, callbackCount.get());
            Assert.assertEquals(1, callbackFunction.closeCount);
        });
    }

    @Test
    public void testGroupByConstructorPreservesPrimaryAndAttemptsAllCleanup() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException constructionFailure = new RuntimeException("record sink configuration");
            final RuntimeException firstCloseFailure = new RuntimeException("first function close");
            final RuntimeException secondCloseFailure = new RuntimeException("second function close");
            final RuntimeException baseCloseFailure = new RuntimeException("base close");
            final CloseTrackingBooleanFunction first = new CloseTrackingBooleanFunction(firstCloseFailure);
            final CloseTrackingBooleanFunction second = new CloseTrackingBooleanFunction(secondCloseFailure);
            final ObjList<Function> recordFunctions = new ObjList<>();
            recordFunctions.add(first);
            recordFunctions.add(second);
            final CloseTrackingFactory base = new CloseTrackingFactory(new GenericRecordMetadata(), baseCloseFailure);
            final CairoConfiguration configuration = new CairoConfigurationWrapper(engine.getConfiguration()) {
                @Override
                public int getCopierType() {
                    throw constructionFailure;
                }
            };

            try {
                new GroupByRecordCursorFactory(
                        new BytecodeAssembler(),
                        configuration,
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
    public void testJoinFactoryCloseReachesSlaveAfterMasterFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException masterFailure = new RuntimeException("master close");
            final CloseTrackingFactory master = new CloseTrackingFactory(new GenericRecordMetadata(), masterFailure);
            final CloseTrackingFactory slave = new CloseTrackingFactory(new GenericRecordMetadata(), null);
            final WindowJoinRecordCursorFactory factory = allocate(WindowJoinRecordCursorFactory.class);
            setField(WindowJoinRecordCursorFactory.class, factory, "masterFactory", master);
            setField(WindowJoinRecordCursorFactory.class, factory, "slaveFactory", slave);

            try {
                factory.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(masterFailure, e);
            }
            factory.close();
            Assert.assertEquals(1, master.getCloseCount());
            Assert.assertEquals(1, slave.getCloseCount());
        });
    }

    @Test
    public void testLatestByDirectPageFrameDetachesLeafBeforeParentCallback() throws Exception {
        assertMemoryLeak(() -> {
            final LatestByValueFilteredRecordCursorFactory factory = allocate(
                    LatestByValueFilteredRecordCursorFactory.class
            );
            final AtomicInteger parentCloseCount = new AtomicInteger();
            final PartitionFrameCursorFactory partitionFrameCursorFactory = closeTrackingPartitionFactory(() -> {
                parentCloseCount.incrementAndGet();
                assertFieldsNull(LatestByValueFilteredRecordCursorFactory.class, factory, "cursor", "filter");
            });
            final AtomicInteger cursorCloseCount = new AtomicInteger();
            final PageFrameRecordCursor cursor = closeTrackingPageFrameRecordCursor(cursorCloseCount);
            final CloseTrackingBooleanFunction filter = new CloseTrackingBooleanFunction(null);
            setField(
                    Class.forName("io.questdb.griffin.engine.table.AbstractPageFrameRecordCursorFactory"),
                    factory,
                    "partitionFrameCursorFactory",
                    partitionFrameCursorFactory
            );
            setField(LatestByValueFilteredRecordCursorFactory.class, factory, "cursor", cursor);
            setField(LatestByValueFilteredRecordCursorFactory.class, factory, "filter", filter);

            factory.close();
            Assert.assertEquals(1, parentCloseCount.get());
            Assert.assertEquals(1, cursorCloseCount.get());
            Assert.assertEquals(1, filter.closeCount);
        });
    }

    @Test
    public void testLatestByTreeSetDetachesLeafBeforeParentCallback() throws Exception {
        assertMemoryLeak(() -> {
            final LatestByAllIndexedRecordCursorFactory factory = allocate(
                    LatestByAllIndexedRecordCursorFactory.class
            );
            final Class<?> abstractTreeSetFactoryClass = Class.forName(
                    "io.questdb.griffin.engine.table.AbstractTreeSetRecordCursorFactory"
            );
            final AtomicInteger parentCloseCount = new AtomicInteger();
            final PartitionFrameCursorFactory partitionFrameCursorFactory = closeTrackingPartitionFactory(() -> {
                parentCloseCount.incrementAndGet();
                assertFieldsNull(LatestByAllIndexedRecordCursorFactory.class, factory, "prefixes");
                assertFieldsNull(abstractTreeSetFactoryClass, factory, "cursor", "rows");
            });
            final AtomicInteger cursorCloseCount = new AtomicInteger();
            final PageFrameRecordCursor cursor = closeTrackingPageFrameRecordCursor(cursorCloseCount);
            final DirectLongList prefixes = new DirectLongList(2, MemoryTag.NATIVE_LATEST_BY_LONG_LIST);
            final DirectLongList rows = new DirectLongList(2, MemoryTag.NATIVE_LATEST_BY_LONG_LIST);
            setField(
                    Class.forName("io.questdb.griffin.engine.table.AbstractPageFrameRecordCursorFactory"),
                    factory,
                    "partitionFrameCursorFactory",
                    partitionFrameCursorFactory
            );
            setField(abstractTreeSetFactoryClass, factory, "cursor", cursor);
            setField(abstractTreeSetFactoryClass, factory, "rows", rows);
            setField(LatestByAllIndexedRecordCursorFactory.class, factory, "prefixes", prefixes);

            factory.close();
            Assert.assertEquals(1, parentCloseCount.get());
            Assert.assertEquals(1, cursorCloseCount.get());
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
                public @NotNull SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
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
            final AsyncWindowJoinRecordCursorFactory factory = allocate(AsyncWindowJoinRecordCursorFactory.class);
            final CallbackCloseFactory master = new CallbackCloseFactory(() -> {
                assertFieldsNull(
                        AsyncWindowJoinRecordCursorFactory.class,
                        factory,
                        "cursor",
                        "frameSequence",
                        "joinMetadata",
                        "masterFactory",
                        "slaveFactory"
                );
                throw masterFailure;
            });
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
    public void testPageFrameFactoryCloseReachesSubclassOwnerAfterSuperFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException partitionFailure = new RuntimeException("partition frame factory close");
            final AtomicInteger partitionCloseCount = new AtomicInteger();
            final PartitionFrameCursorFactory partitionFrameCursorFactory = (PartitionFrameCursorFactory) Proxy.newProxyInstance(
                    PartitionFrameCursorFactory.class.getClassLoader(),
                    new Class[]{PartitionFrameCursorFactory.class},
                    (proxy, method, args) -> {
                        if ("close".equals(method.getName())) {
                            partitionCloseCount.incrementAndGet();
                            throw partitionFailure;
                        }
                        return defaultValue(method.getReturnType());
                    }
            );
            final CloseTrackingBooleanFunction filter = new CloseTrackingBooleanFunction(null);
            final PageFrameRecordCursorFactory factory = allocate(PageFrameRecordCursorFactory.class);
            final Class<?> abstractPageFrameFactoryClass = Class.forName(
                    "io.questdb.griffin.engine.table.AbstractPageFrameRecordCursorFactory"
            );
            setField(abstractPageFrameFactoryClass, factory, "partitionFrameCursorFactory", partitionFrameCursorFactory);
            setField(PageFrameRecordCursorFactory.class, factory, "filter", filter);

            try {
                factory.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(partitionFailure, e);
            }
            factory.close();
            Assert.assertEquals(1, partitionCloseCount.get());
            Assert.assertEquals(1, filter.closeCount);
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

    @Test
    public void testSetFactoryCloseDetachesLeafAndParentBeforeCursorAndFlattensSuppression() throws Exception {
        assertMemoryLeak(() -> {
            final Class<?> factoryClass = Class.forName("io.questdb.griffin.engine.union.UnionRecordCursorFactory");
            final RecordCursorFactory factory = (RecordCursorFactory) allocate(factoryClass);
            final Class<?> abstractSetFactoryClass = Class.forName(
                    "io.questdb.griffin.engine.union.AbstractSetRecordCursorFactory"
            );
            final Class<?> cursorClass = Class.forName("io.questdb.griffin.engine.union.UnionRecordCursor");
            final Object cursor = allocate(cursorClass);
            final Error cursorFailure = new AssertionError("cursor close");
            final RuntimeException firstInputFailure = new RuntimeException("first input close");
            final RuntimeException secondInputFailure = new RuntimeException("second input close");
            final RuntimeException firstCastFailure = new RuntimeException("first cast close");
            final RuntimeException secondCastFailure = new RuntimeException("second cast close");
            final Map map = (Map) Proxy.newProxyInstance(
                    Map.class.getClassLoader(),
                    new Class[]{Map.class},
                    (proxy, method, args) -> {
                        if ("close".equals(method.getName())) {
                            Assert.assertNull(getField(abstractSetFactoryClass, factory, "cursor"));
                            Assert.assertNull(getField(abstractSetFactoryClass, factory, "factoryA"));
                            Assert.assertNull(getField(abstractSetFactoryClass, factory, "factoryB"));
                            Assert.assertNull(getField(abstractSetFactoryClass, factory, "castFunctionsA"));
                            Assert.assertNull(getField(abstractSetFactoryClass, factory, "castFunctionsB"));
                            throw cursorFailure;
                        }
                        return defaultValue(method.getReturnType());
                    }
            );
            final CloseTrackingFactory first = new CloseTrackingFactory(new GenericRecordMetadata(), firstInputFailure);
            final CloseTrackingFactory second = new CloseTrackingFactory(new GenericRecordMetadata(), secondInputFailure);
            final ObjList<Function> castFunctionsA = new ObjList<>();
            castFunctionsA.add(new CloseTrackingBooleanFunction(firstCastFailure));
            final ObjList<Function> castFunctionsB = new ObjList<>();
            castFunctionsB.add(new CloseTrackingBooleanFunction(secondCastFailure));
            setField(cursorClass, cursor, "map", map);
            setBooleanField(cursorClass, cursor, "isOpen", true);
            setField(abstractSetFactoryClass, factory, "cursor", cursor);
            setField(abstractSetFactoryClass, factory, "factoryA", first);
            setField(abstractSetFactoryClass, factory, "factoryB", second);
            setField(abstractSetFactoryClass, factory, "castFunctionsA", castFunctionsA);
            setField(abstractSetFactoryClass, factory, "castFunctionsB", castFunctionsB);

            try {
                factory.close();
                Assert.fail();
            } catch (Error e) {
                Assert.assertSame(cursorFailure, e);
                Assert.assertArrayEquals(
                        new Throwable[]{firstInputFailure, secondInputFailure, firstCastFailure, secondCastFailure},
                        e.getSuppressed()
                );
            }
            Assert.assertEquals(1, first.getCloseCount());
            Assert.assertEquals(1, second.getCloseCount());
        });
    }

    @Test
    public void testSetFactoryCloseDetachesAliasedInputsBeforeCallback() throws Exception {
        assertMemoryLeak(() -> {
            final Class<?> factoryClass = Class.forName("io.questdb.griffin.engine.union.UnionRecordCursorFactory");
            final Object factory = allocate(factoryClass);
            final Class<?> abstractSetFactoryClass = Class.forName(
                    "io.questdb.griffin.engine.union.AbstractSetRecordCursorFactory"
            );
            final AtomicInteger closeCount = new AtomicInteger();
            final AtomicReference<Object> factoryReference = new AtomicReference<>(factory);
            final RecordCursorFactory input = (RecordCursorFactory) Proxy.newProxyInstance(
                    RecordCursorFactory.class.getClassLoader(),
                    new Class[]{RecordCursorFactory.class},
                    (proxy, method, args) -> {
                        if ("close".equals(method.getName())) {
                            closeCount.incrementAndGet();
                            Assert.assertNull(getField(abstractSetFactoryClass, factoryReference.get(), "factoryA"));
                            Assert.assertNull(getField(abstractSetFactoryClass, factoryReference.get(), "factoryB"));
                        } else if ("getMetadata".equals(method.getName())) {
                            return new GenericRecordMetadata();
                        }
                        return defaultValue(method.getReturnType());
                    }
            );
            setField(abstractSetFactoryClass, factory, "factoryA", input);
            setField(abstractSetFactoryClass, factory, "factoryB", input);
            setField(abstractSetFactoryClass, factory, "castFunctionsA", new ObjList<>());
            setField(abstractSetFactoryClass, factory, "castFunctionsB", new ObjList<>());

            ((RecordCursorFactory) factory).close();
            Assert.assertEquals(1, closeCount.get());
        });
    }

    @Test
    public void testSetFactoryCloseReachesSecondInputAfterFirstFailure() throws Exception {
        assertMemoryLeak(() -> {
            final Error firstFailure = new AssertionError("first input close");
            final RuntimeException secondFailure = new RuntimeException("second input close");
            final CloseTrackingFactory first = new CloseTrackingErrorFactory(firstFailure);
            final CloseTrackingFactory second = new CloseTrackingFactory(new GenericRecordMetadata(), secondFailure);
            final Class<?> factoryClass = Class.forName("io.questdb.griffin.engine.union.UnionRecordCursorFactory");
            final Object factory = allocate(factoryClass);
            final Class<?> abstractSetFactoryClass = Class.forName(
                    "io.questdb.griffin.engine.union.AbstractSetRecordCursorFactory"
            );
            setField(abstractSetFactoryClass, factory, "factoryA", first);
            setField(abstractSetFactoryClass, factory, "factoryB", second);
            setField(abstractSetFactoryClass, factory, "castFunctionsA", new ObjList<>());
            setField(abstractSetFactoryClass, factory, "castFunctionsB", new ObjList<>());

            try {
                ((RecordCursorFactory) factory).close();
                Assert.fail();
            } catch (Error e) {
                Assert.assertSame(firstFailure, e);
                Assert.assertArrayEquals(new Throwable[]{secondFailure}, e.getSuppressed());
            }
            ((RecordCursorFactory) factory).close();
            Assert.assertEquals(1, first.getCloseCount());
            Assert.assertEquals(1, second.getCloseCount());
        });
    }

    @Test
    public void testSortedFactoryCloseReachesCursorAfterBaseFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException baseFailure = new RuntimeException("base close");
            final CloseTrackingFactory base = new CloseTrackingFactory(new GenericRecordMetadata(), baseFailure);
            final AtomicInteger cursorCloseCount = new AtomicInteger();
            final RecordCursor baseCursor = (RecordCursor) Proxy.newProxyInstance(
                    RecordCursor.class.getClassLoader(),
                    new Class[]{RecordCursor.class},
                    (proxy, method, args) -> {
                        if ("close".equals(method.getName())) {
                            cursorCloseCount.incrementAndGet();
                        }
                        return defaultValue(method.getReturnType());
                    }
            );
            final Class<?> sortedCursorClass = Class.forName("io.questdb.griffin.engine.orderby.SortedRecordCursor");
            final Object sortedCursor = allocate(sortedCursorClass);
            setField(sortedCursorClass, sortedCursor, "baseCursor", baseCursor);
            setField(sortedCursorClass, sortedCursor, "rankMaps", new ObjList<>());
            setBooleanField(sortedCursorClass, sortedCursor, "isOpen", true);
            final SortedRecordCursorFactory factory = allocate(SortedRecordCursorFactory.class);
            setField(SortedRecordCursorFactory.class, factory, "base", base);
            setField(SortedRecordCursorFactory.class, factory, "cursor", sortedCursor);

            try {
                factory.close();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertSame(baseFailure, e);
            }
            factory.close();
            Assert.assertEquals(1, base.getCloseCount());
            Assert.assertEquals(1, cursorCloseCount.get());
        });
    }

    @Test
    public void testUnionAllFactoryClosesOpenCursorAndFlattensSuppression() throws Exception {
        assertMemoryLeak(() -> {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            final Error firstCursorFailure = new AssertionError("first cursor close");
            final RuntimeException secondCursorFailure = new RuntimeException("second cursor close");
            final RuntimeException firstInputFailure = new RuntimeException("first input close");
            final RuntimeException secondInputFailure = new RuntimeException("second input close");
            final RuntimeException firstCastFailure = new RuntimeException("first cast close");
            final RuntimeException secondCastFailure = new RuntimeException("second cast close");
            final AtomicInteger firstCursorCloseCount = new AtomicInteger();
            final AtomicInteger secondCursorCloseCount = new AtomicInteger();
            final RecordCursor firstCursor = (RecordCursor) Proxy.newProxyInstance(
                    RecordCursor.class.getClassLoader(),
                    new Class[]{RecordCursor.class},
                    (proxy, method, args) -> {
                        if ("close".equals(method.getName())) {
                            firstCursorCloseCount.incrementAndGet();
                            throw firstCursorFailure;
                        }
                        return defaultValue(method.getReturnType());
                    }
            );
            final RecordCursor secondCursor = (RecordCursor) Proxy.newProxyInstance(
                    RecordCursor.class.getClassLoader(),
                    new Class[]{RecordCursor.class},
                    (proxy, method, args) -> {
                        if ("close".equals(method.getName())) {
                            secondCursorCloseCount.incrementAndGet();
                            throw secondCursorFailure;
                        }
                        return defaultValue(method.getReturnType());
                    }
            );
            final AtomicInteger firstInputCloseCount = new AtomicInteger();
            final AtomicInteger secondInputCloseCount = new AtomicInteger();
            final AtomicReference<UnionAllRecordCursorFactory> factoryReference = new AtomicReference<>();
            final Class<?> abstractSetFactoryClass = Class.forName(
                    "io.questdb.griffin.engine.union.AbstractSetRecordCursorFactory"
            );
            final RecordCursorFactory firstInput = (RecordCursorFactory) Proxy.newProxyInstance(
                    RecordCursorFactory.class.getClassLoader(),
                    new Class[]{RecordCursorFactory.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "close" -> {
                            firstInputCloseCount.incrementAndGet();
                            assertFieldsNull(
                                    abstractSetFactoryClass,
                                    factoryReference.get(),
                                    "cursor",
                                    "factoryA",
                                    "factoryB",
                                    "castFunctionsA",
                                    "castFunctionsB"
                            );
                            throw firstInputFailure;
                        }
                        case "getCursor" -> firstCursor;
                        case "getMetadata" -> metadata;
                        default -> defaultValue(method.getReturnType());
                    }
            );
            final RecordCursorFactory secondInput = (RecordCursorFactory) Proxy.newProxyInstance(
                    RecordCursorFactory.class.getClassLoader(),
                    new Class[]{RecordCursorFactory.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "close" -> {
                            secondInputCloseCount.incrementAndGet();
                            throw secondInputFailure;
                        }
                        case "getCursor" -> secondCursor;
                        case "getMetadata" -> metadata;
                        default -> defaultValue(method.getReturnType());
                    }
            );
            final CloseTrackingBooleanFunction firstCast = new CloseTrackingBooleanFunction(firstCastFailure);
            final CloseTrackingBooleanFunction secondCast = new CloseTrackingBooleanFunction(secondCastFailure);
            final ObjList<Function> castFunctionsA = new ObjList<>();
            castFunctionsA.add(firstCast);
            final ObjList<Function> castFunctionsB = new ObjList<>();
            castFunctionsB.add(secondCast);
            final UnionAllRecordCursorFactory factory = new UnionAllRecordCursorFactory(
                    metadata,
                    firstInput,
                    secondInput,
                    castFunctionsA,
                    castFunctionsB
            );
            factoryReference.set(factory);
            Assert.assertNotNull(factory.getCursor(sqlExecutionContext));

            try {
                factory.close();
                Assert.fail();
            } catch (Error e) {
                Assert.assertSame(firstCursorFailure, e);
                Assert.assertArrayEquals(
                        new Throwable[]{
                                secondCursorFailure,
                                firstInputFailure,
                                secondInputFailure,
                                firstCastFailure,
                                secondCastFailure
                        },
                        e.getSuppressed()
                );
            }
            factory.close();
            Assert.assertEquals(1, firstCursorCloseCount.get());
            Assert.assertEquals(1, secondCursorCloseCount.get());
            Assert.assertEquals(1, firstInputCloseCount.get());
            Assert.assertEquals(1, secondInputCloseCount.get());
            Assert.assertEquals(1, firstCast.closeCount);
            Assert.assertEquals(1, secondCast.closeCount);
        });
    }

    @Test
    public void testVectorGroupByDetachesContainersAndRostiEntryBeforeFreeCallback() throws Exception {
        assertMemoryLeak(() -> {
            final Class<?> factoryClass = Class.forName(
                    "io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory"
            );
            final RecordCursorFactory factory = (RecordCursorFactory) allocate(factoryClass);
            final long[] pRosti = {11, 22};
            final AtomicInteger freeCount = new AtomicInteger();
            final CloseTrackingFactory base = new CloseTrackingFactory(new GenericRecordMetadata(), null);
            final RostiAllocFacade raf = new RostiAllocFacade() {
                @Override
                public long alloc(io.questdb.cairo.ColumnTypes types, long capacity) {
                    return 0;
                }

                @Override
                public void clear(long pRosti) {
                }

                @Override
                public void free(long pointer) {
                    assertFieldsNull(
                            factoryClass,
                            factory,
                            "base",
                            "frameMemoryPools",
                            "pRosti",
                            "sharedCursors",
                            "vafList"
                    );
                    final int index = freeCount.getAndIncrement();
                    Assert.assertEquals(0, pRosti[index]);
                    Assert.assertEquals(index == 0 ? 11 : 22, pointer);
                }

                @Override
                public long getSize(long pRosti) {
                    return 0;
                }

                @Override
                public boolean reset(long pRosti, int toSize) {
                    return false;
                }

                @Override
                public void updateMemoryUsage(long pRosti, long oldSize) {
                }
            };
            setField(factoryClass, factory, "base", base);
            setField(factoryClass, factory, "frameMemoryPools", new ObjList<>());
            setField(factoryClass, factory, "pRosti", pRosti);
            setField(factoryClass, factory, "raf", raf);
            setField(factoryClass, factory, "sharedCursors", new ObjList<>());
            setField(factoryClass, factory, "vafList", new ObjList<>());

            factory.close();
            Assert.assertEquals(2, freeCount.get());
            Assert.assertArrayEquals(new long[]{0, 0}, pRosti);
            Assert.assertEquals(1, base.getCloseCount());
        });
    }

    private static <T> T allocate(Class<T> clazz) throws InstantiationException {
        return (T) Unsafe.getUnsafe().allocateInstance(clazz);
    }

    private static void assertFieldsNull(Class<?> clazz, Object instance, String... names) {
        try {
            for (int i = 0, n = names.length; i < n; i++) {
                Assert.assertNull(names[i], getField(clazz, instance, names[i]));
            }
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }

    private static PartitionFrameCursorFactory closeTrackingPartitionFactory(Runnable closeCallback) {
        return (PartitionFrameCursorFactory) Proxy.newProxyInstance(
                PartitionFrameCursorFactory.class.getClassLoader(),
                new Class[]{PartitionFrameCursorFactory.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        closeCallback.run();
                    }
                    return defaultValue(method.getReturnType());
                }
        );
    }

    private static PageFrameRecordCursor closeTrackingPageFrameRecordCursor(AtomicInteger closeCount) {
        return (PageFrameRecordCursor) Proxy.newProxyInstance(
                PageFrameRecordCursor.class.getClassLoader(),
                new Class[]{PageFrameRecordCursor.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        closeCount.incrementAndGet();
                    }
                    return defaultValue(method.getReturnType());
                }
        );
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive() || type == void.class) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == char.class) {
            return (char) 0;
        }
        if (type == double.class) {
            return 0.0;
        }
        if (type == float.class) {
            return 0.0f;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == short.class) {
            return (short) 0;
        }
        throw new AssertionError(type);
    }

    private static Object getField(Class<?> clazz, Object instance, String name) throws NoSuchFieldException {
        final Field field = clazz.getDeclaredField(name);
        return Unsafe.getUnsafe().getObject(instance, Unsafe.objectFieldOffset(field));
    }

    private static GenericRecordMetadata joinMetadata() {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("master_ts", ColumnType.TIMESTAMP));
        metadata.add(new TableColumnMetadata("slave_ts", ColumnType.TIMESTAMP));
        metadata.setTimestampIndex(0);
        return metadata;
    }

    private static void setBooleanField(Class<?> clazz, Object instance, String name, boolean isValue) throws NoSuchFieldException {
        final Field field = clazz.getDeclaredField(name);
        Unsafe.getUnsafe().putBoolean(instance, Unsafe.objectFieldOffset(field), isValue);
    }

    private static void setField(Class<?> clazz, Object instance, String name, Object value) throws NoSuchFieldException {
        final Field field = clazz.getDeclaredField(name);
        Unsafe.putObject(instance, Unsafe.objectFieldOffset(field), value);
    }

    private static GenericRecordMetadata timestampMetadata(String name) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata(name, ColumnType.TIMESTAMP));
        metadata.setTimestampIndex(0);
        return metadata;
    }

    private static class CallbackCloseFactory extends CloseTrackingFactory {
        private final Runnable closeCallback;

        private CallbackCloseFactory(Runnable closeCallback) {
            super(new GenericRecordMetadata(), null);
            this.closeCallback = closeCallback;
        }

        @Override
        protected void _close() {
            super._close();
            closeCallback.run();
        }
    }

    private static class CallbackCloseFunction extends BooleanFunction {
        private final Runnable closeCallback;
        private int closeCount;

        private CallbackCloseFunction(Runnable closeCallback) {
            this.closeCallback = closeCallback;
        }

        @Override
        public void close() {
            closeCount++;
            closeCallback.run();
        }

        @Override
        public boolean getBool(Record rec) {
            return true;
        }
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

    private static class CloseTrackingErrorFactory extends CloseTrackingFactory {
        private final Error closeFailure;

        private CloseTrackingErrorFactory(Error closeFailure) {
            super(new GenericRecordMetadata(), null);
            this.closeFailure = closeFailure;
        }

        @Override
        protected void _close() {
            super._close();
            throw closeFailure;
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

    private static class CloseTrackingUnorderedPageFrameSequence extends UnorderedPageFrameSequence<AsyncGroupByAtom> {
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
