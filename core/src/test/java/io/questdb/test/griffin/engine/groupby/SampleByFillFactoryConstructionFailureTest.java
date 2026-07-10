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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SingleSymbolFilter;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.SampleByFillNoneNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillNoneRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillNullNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillNullRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillPrevNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillPrevRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillValueNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillValueRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFirstLastRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByInterpolateRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SimpleTimestampSampler;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The SAMPLE BY factories run fallible work after their superclass constructor adopted the
 * record functions, the base factory, the map (keyed variants), and the temporal parameter
 * functions: record-sink and group-by updater bytecode generation, placeholder-function
 * assembly, and cursor construction. Java cannot run close() on the unreturned partial object,
 * and the generator has already transferred ownership, so the constructors themselves must free
 * every adopted resource exactly once when that post-super work throws. Every fill variant
 * (none/null/prev/value), keyed and not-keyed, plus the interpolation and index-backed
 * first/last factories carry their own constructor branch; each is covered here with an
 * injected deterministic failure and exact close-count assertions.
 */
public class SampleByFillFactoryConstructionFailureTest extends AbstractCairoTest {

    private static final String SINK_FAILURE = "injected record sink generation failure";
    private static final String SYMBOL_FILTER_FAILURE = "injected symbol filter failure";
    private static final String UPDATER_FAILURE = "injected updater generation failure";

    @Test
    public void testFillNoneConstructorFailureClosesAdoptedResources() throws Exception {
        // keyed fill(none) builds its own sink, map, and updater after super() adopted resources
        assertConstructionFailureClosesAdoptedResources(UPDATER_FAILURE, true, fixture ->
                new SampleByFillNoneRecordCursorFactory(
                        new TargetFailingAssembler(GroupByFunctionsUpdater.class, UPDATER_FAILURE),
                        configuration,
                        fixture.base(),
                        fixture.groupByMetadata,
                        new ObjList<>(),
                        fixture.recordFunctions,
                        fixture.sampler(),
                        fixture.listColumnFilter,
                        fixture.keyTypes,
                        fixture.valueTypes,
                        1,
                        ColumnType.TIMESTAMP,
                        fixture.timezoneNameFunc,
                        0,
                        fixture.offsetFunc,
                        0,
                        fixture.sampleFromFunc,
                        0,
                        fixture.sampleToFunc,
                        0
                )
        );
    }

    @Test
    public void testFillNoneNotKeyedConstructorFailureClosesAdoptedResources() throws Exception {
        assertConstructionFailureClosesAdoptedResources(UPDATER_FAILURE, true, fixture ->
                new SampleByFillNoneNotKeyedRecordCursorFactory(
                        new TargetFailingAssembler(GroupByFunctionsUpdater.class, UPDATER_FAILURE),
                        configuration,
                        fixture.base(),
                        fixture.sampler(),
                        fixture.groupByMetadata,
                        new ObjList<>(),
                        fixture.recordFunctions,
                        1,
                        1,
                        ColumnType.TIMESTAMP,
                        fixture.timezoneNameFunc,
                        0,
                        fixture.offsetFunc,
                        0,
                        fixture.sampleFromFunc,
                        0,
                        fixture.sampleToFunc,
                        0
                )
        );
    }

    @Test
    public void testFillNullConstructorFailureClosesAdoptedResources() throws Exception {
        assertConstructionFailureClosesAdoptedResources(UPDATER_FAILURE, true, fixture ->
                new SampleByFillNullRecordCursorFactory(
                        new TargetFailingAssembler(GroupByFunctionsUpdater.class, UPDATER_FAILURE),
                        configuration,
                        fixture.base(),
                        fixture.sampler(),
                        fixture.listColumnFilter,
                        fixture.keyTypes,
                        fixture.valueTypes,
                        fixture.groupByMetadata,
                        new ObjList<>(),
                        fixture.recordFunctions,
                        fixture.recordFunctionPositions,
                        1,
                        ColumnType.TIMESTAMP,
                        fixture.timezoneNameFunc,
                        0,
                        fixture.offsetFunc,
                        0,
                        fixture.sampleFromFunc,
                        0,
                        fixture.sampleToFunc,
                        0
                )
        );
    }

    @Test
    public void testFillNullNotKeyedConstructorFailureClosesAdoptedResources() throws Exception {
        assertConstructionFailureClosesAdoptedResources(UPDATER_FAILURE, true, fixture ->
                new SampleByFillNullNotKeyedRecordCursorFactory(
                        new TargetFailingAssembler(GroupByFunctionsUpdater.class, UPDATER_FAILURE),
                        configuration,
                        fixture.base(),
                        fixture.sampler(),
                        fixture.groupByMetadata,
                        new ObjList<>(),
                        fixture.recordFunctions,
                        fixture.recordFunctionPositions,
                        1,
                        1,
                        ColumnType.TIMESTAMP,
                        fixture.timezoneNameFunc,
                        0,
                        fixture.offsetFunc,
                        0,
                        fixture.sampleFromFunc,
                        0,
                        fixture.sampleToFunc,
                        0
                )
        );
    }

    @Test
    public void testFillPrevConstructorFailureClosesAdoptedResources() throws Exception {
        assertConstructionFailureClosesAdoptedResources(UPDATER_FAILURE, true, fixture ->
                new SampleByFillPrevRecordCursorFactory(
                        new TargetFailingAssembler(GroupByFunctionsUpdater.class, UPDATER_FAILURE),
                        configuration,
                        fixture.base(),
                        fixture.sampler(),
                        fixture.listColumnFilter,
                        fixture.keyTypes,
                        fixture.valueTypes,
                        fixture.groupByMetadata,
                        new ObjList<>(),
                        fixture.recordFunctions,
                        1,
                        ColumnType.TIMESTAMP,
                        fixture.timezoneNameFunc,
                        0,
                        fixture.offsetFunc,
                        0,
                        fixture.sampleFromFunc,
                        0,
                        fixture.sampleToFunc,
                        0
                )
        );
    }

    @Test
    public void testFillPrevNotKeyedConstructorFailureClosesAdoptedResources() throws Exception {
        assertConstructionFailureClosesAdoptedResources(UPDATER_FAILURE, true, fixture ->
                new SampleByFillPrevNotKeyedRecordCursorFactory(
                        new TargetFailingAssembler(GroupByFunctionsUpdater.class, UPDATER_FAILURE),
                        configuration,
                        fixture.base(),
                        fixture.sampler(),
                        fixture.groupByMetadata,
                        new ObjList<>(),
                        fixture.recordFunctions,
                        1,
                        ColumnType.TIMESTAMP,
                        1,
                        fixture.timezoneNameFunc,
                        0,
                        fixture.offsetFunc,
                        0,
                        fixture.sampleFromFunc,
                        0,
                        fixture.sampleToFunc,
                        0
                )
        );
    }

    @Test
    public void testFillValueConstructorFailureClosesAdoptedResources() throws Exception {
        assertConstructionFailureClosesAdoptedResources(UPDATER_FAILURE, true, fixture ->
                new SampleByFillValueRecordCursorFactory(
                        new TargetFailingAssembler(GroupByFunctionsUpdater.class, UPDATER_FAILURE),
                        configuration,
                        fixture.base(),
                        fixture.sampler(),
                        fixture.listColumnFilter,
                        new ObjList<>(),
                        fixture.keyTypes,
                        fixture.valueTypes,
                        fixture.groupByMetadata,
                        new ObjList<>(),
                        fixture.recordFunctions,
                        fixture.recordFunctionPositions,
                        1,
                        ColumnType.TIMESTAMP,
                        fixture.timezoneNameFunc,
                        0,
                        fixture.offsetFunc,
                        0,
                        fixture.sampleFromFunc,
                        0,
                        fixture.sampleToFunc,
                        0
                )
        );
    }

    @Test
    public void testFillValueNotKeyedConstructorFailureClosesAdoptedResources() throws Exception {
        assertConstructionFailureClosesAdoptedResources(UPDATER_FAILURE, true, fixture ->
                new SampleByFillValueNotKeyedRecordCursorFactory(
                        new TargetFailingAssembler(GroupByFunctionsUpdater.class, UPDATER_FAILURE),
                        configuration,
                        fixture.base(),
                        fixture.sampler(),
                        new ObjList<>(),
                        fixture.groupByMetadata,
                        new ObjList<>(),
                        fixture.recordFunctions,
                        fixture.recordFunctionPositions,
                        1,
                        1,
                        ColumnType.TIMESTAMP,
                        fixture.timezoneNameFunc,
                        0,
                        fixture.offsetFunc,
                        0,
                        fixture.sampleFromFunc,
                        0,
                        fixture.sampleToFunc,
                        0
                )
        );
    }

    @Test
    public void testFirstLastConstructorFailureClosesAdoptedResources() throws Exception {
        // the index-backed first/last factory adopts the temporal parameter functions first;
        // a failure in the very next statement must still reach them through close()
        assertConstructionFailureClosesAdoptedResources(SYMBOL_FILTER_FAILURE, false, fixture ->
                new SampleByFirstLastRecordCursorFactory(
                        fixture.base(),
                        fixture.sampler(),
                        fixture.groupByMetadata,
                        new ObjList<>(),
                        fixture.baseMetadata,
                        fixture.timezoneNameFunc,
                        0,
                        fixture.offsetFunc,
                        0,
                        1,
                        new SingleSymbolFilter() {
                            @Override
                            public int getColumnIndex() {
                                throw CairoException.nonCritical().put(SYMBOL_FILTER_FAILURE);
                            }

                            @Override
                            public int getSymbolFilterKey() {
                                return 0;
                            }
                        },
                        16,
                        fixture.sampleFromFunc,
                        0,
                        fixture.sampleToFunc,
                        0
                )
        );
    }

    @Test
    public void testInterpolateConstructorFailureClosesAdoptedResources() throws Exception {
        // fill(linear): the record-sink generation is the last fallible step before the cursor
        // is constructed; the catch must free the adopted record and temporal functions
        assertConstructionFailureClosesAdoptedResources(SINK_FAILURE, true, fixture ->
                new SampleByInterpolateRecordCursorFactory(
                        new TargetFailingAssembler(RecordSink.class, SINK_FAILURE),
                        configuration,
                        fixture.base(),
                        fixture.groupByMetadata,
                        new ObjList<>(),
                        fixture.recordFunctions,
                        fixture.sampler(),
                        QueryModel.FACTORY.newInstance(),
                        fixture.listColumnFilter,
                        new ArrayColumnTypes(),
                        new ArrayColumnTypes(),
                        new EntityColumnFilter(),
                        new IntList(),
                        1,
                        ColumnType.TIMESTAMP,
                        fixture.timezoneNameFunc,
                        0,
                        fixture.offsetFunc,
                        0,
                        fixture.sampleFromFunc,
                        fixture.sampleToFunc
                )
        );
    }

    @Test
    public void testKeyedSuperConstructorSinkFailureClosesAdoptedResources() throws Exception {
        // the shared keyed-fill superclass generates the record sink itself; when that throws,
        // its own catch - not the leaf constructor's - must free the adopted resources
        assertConstructionFailureClosesAdoptedResources(SINK_FAILURE, true, fixture ->
                new SampleByFillPrevRecordCursorFactory(
                        new TargetFailingAssembler(RecordSink.class, SINK_FAILURE),
                        configuration,
                        fixture.base(),
                        fixture.sampler(),
                        fixture.listColumnFilter,
                        fixture.keyTypes,
                        fixture.valueTypes,
                        fixture.groupByMetadata,
                        new ObjList<>(),
                        fixture.recordFunctions,
                        1,
                        ColumnType.TIMESTAMP,
                        fixture.timezoneNameFunc,
                        0,
                        fixture.offsetFunc,
                        0,
                        fixture.sampleFromFunc,
                        0,
                        fixture.sampleToFunc,
                        0
                )
        );
    }

    private void assertConstructionFailureClosesAdoptedResources(
            String expectedError,
            boolean recordFunctionsAdopted,
            FactoryConstructor constructor
    ) throws Exception {
        assertMemoryLeak(() -> {
            final Fixture fixture = new Fixture();
            try {
                constructor.construct(fixture);
                Assert.fail("injected construction failure expected");
            } catch (Throwable e) {
                TestUtils.assertContains(e.getMessage(), expectedError);
            }
            if (recordFunctionsAdopted) {
                Assert.assertEquals("record functions must close exactly once", 1, fixture.recordFunc.closeCount);
            }
            Assert.assertEquals("timezone function must close exactly once", 1, fixture.timezoneNameFunc.closeCount);
            Assert.assertEquals("offset function must close exactly once", 1, fixture.offsetFunc.closeCount);
            Assert.assertEquals("FROM function must close exactly once", 1, fixture.sampleFromFunc.closeCount);
            Assert.assertEquals("TO function must close exactly once", 1, fixture.sampleToFunc.closeCount);
        });
    }

    @FunctionalInterface
    private interface FactoryConstructor {
        void construct(Fixture fixture) throws Exception;
    }

    private static class CloseCountingFunction extends LongFunction {
        int closeCount;

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }
    }

    private static class Fixture {
        final GenericRecordMetadata baseMetadata = new GenericRecordMetadata();
        final GenericRecordMetadata groupByMetadata = new GenericRecordMetadata();
        final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        final ListColumnFilter listColumnFilter = new ListColumnFilter();
        final CloseCountingFunction offsetFunc = new CloseCountingFunction();
        final CloseCountingFunction recordFunc = new CloseCountingFunction();
        final IntList recordFunctionPositions = new IntList();
        final ObjList<Function> recordFunctions = new ObjList<>();
        final CloseCountingFunction sampleFromFunc = new CloseCountingFunction();
        final CloseCountingFunction sampleToFunc = new CloseCountingFunction();
        final CloseCountingFunction timezoneNameFunc = new CloseCountingFunction();
        final ArrayColumnTypes valueTypes = new ArrayColumnTypes();

        Fixture() {
            baseMetadata.add(new TableColumnMetadata("k", ColumnType.INT));
            baseMetadata.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP));
            baseMetadata.setTimestampIndex(1);
            groupByMetadata.add(new TableColumnMetadata("k", ColumnType.INT));
            groupByMetadata.add(new TableColumnMetadata("c", ColumnType.LONG));
            listColumnFilter.add(1);
            keyTypes.add(ColumnType.INT);
            valueTypes.add(ColumnType.LONG);
            recordFunctions.add(recordFunc);
            recordFunctionPositions.add(0);
        }

        EmptyTableRecordCursorFactory base() {
            return new EmptyTableRecordCursorFactory(baseMetadata);
        }

        SimpleTimestampSampler sampler() {
            return new SimpleTimestampSampler(100L, ColumnType.TIMESTAMP);
        }
    }

    /**
     * Lets every other bytecode generation succeed and fails deterministically when asked to
     * assemble the target class, simulating a bytecode/allocation failure at that exact point
     * of the construction.
     */
    private static class TargetFailingAssembler extends BytecodeAssembler {
        private final Class<?> failOn;
        private final String message;

        TargetFailingAssembler(Class<?> failOn, String message) {
            this.failOn = failOn;
            this.message = message;
        }

        @Override
        public void init(Class<?> host) {
            if (host == failOn) {
                throw CairoException.nonCritical().put(message);
            }
            super.init(host);
        }
    }
}
