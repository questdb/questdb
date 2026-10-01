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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SingleSymbolFilter;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRandomRecordCursor;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.SampleByFillNoneNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillNoneRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillNullNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillNullRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillPrevNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillPrevRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillValueNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillValueRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFirstLastRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByInterpolateRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SimpleTimestampSampler;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
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
    public void testFillNoneNotKeyedConstructorFailureKeepsPrimaryWhenCleanupThrows() throws Exception {
        assertMemoryLeak(() -> {
            final Fixture fixture = new Fixture();
            final ThrowingCloseFunction throwingRecordFunc = new ThrowingCloseFunction("injected record cleanup failure");
            final ThrowingCloseFunction throwingTimezoneFunc = new ThrowingCloseFunction("injected timezone cleanup failure");
            final ThrowingCloseFunction throwingSampleFromFunc = new ThrowingCloseFunction("injected FROM cleanup failure");
            fixture.recordFunctions.clear();
            fixture.recordFunctions.add(throwingRecordFunc);
            fixture.recordFunctions.add(fixture.recordFunc);
            final CloseCountingBaseFactory base = fixture.base();
            base.closeFailure = "injected base cleanup failure";

            final Throwable e = Assert.assertThrows(Throwable.class, () ->
                    new SampleByFillNoneNotKeyedRecordCursorFactory(
                            new TargetFailingAssembler(GroupByFunctionsUpdater.class, UPDATER_FAILURE),
                            configuration,
                            base,
                            fixture.sampler(),
                            fixture.groupByMetadata,
                            new ObjList<>(),
                            fixture.recordFunctions,
                            1,
                            1,
                            ColumnType.TIMESTAMP,
                            throwingTimezoneFunc,
                            0,
                            fixture.offsetFunc,
                            0,
                            throwingSampleFromFunc,
                            0,
                            fixture.sampleToFunc,
                            0
                    )
            );

            TestUtils.assertContains(e.getMessage(), UPDATER_FAILURE);
            Assert.assertEquals(1, e.getSuppressed().length);
            final Throwable cleanupFailure = e.getSuppressed()[0];
            TestUtils.assertContains(cleanupFailure.getMessage(), "injected record cleanup failure");
            Assert.assertEquals(3, cleanupFailure.getSuppressed().length);
            TestUtils.assertContains(cleanupFailure.getSuppressed()[0].getMessage(), "injected base cleanup failure");
            TestUtils.assertContains(cleanupFailure.getSuppressed()[1].getMessage(), "injected timezone cleanup failure");
            TestUtils.assertContains(cleanupFailure.getSuppressed()[2].getMessage(), "injected FROM cleanup failure");
            Assert.assertEquals(1, throwingRecordFunc.closeCount);
            Assert.assertEquals("later record functions must close despite the earlier failure", 1, fixture.recordFunc.closeCount);
            Assert.assertEquals(1, base.closeCount);
            Assert.assertEquals(1, throwingTimezoneFunc.closeCount);
            Assert.assertEquals(1, fixture.offsetFunc.closeCount);
            Assert.assertEquals(1, throwingSampleFromFunc.closeCount);
            Assert.assertEquals(1, fixture.sampleToFunc.closeCount);
        });
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
    public void testFillRecordFunctionInitFailureReleasesMapAndAllowsRetry() throws Exception {
        assertFillRecordFunctionInitFailureReleasesMapAndAllowsRetry(false);
    }

    @Test
    public void testFillRecordFunctionInitFailureWithThrowingCloseReleasesMapAndAllowsRetry() throws Exception {
        assertFillRecordFunctionInitFailureReleasesMapAndAllowsRetry(true);
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
                        configuration,
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
    public void testFirstLastConstructorFailureKeepsPrimaryWhenBaseCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            final Fixture fixture = new Fixture();
            final CloseCountingBaseFactory base = fixture.base();
            final ThrowingCloseFunction throwingTimezoneFunc = new ThrowingCloseFunction("injected timezone cleanup failure");
            final ThrowingCloseFunction throwingSampleFromFunc = new ThrowingCloseFunction("injected FROM cleanup failure");
            base.closeFailure = "injected base cleanup failure";

            final Throwable e = Assert.assertThrows(Throwable.class, () ->
                    new SampleByFirstLastRecordCursorFactory(
                            configuration,
                            base,
                            fixture.sampler(),
                            fixture.groupByMetadata,
                            new ObjList<>(),
                            fixture.baseMetadata,
                            throwingTimezoneFunc,
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
                            throwingSampleFromFunc,
                            0,
                            fixture.sampleToFunc,
                            0
                    )
            );

            TestUtils.assertContains(e.getMessage(), SYMBOL_FILTER_FAILURE);
            Assert.assertEquals(1, e.getSuppressed().length);
            final Throwable cleanupFailure = e.getSuppressed()[0];
            TestUtils.assertContains(cleanupFailure.getMessage(), "injected base cleanup failure");
            Assert.assertEquals(2, cleanupFailure.getSuppressed().length);
            TestUtils.assertContains(cleanupFailure.getSuppressed()[0].getMessage(), "injected timezone cleanup failure");
            TestUtils.assertContains(cleanupFailure.getSuppressed()[1].getMessage(), "injected FROM cleanup failure");
            Assert.assertEquals(1, base.closeCount);
            Assert.assertEquals(1, throwingTimezoneFunc.closeCount);
            Assert.assertEquals(1, fixture.offsetFunc.closeCount);
            Assert.assertEquals(1, throwingSampleFromFunc.closeCount);
            Assert.assertEquals(1, fixture.sampleToFunc.closeCount);

            // A second owner may still hold the base after the failed construction. Its close
            // must neither throw again nor retry resources from the first close attempt.
            base.close();
            Assert.assertEquals(1, base.closeCount);
        });
    }

    @Test
    public void testInterpolateCloseClosesAliasedTemporalFunctionsOnce() throws Exception {
        assertMemoryLeak(() -> {
            final Fixture fixture = new Fixture();
            final ThrowingCloseFunction first = new ThrowingCloseFunction("first temporal cleanup failure");
            final ThrowingCloseFunction second = new ThrowingCloseFunction("second temporal cleanup failure");
            final ThrowingCloseFunction third = new ThrowingCloseFunction("third temporal cleanup failure");
            final SampleByInterpolateRecordCursorFactory factory = new SampleByInterpolateRecordCursorFactory(
                    new BytecodeAssembler(),
                    configuration,
                    fixture.base(),
                    fixture.groupByMetadata,
                    new ObjList<>(),
                    fixture.recordFunctions,
                    fixture.sampler(),
                    QueryModel.FACTORY.newInstance(),
                    fixture.listColumnFilter,
                    fixture.keyTypes,
                    fixture.valueTypes,
                    new EntityColumnFilter(),
                    new IntList(),
                    1,
                    ColumnType.TIMESTAMP,
                    first,
                    0,
                    second,
                    0,
                    first,
                    third
            );

            final Throwable e = Assert.assertThrows(Throwable.class, factory::close);
            Assert.assertSame(first.failure, e);
            Assert.assertArrayEquals(new Throwable[]{second.failure, third.failure}, e.getSuppressed());
            Assert.assertEquals(1, first.closeCount);
            Assert.assertEquals(1, second.closeCount);
            Assert.assertEquals(1, third.closeCount);

            factory.close();
            Assert.assertEquals(1, first.closeCount);
            Assert.assertEquals(1, second.closeCount);
            Assert.assertEquals(1, third.closeCount);

            final Fixture aliasedFixture = new Fixture();
            final ThrowingCloseFunction aliased = new ThrowingCloseFunction("aliased temporal cleanup failure");
            final SampleByInterpolateRecordCursorFactory aliasedFactory = new SampleByInterpolateRecordCursorFactory(
                    new BytecodeAssembler(),
                    configuration,
                    aliasedFixture.base(),
                    aliasedFixture.groupByMetadata,
                    new ObjList<>(),
                    aliasedFixture.recordFunctions,
                    aliasedFixture.sampler(),
                    QueryModel.FACTORY.newInstance(),
                    aliasedFixture.listColumnFilter,
                    aliasedFixture.keyTypes,
                    aliasedFixture.valueTypes,
                    new EntityColumnFilter(),
                    new IntList(),
                    1,
                    ColumnType.TIMESTAMP,
                    aliased,
                    0,
                    aliased,
                    0,
                    aliased,
                    aliased
            );
            Assert.assertSame(aliased.failure, Assert.assertThrows(Throwable.class, aliasedFactory::close));
            Assert.assertEquals(1, aliased.closeCount);
        });
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

    @Test
    public void testUnifiedFillCloseClosesAliasedTemporalFunctionsOnce() throws Exception {
        assertMemoryLeak(() -> {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("value", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP));
            metadata.setTimestampIndex(1);
            final ThrowingCloseFunction first = new ThrowingCloseFunction("first temporal cleanup failure");
            final ThrowingCloseFunction second = new ThrowingCloseFunction("second temporal cleanup failure");
            final ThrowingCloseFunction third = new ThrowingCloseFunction("third temporal cleanup failure");
            final IntList fillModes = new IntList();
            fillModes.add(SampleByFillRecordCursorFactory.FILL_CONSTANT);
            fillModes.add(SampleByFillRecordCursorFactory.FILL_CONSTANT);
            final ObjList<Function> constantFills = new ObjList<>();
            constantFills.add(null);
            constantFills.add(null);
            final SampleByFillRecordCursorFactory factory = new SampleByFillRecordCursorFactory(
                    configuration,
                    metadata,
                    new CloseCountingBaseFactory(metadata),
                    first,
                    second,
                    0,
                    1,
                    'h',
                    new SimpleTimestampSampler(100L, ColumnType.TIMESTAMP),
                    fillModes,
                    constantFills,
                    1,
                    ColumnType.TIMESTAMP,
                    null,
                    new ArrayColumnTypes(),
                    new ArrayColumnTypes(),
                    new IntList(),
                    new IntList(),
                    first,
                    0,
                    third,
                    0,
                    new IntList(),
                    new IntList(),
                    new IntList(),
                    false
            );

            final Throwable e = Assert.assertThrows(Throwable.class, factory::close);
            Assert.assertSame(first.failure, e);
            Assert.assertArrayEquals(new Throwable[]{second.failure, third.failure}, e.getSuppressed());
            Assert.assertEquals(1, first.closeCount);
            Assert.assertEquals(1, second.closeCount);
            Assert.assertEquals(1, third.closeCount);

            factory.close();
            Assert.assertEquals(1, first.closeCount);
            Assert.assertEquals(1, second.closeCount);
            Assert.assertEquals(1, third.closeCount);

            final ThrowingCloseFunction aliased = new ThrowingCloseFunction("aliased temporal cleanup failure");
            final SampleByFillRecordCursorFactory aliasedFactory = new SampleByFillRecordCursorFactory(
                    configuration,
                    metadata,
                    new CloseCountingBaseFactory(metadata),
                    aliased,
                    aliased,
                    0,
                    1,
                    'h',
                    new SimpleTimestampSampler(100L, ColumnType.TIMESTAMP),
                    new IntList(fillModes),
                    new ObjList<>(constantFills),
                    1,
                    ColumnType.TIMESTAMP,
                    null,
                    new ArrayColumnTypes(),
                    new ArrayColumnTypes(),
                    new IntList(),
                    new IntList(),
                    aliased,
                    0,
                    aliased,
                    0,
                    new IntList(),
                    new IntList(),
                    new IntList(),
                    false
            );
            Assert.assertSame(aliased.failure, Assert.assertThrows(Throwable.class, aliasedFactory::close));
            Assert.assertEquals(1, aliased.closeCount);
        });
    }

    @Test
    public void testUnifiedFillCloseContinuesAfterThrowingChildren() throws Exception {
        assertMemoryLeak(() -> {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("value", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP));
            metadata.setTimestampIndex(1);
            final CloseCountingBaseFactory base = new CloseCountingBaseFactory(metadata);
            base.closeFailure = "injected base cleanup failure";
            final ThrowingCloseFunction fromFunc = new ThrowingCloseFunction("injected FROM cleanup failure");
            final CloseCountingFunction toFunc = new CloseCountingFunction();
            final CloseCountingFunction offsetFunc = new CloseCountingFunction();
            final ThrowingCloseFunction timezoneFunc = new ThrowingCloseFunction("injected timezone cleanup failure");
            final ThrowingCloseFunction constantFill = new ThrowingCloseFunction("injected constant cleanup failure");
            final IntList fillModes = new IntList();
            fillModes.add(SampleByFillRecordCursorFactory.FILL_CONSTANT);
            fillModes.add(SampleByFillRecordCursorFactory.FILL_CONSTANT);
            final ObjList<Function> constantFills = new ObjList<>();
            constantFills.add(constantFill);
            constantFills.add(null);

            final SampleByFillRecordCursorFactory factory = new SampleByFillRecordCursorFactory(
                    configuration,
                    metadata,
                    base,
                    fromFunc,
                    toFunc,
                    0,
                    1,
                    'h',
                    new SimpleTimestampSampler(100L, ColumnType.TIMESTAMP),
                    fillModes,
                    constantFills,
                    1,
                    ColumnType.TIMESTAMP,
                    null,
                    new ArrayColumnTypes(),
                    new ArrayColumnTypes(),
                    new IntList(),
                    new IntList(),
                    offsetFunc,
                    0,
                    timezoneFunc,
                    0,
                    new IntList(),
                    new IntList(),
                    new IntList(),
                    false
            );

            final Throwable e = Assert.assertThrows(Throwable.class, factory::close);
            TestUtils.assertContains(e.getMessage(), "injected base cleanup failure");
            Assert.assertEquals(3, e.getSuppressed().length);
            TestUtils.assertContains(e.getSuppressed()[0].getMessage(), "injected FROM cleanup failure");
            TestUtils.assertContains(e.getSuppressed()[1].getMessage(), "injected timezone cleanup failure");
            TestUtils.assertContains(e.getSuppressed()[2].getMessage(), "injected constant cleanup failure");
            Assert.assertEquals(1, base.closeCount);
            Assert.assertEquals(1, fromFunc.closeCount);
            Assert.assertEquals(1, toFunc.closeCount);
            Assert.assertEquals(1, offsetFunc.closeCount);
            Assert.assertEquals(1, timezoneFunc.closeCount);
            Assert.assertEquals(1, constantFill.closeCount);

            factory.close();
            Assert.assertEquals(1, base.closeCount);
            Assert.assertEquals(1, constantFill.closeCount);
        });
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
            Assert.assertEquals("adopted base factory must be closed", 1, fixture.baseFactory.closeCount);
            Assert.assertEquals("timezone function must close exactly once", 1, fixture.timezoneNameFunc.closeCount);
            Assert.assertEquals("offset function must close exactly once", 1, fixture.offsetFunc.closeCount);
            Assert.assertEquals("FROM function must close exactly once", 1, fixture.sampleFromFunc.closeCount);
            Assert.assertEquals("TO function must close exactly once", 1, fixture.sampleToFunc.closeCount);
        });
    }

    private void assertFillRecordFunctionInitFailureReleasesMapAndAllowsRetry(boolean isCloseFailing) throws Exception {
        assertMemoryLeak(() -> {
            final GenericRecordMetadata baseMetadata = new GenericRecordMetadata();
            baseMetadata.add(new TableColumnMetadata("k", ColumnType.INT));
            baseMetadata.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP));
            baseMetadata.setTimestampIndex(1);

            final GenericRecordMetadata groupByMetadata = new GenericRecordMetadata();
            groupByMetadata.add(new TableColumnMetadata("value", ColumnType.LONG));
            final ObjList<Function> recordFunctions = new ObjList<>();
            final ToggleFailingInitFunction recordFunction = new ToggleFailingInitFunction();
            recordFunctions.add(recordFunction);
            final IntList recordFunctionPositions = new IntList();
            recordFunctionPositions.add(0);
            final ListColumnFilter keyColumnFilter = new ListColumnFilter();
            keyColumnFilter.add(1);
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            keyTypes.add(ColumnType.INT);
            final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.LONG);
            final Function timestampNull = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP).getTimestampConstantNull();

            final CloseThrowingBaseFactory baseFactory = new CloseThrowingBaseFactory(baseMetadata);
            try (SampleByFillNullRecordCursorFactory factory = new SampleByFillNullRecordCursorFactory(
                    new BytecodeAssembler(),
                    configuration,
                    baseFactory,
                    new SimpleTimestampSampler(100L, ColumnType.TIMESTAMP),
                    keyColumnFilter,
                    keyTypes,
                    valueTypes,
                    groupByMetadata,
                    new ObjList<>(),
                    recordFunctions,
                    recordFunctionPositions,
                    1,
                    ColumnType.TIMESTAMP,
                    StrConstant.NULL,
                    0,
                    StrConstant.NULL,
                    0,
                    timestampNull,
                    0,
                    timestampNull,
                    0
            )) {
                // Establish the reused-cursor state: one successful open and close leaves the
                // lazy map closed, ready for getCursor() to reopen it on the next execution.
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    // no rows required
                }

                final long memoryBeforeFailure = Unsafe.getMemUsed();
                final int closeCountBeforeFailure = baseFactory.cursor.closeCount;
                baseFactory.cursor.isCloseFailing = isCloseFailing;
                recordFunction.isFailing = true;
                try {
                    factory.getCursor(sqlExecutionContext);
                    Assert.fail("record-function init failure expected");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "injected record-function init failure");
                    Assert.assertEquals(isCloseFailing ? 1 : 0, e.getSuppressed().length);
                    if (isCloseFailing) {
                        TestUtils.assertContains(e.getSuppressed()[0].getMessage(), "injected base-cursor close failure");
                    }
                } finally {
                    baseFactory.cursor.isCloseFailing = false;
                    recordFunction.isFailing = false;
                }
                Assert.assertEquals(
                        "base cursor close must be attempted",
                        closeCountBeforeFailure + 1,
                        baseFactory.cursor.closeCount
                );
                Assert.assertEquals(
                        "failed post-reopen init must release the map",
                        memoryBeforeFailure,
                        Unsafe.getMemUsed()
                );

                // A later execution of the same cached factory must reopen and close normally.
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    // no rows required
                }
                Assert.assertEquals(
                        "successful retry must keep native memory balanced",
                        memoryBeforeFailure,
                        Unsafe.getMemUsed()
                );
            }
        });
    }

    @FunctionalInterface
    private interface FactoryConstructor {
        void construct(Fixture fixture) throws Exception;
    }

    /**
     * The idempotent close() of AbstractRecordCursorFactory runs _close() at most once, so a
     * count of 1 means the constructor freed the adopted base and 0 means it leaked.
     */
    private static class CloseCountingBaseFactory extends EmptyTableRecordCursorFactory {
        String closeFailure;
        int closeCount;

        CloseCountingBaseFactory(GenericRecordMetadata metadata) {
            super(metadata);
        }

        @Override
        protected void _close() {
            closeCount++;
            super._close();
            if (closeFailure != null) {
                throw new RuntimeException(closeFailure);
            }
        }
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

    private static class CloseThrowingBaseFactory extends EmptyTableRecordCursorFactory {
        private final CloseThrowingCursor cursor = new CloseThrowingCursor();

        CloseThrowingBaseFactory(GenericRecordMetadata metadata) {
            super(metadata);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            return cursor;
        }
    }

    private static class CloseThrowingCursor implements RecordCursor {
        private int closeCount;
        private boolean isCloseFailing;

        @Override
        public void close() {
            closeCount++;
            if (isCloseFailing) {
                throw new RuntimeException("injected base-cursor close failure");
            }
        }

        @Override
        public Record getRecord() {
            return EmptyTableRandomRecordCursor.INSTANCE.getRecord();
        }

        @Override
        public Record getRecordB() {
            return EmptyTableRandomRecordCursor.INSTANCE.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return EmptyTableRandomRecordCursor.INSTANCE.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return EmptyTableRandomRecordCursor.INSTANCE.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public void toTop() {
        }
    }

    private static class Fixture {
        final GenericRecordMetadata baseMetadata = new GenericRecordMetadata();
        CloseCountingBaseFactory baseFactory;
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

        CloseCountingBaseFactory base() {
            baseFactory = new CloseCountingBaseFactory(baseMetadata);
            return baseFactory;
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

    private static class ThrowingCloseFunction extends CloseCountingFunction {
        private final RuntimeException failure;

        private ThrowingCloseFunction(String message) {
            this.failure = new RuntimeException(message);
        }

        @Override
        public void close() {
            super.close();
            throw failure;
        }
    }

    private static class ToggleFailingInitFunction extends LongFunction {
        private boolean isFailing;

        @Override
        public long getLong(Record rec) {
            return 0;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            if (isFailing) {
                throw SqlException.$(0, "injected record-function init failure");
            }
        }
    }
}
