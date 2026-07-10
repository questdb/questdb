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
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.SampleByFillNullRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillPrevRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SimpleTimestampSampler;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The keyed SAMPLE BY fill factories run fallible work after their superclass constructor
 * adopted the record functions, the base factory, the map, and the temporal parameter
 * functions: the group-by updater bytecode generation and the cursor construction. Java cannot
 * run close() on the unreturned partial object, and the generator has already transferred
 * ownership, so the constructors themselves must free every adopted resource exactly once when
 * that post-super work throws. The injected {@link BytecodeAssembler} makes the updater
 * generation fail deterministically while the superclass's record-sink generation succeeds.
 */
public class SampleByFillFactoryConstructionFailureTest extends AbstractCairoTest {

    @Test
    public void testFillNullConstructorFailureClosesAdoptedResources() throws Exception {
        assertMemoryLeak(() -> assertConstructionFailureClosesAdoptedResources(false));
    }

    @Test
    public void testFillPrevConstructorFailureClosesAdoptedResources() throws Exception {
        assertMemoryLeak(() -> assertConstructionFailureClosesAdoptedResources(true));
    }

    private void assertConstructionFailureClosesAdoptedResources(boolean fillPrev) throws SqlException {
        final GenericRecordMetadata baseMetadata = new GenericRecordMetadata();
        baseMetadata.add(new TableColumnMetadata("k", ColumnType.INT));
        baseMetadata.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP));

        final GenericRecordMetadata groupByMetadata = new GenericRecordMetadata();
        groupByMetadata.add(new TableColumnMetadata("k", ColumnType.INT));
        groupByMetadata.add(new TableColumnMetadata("c", ColumnType.LONG));

        final ListColumnFilter listColumnFilter = new ListColumnFilter();
        listColumnFilter.add(1);
        final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.INT);
        final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);

        final CloseCountingFunction recordFunc = new CloseCountingFunction();
        final ObjList<Function> recordFunctions = new ObjList<>();
        recordFunctions.add(recordFunc);
        final IntList recordFunctionPositions = new IntList();
        recordFunctionPositions.add(0);

        final CloseCountingFunction timezoneNameFunc = new CloseCountingFunction();
        final CloseCountingFunction offsetFunc = new CloseCountingFunction();
        final CloseCountingFunction sampleFromFunc = new CloseCountingFunction();
        final CloseCountingFunction sampleToFunc = new CloseCountingFunction();

        try {
            if (fillPrev) {
                new SampleByFillPrevRecordCursorFactory(
                        new UpdaterGenFailingAssembler(),
                        configuration,
                        new EmptyTableRecordCursorFactory(baseMetadata),
                        new SimpleTimestampSampler(100L, ColumnType.TIMESTAMP),
                        listColumnFilter,
                        keyTypes,
                        valueTypes,
                        groupByMetadata,
                        new ObjList<>(),
                        recordFunctions,
                        1,
                        ColumnType.TIMESTAMP,
                        timezoneNameFunc,
                        0,
                        offsetFunc,
                        0,
                        sampleFromFunc,
                        0,
                        sampleToFunc,
                        0
                );
            } else {
                new SampleByFillNullRecordCursorFactory(
                        new UpdaterGenFailingAssembler(),
                        configuration,
                        new EmptyTableRecordCursorFactory(baseMetadata),
                        new SimpleTimestampSampler(100L, ColumnType.TIMESTAMP),
                        listColumnFilter,
                        keyTypes,
                        valueTypes,
                        groupByMetadata,
                        new ObjList<>(),
                        recordFunctions,
                        recordFunctionPositions,
                        1,
                        ColumnType.TIMESTAMP,
                        timezoneNameFunc,
                        0,
                        offsetFunc,
                        0,
                        sampleFromFunc,
                        0,
                        sampleToFunc,
                        0
                );
            }
            Assert.fail("injected updater generation failure expected");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "injected updater generation failure");
        }

        Assert.assertEquals("record functions must close exactly once", 1, recordFunc.closeCount);
        Assert.assertEquals("timezone function must close exactly once", 1, timezoneNameFunc.closeCount);
        Assert.assertEquals("offset function must close exactly once", 1, offsetFunc.closeCount);
        Assert.assertEquals("FROM function must close exactly once", 1, sampleFromFunc.closeCount);
        Assert.assertEquals("TO function must close exactly once", 1, sampleToFunc.closeCount);
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

    /**
     * Lets the superclass's record-sink generation succeed and fails the leaf constructor's
     * group-by updater generation, simulating a deterministic bytecode/allocation failure in
     * the post-super construction work.
     */
    private static class UpdaterGenFailingAssembler extends BytecodeAssembler {
        @Override
        public void init(Class<?> host) {
            if (host == GroupByFunctionsUpdater.class) {
                throw CairoException.nonCritical().put("injected updater generation failure");
            }
            super.init(host);
        }
    }
}
