/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.CairoTestUtils;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.orderby.SortedRecordCursorFactory;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SortedMergeRecordCursorFactoryTest extends AbstractGriffinTest {

    @Test
    public void testCompareCursorWithUnionAll() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            CairoTestUtils.createTestTable(configuration, "x", 30, rnd, new io.questdb.cairo.TestRecord.ArrayBinarySequence());
            CairoTestUtils.createTestTable(configuration, "y", 30, rnd, new io.questdb.cairo.TestRecord.ArrayBinarySequence());

            // both factories will be closed by SortedMergeRecordCursorFactory
            RecordCursorFactory factoryX = compiler.compile("select * from x order by 1, 2", sqlExecutionContext).getRecordCursorFactory();
            RecordCursorFactory factoryY = compiler.compile("select * from y order by 1, 2", sqlExecutionContext).getRecordCursorFactory();
            GenericRecordMetadata metadata = GenericRecordMetadata.copyOfSansTimestamp(factoryX.getMetadata());
            RecordComparator recordComparator = compileComparator(metadata, 1, 2);

            try (SortedMergeRecordCursorFactory sortedMergeRecordCursorFactory = new SortedMergeRecordCursorFactory(metadata, factoryX, factoryY, recordComparator);
                 RecordCursor sortedMergeCursor = sortedMergeRecordCursorFactory.getCursor(sqlExecutionContext);
                 RecordCursorFactory factoryUnionAll = compiler.compile("(select * from y union all select * from x) order by 1, 2", sqlExecutionContext).getRecordCursorFactory();
                 RecordCursor unionAllCursor = factoryUnionAll.getCursor(sqlExecutionContext)) {
                Assert.assertTrue("It seems 'union all' no longer uses SortedRecordCursorFactory. Check if this test still makes sense. " +
                        "If 'union all' already switched to SortedMergeRecordCursorFactory then this test is pointless.", factoryUnionAll instanceof SortedRecordCursorFactory);
                TestUtils.assertEquals(unionAllCursor, factoryUnionAll.getMetadata(), sortedMergeCursor, sortedMergeRecordCursorFactory.getMetadata(), true);
            }
        });
    }

    private static RecordComparator compileComparator(GenericRecordMetadata metadata, int...columns) {
        IntList intList = new IntList(1);
        for (int i : columns) {
            intList.add(i);
        }
        BytecodeAssembler assembler = new BytecodeAssembler();
        RecordComparatorCompiler comparatorCompiler = new RecordComparatorCompiler(assembler);
        RecordComparator recordComparator = comparatorCompiler.compile(metadata, intList);
        return recordComparator;
    }

}