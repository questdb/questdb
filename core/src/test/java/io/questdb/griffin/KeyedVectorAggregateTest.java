/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Rosti;
import org.junit.Test;

public class KeyedVectorAggregateTest extends AbstractGriffinTest {
    @Test
    public void testSimple() throws SqlException {
        ArrayColumnTypes columnType = new ArrayColumnTypes();
        columnType.add(ColumnType.INT);
        columnType.add(ColumnType.DOUBLE);
        long pRosti = Rosti.alloc(columnType, 2047);
        long pRosti2 = Rosti.alloc(columnType, 2047);
        compiler.compile("create table x as (select abs(rnd_int()) % 8 sym, rnd_double(2) val from long_sequence(1000000))", sqlExecutionContext);
        CompiledQuery cc = compiler.compile("x", sqlExecutionContext);
        RecordCursorFactory factory = cc.getRecordCursorFactory();
        PageFrameCursor pfc = factory.getPageFrameCursor(sqlExecutionContext);
        PageFrame frame;
        long t = System.nanoTime();
        while ((frame = pfc.next()) != null) {
            long symPageAddress = frame.getPageAddress(0);
            long valPageAddress = frame.getPageAddress(1);
            Rosti.keyedIntSumDouble(pRosti, symPageAddress, valPageAddress, frame.getPageValueCount(0), 1);
        }
        System.out.println(System.nanoTime() - t);
        pfc.close();

        pfc = factory.getPageFrameCursor(sqlExecutionContext);
        t = System.nanoTime();
        while ((frame = pfc.next()) != null) {
            long symPageAddress = frame.getPageAddress(0);
            long valPageAddress = frame.getPageAddress(1);
            Rosti.keyedIntSumDouble(pRosti2, symPageAddress, valPageAddress, frame.getPageValueCount(0), 1);
        }
        System.out.println(System.nanoTime() - t);
        pfc.close();

        factory.close();

        Rosti.printRosti(pRosti);
        System.out.println("----------------------------");
        Rosti.printRosti(pRosti2);

        Rosti.keyedIntSumDoubleMerge(pRosti, pRosti2, 1);
        System.out.println("++++++++++++++++++++++");
        Rosti.printRosti(pRosti);

        Rosti.free(pRosti);
        Rosti.free(pRosti2);
    }

}
