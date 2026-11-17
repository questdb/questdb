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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.table.ExtraEmptyValueColumnRecord;
import io.questdb.griffin.engine.table.ExtraNullColumnRecord;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class ExtraNullColumnRecordTest {

    @Test
    public void testExtraEmptyValueColumnRecordReportsEmptyValueOnlyThroughGetterOfItsType() {
        // Column 0 comes from the base record, columns 1-4 are spliced: a BOOLEAN, DOUBLE and LONG
        // empty value, then a NULL column. Each empty value is visible only through the getter of
        // its own type; every other getter reports the NULL that ExtraNullColumnRecord reports.
        final LongList emptyValues = new LongList();
        emptyValues.add(1);
        emptyValues.add(Double.doubleToRawLongBits(-0.0));
        emptyValues.add(0);
        emptyValues.add(Numbers.LONG_NULL);
        final IntList emptyValueTypes = new IntList();
        emptyValueTypes.add(ColumnType.BOOLEAN);
        emptyValueTypes.add(ColumnType.DOUBLE);
        emptyValueTypes.add(ColumnType.LONG);
        emptyValueTypes.add(ColumnType.UNDEFINED);
        final ExtraEmptyValueColumnRecord record = new ExtraEmptyValueColumnRecord(1, emptyValues, emptyValueTypes);
        record.of(new Record() {
            @Override
            public boolean getBool(int col) {
                return true;
            }

            @Override
            public double getDouble(int col) {
                return 42.5;
            }

            @Override
            public long getLong(int col) {
                return 42;
            }
        });

        // The base column delegates every getter.
        Assert.assertTrue(record.getBool(0));
        Assert.assertEquals(42.5, record.getDouble(0), 0.0);
        Assert.assertEquals(42, record.getLong(0));

        // BOOLEAN empty value.
        Assert.assertTrue(record.getBool(1));
        Assert.assertTrue(Double.isNaN(record.getDouble(1)));
        Assert.assertEquals(Numbers.LONG_NULL, record.getLong(1));

        // DOUBLE empty value, with its raw bits: -0.0 stays -0.0.
        Assert.assertFalse(record.getBool(2));
        Assert.assertEquals(Double.doubleToRawLongBits(-0.0), Double.doubleToRawLongBits(record.getDouble(2)));
        Assert.assertEquals(Numbers.LONG_NULL, record.getLong(2));

        // LONG empty value.
        Assert.assertFalse(record.getBool(3));
        Assert.assertTrue(Double.isNaN(record.getDouble(3)));
        Assert.assertEquals(0, record.getLong(3));

        // NULL column.
        Assert.assertFalse(record.getBool(4));
        Assert.assertTrue(Double.isNaN(record.getDouble(4)));
        Assert.assertEquals(Numbers.LONG_NULL, record.getLong(4));
    }

    // A constant-false WINDOW JOIN drops the join and splices one NULL aggregate column per window
    // function through ExtraNullColumnRecord (SqlCodeGenerator wraps the master in
    // ExtraNullColumnCursorFactory). For a spliced LONG256 column, both CountLong256GroupByFunction
    // and PGUtils.calculateColumnBinSize (the pgwire binary path) read the value and call
    // Long256Impl.isNull(value), whose Long256Impl.equals does an unchecked (Long256Impl) cast.
    // The record returned Long256NullConstant.INSTANCE - a Long256 (and a Function) but NOT a
    // Long256Impl - so that cast threw ClassCastException instead of reading NULL. The sibling
    // HorizonJoinRecord / MultiHorizonJoinRecord return Long256Impl.NULL_LONG256; this record was
    // missed. A columnSplit of 0 makes every column spliced, so the base record is never consulted.
    @Test
    public void testGetLong256SentinelSurvivesLong256ImplIsNull() {
        final ExtraNullColumnRecord record = new ExtraNullColumnRecord(0);
        final Long256 a = record.getLong256A(0);
        final Long256 b = record.getLong256B(0);
        // The exact call the production callers make. Pre-fix this threw ClassCastException from the
        // (Long256Impl) cast in Long256Impl.equals.
        Assert.assertTrue(Long256Impl.isNull(a));
        Assert.assertTrue(Long256Impl.isNull(b));
        // And it is the canonical NULL sentinel the sibling join records hand out.
        Assert.assertSame(Long256Impl.NULL_LONG256, a);
        Assert.assertSame(Long256Impl.NULL_LONG256, b);
    }
}
