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

package io.questdb.test.griffin.engine.functions.columns;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.columns.BindableColumn;
import io.questdb.griffin.engine.functions.columns.CharColumn;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.std.str.Utf8Sequence;
import org.junit.Assert;
import org.junit.Test;

public class CharColumnTest {

    @Test
    public void testBindableAndBuiltColumnsAgreeOnThreadSafety() throws SqlException {
        final int[] types = {
                ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.SHORT, ColumnType.CHAR, ColumnType.INT, ColumnType.LONG,
                ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.STRING,
                ColumnType.SYMBOL, ColumnType.VARCHAR, ColumnType.LONG256, ColumnType.UUID, ColumnType.IPv4,
                ColumnType.getGeoHashTypeWithBits(5), ColumnType.getGeoHashTypeWithBits(40),
                ColumnType.encodeArrayType(ColumnType.DOUBLE, 1)
        };
        for (int type : types) {
            final boolean isThreadSafe = ColumnFunction.isThreadSafeType(type);
            Assert.assertEquals(ColumnType.nameOf(type), isThreadSafe, BindableColumn.newInstance(1, type, false).isThreadSafe());
            Assert.assertEquals(ColumnType.nameOf(type), isThreadSafe, FunctionResolver.createColumn(0, 0, type, false).isThreadSafe());
        }
        Assert.assertFalse(BindableColumn.newInstance(1, ColumnType.CHAR, false).isThreadSafe());
    }

    @Test
    public void testGetStrNullChar() {
        final CharColumn column = new CharColumn(0);
        final CharRecord rec = new CharRecord();
        rec.value = 0;
        Assert.assertNull(column.getStrA(rec));
        Assert.assertNull(column.getStrB(rec));
        Assert.assertNull(column.getVarcharA(rec));
        Assert.assertNull(column.getVarcharB(rec));
    }

    @Test
    public void testGetStrReusesInstanceSinks() {
        final CharColumn column = new CharColumn(0);
        final CharRecord rec = new CharRecord();
        rec.value = 'a';
        final CharSequence a = column.getStrA(rec);
        final CharSequence b = column.getStrB(rec);
        Assert.assertNotSame(a, b);
        Assert.assertEquals("a", a.toString());
        Assert.assertEquals("a", b.toString());

        rec.value = 'b';
        Assert.assertSame(a, column.getStrA(rec));
        Assert.assertSame(b, column.getStrB(rec));
        Assert.assertEquals("b", a.toString());
        Assert.assertEquals("b", b.toString());
    }

    @Test
    public void testGetVarcharReusesInstanceSinks() {
        final CharColumn column = new CharColumn(0);
        final CharRecord rec = new CharRecord();
        rec.value = 'a';
        final Utf8Sequence a = column.getVarcharA(rec);
        final Utf8Sequence b = column.getVarcharB(rec);
        Assert.assertNotSame(a, b);
        Assert.assertEquals((byte) 'a', a.byteAt(0));
        Assert.assertEquals((byte) 'a', b.byteAt(0));

        rec.value = 'b';
        Assert.assertSame(a, column.getVarcharA(rec));
        Assert.assertSame(b, column.getVarcharB(rec));
        Assert.assertEquals((byte) 'b', a.byteAt(0));
        Assert.assertEquals((byte) 'b', b.byteAt(0));
    }

    @Test
    public void testIsNotThreadSafe() {
        Assert.assertFalse(new CharColumn(0).isThreadSafe());
    }

    private static final class CharRecord implements Record {
        char value;

        @Override
        public char getChar(int col) {
            return value;
        }
    }
}
